import json
import glob
import os

import polars as pl

from app.utils.helpers import (
    compute_age,
    enforce_schema,
    get_value_from_path,
    load_json_flexible,
    _compute_expected_columns,
    _coalesce_from,
)


# =============================================================================
# CONFIGURATION DES CHEMINS
# =============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

MAPPING_FILE = os.path.join(PROJECT_ROOT, "app", "core", "config", "mapping.json")
FHIR_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
EDS_DIR = os.path.join(PROJECT_ROOT, "eds")


"""Build EDS Parquet tables from FHIR bundles.

Note: reusable utilities were moved to app.utils.helpers (safe refactor).
"""


# =============================================================================
# BUILD
# =============================================================================

def build_eds(
    fhir_dir: str | None = None,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
    verbose: bool = True,
) -> dict:

    if verbose:
        print("Démarrage de la construction de l'EDS...")

    fhir_dir = fhir_dir or FHIR_DIR
    eds_dir = eds_dir or EDS_DIR
    mapping_file = mapping_file or MAPPING_FILE

    summary = {
        "fhir_dir": fhir_dir,
        "eds_dir": eds_dir,
        "mapping_file": mapping_file,
        "files_processed": 0,
        "tables": {},
        "empty_tables": [],
        "warnings": [],
    }

    if not os.path.exists(mapping_file):
        msg = f"[ERREUR] mapping.json introuvable : {mapping_file}"
        if verbose:
            print(msg)
        summary["warnings"].append(msg)
        return summary

    mapping_raw = load_json_flexible(mapping_file)

    schemas = mapping_raw.get("_schemas", {})
    mapping_rules = {k: v for k, v in mapping_raw.items() if not str(k).startswith("_")}
    expected_columns = _compute_expected_columns(mapping_rules, schemas)

    # buffers par table
    table_names = {rule["table_name"] for rule in mapping_rules.values()}
    buffers = {t: [] for t in table_names}

    fhir_files = glob.glob(os.path.join(fhir_dir, "*.json"))
    if verbose:
        print(f"Traitement de {len(fhir_files)} fichiers source...")

    os.makedirs(eds_dir, exist_ok=True)

    # --- extraction ---
    for idx, file_path in enumerate(fhir_files, start=1):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)
        except Exception as e:
            msg = f"[ATTENTION] Erreur lecture {file_path}: {e}"
            if verbose:
                print(msg)
            summary["warnings"].append(msg)
            continue

        if "entry" not in bundle:
            continue

        for entry in bundle["entry"]:
            resource = entry.get("resource", {})
            rtype = resource.get("resourceType")

            if rtype in mapping_rules:
                rule = mapping_rules[rtype]
                target_table = rule["table_name"]
                columns_map = rule.get("columns", {})

                new_row = {}
                for col_name, json_path in columns_map.items():
                    new_row[col_name] = get_value_from_path(resource, json_path)

                buffers[target_table].append(new_row)

        summary["files_processed"] += 1
        if verbose and idx % 10 == 0:
            print(f"   ... {idx} fichiers traités")

    # --- construire tous les DF en mémoire (pour enrichir via joins) ---
    dfs: dict[str, pl.DataFrame] = {}

    for table_name in table_names:
        rows = buffers.get(table_name, [])
        if rows:
            dfs[table_name] = pl.DataFrame(rows)
        else:
            dfs[table_name] = pl.DataFrame()

    # --- règles métiers de base ---
    # PATAGE dans patient
    if "patient.parquet" in dfs and dfs["patient.parquet"].height > 0 and "PATBD" in dfs["patient.parquet"].columns:
        dfs["patient.parquet"] = dfs["patient.parquet"].with_columns(
            pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE")
        )
        if verbose:
            print("   - Colonne PATAGE calculée pour patient.parquet")

    # SEJUM défaut dans mvt
    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0 and "SEJUM" in dfs["mvt.parquet"].columns:
        dfs["mvt.parquet"] = dfs["mvt.parquet"].with_columns(pl.col("SEJUM").fill_null("Service Général"))

    # --- enrichissements pour supprimer les NULL "structurels" ---
    # 1) Préparer patient light
    patient_light = None
    if "patient.parquet" in dfs and dfs["patient.parquet"].height > 0 and "PATID" in dfs["patient.parquet"].columns:
        cols = [c for c in ["PATID", "PATBD", "PATAGE", "PATSEX"] if c in dfs["patient.parquet"].columns]
        patient_light = dfs["patient.parquet"].select(cols)

    # 2) Enrichir mvt avec PATAGE/PATSEX/PATBD via PATID
    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0 and patient_light is not None and "PATID" in dfs["mvt.parquet"].columns:
        df = dfs["mvt.parquet"].join(patient_light, on="PATID", how="left", suffix="_pat")
        df = _coalesce_from(df, "PATAGE", "PATAGE_pat")
        df = _coalesce_from(df, "PATSEX", "PATSEX_pat")
        df = _coalesce_from(df, "PATBD", "PATBD_pat")
        dfs["mvt.parquet"] = df

    # 3) Préparer mvt light pour enrichir les autres tables via EVTID
    mvt_light = None
    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0 and "EVTID" in dfs["mvt.parquet"].columns:
        keep = [c for c in ["EVTID", "PATID", "ELTID", "DATENT", "DATSORT", "SEJUM", "SEJUF", "PATAGE", "PATSEX", "PATBD"] if c in dfs["mvt.parquet"].columns]
        mvt_light = dfs["mvt.parquet"].select(keep)

    # helper: enrichir une table en 2 joins (PATID puis EVTID) avec coalesce
    def enrich_table(table: str):
        if table not in dfs or dfs[table].height == 0:
            return

        df = dfs[table]

        # Join patient (PATID)
        if patient_light is not None and "PATID" in df.columns:
            df = df.join(patient_light, on="PATID", how="left", suffix="_pat")
            df = _coalesce_from(df, "PATBD", "PATBD_pat")
            df = _coalesce_from(df, "PATAGE", "PATAGE_pat")
            df = _coalesce_from(df, "PATSEX", "PATSEX_pat")

        # Join mvt (EVTID)
        if mvt_light is not None and "EVTID" in df.columns:
            df = df.join(mvt_light, on="EVTID", how="left", suffix="_mvt")
            # champs souvent vides dans biol/pharma/doceds/pmsi
            df = _coalesce_from(df, "PATID", "PATID_mvt")
            df = _coalesce_from(df, "ELTID", "ELTID_mvt")
            df = _coalesce_from(df, "DATENT", "DATENT_mvt")
            df = _coalesce_from(df, "DATSORT", "DATSORT_mvt")
            df = _coalesce_from(df, "SEJUM", "SEJUM_mvt")
            df = _coalesce_from(df, "SEJUF", "SEJUF_mvt")
            df = _coalesce_from(df, "PATBD", "PATBD_mvt")
            df = _coalesce_from(df, "PATAGE", "PATAGE_mvt")
            df = _coalesce_from(df, "PATSEX", "PATSEX_mvt")

        dfs[table] = df

    # Enrichir toutes les tables sauf patient/mvt
    for t in ["biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]:
        enrich_table(t)

    # --- écrire les Parquet dans un ordre stable + enforce_schema ---
    if verbose:
        print("Sauvegarde des fichiers Parquet et application des schémas...")

    output_order = ["patient.parquet", "mvt.parquet", "biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]

    for table_name in output_order:
        df = dfs.get(table_name, pl.DataFrame())

        if df.height == 0:
            summary["tables"][table_name] = {"rows": 0, "cols": 0, "generated": False}
            summary["empty_tables"].append(table_name)
            if verbose:
                print(f"[INFO] {table_name} vide, aucun fichier généré.")
            continue

        df = enforce_schema(df, table_name, expected_columns)

        output_path = os.path.join(eds_dir, table_name)
        df.write_parquet(output_path)

        summary["tables"][table_name] = {"rows": df.height, "cols": len(df.columns), "generated": True}

        if verbose:
            print(f"[SUCCES] {table_name} généré ({df.height} lignes)")

    if verbose:
        print("Construction terminée.")

    return summary


if __name__ == "__main__":
    res = build_eds()
    print(json.dumps(res, ensure_ascii=False, indent=2))
