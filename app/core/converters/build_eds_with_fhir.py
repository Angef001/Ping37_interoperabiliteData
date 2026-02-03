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
    _coalesce_from_path,
    _coalesce_from,
    _normalize_value,

)

# =============================================================================
# CONFIGURATION
# =============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

MAPPING_FILE = os.path.join(PROJECT_ROOT, "app", "core", "config", "mapping.json")
FHIR_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
EDS_DIR = os.path.join(PROJECT_ROOT, "eds")

# =============================================================================
# FONCTION PRINCIPALE ETL
# =============================================================================

def build_eds(
    fhir_dir: str | None = None,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
    verbose: bool = True,
) -> dict:
    """
    Construit les tables Parquet de l'EDS a partir des bundles FHIR.
    Effectue l'extraction, la transformation, l'enrichissement et le chargement.
    """

    if verbose:
        print("Demarrage de la construction de l'EDS...")

    # Initialisation des chemins
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

    # Verification du fichier de mapping
    if not os.path.exists(mapping_file):
        msg = f"[ERREUR] mapping.json introuvable : {mapping_file}"
        if verbose:
            print(msg)
        summary["warnings"].append(msg)
        return summary

    # Chargement de la configuration
    mapping_raw = load_json_flexible(mapping_file)
    schemas = mapping_raw.get("_schemas", {})  # dict attendu: { "table.parquet": { "COL": "Utf8", ... }, ... }
    mapping_rules = {k: v for k, v in mapping_raw.items() if not str(k).startswith("_")}
    expected_columns = _compute_expected_columns(mapping_rules, schemas)

    # Preparation des buffers d'extraction
    table_names = {rule["table_name"] for rule in mapping_rules.values()}
    buffers = {t: [] for t in table_names}

    fhir_files = glob.glob(os.path.join(fhir_dir, "*.json"))
    if verbose:
        print(f"Traitement de {len(fhir_files)} fichiers source...")

    os.makedirs(eds_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # EXTRACTION (Parsing JSON)
    # -------------------------------------------------------------------------
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

            # Application des regles de mapping si le type de ressource est configure
            if rtype in mapping_rules:
                rule = mapping_rules[rtype]
                target_table = rule["table_name"]
                columns_map = rule.get("columns", {})

                # schema attendu pour cette table (si présent)
                table_schema = {}
                if isinstance(schemas, dict):
                    table_schema = schemas.get(target_table, {}) or {}

                new_row = {}
                for col_name, json_path in columns_map.items():
                    raw_val = get_value_from_path(resource, json_path)

                    # ✅ NEW: normalisation selon _schemas pour éviter colonnes mixtes
                    expected_dtype_str = None
                    if isinstance(table_schema, dict):
                        expected_dtype_str = table_schema.get(col_name)

                    new_row[col_name] = _normalize_value(raw_val, expected_dtype_str)

                buffers[target_table].append(new_row)

        summary["files_processed"] += 1
        if verbose and idx % 10 == 0:
            print(f"   ... {idx} fichiers traites")

    # -------------------------------------------------------------------------
    # Conversion en DataFrames Polars (NEW: schema_overrides + fallback)
    # -------------------------------------------------------------------------
    # Conversion en DataFrames Polars (robuste aux types mixtes)
    dfs: dict[str, pl.DataFrame] = {}

    for table_name in table_names:
        rows = buffers.get(table_name, [])
        dfs[table_name] = pl.DataFrame(rows , infer_schema_length=None) if rows else pl.DataFrame()

    # -------------------------------------------------------------------------
    # ETAPE 1 : NETTOYAGE DES IDENTIFIANTS
    # -------------------------------------------------------------------------
    # Suppression des prefixes techniques (urn:uuid:, Patient/, etc.)

    id_cols = ["PATID", "EVTID", "ELTID"]
    id_cleaning_regex = r"^(urn:uuid:|urn:oid:|[\w]+/|.*\|)"

    for table_name, df in dfs.items():
        if df.height > 0:
            cols_to_clean = [c for c in id_cols if c in df.columns]

            if cols_to_clean:
                # Cast explicite en Utf8 pour gerer les colonnes potentiellement nulles
                dfs[table_name] = df.with_columns([
                    pl.col(c).cast(pl.Utf8).str.replace(id_cleaning_regex, "").alias(c)
                    for c in cols_to_clean
                ])
                if verbose:
                    print(f"   [Nettoyage] IDs nettoyes pour {table_name}")

    # -------------------------------------------------------------------------
    # ETAPE 2 : REGLES METIERS PATIENT
    # -------------------------------------------------------------------------

    if "patient.parquet" in dfs and dfs["patient.parquet"].height > 0:
        df_pat = dfs["patient.parquet"]

        # Normalisation du sexe (Standardisation M/F/I)
        if "PATSEX" in df_pat.columns:
            gender_map = {
                "male": "M", "female": "F",
                "other": "I", "unknown": "I"
            }
            # Utilisation de replace pour stabilite
            df_pat = df_pat.with_columns(
                pl.col("PATSEX").replace(gender_map, default="I").alias("PATSEX")
            )

        # Calcul de l'age a partir de la date de naissance
        if "PATBD" in df_pat.columns:
            df_pat = df_pat.with_columns(
                pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE")
            )

        dfs["patient.parquet"] = df_pat
        if verbose:
            print("   [Regles] Patient : Normalisation sexe et calcul age.")

    # -------------------------------------------------------------------------
    # ETAPE 3 : REGLES METIERS MOUVEMENT (MVT)
    # -------------------------------------------------------------------------

    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0:
        df_mvt = dfs["mvt.parquet"]

        # Valeur par defaut pour l'unite medicale
        if "SEJUM" in df_mvt.columns:
            df_mvt = df_mvt.with_columns(
                pl.col("SEJUM").fill_null("Hôpital Indéterminé")
            )

        dfs["mvt.parquet"] = df_mvt

    # -------------------------------------------------------------------------
    # ETAPE 4 : ENRICHISSEMENT (JOINTURES)
    # -------------------------------------------------------------------------

    patient_light = None
    if "patient.parquet" in dfs and dfs["patient.parquet"].height > 0 and "PATID" in dfs["patient.parquet"].columns:
        cols_needed = [c for c in ["PATID", "PATBD", "PATAGE", "PATSEX"] if c in dfs["patient.parquet"].columns]
        patient_light = dfs["patient.parquet"].select(cols_needed)

    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0 and patient_light is not None:
        if "PATID" in dfs["mvt.parquet"].columns:
            df_mvt = dfs["mvt.parquet"].join(patient_light, on="PATID", how="left", suffix="_pat")

            df_mvt = _coalesce_from(df_mvt, "PATAGE", "PATAGE_pat")
            df_mvt = _coalesce_from(df_mvt, "PATSEX", "PATSEX_pat")

            dfs["mvt.parquet"] = df_mvt
            if verbose:
                print("   [Enrichissement] Mvt enrichi avec donnees Patient.")

    mvt_light = None
    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0 and "EVTID" in dfs["mvt.parquet"].columns:
        cols_needed = [c for c in ["EVTID", "PATID", "SEJUM", "SEJUF", "DATENT", "DATSORT", "PATAGE", "PATSEX"]
                       if c in dfs["mvt.parquet"].columns]
        mvt_light = dfs["mvt.parquet"].select(cols_needed)

    def apply_enrichment(target_table_name):
        if target_table_name not in dfs or dfs[target_table_name].height == 0:
            return

        df = dfs[target_table_name]

        if patient_light is not None and "PATID" in df.columns:
            df = df.join(patient_light, on="PATID", how="left", suffix="_pat")
            df = _coalesce_from(df, "PATAGE", "PATAGE_pat")
            df = _coalesce_from(df, "PATSEX", "PATSEX_pat")
            df = _coalesce_from(df, "PATBD", "PATBD_pat")

        if mvt_light is not None and "EVTID" in df.columns:
            df = df.join(mvt_light, on="EVTID", how="left", suffix="_mvt")

            df = _coalesce_from(df, "SEJUM", "SEJUM_mvt")
            df = _coalesce_from(df, "SEJUF", "SEJUF_mvt")
            df = _coalesce_from(df, "DATENT", "DATENT_mvt")
            df = _coalesce_from(df, "DATSORT", "DATSORT_mvt")

            df = _coalesce_from(df, "PATID", "PATID_mvt")
            df = _coalesce_from(df, "PATAGE", "PATAGE_mvt")
            df = _coalesce_from(df, "PATSEX", "PATSEX_mvt")

        dfs[target_table_name] = df

    tables_to_enrich = ["biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]
    for t in tables_to_enrich:
        apply_enrichment(t)
        if verbose and t in dfs and dfs[t].height > 0:
            print(f"   [Enrichissement] {t} enrichi.")

    # -------------------------------------------------------------------------
    # ETAPE 5 : CALCUL DUREE SEJOUR (PMSI)
    # -------------------------------------------------------------------------

    if "pmsi.parquet" in dfs and dfs["pmsi.parquet"].height > 0:
        df_pmsi = dfs["pmsi.parquet"]

        if "DATENT" in df_pmsi.columns and "DATSORT" in df_pmsi.columns:
            df_pmsi = df_pmsi.with_columns(
                (
                    pl.col("DATSORT").str.replace(r"[+-]\d{2}:\d{2}$", "").str.to_datetime(strict=False) -
                    pl.col("DATENT").str.replace(r"[+-]\d{2}:\d{2}$", "").str.to_datetime(strict=False)
                )
                .dt.total_days()
                .cast(pl.Int64)
                .fill_null(0)
                .alias("SEJDUR")
            )

            if verbose:
                print("   [Calcul] SEJDUR calcule pour PMSI.")

        dfs["pmsi.parquet"] = df_pmsi

    # -------------------------------------------------------------------------
    # SAUVEGARDE ET SCHEMA
    # -------------------------------------------------------------------------
    if verbose:
        print("Sauvegarde des fichiers Parquet...")

    output_order = ["patient.parquet", "mvt.parquet", "biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]

    for table_name in output_order:
        df = dfs.get(table_name, pl.DataFrame())

        if df.height == 0:
            summary["tables"][table_name] = {"rows": 0, "cols": 0, "generated": False}
            summary["empty_tables"].append(table_name)
            if verbose:
                print(f"[INFO] {table_name} vide, fichier non genere.")
            continue

        # Application stricte du schema attendu
        df = enforce_schema(df, table_name, expected_columns)

        output_path = os.path.join(eds_dir, table_name)

        try:
            df.write_parquet(output_path)
        except Exception as e:
            raise RuntimeError(
                f"[WRITE_PARQUET FAIL] table={table_name} path={output_path} schema={df.schema} -> {e}"
            ) from e

        summary["tables"][table_name] = {"rows": df.height, "cols": len(df.columns), "generated": True}

        if verbose:
            print(f"[SUCCES] {table_name} genere ({df.height} lignes)")

    if verbose:
        print("Construction terminee.")

    return summary


if __name__ == "__main__":
    res = build_eds()
    print(json.dumps(res, ensure_ascii=False, indent=2))