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
# HELPERS: Coalesce de paths + index Bundle + BUILD_PRES + Medication fallback
# =============================================================================

def _index_bundle_resources(bundle: dict) -> dict:
    """
    Indexe les ressources du Bundle pour lookup rapide par:
    - (resourceType, id)
    - "urn:uuid:{id}"
    """
    idx = {
        "by_type_id": {},      # (rtype, id) -> resource
        "by_full_ref": {},     # "urn:uuid:<id>" -> resource
    }
    for entry in bundle.get("entry", []):
        r = entry.get("resource") or {}
        rtype = r.get("resourceType")
        rid = r.get("id")
        if not rtype or not rid:
            continue
        idx["by_type_id"][(rtype, rid)] = r
        idx["by_full_ref"][f"urn:uuid:{rid}"] = r
    return idx


def _get_value_coalesce(resource: dict, path_expr: str):
    """
    Supporte "a.b||c.d||e[0].f"
    Retourne la première valeur non nulle/non vide.
    """
    if not path_expr:
        return None
    paths = [p.strip() for p in path_expr.split("||") if p.strip()]
    for p in paths:
        v = get_value_from_path(resource, p)
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

#============================================================================================================
#helpers
def build_pres_from_dosage(resource: dict) -> str | None:
    """
    Construit la PRES au format:
      'Prendre X unité, Y fois par jour'
    à partir de dosageInstruction[0].
    """
    di = resource.get("dosageInstruction")
    if not di or not isinstance(di, list):
        return None
    d0 = di[0] or {}

    # dose
    dose = None
    dar = d0.get("doseAndRate")
    if isinstance(dar, list) and dar:
        dq = (dar[0] or {}).get("doseQuantity") or {}
        dose = dq.get("value")

    # timing
    timing = (d0.get("timing") or {}).get("repeat") or {}
    freq = timing.get("frequency")
    period = timing.get("period")
    unit = timing.get("periodUnit")

    if dose is None or freq is None or period is None or unit is None:
        # fallback: si dosageInstruction.text existe
        txt = d0.get("text")
        return txt if txt else None

    dose_str = str(int(dose)) if isinstance(dose, (int, float)) and float(dose).is_integer() else str(dose)
    freq_str = str(int(freq)) if isinstance(freq, (int, float)) and float(freq).is_integer() else str(freq)

    unit_map = {"d": "jour", "wk": "semaine", "mo": "mois", "h": "heure"}
    unit_fr = unit_map.get(unit, unit)

    if unit == "d" and float(period) == 1.0:
        return f"Prendre {dose_str} unité, {freq_str} fois par jour"
    else:
        period_str = str(int(period)) if isinstance(period, (int, float)) and float(period).is_integer() else str(period)
        return f"Prendre {dose_str} unité, {freq_str} fois toutes les {period_str} {unit_fr}"


def _lookup_medication_fields(med_ref: str | None, idx: dict) -> tuple[str | None, str | None]:
    """
    Fallback si MedicationRequest.medicationReference.reference existe:
      -> Medication.code.text (label)
      -> Medication.code.coding[0].code (code)
    """
    if not med_ref:
        return (None, None)

    med = idx["by_full_ref"].get(med_ref)
    if not med or med.get("resourceType") != "Medication":
        return (None, None)

    code = med.get("code") or {}
    label = code.get("text")
    coding = code.get("coding") or []
    c0 = coding[0] if isinstance(coding, list) and coding else {}
    code_val = c0.get("code")
    return (label, code_val)






# =============================================================================
# CONFIGURATION
# =============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

MAPPING_FILE = os.path.join(PROJECT_ROOT, "app", "core", "config", "mapping.json")
FHIR_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
EDS_DIR = os.path.join(PROJECT_ROOT, "eds")

# =============================================================================
# SCHEMAS: fallback robuste (_schemas peut être {table:[cols]} ou {table:{col:dtype}})
# =============================================================================
def _compute_expected_columns_fallback(mapping_rules: dict, schemas: dict) -> dict[str, list[str]]:
    expected: dict[str, list[str]] = {}

    if isinstance(schemas, dict) and schemas:
        for table_name, schema_def in schemas.items():
            if isinstance(schema_def, list):
                expected[table_name] = list(schema_def)
            elif isinstance(schema_def, dict):
                expected[table_name] = list(schema_def.keys())

    for _, rule in mapping_rules.items():
        t = rule.get("table_name")
        cols_map = rule.get("columns", {}) or {}
        if not t:
            continue
        expected.setdefault(t, [])
        for c in cols_map.keys():
            if c not in expected[t]:
                expected[t].append(c)

    return expected

# =============================================================================
# NORMALISATION DES IDS (important pour que joins EVTID matchent => moins de NULLs)
# =============================================================================
def _final_clean_id_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8)
        .str.split("|").list.last()
        .str.replace(r"^(urn:uuid:|urn:oid:)", "")
        .str.replace(r"^[A-Za-z]+\/", "")
        .str.strip_chars()
    )

def normalize_all_ids(dfs: dict[str, pl.DataFrame], verbose: bool = False) -> None:
    id_cols = ["PATID", "EVTID", "ELTID"]
    for name, df in list(dfs.items()):
        if df is None or df.height == 0:
            continue
        cols = [c for c in id_cols if c in df.columns]
        if not cols:
            continue
        dfs[name] = df.with_columns([_final_clean_id_expr(c).alias(c) for c in cols])
        if verbose:
            print(f"   [NormalizeIDs] {name} -> {cols}")

# =============================================================================
# SAFE WRITE: convertir tout ce qui n'est pas scalaire en Utf8 (sinon parquet cassé)
# =============================================================================
def _json_stringify_if_needed(val):
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)

def safe_textify_non_scalar_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Empêche les parquets illisibles:
    - si une colonne est List/Struct/Object -> on la stringify en Utf8
    """
    if df.height == 0:
        return df

    exprs = []
    for c in df.columns:
        dt = df.schema.get(c)
        if dt in (pl.Object, pl.Struct) or isinstance(dt, pl.List):
            exprs.append(
                pl.col(c).map_elements(_json_stringify_if_needed, return_dtype=pl.Utf8).alias(c)
            )
        else:
            # on laisse tel quel (Utf8/Int/Float/etc.)
            pass

    if exprs:
        df = df.with_columns(exprs)

    return df

# =============================================================================
# PMSI/DOCEDS CANONIQUES (utilisés pour stats stables)
# =============================================================================
def _first_non_null(expr: pl.Expr) -> pl.Expr:
    return expr.drop_nulls().first()

def _join_unique_as_text(col: str, sep: str = " | ") -> pl.Expr:
    """
    Agrège en texte: unique + tri + concat dans une string.
    IMPORTANT: on doit produire une List dans l'agg avant de join.
    """
    return (
        pl.col(col)
        .drop_nulls()
        .cast(pl.Utf8)
        .unique()
        .sort()
        .implode()          # <-- transforme en List[Utf8] dans l'agrégation
        .list.join(sep)     # <-- joint la liste en string
    )

def rebuild_pmsi_canonique(dfs: dict[str, pl.DataFrame]) -> None:
    if "mvt.parquet" not in dfs or dfs["mvt.parquet"].height == 0:
        dfs["pmsi.parquet"] = pl.DataFrame()
        return

    base = dfs["mvt.parquet"]

    actes = dfs.get("actes.parquet")
    actes_agg = None
    if actes is not None and actes.height > 0 and "EVTID" in actes.columns:
        actes_agg = actes.group_by("EVTID").agg([
            _first_non_null(pl.col("PATID")).alias("PATID"),
            _first_non_null(pl.col("ELTID")).alias("ELTID"),
            _first_non_null(pl.col("DATENT")).alias("DATENT_ACTE"),
            _join_unique_as_text("CODEACTES").alias("CODEACTES"),
            _join_unique_as_text("ACTES").alias("ACTES"),
        ])

    diag = dfs.get("pmsi_diag.parquet")
    diag_agg = None
    if diag is not None and diag.height > 0 and "EVTID" in diag.columns:
        diag_agg = diag.group_by("EVTID").agg([
            _first_non_null(pl.col("PATID")).alias("PATID"),
            _first_non_null(pl.col("ELTID")).alias("ELTID"),
            _first_non_null(pl.col("DATENT")).alias("DATENT_DIAG"),
            _first_non_null(pl.col("DALL")).alias("DALL"),
            _first_non_null(pl.col("GHM")).alias("GHM"),
            _first_non_null(pl.col("SEVERITE")).alias("SEVERITE"),
            _first_non_null(pl.col("PMSISTATUT")).alias("PMSISTATUT"),
        ])

    pmsi = base

    
    # Calcul SEJDUR en heures (float), uniquement dans PMSI
    if "DATENT" in pmsi.columns and "DATSORT" in pmsi.columns:
        pmsi = pmsi.with_columns(
            (
                (
                    pl.col("DATSORT")
                    .cast(pl.Utf8)
                    .str.replace(r"[+-]\d{2}:\d{2}$", "")
                    .str.to_datetime(strict=False)
                    -
                    pl.col("DATENT")
                    .cast(pl.Utf8)
                    .str.replace(r"[+-]\d{2}:\d{2}$", "")
                    .str.to_datetime(strict=False)
                )
                .dt.total_minutes()
                / 60.0
            )
            .cast(pl.Float64)
            .fill_null(0.0)
            .alias("SEJDUR")
        )


    if diag_agg is not None:
        pmsi = pmsi.join(diag_agg, on="EVTID", how="left", suffix="_diag")
    if actes_agg is not None:
        pmsi = pmsi.join(actes_agg, on="EVTID", how="left", suffix="_act")

    for c in ["PATID", "ELTID"]:
        if f"{c}_diag" in pmsi.columns:
            pmsi = _coalesce_from(pmsi, c, f"{c}_diag")
        if f"{c}_act" in pmsi.columns:
            pmsi = _coalesce_from(pmsi, c, f"{c}_act")

    if "DATENT_DIAG" in pmsi.columns:
        pmsi = _coalesce_from(pmsi, "DATENT", "DATENT_DIAG")
    if "DATENT_ACTE" in pmsi.columns:
        pmsi = _coalesce_from(pmsi, "DATENT", "DATENT_ACTE")

    keep = [c for c in [
        "PATID","EVTID","ELTID","DALL","DATENT","DATSORT","SEJDUR","SEJUM","SEJUF","PATBD","PATAGE","PATSEX",
        "CODEACTES","ACTES","MODEENT","MODESORT","PMSISTATUT","GHM","SEVERITE","SRC"
    ] if c in pmsi.columns]


    if "SRC" in pmsi.columns:
        pmsi = pmsi.with_columns(pl.lit("synthea").alias("SRC"))

    dfs["pmsi.parquet"] = pmsi.select(keep)

def rebuild_doceds_canonique(dfs: dict[str, pl.DataFrame]) -> None:
    parts = []
    for name in ["diagreport.parquet", "docref.parquet", "composition.parquet"]:
        df = dfs.get(name)
        if df is None or df.height == 0:
            continue
        parts.append(df)

    if not parts:
        dfs["doceds.parquet"] = pl.DataFrame()
        return

    dfs["doceds.parquet"] = pl.concat(parts, how="diagonal")

# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================
def build_eds(
    fhir_dir: str | None = None,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
    verbose: bool = True,
    write_empty_tables: bool = True,
) -> dict:


    


    if verbose:
        print("Demarrage de la construction de l'EDS...")

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

    # expected columns
    try:
        expected_columns = _compute_expected_columns(mapping_rules, schemas)
        if not isinstance(expected_columns, dict) or len(expected_columns) == 0:
            expected_columns = _compute_expected_columns_fallback(mapping_rules, schemas)
    except Exception:
        expected_columns = _compute_expected_columns_fallback(mapping_rules, schemas)

    # buffers
    table_names = {rule["table_name"] for rule in mapping_rules.values() if rule.get("table_name")}
    buffers = {t: [] for t in table_names}

    fhir_files = glob.glob(os.path.join(fhir_dir, "*.json"))
    if verbose:
        print(f"Traitement de {len(fhir_files)} fichiers source...")

    os.makedirs(eds_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # EXTRACTION
    # -------------------------------------------------------------------------
    for idx, file_path in enumerate(fhir_files, start=1):



        if verbose:
            print("\n[DEBUG] Rows extracted per table:")
            for t in sorted(buffers.keys()):
                print(f" - {t:20s} {len(buffers[t])}")
            print()




        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)
            idx_bundle = _index_bundle_resources(bundle)

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
            if rtype not in mapping_rules:
                continue

            rule = mapping_rules[rtype]
            target_table = rule["table_name"]
            columns_map = rule.get("columns", {}) or {}

            # ---------------------------------------------------------------------
            # CAS SPECIAL : Observation (BIOL) avec component[] -> 1 ligne par component
            # ---------------------------------------------------------------------
            if rtype == "Observation" and target_table == "biol.parquet":
                components = resource.get("component")
                if isinstance(components, list) and components:
                    for comp in components:
                        row = {}
                        for col_name, json_path in columns_map.items():
                            if not json_path:
                                row[col_name] = None
                                continue

                            if col_name == "PNAME":
                                row[col_name] = _get_value_coalesce(resource, "code.text")

                            elif col_name == "ANAME":
                                row[col_name] = get_value_from_path(comp, "code.text")

                            elif col_name == "RNAME":
                                row[col_name] = get_value_from_path(comp, "code.coding[0].display")

                            elif col_name == "LOINC":
                                row[col_name] = get_value_from_path(comp, "code.coding[0].code")

                            elif col_name == "RESULT":
                                row[col_name] = _get_value_coalesce(
                                    comp, "valueQuantity.value||valueCodeableConcept.text"
                                )

                            elif col_name == "UNIT":
                                row[col_name] = get_value_from_path(comp, "valueQuantity.unit")

                            else:
                                row[col_name] = _get_value_coalesce(resource, json_path)

                        buffers[target_table].append(row)

                    continue  # on saute le mode standard pour cette Observation


            new_row = {}
            for col_name, json_path in columns_map.items():
                if not json_path:
                    new_row[col_name] = None
                else:
                    raw_val = _get_value_coalesce(resource, json_path)

                    # MedicationRequest : construction de la prescription (PRES)
                    if rtype == "MedicationRequest" and col_name == "PRES":
                        raw_val = build_pres_from_dosage(resource)

                    # MedicationRequest : fallback si medicationReference (au lieu de medicationCodeableConcept)
                    if rtype == "MedicationRequest" and col_name in ("ALLSPELABEL", "ALLUCD13") and raw_val is None:
                        med_ref = get_value_from_path(resource, "medicationReference.reference")
                        med_label, med_code = _lookup_medication_fields(med_ref, idx_bundle)
                        raw_val = med_label if col_name == "ALLSPELABEL" else med_code


                    # stringify dict/list dès extraction => évite Object/List plus tard
                    if isinstance(raw_val, (dict, list)):
                        raw_val = json.dumps(raw_val, ensure_ascii=False)
                    new_row[col_name] = raw_val

            buffers[target_table].append(new_row)

        summary["files_processed"] += 1
        if verbose and idx % 10 == 0:
            print(f"   ... {idx} fichiers traites")

    # -------------------------------------------------------------------------
    # BUILD DFS (force Utf8 au build => pas de crash type mixte)
    # -------------------------------------------------------------------------
    dfs: dict[str, pl.DataFrame] = {}
    for table_name in table_names:
        rows = buffers.get(table_name, [])
        cols = expected_columns.get(table_name, [])

        if not rows:
            dfs[table_name] = pl.DataFrame({c: [] for c in cols}) if cols else pl.DataFrame()
            continue

        schema = {c: pl.Utf8 for c in cols} if cols else None
        dfs[table_name] = pl.from_dicts(rows, schema=schema, infer_schema_length=None)

    # -------------------------------------------------------------------------
    # IDS CLEAN + NORMALIZE (critique pour éviter NULLs aux joins)
    # -------------------------------------------------------------------------
    
    normalize_all_ids(dfs, verbose=verbose)

    # -------------------------------------------------------------------------
    # REGLES PATIENT
    # -------------------------------------------------------------------------
    if "patient.parquet" in dfs and dfs["patient.parquet"].height > 0:
        df_pat = dfs["patient.parquet"]

        if "PATSEX" in df_pat.columns:
            gender_map = {"male": "M", "female": "F", "other": "I", "unknown": "I"}
            df_pat = df_pat.with_columns(pl.col("PATSEX").replace(gender_map, default="I").alias("PATSEX"))

        if "PATBD" in df_pat.columns:
            df_pat = df_pat.with_columns(pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE"))

        dfs["patient.parquet"] = df_pat
        if verbose:
            print("   [Regles] Patient : Normalisation sexe et calcul age.")

    # -------------------------------------------------------------------------
    # REGLES MVT + SEJDUR AVANT ENRICHISSEMENT
    # -------------------------------------------------------------------------
    if "mvt.parquet" in dfs and dfs["mvt.parquet"].height > 0:
        df_mvt = dfs["mvt.parquet"]

        if "SEJUM" in df_mvt.columns:
            df_mvt = df_mvt.with_columns(pl.col("SEJUM").fill_null("Hôpital Indéterminé"))

        

        dfs["mvt.parquet"] = df_mvt

    # -------------------------------------------------------------------------
    # ENRICHISSEMENT
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

    def apply_enrichment(target_table_name: str):
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

        # UFPRO : si vide, reprendre SEJUF (règle projet)
        if "UFPRO" in df.columns and "SEJUF" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("UFPRO").is_null() | (pl.col("UFPRO").cast(pl.Utf8).str.strip_chars() == ""))
                .then(pl.col("SEJUF"))
                .otherwise(pl.col("UFPRO"))
                .alias("UFPRO")
            )


        dfs[target_table_name] = df

    tables_to_enrich = [
        "biol.parquet",
        "pharma.parquet",
        "pmsi_diag.parquet",
        "actes.parquet",
        "diagreport.parquet",
        "docref.parquet",
        "composition.parquet",
    ]
    for t in tables_to_enrich:
        apply_enrichment(t)
        if verbose and t in dfs and dfs[t].height > 0:
            print(f"   [Enrichissement] {t} enrichi.")

    # -------------------------------------------------------------------------
    # TABLES CANONIQUES
    # -------------------------------------------------------------------------
    rebuild_pmsi_canonique(dfs)
    rebuild_doceds_canonique(dfs)

    # -------------------------------------------------------------------------
    # WRITE (1 seule boucle d'écriture, safe)
    # -------------------------------------------------------------------------
    if verbose:
        print("Sauvegarde des fichiers Parquet...")

    output_order = [
        "patient.parquet",
        "mvt.parquet",
        "biol.parquet",
        "pharma.parquet",
        "pmsi_diag.parquet",
        "actes.parquet",
        "diagreport.parquet",
        "docref.parquet",
        "composition.parquet",
        "pmsi.parquet",
        "doceds.parquet",
    ]

    for table_name in output_order:
        df = dfs.get(table_name, pl.DataFrame())
        output_path = os.path.join(eds_dir, table_name)
        cols = expected_columns.get(table_name, [])

        if df.height == 0:
            if write_empty_tables:
                df = pl.DataFrame({c: [] for c in cols}) if cols else pl.DataFrame()
                df = enforce_schema(df, table_name, expected_columns)
                df = safe_textify_non_scalar_columns(df)
                df.write_parquet(output_path)

                summary["tables"][table_name] = {"rows": 0, "cols": len(df.columns), "generated": True}
                summary["empty_tables"].append(table_name)
                if verbose:
                    print(f"[INFO] {table_name} vide, fichier genere (schema uniquement).")
            else:
                summary["tables"][table_name] = {"rows": 0, "cols": 0, "generated": False}
                summary["empty_tables"].append(table_name)
                if verbose:
                    print(f"[INFO] {table_name} vide, fichier non genere.")
            continue

        # Non vide
        df = enforce_schema(df, table_name, expected_columns)
        df = safe_textify_non_scalar_columns(df)
        df.write_parquet(output_path)

        summary["tables"][table_name] = {"rows": df.height, "cols": len(df.columns), "generated": True}
        if verbose:
            print(f"[SUCCES] {table_name} genere ({df.height} lignes)")

    if verbose:
        print("Construction terminee.")

    return summary


if __name__ == "__main__":
    res = build_eds()
    print(json.dumps(res, ensure_ascii=False, indent=2))
