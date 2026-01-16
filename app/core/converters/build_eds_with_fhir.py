import json
import glob
import os
import polars as pl
from datetime import datetime

# =============================================================================
# CONFIGURATION DES CHEMINS
# =============================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

MAPPING_FILE = os.path.join(PROJECT_ROOT, "app", "core", "config", "mapping.json")
FHIR_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
EDS_DIR = os.path.join(PROJECT_ROOT, "eds")

# =============================================================================
# COLONNES EDSaN ATTENDUES PAR MODULE (d'après tes captures)
# =============================================================================
EDSAN_COLUMNS = {
    "patient.parquet": [
        "PATID", "PATBD", "PATAGE", "PATSEX"
    ],
    "mvt.parquet": [
        "PATAGE", "PATSEX", "DATENT", "DATSORT", "SEJUF", "SEJUM", "PATID", "EVTID", "ELTID"
    ],
    "biol.parquet": [
        "PRLVTDATE", "SEJUM", "SEJUF", "PNAME", "ANAME", "RNAME", "LOINC",
        "RESULT", "UNIT", "MINREF", "MAXREF", "VALIDADATE",
        "PATAGE", "PATSEX", "PATID", "EVTID", "ELTID"
    ],
    "pharma.parquet": [
        "PRES", "ALLSPELABEL", "ALLUCD13", "DATENT", "DATSORT", "DATPRES", "CAT",
        "SEJUM", "SEJUF", "UFPRO", "PATBD", "PATAGE", "PATSEX", "SRC",
        "PATID", "EVTID", "ELTID"
    ],
    "doceds.parquet": [
        "RECTXT", "RECFAMTXT", "RECDATE", "RECTYPE", "SEJUM", "SEJUF",
        "PATBD", "PATAGE", "PATSEX", "PATID", "EVTID", "ELTID"
    ],
    "pmsi.parquet": [
        "DALL", "DATENT", "DATSORT", "SEJDUR", "SEJUM", "SEJUF",
        "PATBD", "PATAGE", "PATSEX", "CODEACTES", "ACTES",
        "MODEENT", "MODESORT", "PMSISTATUT", "GHM", "SEVERITE", "SRC",
        "PATID", "EVTID", "ELTID"
    ]
}

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def compute_age(birthdate_str):
    """
    Calcule l'âge en années à partir d'une chaîne de date (format YYYY-MM-DD).
    Retourne None si la date est invalide ou manquante.
    """
    if not birthdate_str:
        return None
    try:
        bd = datetime.strptime(str(birthdate_str)[:10], "%Y-%m-%d")
        today = datetime.now()
        return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
    except:
        return None


def get_value_from_path(data: dict, path: str):
    """
    Navigue dans un dictionnaire imbriqué (JSON) via un chemin sous forme de chaîne.
    Supporte la notation par points (.) et les index de listes (ex: [0]).
    Nettoie automatiquement les préfixes techniques FHIR (urn:uuid:, Patient/, etc).
    """
    if not path or data is None:
        return None

    if path == "resourceType":
        return data.get("resourceType")

    elements = path.replace("[", ".").replace("]", "").split(".")
    current = data

    for key in elements:
        if current is None:
            return None

        if key.isdigit():
            idx = int(key)
            if isinstance(current, list) and len(current) > idx:
                current = current[idx]
            else:
                return None
        elif isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None

    if isinstance(current, str):
        for prefix in ["urn:uuid:", "Patient/", "Encounter/", "Practitioner/", "Location/"]:
            current = current.replace(prefix, "")

    return current


def enforce_schema(df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """
    Force un DataFrame à ne contenir QUE les colonnes attendues pour le module EDSaN.
    - Ajoute les colonnes manquantes en null
    - Supprime les colonnes en trop
    """
    expected = EDSAN_COLUMNS.get(table_name)
    if not expected:
        return df

    missing = [c for c in expected if c not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing])

    df = df.select([c for c in expected if c in df.columns])
    return df


# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================

def build_eds():
    print("Démarrage de la construction de l'EDS...")

    if not os.path.exists(MAPPING_FILE):
        print(f"[ERREUR] Fichier de mapping introuvable : {MAPPING_FILE}")
        return

    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping_rules = json.load(f)

    buffers = {rule["table_name"]: [] for rule in mapping_rules.values()}

    fhir_files = glob.glob(os.path.join(FHIR_DIR, "*.json"))
    print(f"Traitement de {len(fhir_files)} fichiers source...")

    os.makedirs(EDS_DIR, exist_ok=True)
    count = 0

    # ----------------------------
    # 1) Extraction depuis les Bundles FHIR
    # ----------------------------
    for file_path in fhir_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)
        except Exception as e:
            print(f"[ATTENTION] Erreur de lecture sur le fichier {file_path}: {e}")
            continue

        if "entry" not in bundle:
            continue

        for entry in bundle["entry"]:
            resource = entry.get("resource", {})
            rtype = resource.get("resourceType")

            if rtype in mapping_rules:
                rule = mapping_rules[rtype]
                target_table = rule["table_name"]
                columns_map = rule["columns"]

                new_row = {}
                for col_name, json_path in columns_map.items():
                    new_row[col_name] = get_value_from_path(resource, json_path)

                buffers[target_table].append(new_row)

        count += 1
        if count % 10 == 0:
            print(f"   ... {count} fichiers traités")

    # ----------------------------
    # 2) Construction des DF + règles métiers
    # ----------------------------
    print("Sauvegarde des fichiers Parquet et application des règles métiers...")

    dfs = {}
    df_patient = None
    df_mvt = None

    for table_name, data_rows in buffers.items():
        if not data_rows:
            print(f"[INFO] La table {table_name} est vide, aucun fichier généré.")
            continue

        all_cols = set()
        for r in data_rows:
            all_cols.update(r.keys())

        schema = {c: pl.Utf8 for c in all_cols}
        df = pl.DataFrame(data_rows, schema=schema)

        # Valeur par défaut pour SEJUM
        if table_name == "mvt.parquet" and "SEJUM" in df.columns:
            df = df.with_columns(pl.col("SEJUM").fill_null("Service Général"))

        # Calcul PATAGE uniquement dans patient.parquet (puis copié)
        if table_name == "patient.parquet" and "PATBD" in df.columns:
            df = df.with_columns(
                pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE")
            )
            print(f"   - Colonne PATAGE calculée pour {table_name}")

        dfs[table_name] = df

    # ----------------------------
    # 3) Référentiels Patient + MVT
    # ----------------------------
    if "patient.parquet" in dfs:
        df_patient = dfs["patient.parquet"].select(
            [c for c in ["PATID", "PATBD", "PATSEX", "PATAGE"] if c in dfs["patient.parquet"].columns]
        )

    if "mvt.parquet" in dfs:
        df_mvt = dfs["mvt.parquet"].select(
            [c for c in ["EVTID", "PATID", "DATENT", "DATSORT", "SEJUM", "SEJUF"] if c in dfs["mvt.parquet"].columns]
        )

    # ----------------------------
    # 4) Enrichir + filtrer colonnes EDSaN + sauvegarder
    # ----------------------------
    for table_name, df in dfs.items():

        # Join patient -> copie PATBD/PATSEX/PATAGE si la table les attend
        if df_patient is not None and "PATID" in df.columns and table_name != "patient.parquet":
            df = df.join(df_patient, on="PATID", how="left", suffix="_pat")

            for col in ["PATBD", "PATSEX", "PATAGE"]:
                if col in df.columns and f"{col}_pat" in df.columns:
                    df = df.with_columns(pl.col(col).fill_null(pl.col(f"{col}_pat")).alias(col))

            df = df.drop([c for c in df.columns if c.endswith("_pat")])

        # Join mvt -> copie infos séjour si attendu
        if df_mvt is not None and "EVTID" in df.columns and table_name not in ["patient.parquet", "mvt.parquet"]:
            df = df.join(df_mvt, on="EVTID", how="left", suffix="_mvt")

            for col in ["DATENT", "DATSORT", "SEJUM", "SEJUF"]:
                if col in df.columns and f"{col}_mvt" in df.columns:
                    df = df.with_columns(pl.col(col).fill_null(pl.col(f"{col}_mvt")).alias(col))

            df = df.drop([c for c in df.columns if c.endswith("_mvt")])

        # Garder UNIQUEMENT les colonnes attendues par module
        df = enforce_schema(df, table_name)

        # Sauvegarde parquet
        output_path = os.path.join(EDS_DIR, table_name)
        df.write_parquet(output_path)
        print(f"[SUCCES] {table_name} généré ({len(df)} lignes)")

    print("Construction terminée.")

    

if __name__ == "__main__":
    build_eds()
