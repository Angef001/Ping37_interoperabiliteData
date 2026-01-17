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

def build_eds(
    fhir_dir: str | None = None,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
    verbose: bool = True,
) -> dict:
    """
    Pipeline FHIR (dossier de bundles *.json) -> EDS (Parquet).

    - fhir_dir: dossier contenant des fichiers *.json (Bundles FHIR)
               par défaut: PROJECT_ROOT/synthea/output/fhir
    - eds_dir: dossier de sortie Parquet
              par défaut: PROJECT_ROOT/eds
    - mapping_file: mapping.json
                   par défaut: PROJECT_ROOT/app/core/config/mapping.json
    - verbose: logs console
    """
    if verbose:
        print("Démarrage de la construction de l'EDS...")

    # Valeurs par défaut (compatibles avec votre projet)
    fhir_dir = fhir_dir or FHIR_DIR
    eds_dir = eds_dir or EDS_DIR
    mapping_file = mapping_file or MAPPING_FILE

    summary = {
        "fhir_dir": fhir_dir,
        "eds_dir": eds_dir,
        "mapping_file": mapping_file,
        "files_processed": 0,
        "tables": {},          # table_name -> {"rows": int, "cols": int, "generated": bool}
        "empty_tables": [],    # tables sans lignes
        "warnings": [],
    }

    # Vérification mapping
    if not os.path.exists(mapping_file):
        msg = f"[ERREUR] Fichier de mapping introuvable : {mapping_file}"
        if verbose:
            print(msg)
        summary["warnings"].append(msg)
        return summary

    # Chargement mapping
    with open(mapping_file, "r", encoding="utf-8") as f:
        mapping_rules = json.load(f)

    # Buffers par table
    buffers = {rule["table_name"]: [] for rule in mapping_rules.values()}

    # Lecture des fichiers FHIR
    fhir_files = glob.glob(os.path.join(fhir_dir, "*.json"))
    if verbose:
        print(f"Traitement de {len(fhir_files)} fichiers source...")

    os.makedirs(eds_dir, exist_ok=True)

    # Extraction
    for idx, file_path in enumerate(fhir_files, start=1):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)
        except Exception as e:
            msg = f"[ATTENTION] Erreur de lecture {file_path}: {e}"
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
                columns_map = rule["columns"]

                new_row = {}
                for col_name, json_path in columns_map.items():
                    new_row[col_name] = get_value_from_path(resource, json_path)

                buffers[target_table].append(new_row)

        summary["files_processed"] += 1
        if verbose and idx % 10 == 0:
            print(f"   ... {idx} fichiers traités")

    # Écriture + règles métiers simples
    if verbose:
        print("Sauvegarde des fichiers Parquet et application des règles métiers...")

    for table_name, data_rows in buffers.items():
        if not data_rows:
            summary["tables"][table_name] = {"rows": 0, "cols": 0, "generated": False}
            summary["empty_tables"].append(table_name)
            if verbose:
                print(f"[INFO] La table {table_name} est vide, aucun fichier généré.")
            continue

        df = pl.DataFrame(data_rows)

        # PATAGE (patient)
        if table_name == "patient.parquet" and "PATBD" in df.columns:
            df = df.with_columns(
                pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE")
            )
            if verbose:
                print(f"   - Colonne PATAGE calculée pour {table_name}")

        # SEJUM défaut (mvt)
        if table_name == "mvt.parquet" and "SEJUM" in df.columns:
            df = df.with_columns(pl.col("SEJUM").fill_null("Service Général"))

        output_path = os.path.join(eds_dir, table_name)
        df.write_parquet(output_path)

        summary["tables"][table_name] = {
            "rows": df.height,
            "cols": len(df.columns),
            "generated": True,
        }

        if verbose:
            print(f"[SUCCES] {table_name} généré ({df.height} lignes)")

    if verbose:
        print("Construction terminée.")

    return summary


if __name__ == "__main__":
    # Conserve le comportement "script" : lance sur synthea/output/fhir par défaut
    res = build_eds()
    print(json.dumps(res, ensure_ascii=False, indent=2))
