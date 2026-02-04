"""Reusable helper utilities used across the project.
 
This file is meant to centralize small, reusable functions (parsing, cleaning,
JSON-path extraction, schema enforcement, etc.) so that converter/build scripts
stay focused on the business logic.
 
Important (stability): functions keep their previous names and remain
backwards-compatible.
"""
 
from __future__ import annotations
from zipfile import ZipFile, ZIP_DEFLATED
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union
import requests
from dotenv import load_dotenv
import os
from collections import Counter

import polars as pl

# app/utils/helpers.py

FHIR_ACCEPT_HEADERS = {"Accept": "application/fhir+json"}
 
load_dotenv()  # charge le .env

FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")
FHIR_ACCEPT_HEADERS = {"Accept": "application/fhir+json"}

# -----------------------------------------------------------------------------
# FHIR / generic text helpers
# -----------------------------------------------------------------------------
 
def clean_id(raw_id: Optional[str]) -> str:
    """Nettoie les identifiants FHIR pour ne garder que la partie unique.
   
    Utile car FHIR stocke souvent les références sous forme relative (ex: "Patient/123").
    Pour l'analyse de données (EDS), nous avons besoin de la clé primaire pure ("123").
 
    Exemples:
    - 'Patient/123' -> '123'
    - 'urn:uuid:abc-def' -> 'abc-def'
    """
    if not raw_id:
        return ""
 
    # Supprime les préfixes courants via une expression régulière (Regex).
    # Le '^' signifie "qui commence par".
    # Le '|' signifie "OU" (Patient/ OU Encounter/ OU ...).
    return re.sub(
        r"^(urn:uuid:|Patient/|Encounter/|Observation/|Procedure/|Condition/|MedicationRequest/|Location/)",
        "",
        raw_id,
    )
 

def _normalize_value(value, expected_dtype: str | None):
    """
    Normalise une valeur brute issue du JSON FHIR selon le type attendu (_schemas).
    Objectif : éviter les colonnes Polars à types mixtes.
    """

    if value is None:
        return None

    # Listes → valeur atomique (1er élément)
    if isinstance(value, list):
        if not value:
            return None
        value = value[0]

    # Dictionnaires → JSON string (cas FHIR complexe)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    if expected_dtype is None:
        return value

    try:
        dtype = expected_dtype.lower()

        if dtype in ("utf8", "string", "str"):
            return str(value)

        if dtype in ("int64", "int", "integer"):
            return int(value)

        if dtype in ("float64", "float", "double"):
            return float(value)

        if dtype in ("bool", "boolean"):
            return bool(value)

        if dtype in ("date", "datetime"):
            # on garde la string → conversion Polars plus tard
            return str(value)

    except Exception:
        # fallback sécurisé
        return None

    return value


 
def format_fhir_date(date_val: Optional[Union[str, datetime]]) -> Optional[str]:
    """Normalise les dates pour l'affichage ou le stockage.
   
    FHIR exige un format ISO 8601 strict (YYYY-MM-DDThh:mm:ss).
    Cette fonction s'assure que les objets Python datetime sont convertis en string correctement.
    """
    if not date_val:
        return None
    # Si c'est un objet datetime Python, on utilise la méthode standard isoformat()
    if isinstance(date_val, datetime):
        return date_val.isoformat()
    # Si c'est déjà une chaîne de caractères, on la renvoie telle quelle
    return date_val
 
 
def get_coding_value(codeable_concept: Optional[dict], system_url: str) -> Optional[str]:
    """Extrait un code d'un CodeableConcept FHIR selon un système (ex: CIM-10).
   
    Un CodeableConcept contient une liste de codes ('coding'). Il faut itérer dessus
    pour trouver celui qui correspond au système de nomenclature demandé (system_url).
    """
    # Vérification de sécurité : si l'objet est vide ou n'a pas de clé "coding", on arrête.
    if not codeable_concept or "coding" not in codeable_concept:
        return None
 
    # On parcourt la liste des codes disponibles pour cet élément
    for coding in codeable_concept["coding"]:
        # Si l'URL du système (ex: 'http://loinc.org') correspond à ce qu'on cherche
        if coding.get("system") == system_url:
            # On retourne la valeur du code (ex: '718-7')
            return coding.get("code")
   
    # Si on a tout parcouru sans trouver le système demandé
    return None
 
 
# -----------------------------------------------------------------------------
# Date helpers
# -----------------------------------------------------------------------------
 
def compute_age(
    birth_date: Union[date, str, datetime],
    reference_date: Optional[Union[date, str, datetime]] = None,
) -> Optional[int]:
    """Calcule l'âge à une date de référence.
 
    Gère la robustesse : accepte des strings, des dates ou des datetimes.
   
    Compatibilité:
    - Ancien usage dans le projet: compute_age(birth_date, reference_date)
    - Usage actuel dans build_eds_with_fhir: compute_age(birth_date) (référence = aujourd'hui)
    """
    if not birth_date:
        return None
 
    # Par défaut, si pas de date de référence, on prend "Aujourd'hui"
    if reference_date is None:
        reference_date = date.today()
 
    try:
        # Conversion en date si on reçoit des chaînes (format ISO YYYY-MM-DD...)
        # .split("T")[0] permet de garder juste la partie date avant l'heure
        if isinstance(birth_date, str):
            birth_date = datetime.fromisoformat(birth_date.split("T")[0]).date()
        if isinstance(reference_date, str):
            reference_date = datetime.fromisoformat(reference_date.split("T")[0]).date()
 
        # Conversion datetime -> date (on ignore les heures/minutes pour l'âge)
        if isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        if isinstance(reference_date, datetime):
            reference_date = reference_date.date()
 
        # Calcul mathématique de l'âge :
        # 1. Différence des années (ex: 2023 - 1990 = 33)
        # 2. Correction : on soustrait 1 si l'anniversaire n'est pas encore passé cette année.
        # (L'expression booléenne < renvoie True (1) ou False (0))
        return reference_date.year - birth_date.year - (
            (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day)
        )
    except Exception:
        # En cas de format de date invalide, on ne fait pas planter le script, on renvoie None
        return None
 
 
# -----------------------------------------------------------------------------
# JSON helpers used by mapping/build scripts
# -----------------------------------------------------------------------------
 
def get_value_from_path(data: dict, path: str):
    """Navigue dans un JSON via un chemin type 'a.b[0].c'.
 
    Fonction utilitaire puissante pour éviter les erreurs 'KeyError' ou 'IndexError'.
   
    - Supporte les listes avec l'indexation [0]
    - Nettoie certains préfixes FHIR sur les chaînes (urn:uuid:, Patient/, ...)
    """
    if not path or data is None:
        return None
 
    # Cas spécial : récupérer le type de ressource directement
    if path == "resourceType":
        return data.get("resourceType")
 
    # Transformation du chemin : "a.b[0].c" devient une liste ["a", "b", "0", "c"]
    elements = path.replace("[", ".").replace("]", "").split(".")
    current = data
 
    for key in elements:
        if current is None:
            return None
 
        # Si la clé est un nombre, on essaie d'accéder à un index de liste
        if key.isdigit():
            idx = int(key)
            if isinstance(current, list) and len(current) > idx:
                current = current[idx]
            else:
                return None # Index hors limites
        # Sinon, on essaie d'accéder à une clé de dictionnaire
        elif isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None # Clé introuvable
 
    # Nettoyage final : si le résultat est une référence FHIR, on la nettoie
    if isinstance(current, str):
        for prefix in ["urn:uuid:", "Patient/", "Encounter/", "Practitioner/", "Location/"]:
            current = current.replace(prefix, "")
 
    return current
 
 
def load_json_flexible(path: str) -> dict:
    """Charge un JSON robuste (mapping.json) même si le fichier est "sale".
 
    Très utile quand les fichiers de config sont copiés-collés depuis Internet ou ChatGPT.
    Gère notamment:
    - BOM utf-8 (caractères invisibles au début du fichier)
    - fences markdown ```json (balises de code)
    - texte avant le premier '{' ou '['
    - plusieurs objets JSON concaténés
    """
    # Lecture avec encodage 'utf-8-sig' pour gérer le BOM automatiquement
    raw = Path(path).read_text(encoding="utf-8-sig", errors="replace").replace("\r\n", "\n")
 
    # Retirer fences markdown éventuels (lignes ```) au début et à la fin
    lines = raw.splitlines()
    while lines and (not lines[0].strip() or lines[0].strip().startswith("```")):
        lines.pop(0)
    while lines and lines[-1].strip().startswith("```"):
        lines.pop()
 
    text = "\n".join(lines).strip()
 
    # Démarrer au premier '{' ou '[' pour ignorer le texte parasite avant le JSON
    m = re.search(r"[\{\[]", text)
    if not m:
        raise ValueError("mapping.json: aucun '{' ou '[' trouvé.")
    text = text[m.start() :].strip()
 
    # Décodage manuel pour gérer le cas où il y aurait plusieurs JSON à la suite
    decoder = json.JSONDecoder()
    idx = 0
    objs = []
 
    while idx < len(text):
        # Sauter les espaces blancs
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        # raw_decode lit un objet JSON valide et renvoie l'index de fin
        obj, end = decoder.raw_decode(text, idx)
        objs.append(obj)
        idx = end
 
    if not objs:
        raise json.JSONDecodeError("Empty JSON after cleaning", text, 0)
 
    # Si un seul objet trouvé, c'est le cas normal
    if len(objs) == 1:
        if not isinstance(objs[0], dict):
            raise ValueError("mapping.json doit être un objet (dict) à la racine.")
        return objs[0]
 
    # Si plusieurs objets trouvés, on les fusionne (merge)
    merged: dict = {}
    for o in objs:
        if not isinstance(o, dict):
            raise ValueError("mapping.json contient plusieurs JSON, mais l'un n'est pas un objet (dict).")
        merged.update(o)
    return merged
 
 
def _compute_expected_columns(mapping_rules: dict, schemas: dict | None) -> dict:
    """Construit les colonnes attendues par table (ordre stable)..
   
    Sert à préparer le schéma pour Polars afin d'éviter les erreurs de colonnes manquantes.
    """
    # Si le schéma est déjà fourni explicitement, on l'utilise
    if isinstance(schemas, dict) and schemas:
        return schemas
 
    # Sinon, on le déduit des règles de mapping
    expected: dict[str, list[str]] = {}
    for rule in mapping_rules.values():
        table = rule["table_name"]
        cols = list(rule.get("columns", {}).keys())
        expected.setdefault(table, [])
        for c in cols:
            if c not in expected[table]:
                expected[table].append(c)
    return expected
 
 
def enforce_schema(df: pl.DataFrame, table_name: str, expected_columns: dict) -> pl.DataFrame:
    """Garde exactement les colonnes attendues (et leur ordre) selon expected_columns.
 
    C'est une étape cruciale de "Data Quality" :
    - Ajoute les colonnes manquantes en mettant 'null' (évite les crashs).
    - Supprime les colonnes en trop.
    - Réordonne les colonnes pour que tous les fichiers Parquet aient la même structure.
    """
    expected = expected_columns.get(table_name)
    if not expected:
        return df # Si pas de schéma défini, on renvoie tel quel
 
    # Identification des colonnes manquantes
    missing = [c for c in expected if c not in df.columns]
    # Ajout des colonnes manquantes remplies avec 'None' (null)
    if missing:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing])
 
    # Sélection stricte : ne garde que ce qui est dans 'expected'
    return df.select([c for c in expected if c in df.columns])
 
 
def _coalesce_from(df: pl.DataFrame, target: str, src: str) -> pl.DataFrame:
    """Remplit target avec src quand target est null, puis supprime src.
   
    Équivalent du COALESCE(target, src) en SQL.
    Utilisé pour consolider des données provenant de deux champs différents.
    """
    if target in df.columns and src in df.columns:
        # pl.coalesce prend la première valeur non-nulle de la liste
        df = df.with_columns(pl.coalesce([pl.col(target), pl.col(src)]).alias(target))
        # On supprime la colonne source intermédiaire pour nettoyer
        df = df.drop(src)
    return df
 
 
# def write_last_run_report(result: dict, target_eds_dir: str, filename: str = "last_run.json") -> None:
#     """
#     - Ecrit le dernier run dans eds/last_run.json
#     - Archive chaque run dans eds/runs/last_run_<timestamp>.json (historique)
#     """
#     try:
#         from pathlib import Path
#         from datetime import datetime
#         import json
 
#         eds_path = Path(target_eds_dir)
#         eds_path.mkdir(parents=True, exist_ok=True)
 
#         # 1) Dernier run (toujours le plus récent)
#         latest = eds_path / filename
#         with open(latest, "w", encoding="utf-8") as f:
#             json.dump(result, f, ensure_ascii=False, indent=2)
 
#         # 2) Historique (jamais écrasé)
#         runs_dir = eds_path / "runs"
#         runs_dir.mkdir(parents=True, exist_ok=True)
 
#         run_id = result.get("run_id") if isinstance(result, dict) else None
#         ts = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
#         archived = runs_dir / f"last_run_{ts}.json"
 
#         with open(archived, "w", encoding="utf-8") as f:
#             json.dump(result, f, ensure_ascii=False, indent=2)
 
#     except Exception:
#         # on ne casse pas la conversion si l’écriture échoue
#         pass
 
# def write_last_run_report(result: dict, reports_dir: str) -> str:
#     """
#     Écrit le dernier report d'import/export.
#     Convention projet :
#     - reports_dir/last_run.json        → dernier run
#     - reports_dir/runs/<run_id>.json   → historique
#     """

#     reports_dir = Path(reports_dir)
#     reports_dir.mkdir(parents=True, exist_ok=True)

#     runs_dir = reports_dir / "runs"
#     runs_dir.mkdir(parents=True, exist_ok=True)

#     # run_id obligatoire
#     run_id = result.get("run_id")
#     if not run_id:
#         run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
#         result["run_id"] = run_id

#     # enrichissement minimal non intrusif
#     result.setdefault("schema_version", 1)
#     result.setdefault("generated_at", datetime.now().isoformat())

#     last_run_path = reports_dir / "last_run.json"
#     archived_path = runs_dir / f"{run_id}.json"

#     with open(last_run_path, "w", encoding="utf-8") as f:
#         json.dump(result, f, ensure_ascii=False, indent=2)

#     with open(archived_path, "w", encoding="utf-8") as f:
#         json.dump(result, f, ensure_ascii=False, indent=2)

#     return run_id

 
def write_last_run_report(result: dict, reports_dir: str) -> str:
    """
    Ecrit le dernier report (import / export).

    Convention projet :
    - reports_dir/last_run.json        -> dernier run
    - reports_dir/runs/<run_id>.json   -> historique
    """

    from pathlib import Path
    from datetime import datetime
    import json

    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    # run_id obligatoire
    run_id = result.get("run_id")
    if not run_id:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        result["run_id"] = run_id

    # métadonnées minimales (non intrusives)
    result.setdefault("schema_version", 1)
    result.setdefault("generated_at", datetime.now().isoformat())

    last_run_path = reports_dir / "last_run.json"
    archived_path = runs_dir / f"{run_id}.json"

    with open(last_run_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    with open(archived_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return run_id

 
#api helpers    eds to fhir
def _fetch_bundle_all_pages(url: str, params: dict | None = None, timeout: int = 60) -> dict:
    """
    Récupère un Bundle FHIR (searchset / $everything) en suivant la pagination (link[next]).
    Retourne un Bundle unique avec toutes les 'entry' concaténées.
    """
    r = requests.get(url, params=params, headers=FHIR_ACCEPT_HEADERS, timeout=timeout)
    r.raise_for_status()
    bundle = r.json()
 
    all_entries = []
    if bundle.get("entry"):
        all_entries.extend(bundle["entry"])
 
    while True:
        next_url = None
        for link in bundle.get("link", []) or []:
            if link.get("relation") == "next":
                next_url = link.get("url")
                break
 
        if not next_url:
            break
 
        r = requests.get(next_url, headers=FHIR_ACCEPT_HEADERS, timeout=timeout)
        r.raise_for_status()
        bundle = r.json()
        if bundle.get("entry"):
            all_entries.extend(bundle["entry"])
 
    # On renvoie un bundle "collection" simple (compatible avec votre pipeline : entry[].resource)
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": all_entries,
    }
 
def _collect_patient_ids(limit: int, page_size: int, timeout: int = 60) -> list[str]:
    """
    Récupère les IDs Patient depuis l'entrepôt en paginant.
    - limit > 0 : s'arrête dès qu'on a 'limit' IDs
    - limit == 0 : récupère tous les patients
    """
    url = f"{FHIR_SERVER_URL}/Patient"
    params = {"_count": page_size}
 
    ids: list[str] = []
 
    r = requests.get(url, params=params, headers=FHIR_ACCEPT_HEADERS, timeout=timeout)
    r.raise_for_status()
    bundle = r.json()
 
    while True:
        # 1) ajouter les IDs de la page courante
        for entry in bundle.get("entry", []) or []:
            res = entry.get("resource", {})
            if res.get("resourceType") == "Patient":
                pid = res.get("id")
                if pid:
                    ids.append(pid)
                    # stop dès qu'on a assez
                    if limit > 0 and len(ids) >= limit:
                        return ids
 
        # 2) trouver la page suivante
        next_url = None
        for link in bundle.get("link", []) or []:
            if link.get("relation") == "next":
                next_url = link.get("url")
                break
 
        if not next_url:
            break
 
        r = requests.get(next_url, headers=FHIR_ACCEPT_HEADERS, timeout=timeout)
        r.raise_for_status()
        bundle = r.json()
 
    return ids

def summarize_bundle(bundle: dict) -> dict:
    """
    Retourne:
      - entries_total: nombre total d'entry dans le bundle
      - resources_per_type: dict {resourceType: count}
    """
    entries = bundle.get("entry", []) or []
    c = Counter()

    for e in entries:
        res = (e.get("resource") or {})
        rt = res.get("resourceType")
        if rt:
            c[rt] += 1

    return {
        "entries_total": len(entries),
        "resources_per_type": dict(c),
    }
 
 
def _zip_folder(folder: str | Path, zip_path: str | Path) -> None:
    folder = Path(folder)
    zip_path = Path(zip_path)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
 
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
        for p in folder.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=p.relative_to(folder))
 
 
 
 
 
def _coalesce_from_path(df: pl.DataFrame, target: str, src: str) -> pl.DataFrame:
    """Remplit target avec src quand target est null, puis supprime src.
   
    Équivalent du COALESCE(target, src) en SQL.
    Utilisé pour consolider des données provenant de deux champs différents.
    """
    if target in df.columns and src in df.columns:
        # pl.coalesce prend la première valeur non-nulle de la liste
        df = df.with_columns(pl.coalesce([pl.col(target), pl.col(src)]).alias(target))
        # On supprime la colonne source intermédiaire pour nettoyer
        df = df.drop(src)
    return df



def parquet_row_count(path: str | Path) -> int:
    """Retourne le nombre de lignes d'un parquet, 0 si fichier absent."""
    p = Path(path)
    if not p.exists():
        return 0
    return pl.scan_parquet(str(p)).select(pl.len()).collect().item()

def snapshot_eds_counts(eds_dir: str | Path, tables: list[str]) -> dict:
    """
    Prend un snapshot {table: nb_lignes} dans eds_dir.
    tables = ["pmsi.parquet", "mvt.parquet", ...]
    """
    eds_dir = Path(eds_dir)
    return {t: parquet_row_count(eds_dir / t) for t in tables}

def build_merge_report(before: dict, after: dict, incoming_acc: dict) -> list[dict]:
    """
    Construit un merge_report final cohérent :
    - before_rows: snapshot avant run
    - after_rows : snapshot après run
    - incoming_rows: somme des lignes tentées d’être injectées (accumulateur)
    - added_rows: after - before
    """
    tables = sorted(set(before.keys()) | set(after.keys()) | set(incoming_acc.keys()))
    report = []
    for t in tables:
        b = int(before.get(t, 0) or 0)
        a = int(after.get(t, 0) or 0)
        inc = int(incoming_acc.get(t, 0) or 0)
        report.append({
            "table": t,
            "before_rows": b,
            "incoming_rows": inc,
            "after_rows": a,
            "added_rows": a - b,
        })
    return sorted(report, key=lambda x: x["table"])
