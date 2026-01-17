"""Reusable helper utilities used across the project.

This file is meant to centralize small, reusable functions (parsing, cleaning,
JSON-path extraction, schema enforcement, etc.) so that converter/build scripts
stay focused on the business logic.

Important (stability): functions keep their previous names and remain
backwards-compatible.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import polars as pl


# -----------------------------------------------------------------------------
# FHIR / generic text helpers
# -----------------------------------------------------------------------------

def clean_id(raw_id: Optional[str]) -> str:
    """Nettoie les identifiants FHIR pour ne garder que la partie unique.

    Exemples:
    - 'Patient/123' -> '123'
    - 'urn:uuid:abc-def' -> 'abc-def'
    """
    if not raw_id:
        return ""

    # Supprime les préfixes courants via une expression régulière
    return re.sub(
        r"^(urn:uuid:|Patient/|Encounter/|Observation/|Procedure/|Condition/|MedicationRequest/|Location/)",
        "",
        raw_id,
    )


def format_fhir_date(date_val: Optional[Union[str, datetime]]) -> Optional[str]:
    """Normalise les dates pour l'affichage ou le stockage."""
    if not date_val:
        return None
    if isinstance(date_val, datetime):
        return date_val.isoformat()
    return date_val


def get_coding_value(codeable_concept: Optional[dict], system_url: str) -> Optional[str]:
    """Extrait un code d'un CodeableConcept FHIR selon un système (ex: CIM-10)."""
    if not codeable_concept or "coding" not in codeable_concept:
        return None

    for coding in codeable_concept["coding"]:
        if coding.get("system") == system_url:
            return coding.get("code")
    return None


# -----------------------------------------------------------------------------
# Date helpers
# -----------------------------------------------------------------------------

def compute_age(
    birth_date: Union[date, str, datetime],
    reference_date: Optional[Union[date, str, datetime]] = None,
) -> Optional[int]:
    """Calcule l'âge à une date de référence.

    Compatibilité:
    - Ancien usage dans le projet: compute_age(birth_date, reference_date)
    - Usage actuel dans build_eds_with_fhir: compute_age(birth_date) (référence = aujourd'hui)
    """
    if not birth_date:
        return None

    if reference_date is None:
        reference_date = date.today()

    try:
        # Conversion en date si on reçoit des chaînes
        if isinstance(birth_date, str):
            birth_date = datetime.fromisoformat(birth_date.split("T")[0]).date()
        if isinstance(reference_date, str):
            reference_date = datetime.fromisoformat(reference_date.split("T")[0]).date()

        # Conversion datetime -> date
        if isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        if isinstance(reference_date, datetime):
            reference_date = reference_date.date()

        return reference_date.year - birth_date.year - (
            (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day)
        )
    except Exception:
        return None


# -----------------------------------------------------------------------------
# JSON helpers used by mapping/build scripts
# -----------------------------------------------------------------------------

def get_value_from_path(data: dict, path: str):
    """Navigue dans un JSON via un chemin type 'a.b[0].c'.

    - Supporte les listes avec l'indexation [0]
    - Nettoie certains préfixes FHIR sur les chaînes (urn:uuid:, Patient/, ...)
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


def load_json_flexible(path: str) -> dict:
    """Charge un JSON robuste (mapping.json) même si le fichier est "sale".

    Gère notamment:
    - BOM utf-8
    - fences markdown ```json
    - texte avant le premier '{' ou '['
    - plusieurs objets JSON concaténés
    """
    raw = Path(path).read_text(encoding="utf-8-sig", errors="replace").replace("\r\n", "\n")

    # Retirer fences markdown éventuels
    lines = raw.splitlines()
    while lines and (not lines[0].strip() or lines[0].strip().startswith("```")):
        lines.pop(0)
    while lines and lines[-1].strip().startswith("```"):
        lines.pop()

    text = "\n".join(lines).strip()

    # Démarrer au premier { ou [
    m = re.search(r"[\{\[]", text)
    if not m:
        raise ValueError("mapping.json: aucun '{' ou '[' trouvé.")
    text = text[m.start() :].strip()

    decoder = json.JSONDecoder()
    idx = 0
    objs = []

    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        obj, end = decoder.raw_decode(text, idx)
        objs.append(obj)
        idx = end

    if not objs:
        raise json.JSONDecodeError("Empty JSON after cleaning", text, 0)

    if len(objs) == 1:
        if not isinstance(objs[0], dict):
            raise ValueError("mapping.json doit être un objet (dict) à la racine.")
        return objs[0]

    merged: dict = {}
    for o in objs:
        if not isinstance(o, dict):
            raise ValueError("mapping.json contient plusieurs JSON, mais l'un n'est pas un objet (dict).")
        merged.update(o)
    return merged


def _compute_expected_columns(mapping_rules: dict, schemas: dict | None) -> dict:
    """Construit les colonnes attendues par table (ordre stable)."""
    if isinstance(schemas, dict) and schemas:
        return schemas

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

    Ajoute les colonnes manquantes en null.
    """
    expected = expected_columns.get(table_name)
    if not expected:
        return df

    missing = [c for c in expected if c not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing])

    return df.select([c for c in expected if c in df.columns])


def _coalesce_from(df: pl.DataFrame, target: str, src: str) -> pl.DataFrame:
    """Remplit target avec src quand target est null, puis supprime src."""
    if target in df.columns and src in df.columns:
        df = df.with_columns(pl.coalesce([pl.col(target), pl.col(src)]).alias(target))
        df = df.drop(src)
    return df


def write_last_run_report(result: dict, target_eds_dir: str, filename: str = "last_run.json") -> None:
    """Écrit un rapport JSON (dernier run) dans le dossier EDS.

    Objectif: centraliser cette écriture (utilisée par fhir_to_edsan) dans Helpers.
    Ne doit jamais faire échouer le pipeline si l'écriture échoue.
    """
    try:
        p = Path(target_eds_dir) / filename
        with open(p, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    except Exception:
        # Non bloquant
        pass
