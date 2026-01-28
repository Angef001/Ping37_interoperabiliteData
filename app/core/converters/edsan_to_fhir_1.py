"""
edsan_to_fhir.py

EDS parquet → FHIR R4 Bundle exporter (mapping-driven).

This script:
- Reads EDS parquet files.
- Applies a JSON mapping (EDS columns → FHIR paths).
- Builds FHIR R4 resources with stable IDs.
- Groups resources into Bundles (per patient or per encounter).
- Writes Bundle JSON files into an output directory.
- Prints a summary to the console.
- Uses environment variables for configuration (best practice).

Environment variables
---------------------
EDS_DIR               : directory containing parquet files
FHIR_OUTPUT_DIR       : directory where Bundle JSON files are written
FHIR_MAPPING_PATH     : path to mapping.json
FHIR_BUNDLE_STRATEGY  : "patient" or "encounter"

Defaults are provided for local execution.
"""

from __future__ import annotations
import os
import json
import base64
import hashlib
import io, zipfile, tempfile
from pathlib import Path
from typing import Any
import pandas as pd

from app.utils.helpers import clean_id, format_fhir_date, write_last_run_report


FHIR_XHTML_NS = ' xmlns="http://www.w3.org/1999/xhtml"'
BASE_DIR = Path(__file__).resolve().parent  # .../app/core/converters
mapping_path = BASE_DIR / "mapping.json"

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # remonte jusqu’à Ping37_interoperabiliteData
EDS_DIR = PROJECT_ROOT / "eds"
OUTPUT_DIR = PROJECT_ROOT / "exports_eds_fhir"

# =============================================================================
# Generic helpers
# =============================================================================

def load_dotenv(dotenv_path: str = ".env") -> None:
    """
    Minimal .env loader (no external dependency).
    Reads KEY=VALUE lines and sets os.environ if not already set.
    """
    p = Path(dotenv_path)
    if not p.exists():
        return

    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def encode_base64(text: str | None) -> str | None:
    """Encode a string to base64 (UTF-8)."""
    if text is None:
        return None
    return base64.b64encode(str(text).encode("utf-8")).decode("utf-8")


def is_missing(x: Any) -> bool:
    """Return True if value is None or NaN."""
    return x is None or pd.isna(x)


def stable_id(*parts: object) -> str:
    """
    Generate a deterministic (stable) identifier using SHA-1.

    Same inputs → same output → stable IDs across runs.
    """
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_fhir_id(raw: Any) -> str:
    """
    Normalize a value into a FHIR-compatible id.

    - keeps part after '|'
    - cleans invalid characters
    - truncates to 64 chars
    """
    if is_missing(raw):
        return ""

    s = str(raw)

    if "|" in s:
        s = s.split("|")[-1]

    s = clean_id(s)

    if any(c in s for c in ["?", "=", "&", "/"]):
        return ""

    return s[:64]


def normalize_gender(patsex: Any) -> str:
    """Map EDS gender to FHIR administrativeGender."""
    if is_missing(patsex):
        return "unknown"
    s = str(patsex).strip().upper()
    if s == "M":
        return "male"
    if s == "F":
        return "female"
    return "unknown"


def ensure_xhtml_div(text: str) -> str:
    """Ensure XHTML <div> wrapper for FHIR narrative fields."""
    if text is None:
        return text
    t = str(text).strip()
    if t.lower().startswith("<div"):
        return t
    return f"<div{FHIR_XHTML_NS}>{t}</div>"


# =============================================================================
# FHIR reference helpers
# =============================================================================

def patient_ref(pid: str) -> str:
    return f"Patient/{pid}"


def encounter_ref(eid: str) -> str:
    return f"Encounter/{eid}"


def location_ref(lid: str) -> str:
    return f"Location/{lid}"


# =============================================================================
# Dot-path setter (supports list indexes)
# =============================================================================

def _parse_path(path: str) -> list[Any]:
    tokens: list[Any] = []
    buf = ""
    i = 0
    while i < len(path):
        c = path[i]
        if c == ".":
            if buf:
                tokens.append(buf)
                buf = ""
            i += 1
            continue
        if c == "[":
            if buf:
                tokens.append(buf)
                buf = ""
            j = path.find("]", i)
            tokens.append(int(path[i + 1:j]))
            i = j + 1
            continue
        buf += c
        i += 1
    if buf:
        tokens.append(buf)
    return tokens


def set_path(obj: dict, path: str, value: Any) -> None:
    """Set a nested dict/list value using a dot-path."""
    tokens = _parse_path(path)
    cur = obj

    for i, k in enumerate(tokens[:-1]):
        nxt = tokens[i + 1]
        if isinstance(k, int):
            while len(cur) <= k:
                cur.append({})
            if cur[k] is None:
                cur[k] = [] if isinstance(nxt, int) else {}
            cur = cur[k]
        else:
            if k not in cur or cur[k] is None:
                cur[k] = [] if isinstance(nxt, int) else {}
            cur = cur[k]

    last = tokens[-1]
    if isinstance(last, int):
        while len(cur) <= last:
            cur.append(None)
        cur[last] = value
    else:
        cur[last] = value


# =============================================================================
# Mapping-driven conversions
# =============================================================================

DATE_HINTS = (
    "birthDate", "effectiveDateTime", "issued", "authoredOn",
    "recordedDate", "performedDateTime",
    "period.start", "period.end",
    "content[0].attachment.creation", "date"
)

DEFAULTS_BY_RESOURCE = {
    "Encounter": {"status": "finished"},
    "Observation": {"status": "final"},
    "MedicationRequest": {"status": "active", "intent": "order"},
    "DiagnosticReport": {"status": "final"},
    "DocumentReference": {"status": "current"},
    "Composition": {"status": "final"},
    "Procedure": {"status": "completed"},
}


def coerce_value(resource_type: str, target_path: str, source_col: str, raw: Any) -> Any:
    if is_missing(raw):
        return None

    if resource_type == "Patient" and target_path == "gender":
        return normalize_gender(raw)

    if any(h in target_path for h in DATE_HINTS):
        return format_fhir_date(raw)

    if target_path.endswith(".data"):
        return encode_base64(raw)

    if resource_type == "Composition" and target_path in ("text.div", "section[0].text.div"):
        return ensure_xhtml_div(raw)

    if target_path.endswith(".reference"):
        nid = normalize_fhir_id(raw) or stable_id(raw)
        if source_col == "PATID":
            return patient_ref(nid)
        if source_col == "EVTID":
            return encounter_ref(nid)
        if source_col == "ELTID":
            return location_ref(nid)
        return raw

    if target_path == "id":
        return normalize_fhir_id(raw) or stable_id(raw)

    return raw


def build_resource(resource_type: str, row: pd.Series, cfg: dict) -> dict:
    res = {"resourceType": resource_type}
    res.update(DEFAULTS_BY_RESOURCE.get(resource_type, {}))

    for src, tgt in cfg.get("columns", {}).items():
        if not tgt:
            continue
        val = coerce_value(resource_type, tgt, src, row.get(src))
        if val is not None:
            set_path(res, tgt, val)

    if not res.get("id"):
        res["id"] = stable_id(resource_type, *row.values)

    return res


# =============================================================================
# Bundle helpers
# =============================================================================

def build_bundle(resources: list[dict], bundle_id: str) -> dict:
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "id": bundle_id,
        "entry": [{"resource": r} for r in resources],
    }


def get_patient_id(res: dict) -> str | None:
    ref = res.get("subject", {}).get("reference")
    if isinstance(ref, str) and ref.startswith("Patient/"):
        return ref.split("/", 1)[1]
    return None


def get_encounter_id(res: dict) -> str | None:
    if res.get("resourceType") == "Encounter":
        return res.get("id")

    ref = res.get("encounter", {}).get("reference")
    if isinstance(ref, str) and ref.startswith("Encounter/"):
        return ref.split("/", 1)[1]

    ctx = res.get("context", {}).get("encounter")
    if isinstance(ctx, dict):
        ref = ctx.get("reference")
        if isinstance(ref, str) and ref.startswith("Encounter/"):
            return ref.split("/", 1)[1]

    return None


# =============================================================================
# Main export function
# =============================================================================

def export_eds_to_fhir(
    eds_dir: str | None = None,
    output_dir: str | None = None,
    mapping_path: str | None = None,
    bundle_strategy: str | None = None,
    print_summary: bool = True,
) -> dict:
    # --- Defaults from env if args not provided
    #eds_dir = eds_dir or os.environ.get("EDS_DIR", "data/eds")
    #out_dir = output_dir or os.environ.get("FHIR_OUTPUT_DIR", "out/fhir")
    #DEFAULT_MAPPING_PATH = Path(__file__).resolve().parents[1] / "config" / "mapping.json"
    #mapping_path = os.environ.get("FHIR_MAPPING_PATH", str(DEFAULT_MAPPING_PATH))
    if mapping_path is None:
        mapping_path = Path(__file__).resolve().parent / "mapping.json"

    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)
        strategy = bundle_strategy or os.environ.get("FHIR_BUNDLE_STRATEGY", "patient")

    #eds_dir = Path(eds_dir)
    #out_dir = Path(out_dir)
    #out_dir.mkdir(parents=True, exist_ok=True)



    all_resources: list[dict] = []
    by_type: dict[str, list[dict]] = {}

    for rtype, cfg in mapping.items():
        if rtype == "_schemas":
            continue

        parquet = eds_dir / cfg["table_name"]
        if not parquet.exists():
            continue

        df = pd.read_parquet(parquet)
        built = [build_resource(rtype, row, cfg) for _, row in df.iterrows()]
        by_type[rtype] = built
        all_resources.extend(built)

    bundles: dict[str, dict] = {}

    if strategy == "patient":
        grouped: dict[str, list[dict]] = {}
        for r in all_resources:
            pid = get_patient_id(r)
            if pid:
                grouped.setdefault(pid, []).append(r)

        for pid, resources in grouped.items():
            bid = f"patient-{pid}"
            bundles[bid] = build_bundle(resources, bid)

    else:
        grouped: dict[str, list[dict]] = {}
        for r in all_resources:
            eid = get_encounter_id(r)
            if eid:
                grouped.setdefault(eid, []).append(r)

        for eid, resources in grouped.items():
            bid = f"encounter-{eid}"
            bundles[bid] = build_bundle(resources, bid)

    # Write bundle files
    for bid, bundle in bundles.items():
        with open(out_dir / f"{bid}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, ensure_ascii=False)

    summary = {
        "eds_dir": str(eds_dir),
        "output_dir": str(out_dir),
        "mapping": mapping_path,
        "bundle_strategy": strategy,
        "resources_per_type": {k: len(v) for k, v in by_type.items()},
        "bundles_generated": len(bundles),
    }

    if print_summary:
        print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


    write_last_run_report(summary, str(out_dir), "last_run.json")
    return summary



def export_eds_to_fhir_zip(cfg: dict) -> bytes:
    """
    cfg keys:
      - EDS_DIR
      - FHIR_OUTPUT_DIR (optionnel, on peut utiliser un temp)
      - FHIR_EXPORT_DIR (optionnel)
      - FHIR_BUNDLE_STRATEGY
      - FHIR_MAPPING_PATH (si tu veux aussi le rendre configurable)
    """
    eds_dir = cfg.get("EDS_DIR")
    strategy = cfg.get("FHIR_BUNDLE_STRATEGY") or "patient"
    mapping_path = cfg.get("FHIR_MAPPING_PATH")  # optionnel

    # On écrit dans un dossier temporaire (safe pour prod)
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = tmpdir  # on force l'output dans temp

        summary = export_eds_to_fhir(
            eds_dir=eds_dir,
            output_dir=out_dir,
            mapping_path=mapping_path,
            bundle_strategy=strategy,
            print_summary=False
        )

        # Zip en mémoire
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            # ajouter tous les .json générés
            for f in os.listdir(out_dir):
                if f.endswith(".json"):
                    z.write(os.path.join(out_dir, f), arcname=f)

            # Optionnel: inclure un résumé
            z.writestr("summary.json", json.dumps(summary, ensure_ascii=False, indent=2))

        buf.seek(0)
        return buf.read()

# =============================================================================
# Script entry point
# =============================================================================

if __name__ == "__main__":
    load_dotenv(".env")
    export_eds_to_fhir()

