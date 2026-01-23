from __future__ import annotations

import json
import base64
import hashlib
import io, zipfile, tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from app.utils.helpers import clean_id, format_fhir_date, write_last_run_report


FHIR_XHTML_NS = ' xmlns="http://www.w3.org/1999/xhtml"'


# Racine projet: .../Ping37_interoperabiliteData
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Defaults (sans env)
DEFAULT_EDS_DIR = PROJECT_ROOT / "eds"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "exports_eds_fhir"
DEFAULT_MAPPING_PATH = Path(__file__).resolve().parent / "mapping.json"


# =============================================================================
# Generic helpers
# =============================================================================

def encode_base64(text: str | None) -> str | None:
    if text is None:
        return None
    return base64.b64encode(str(text).encode("utf-8")).decode("utf-8")


def is_missing(x: Any) -> bool:
    return x is None or pd.isna(x)


def stable_id(*parts: object) -> str:
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_fhir_id(raw: Any) -> str:
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
    if is_missing(patsex):
        return "unknown"
    s = str(patsex).strip().upper()
    if s == "M":
        return "male"
    if s == "F":
        return "female"
    return "unknown"


def ensure_xhtml_div(text: str) -> str:
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
# Main export function (SANS ENV)
# =============================================================================

def export_eds_to_fhir(
    eds_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    mapping_path: str | Path | None = None,
    bundle_strategy: str = "patient",
    print_summary: bool = True,
) -> dict:
    eds_dir = Path(eds_dir) if eds_dir is not None else DEFAULT_EDS_DIR
    out_dir = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    mapping_path = Path(mapping_path) if mapping_path is not None else DEFAULT_MAPPING_PATH

    if not eds_dir.exists():
        raise FileNotFoundError(f"EDS_DIR introuvable: {eds_dir}")

    if not mapping_path.exists():
        raise FileNotFoundError(f"mapping.json introuvable: {mapping_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    all_resources: list[dict] = []
    by_type: dict[str, list[dict]] = {}

    for rtype, cfg in mapping.items():
        if rtype == "_schemas":
            continue

        table_name = cfg.get("table_name")
        if not table_name:
            continue

        parquet = eds_dir / table_name
        if not parquet.exists():
            continue

        df = pd.read_parquet(parquet)
        built = [build_resource(rtype, row, cfg) for _, row in df.iterrows()]
        by_type[rtype] = built
        all_resources.extend(built)

    bundles: dict[str, dict] = {}

    if bundle_strategy == "patient":
        grouped: dict[str, list[dict]] = {}
        for r in all_resources:
            pid = get_patient_id(r)
            if pid:
                grouped.setdefault(pid, []).append(r)

        for pid, resources in grouped.items():
            bid = f"patient-{pid}"
            bundles[bid] = build_bundle(resources, bid)

    elif bundle_strategy == "encounter":
        grouped: dict[str, list[dict]] = {}
        for r in all_resources:
            eid = get_encounter_id(r)
            if eid:
                grouped.setdefault(eid, []).append(r)

        for eid, resources in grouped.items():
            bid = f"encounter-{eid}"
            bundles[bid] = build_bundle(resources, bid)

    else:
        raise ValueError('bundle_strategy doit être "patient" ou "encounter"')

    # Write bundle files
    for bid, bundle in bundles.items():
        with open(out_dir / f"{bid}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, ensure_ascii=False)

    summary = {
        "eds_dir": str(eds_dir),
        "output_dir": str(out_dir),
        "mapping": str(mapping_path),
        "bundle_strategy": bundle_strategy,
        "resources_per_type": {k: len(v) for k, v in by_type.items()},
        "bundles_generated": len(bundles),
    }

    if print_summary:
        print(json.dumps(summary, indent=2, ensure_ascii=False))

    write_last_run_report(summary, str(out_dir), "last_run.json")
    return summary


# ==============================================================================
# Script entry point
# =============================================================================

if __name__ == "__main__":
    export_eds_to_fhir()
