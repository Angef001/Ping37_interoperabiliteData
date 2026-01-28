from __future__ import annotations
import json
import base64
import hashlib
from pathlib import Path
from typing import Any
import requests
import pandas as pd
from app.utils.helpers import clean_id, format_fhir_date, write_last_run_report
FHIR_XHTML_NS = ' xmlns="http://www.w3.org/1999/xhtml"'
# Racine projet
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EDS_DIR = PROJECT_ROOT / "eds"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "exports_eds_fhir"
DEFAULT_MAPPING_PATH = Path(__file__).resolve().parent / "mapping.json"
# =============================================================================
# Generic helpers
# =============================================================================
def encode_base64(text: str | None) -> str | None:
    if text is None: return None
    return base64.b64encode(str(text).encode("utf-8")).decode("utf-8")
def is_missing(x: Any) -> bool:
    return x is None or pd.isna(x)
def stable_id(*parts: object) -> str:
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()
def normalize_fhir_id(raw: Any) -> str:
    if is_missing(raw): return ""
    s = str(raw)
    if "|" in s: s = s.split("|")[-1]
    if "?" in s or "=" in s:
        s = s.split("|")[-1].split("=")[-1]
    s = clean_id(s)
    return s[:64]
def normalize_gender(patsex: Any) -> str:
    if is_missing(patsex): return "unknown"
    s = str(patsex).strip().upper()
    if s == "M": return "male"
    if s == "F": return "female"
    return "unknown"
def ensure_xhtml_div(text: str) -> str:
    if text is None: return text
    t = str(text).strip()
    if t.lower().startswith("<div"): return t
    return f"<div{FHIR_XHTML_NS}>{t}</div>"
# =============================================================================
# FHIR Path & Resource Building
# =============================================================================
def _parse_path(path: str) -> list[Any]:
    tokens: list[Any] = []
    buf = ""; i = 0
    while i < len(path):
        c = path[i]
        if c == ".":
            if buf: tokens.append(buf); buf = ""
            i += 1; continue
        if c == "[":
            if buf: tokens.append(buf); buf = ""
            j = path.find("]", i)
            tokens.append(int(path[i + 1:j]))
            i = j + 1; continue
        buf += c; i += 1
    if buf: tokens.append(buf)
    return tokens
def set_path(obj: dict, path: str, value: Any) -> None:
    tokens = _parse_path(path)
    cur = obj
    for i, k in enumerate(tokens[:-1]):
        nxt = tokens[i + 1]
        if isinstance(k, int):
            while len(cur) <= k: cur.append({})
            if cur[k] is None: cur[k] = [] if isinstance(nxt, int) else {}
            cur = cur[k]
        else:
            if k not in cur or cur[k] is None:
                cur[k] = [] if isinstance(nxt, int) else {}
            cur = cur[k]
    last = tokens[-1]
    if isinstance(last, int):
        while len(cur) <= last: cur.append(None)
        cur[last] = value
    else: cur[last] = value
def build_resource(resource_type: str, row: pd.Series, cfg: dict) -> dict:
    res = {"resourceType": resource_type}
    defaults = {
        "Encounter": "finished", "Observation": "final", "MedicationRequest": "active",
        "DiagnosticReport": "final", "DocumentReference": "current", "Procedure": "completed"
    }
    if resource_type in defaults: res["status"] = defaults[resource_type]
    if resource_type == "MedicationRequest": res["intent"] = "order"
    for src, tgt in cfg.get("columns", {}).items():
        if not tgt: continue
        val = coerce_value(resource_type, tgt, src, row.get(src))
        if val is not None: set_path(res, tgt, val)
    if not res.get("id"):
        res["id"] = stable_id(resource_type, *row.values)
    return res
def coerce_value(resource_type: str, target_path: str, source_col: str, raw: Any) -> Any:
    if is_missing(raw): return None
    if resource_type == "Patient" and target_path == "gender": return normalize_gender(raw)
    if any(h in target_path for h in ["Date", "DateTime", "recorded", "period"]): return format_fhir_date(raw)
    if target_path == "id": return normalize_fhir_id(raw) or stable_id(raw)
    if target_path.endswith(".reference"):
        nid = normalize_fhir_id(raw) or stable_id(raw)
        if source_col == "PATID": return f"Patient/{nid}"
        if source_col == "EVTID": return f"Encounter/{nid}"
        if source_col == "ELTID" or "Location" in target_path: return f"Location/{nid}"
    return raw
# =============================================================================
# Bundle & Push Logic
# =============================================================================
def make_location_stub(location_id: str) -> dict:
    return {
        "resourceType": "Location",
        "id": location_id,
        "name": f"Auto-generated Location {location_id}",
        "status": "active"
    }
def build_transaction_bundle(resources: list[dict], bundle_id: str) -> dict:
    entries = []
    seen_ids = set()
    referenced_locations = set()
    # 1. Identifier les références Location pour l'intégrité référentielle
    for r in resources:
        if r.get("resourceType") == "Encounter":
            loc_list = r.get("location", [])
            for item in loc_list:
                ref = item.get("location", {}).get("reference")
                if ref and ref.startswith("Location/"):
                    referenced_locations.add(ref.split("/")[1])
    # 2. Ajouter les ressources principales
    for r in resources:
        rtype, rid = r.get("resourceType"), r.get("id")
        if not rid or f"{rtype}/{rid}" in seen_ids: continue
        seen_ids.add(f"{rtype}/{rid}")
        entries.append({
            "resource": r,
            "request": {"method": "PUT", "url": f"{rtype}/{rid}"}
        })
    # 3. Ajouter les Location manquantes (Stubs) pour éviter les erreurs 400/404
    for lid in referenced_locations:
        if f"Location/{lid}" not in seen_ids:
            seen_ids.add(f"Location/{lid}")
            loc_res = make_location_stub(lid)
            entries.insert(0, { # Insérer au début pour être créé avant l'Encounter
                "resource": loc_res,
                "request": {"method": "PUT", "url": f"Location/{lid}"}
            })
    return {"resourceType": "Bundle", "type": "transaction", "id": bundle_id, "entry": entries}
def push_bundle_to_fhir(bundle: dict, fhir_base_url: str) -> dict:
    resp = requests.post(
        fhir_base_url.rstrip("/"),
        json=bundle,
        headers={"Content-Type": "application/fhir+json"},
        timeout=60
    )
    if not resp.ok: raise RuntimeError(f"FHIR {resp.status_code}: {resp.text[:500]}")
    return resp.json()
def get_patient_id(res: dict) -> str | None:
    ref = res.get("subject", {}).get("reference") or res.get("patient", {}).get("reference")
    return ref.split("/", 1)[1] if isinstance(ref, str) and "/" in ref else None
# =============================================================================
# Main Export Function
# =============================================================================
def export_eds_to_fhir(
    eds_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    mapping_path: str | Path | None = None,
    bundle_strategy: str = "patient",
    print_summary: bool = True,
    fhir_base_url: str | None = None,
) -> dict:
    eds_dir = Path(eds_dir or DEFAULT_EDS_DIR)
    mapping_path = Path(mapping_path or DEFAULT_MAPPING_PATH)
    out_dir = Path(output_dir) if output_dir else None
    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    all_resources = []
    by_type = {}
    for rtype, cfg in mapping.items():
        if rtype.startswith("_"): continue
        parquet = eds_dir / cfg.get("table_name", "")
        if not parquet.exists(): continue
        df = pd.read_parquet(parquet)
        built = [build_resource(rtype, row, cfg) for _, row in df.iterrows()]
        by_type[rtype] = len(built)
        all_resources.extend(built)
    bundles = {}
    grouped = {}
    for r in all_resources:
        pid = r["id"] if r["resourceType"] == "Patient" else get_patient_id(r)
        if pid: grouped.setdefault(pid, []).append(r)
    for pid, resources in grouped.items():
        bid = f"patient-{pid}"
        bundles[bid] = build_transaction_bundle(resources, bid)
    push_results = {}
    for bid, bundle in bundles.items():
        if out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
            with open(out_dir / f"{bid}.json", "w", encoding="utf-8") as f:
                json.dump(bundle, f, indent=2, ensure_ascii=False)
        if fhir_base_url:
            push_results[bid] = push_bundle_to_fhir(bundle, fhir_base_url)
    summary = {"bundles_generated": len(bundles), "resources_per_type": by_type}
    if print_summary: print(json.dumps(summary, indent=2))
    return {"summary": summary, "push_results": push_results}
if __name__ == "__main__":
    export_eds_to_fhir(
        eds_dir="eds",
        output_dir="exports_eds_fhir",
        fhir_base_url="http://localhost:8080/fhir"
    )