import json
import glob
import os
from typing import Any, Dict, List, Tuple, Optional


# ----------------------------
# JSON helpers
# ----------------------------

IGNORED_TOP_LEVEL_KEYS = {"meta", "text"}  # souvent non re-exportés, bruit
IGNORED_ENTRY_KEYS = {"request", "response", "fullUrl"}  # bundle transaction/response


def _load_bundle(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_resources(bundle: dict) -> List[dict]:
    out = []
    for e in bundle.get("entry", []) or []:
        if not isinstance(e, dict):
            continue
        r = e.get("resource")
        if isinstance(r, dict) and r.get("resourceType"):
            out.append(r)
    return out


def _norm_id(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    # synthea fullUrl / references: "urn:uuid:xxxx"
    if s.startswith("urn:uuid:"):
        s = s[len("urn:uuid:") :]
    # references: "Patient/xxx"
    if "/" in s and s.split("/", 1)[0] in {"Patient", "Encounter", "Location", "Observation",
                                           "Procedure", "MedicationRequest", "DiagnosticReport",
                                           "DocumentReference", "Condition", "Immunization",
                                           "CarePlan", "Claim", "Composition"}:
        s = s.split("/", 1)[1]
    return s.strip()


def _flatten_json(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """
    Flatten JSON into path -> leaf_value
    Lists are indexed with [i].
    """
    out: Dict[str, Any] = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            if prefix == "" and k in IGNORED_TOP_LEVEL_KEYS:
                continue
            p = f"{prefix}.{k}" if prefix else k
            out.update(_flatten_json(v, p))
        return out

    if isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{prefix}[{i}]"
            out.update(_flatten_json(v, p))
        return out

    # leaf
    out[prefix] = obj
    return out


def _get_ref(res: dict, path: str) -> str:
    """
    Very small getter for common ref locations.
    path examples:
      - "subject.reference"
      - "encounter.reference"
      - "context.encounter.reference"
    """
    cur: Any = res
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return ""
        cur = cur[part]
    return _norm_id(cur) if isinstance(cur, str) else ""


def _get_code_signature(res: dict) -> str:
    """
    Try to build a stable signature from code-ish fields:
    - code.coding[0].system + code.coding[0].code
    - type.text (DocumentReference)
    - medicationCodeableConcept.coding...
    """
    # Observation/Condition/Procedure/etc.
    code = res.get("code")
    if isinstance(code, dict):
        codings = code.get("coding") or []
        if isinstance(codings, list) and codings:
            c0 = codings[0] if isinstance(codings[0], dict) else {}
            return f"{c0.get('system','')}|{c0.get('code','')}|{code.get('text','')}"
        return f"||{code.get('text','')}"
    # DocumentReference.type.text
    t = res.get("type")
    if isinstance(t, dict):
        return f"TYPE_TEXT|{t.get('text','')}"
    # MedicationRequest.medicationCodeableConcept
    med = res.get("medicationCodeableConcept")
    if isinstance(med, dict):
        codings = med.get("coding") or []
        if isinstance(codings, list) and codings:
            c0 = codings[0] if isinstance(codings[0], dict) else {}
            return f"MED|{c0.get('system','')}|{c0.get('code','')}|{med.get('text','')}"
        return f"MED||{med.get('text','')}"
    return ""


def _get_time_signature(res: dict) -> str:
    """
    Try to build a signature from date/period-ish fields.
    """
    for k in ("effectiveDateTime", "issued", "authoredOn", "recordedDate", "date"):
        v = res.get(k)
        if isinstance(v, str) and v:
            return v
    # Encounter.period
    period = res.get("period")
    if isinstance(period, dict):
        return f"{period.get('start','')}|{period.get('end','')}"
    # DocumentReference.content[0].attachment.creation
    content = res.get("content")
    if isinstance(content, list) and content:
        c0 = content[0] if isinstance(content[0], dict) else {}
        att = c0.get("attachment")
        if isinstance(att, dict):
            v = att.get("creation")
            if isinstance(v, str) and v:
                return v
    return ""


# ----------------------------
# Matching & diff
# ----------------------------

def _resource_key(res: dict) -> Tuple[str, str]:
    """Primary key attempt: (resourceType, id_norm)."""
    return (res.get("resourceType", ""), _norm_id(res.get("id")))


def _candidate_score(r_exp: dict, r_orig: dict) -> int:
    """
    Score how likely r_orig matches r_exp.
    Higher is better.
    """
    score = 0

    # Strong: same id
    if _norm_id(r_exp.get("id")) and _norm_id(r_exp.get("id")) == _norm_id(r_orig.get("id")):
        score += 100

    # Identifiers intersection
    exp_ids = set()
    for it in (r_exp.get("identifier") or []):
        if isinstance(it, dict):
            exp_ids.add(_norm_id(it.get("value")))
    orig_ids = set()
    for it in (r_orig.get("identifier") or []):
        if isinstance(it, dict):
            orig_ids.add(_norm_id(it.get("value")))
    if exp_ids and orig_ids and (exp_ids & orig_ids):
        score += 80

    # Patient/Encounter refs
    exp_subj = _get_ref(r_exp, "subject.reference")
    if exp_subj:
        orig_subj = _get_ref(r_orig, "subject.reference")
        if exp_subj == orig_subj:
            score += 30

    exp_enc = _get_ref(r_exp, "encounter.reference") or _get_ref(r_exp, "context.encounter.reference")
    if exp_enc:
        orig_enc = _get_ref(r_orig, "encounter.reference") or _get_ref(r_orig, "context.encounter.reference")
        if exp_enc == orig_enc:
            score += 30

    # Code signature
    exp_code = _get_code_signature(r_exp)
    orig_code = _get_code_signature(r_orig)
    if exp_code and exp_code == orig_code:
        score += 25

    # Time signature
    exp_time = _get_time_signature(r_exp)
    orig_time = _get_time_signature(r_orig)
    if exp_time and exp_time == orig_time:
        score += 15

    return score


def _diff_on_exported_fields(r_export: dict, r_original: dict) -> List[dict]:
    """
    Compare ONLY fields that exist in re-export resource:
    - flatten re-export
    - for each path, check exists in original and equals
    """
    flat_exp = _flatten_json({k: v for k, v in r_export.items() if k not in IGNORED_TOP_LEVEL_KEYS})
    flat_org = _flatten_json({k: v for k, v in r_original.items() if k not in IGNORED_TOP_LEVEL_KEYS})

    diffs = []
    for path, sent_val in flat_exp.items():
        # ignore noisy / transport-only keys if they appear inside
        if any(path.endswith("." + k) for k in IGNORED_ENTRY_KEYS):
            continue

        if path not in flat_org:
            diffs.append({
                "path": path,
                "sent": sent_val,
                "found": None,
                "status": "MISSING_IN_ORIGINAL",
            })
            continue

        found_val = flat_org[path]
        if sent_val != found_val:
            diffs.append({
                "path": path,
                "sent": sent_val,
                "found": found_val,
                "status": "VALUE_MISMATCH",
            })

    return diffs


def compare_fhir_bundles(
    fhir_original_path: str,
    fhir_reexport_path: str,
    *,
    min_match_score: int = 40,
    max_diffs_per_resource: int = 30,
    print_report: bool = True,
) -> dict:
    """
    Compare a Synthea original Bundle vs an EDS re-exported Bundle.

    Returns a dict report with:
      - counts per resourceType
      - matching success/fail
      - per-resource diffs (only on fields exported)
    """
    b0 = _load_bundle(fhir_original_path)
    b1 = _load_bundle(fhir_reexport_path)

    orig_resources = _iter_resources(b0)
    exp_resources = _iter_resources(b1)

    # index originals by type (for candidate search)
    orig_by_type: Dict[str, List[dict]] = {}
    for r in orig_resources:
        orig_by_type.setdefault(r["resourceType"], []).append(r)

    report = {
        "original_path": fhir_original_path,
        "reexport_path": fhir_reexport_path,
        "original_resource_count": len(orig_resources),
        "reexport_resource_count": len(exp_resources),
        "matched": 0,
        "unmatched": 0,
        "diff_resources": 0,
        "by_type": {},
        "items": [],  # per re-export resource
    }

    # quick stats by type
    def _count_by_type(resources: List[dict]) -> Dict[str, int]:
        d: Dict[str, int] = {}
        for r in resources:
            d[r["resourceType"]] = d.get(r["resourceType"], 0) + 1
        return d

    report["by_type"]["original"] = _count_by_type(orig_resources)
    report["by_type"]["reexport"] = _count_by_type(exp_resources)

    for r_exp in exp_resources:
        rtype = r_exp["resourceType"]
        candidates = orig_by_type.get(rtype, [])

        best = None
        best_score = -1
        for r_org in candidates:
            sc = _candidate_score(r_exp, r_org)
            if sc > best_score:
                best_score = sc
                best = r_org

        if best is None or best_score < min_match_score:
            report["unmatched"] += 1
            report["items"].append({
                "resourceType": rtype,
                "export_id": _norm_id(r_exp.get("id")),
                "status": "UNMATCHED",
                "best_score": best_score,
                "diffs": [],
            })
            continue

        diffs = _diff_on_exported_fields(r_exp, best)
        if len(diffs) > max_diffs_per_resource:
            diffs = diffs[:max_diffs_per_resource] + [{
                "path": "...",
                "sent": None,
                "found": None,
                "status": f"TRUNCATED (>{max_diffs_per_resource} diffs)"
            }]

        report["matched"] += 1
        if diffs:
            report["diff_resources"] += 1

        report["items"].append({
            "resourceType": rtype,
            "export_id": _norm_id(r_exp.get("id")),
            "original_id": _norm_id(best.get("id")),
            "match_score": best_score,
            "status": "OK" if not diffs else "DIFF",
            "diffs": diffs,
        })

    if print_report:
        print("=" * 100)
        print("FHIR BUNDLE COMPARISON (original vs re-export)")
        print("-" * 100)
        print(f"Original  : {fhir_original_path}")
        print(f"Re-export : {fhir_reexport_path}")
        print(f"Resources : original={report['original_resource_count']} reexport={report['reexport_resource_count']}")
        print(f"Matching  : matched={report['matched']} unmatched={report['unmatched']}")
        print(f"Diffs     : resources_with_diffs={report['diff_resources']}")
        print("-" * 100)
        print("By type (original):", report["by_type"]["original"])
        print("By type (reexport):", report["by_type"]["reexport"])
        print("=" * 100)

        # Print details only for diffs/unmatched
        for it in report["items"]:
            if it["status"] == "OK":
                continue

            print("\n" + "-" * 100)
            if it["status"] == "UNMATCHED":
                print(f"[UNMATCHED] {it['resourceType']} export.id={it['export_id']} best_score={it['best_score']}")
                continue

            print(f"[DIFF] {it['resourceType']} export.id={it.get('export_id')}  ~ original.id={it.get('original_id')} (score={it.get('match_score')})")
            for d in it["diffs"]:
                p = d["path"]
                print(f"  - {d['status']}: {p}")
                print(f"      sent : {d['sent']}")
                print(f"      found: {d['found']}")

        print("\n" + "=" * 100)
        print("END")

    return report




CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)


FHIR_S_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
FHIR_DIR = os.path.join(PROJECT_ROOT, "exports_eds_fhir")




# =============================================================================
# Script entry point
# =============================================================================
if __name__ == "__main__":
    report = compare_fhir_bundles(
        os.path.join(FHIR_S_DIR,"Alvaro283_Kemmer137_9f6e894f-3ee0-0c4a-050d-735e46feab23.json"),
        os.path.join(FHIR_DIR,"patient-9f6e894f-3ee0-0c4a-050d-735e46feab23.json"),
        min_match_score=40,
        print_report=True,
    )

    # Optionnel : sauvegarder le rapport complet
    with open("fhir_comparison_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
