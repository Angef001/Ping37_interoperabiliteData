import base64
import json
import hashlib
import pandas as pd
from pathlib import Path
from app.utils.helpers import clean_id, format_fhir_date, write_last_run_report


# =============================================================================
# Utilitaires
# =============================================================================

def encode_base64(text: str | None) -> str | None:
    if text is None:
        return None
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def is_missing(x) -> bool:
    return x is None or pd.isna(x)


def stable_id(*parts: object) -> str:
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_fhir_id(raw) -> str:
    if is_missing(raw):
        return ""

    s = str(raw)

    # garder la partie après |
    if "|" in s:
        s = s.split("|")[-1]

    s = clean_id(s)

    # si encore "sale", on ignore
    if any(c in s for c in ["?", "=", "&", "/"]):
        return ""

    return s


def patient_ref(patid: str) -> str:
    return f"Patient/{patid}"


def encounter_ref(evtid: str) -> str:
    return f"Encounter/{evtid}"


def normalize_gender(patsex) -> str:
    if is_missing(patsex):
        return "unknown"

    s = str(patsex).strip().upper()

    if s == "M":
        return "male"
    if s == "F":
        return "female"

    return "unknown"


# =============================================================================
# Builders FHIR
# =============================================================================

def build_patient(row: pd.Series) -> dict:
    patid = normalize_fhir_id(row.get("PATID")) or str(row.get("PATID"))

    return {
        "resourceType": "Patient",
        "id": patid,
        "gender": normalize_gender(row.get("PATSEX")),
        "birthDate": format_fhir_date(row.get("PATBD")),
    }


def build_encounter(row: pd.Series) -> dict:
    evtid = normalize_fhir_id(row.get("EVTID")) or stable_id(
        row.get("PATID"), row.get("DATENT"), row.get("DATSORT")
    )

    patid = normalize_fhir_id(row.get("PATID")) or str(row.get("PATID"))

    return {
        "resourceType": "Encounter",
        "id": evtid,
        "status": "finished",
        "subject": {"reference": patient_ref(patid)},
        "period": {
            "start": format_fhir_date(row.get("DATENT")),
            "end": format_fhir_date(row.get("DATSORT")),
        },
    }


def build_observation(row: pd.Series) -> dict:
    elt_clean = normalize_fhir_id(row.get("ELTID"))
    obs_id = elt_clean if elt_clean else stable_id(
        row.get("PATID"),
        row.get("EVTID"),
        row.get("LOINC"),
        row.get("PRLVTDATE"),
        row.get("RESULT"),
        row.get("UNIT"),
    )

    patid = normalize_fhir_id(row.get("PATID")) or str(row.get("PATID"))
    evtid = normalize_fhir_id(row.get("EVTID")) or str(row.get("EVTID"))

    obs = {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "subject": {"reference": patient_ref(patid)},
        "encounter": {"reference": encounter_ref(evtid)},
        "effectiveDateTime": format_fhir_date(row.get("PRLVTDATE")),
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": row.get("LOINC"),
                "display": row.get("RNAME"),
            }],
            "text": row.get("PNAME"),
        },
    }

    result = row.get("RESULT")
    unit = row.get("UNIT")

    if not is_missing(result):
        vq = {"value": float(result)}
        if not is_missing(unit):
            u = str(unit)
            vq["unit"] = u
            vq["system"] = "http://unitsofmeasure.org"
            vq["code"] = u
        obs["valueQuantity"] = vq

    minref = row.get("MINREF")
    maxref = row.get("MAXREF")
    if not is_missing(minref) or not is_missing(maxref):
        rr = {}
        if not is_missing(minref):
            rr["low"] = {"value": float(minref)}
        if not is_missing(maxref):
            rr["high"] = {"value": float(maxref)}
        obs["referenceRange"] = [rr]

    issued = row.get("VALIDADATE")
    if not is_missing(issued):
        obs["issued"] = format_fhir_date(issued)

    return obs


def build_document_reference(row: pd.Series) -> dict:
    elt_clean = normalize_fhir_id(row.get("ELTID"))
    doc_id = elt_clean if elt_clean else stable_id(
        row.get("PATID"), row.get("EVTID"), row.get("RECDATE")
    )

    patid = normalize_fhir_id(row.get("PATID")) or str(row.get("PATID"))
    evtid = normalize_fhir_id(row.get("EVTID")) or str(row.get("EVTID"))

    attachment = {
        "contentType": "text/plain",
        "creation": format_fhir_date(row.get("RECDATE")),
    }

    txt = row.get("RECTXT")
    if not is_missing(txt):
        attachment["data"] = encode_base64(txt)

    return {
        "resourceType": "DocumentReference",
        "id": doc_id,
        "status": "current",
        "subject": {"reference": patient_ref(patid)},
        "context": {"encounter": [{"reference": encounter_ref(evtid)}]},
        "content": [{"attachment": attachment}],
    }


# =============================================================================
# Bundle
# =============================================================================

def build_bundle(resources: list[dict], bundle_id: str) -> dict:
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "id": bundle_id,
        "entry": [{"resource": r} for r in resources],
    }


# =============================================================================
# Export principal
# =============================================================================

def export_eds_to_fhir(
    eds_dir: str,
    output_dir: str,
    bundle_strategy: str = "patient",  # "patient" ou "encounter"
) -> dict:

    eds_dir = Path(eds_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    patient_df = pd.read_parquet(eds_dir / "patient.parquet")
    mvt_df = pd.read_parquet(eds_dir / "mvt.parquet")
    biol_df = pd.read_parquet(eds_dir / "biol.parquet")
    doc_df = pd.read_parquet(eds_dir / "doceds.parquet")

    patients = {row.PATID: build_patient(row) for _, row in patient_df.iterrows()}
    encounters = {row.EVTID: build_encounter(row) for _, row in mvt_df.iterrows()}
    observations = [build_observation(row) for _, row in biol_df.iterrows()]
    documents = [build_document_reference(row) for _, row in doc_df.iterrows()]

    bundles = {}

    if bundle_strategy == "patient":
        for pid, patient in patients.items():
            pid_norm = patient["id"]
            resources = [patient]

            resources += [e for e in encounters.values() if e["subject"]["reference"] == patient_ref(pid_norm)]
            resources += [o for o in observations if o["subject"]["reference"] == patient_ref(pid_norm)]
            resources += [d for d in documents if d["subject"]["reference"] == patient_ref(pid_norm)]

            bid = f"patient-{pid_norm}"
            bundles[bid] = build_bundle(resources, bid)

    else:
        for evtid, encounter in encounters.items():
            pid_norm = encounter["subject"]["reference"].split("/")[-1]
            evtid_norm = encounter["id"]

            resources = [patients[pid_norm], encounter]
            resources += [o for o in observations if o["encounter"]["reference"] == encounter_ref(evtid_norm)]
            resources += [
                d for d in documents
                if encounter_ref(evtid_norm) in [e["reference"] for e in d["context"]["encounter"]]
            ]

            bid = f"encounter-{evtid_norm}"
            bundles[bid] = build_bundle(resources, bid)

    for bundle in bundles.values():
        with open(output_dir / f"{bundle['id']}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, ensure_ascii=False)

    summary = {
        "eds_dir": str(eds_dir),
        "output_dir": str(output_dir),
        "bundle_strategy": bundle_strategy,
        "bundles_generated": len(bundles),
    }

    write_last_run_report(summary, str(output_dir), filename="last_run.json")

    return summary
