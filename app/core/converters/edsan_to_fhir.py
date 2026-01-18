import base64
import json
import tempfile
from pathlib import Path
import pandas as pd
import hashlib

# -----------------------------
# Utilitaires
# -----------------------------

def encode_base64(text: str | None) -> str | None:
    """Encode un texte en base64 (FHIR strict)."""
    if text is None:
        return None
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def is_missing(x) -> bool:
    """True si valeur manquante (None, NaN, NaT...)."""
    return x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x)

def stable_id(*parts: object) -> str:
    """Génère un id déterministe (hash) à partir de champs stables."""
    s = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()



def patient_ref(patid: str) -> str:
    return f"Patient/{patid}"


def encounter_ref(evtid: str) -> str:
    return f"Encounter/{evtid}"


# -----------------------------
# Builders de ressources FHIR
# -----------------------------

def build_patient(row: pd.Series) -> dict:
    return {
        "resourceType": "Patient",
        "id": str(row["PATID"]),
        "gender": row["PATSEX"],
        "birthDate": row["PATBD"]
    }


def build_encounter(row: pd.Series) -> dict:
    return {
        "resourceType": "Encounter",
        "id": str(row["EVTID"]),
        "subject": {"reference": patient_ref(row["PATID"])},
        "period": {
            "start": row["DATENT"],
            "end": row["DATSORT"]
        }
    }


def build_observation(row: pd.Series) -> dict:
    # 1) ID stable : on préfère ELTID si présent et "propre", sinon hash déterministe
    elt = row.get("ELTID")

    # Si ELTID est présent mais "sale" (contient une requête), on le nettoie
    if not is_missing(elt):
        elt_str = str(elt)

        # Cas fréquent : "...|<uuid>" -> on garde la partie après le dernier "|"
        if "|" in elt_str:
            elt_str = elt_str.split("|")[-1]

        # Si ça contient encore des caractères typiques d'une requête, on n'utilise pas
        if any(ch in elt_str for ch in ["?", "=", "&", "/"]):
            elt_str = ""

        obs_id = elt_str if elt_str else stable_id(
            row.get("PATID"),
            row.get("EVTID"),
            row.get("LOINC"),
            row.get("PRLVTDATE"),
            row.get("RESULT"),
            row.get("UNIT"),
        )
    else:
        obs_id = stable_id(
            row.get("PATID"),
            row.get("EVTID"),
            row.get("LOINC"),
            row.get("PRLVTDATE"),
            row.get("RESULT"),
            row.get("UNIT"),
        )


    obs = {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "subject": {"reference": patient_ref(row["PATID"])},
        "encounter": {"reference": encounter_ref(row["EVTID"])},
        "effectiveDateTime": row.get("PRLVTDATE"),
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": row.get("LOINC"),
                "display": row.get("RNAME")
            }],
            "text": row.get("PNAME")
        },
    }

    # 2) Gérer RESULT/UNIT : si RESULT est manquant -> on n'écrit pas valueQuantity
    result = row.get("RESULT")
    unit = row.get("UNIT")

    if not is_missing(result):
        # si result est un nombre (ou convertible), valueQuantity OK
        obs["valueQuantity"] = {"value": float(result)}
        if not is_missing(unit):
            obs["valueQuantity"]["unit"] = str(unit)
    # sinon: pas de value[x] (FHIR valide)

    # 3) ReferenceRange seulement si min/max présents (évite NaN)
    minref = row.get("MINREF")
    maxref = row.get("MAXREF")
    if (not is_missing(minref)) or (not is_missing(maxref)):
        rr = {}
        if not is_missing(minref):
            rr["low"] = {"value": float(minref)}
        if not is_missing(maxref):
            rr["high"] = {"value": float(maxref)}
        obs["referenceRange"] = [rr]

    # 4) issued seulement si présent
    issued = row.get("VALIDADATE")
    if not is_missing(issued):
        obs["issued"] = issued

    return obs



def build_document_reference(row: pd.Series) -> dict:
    return {
        "resourceType": "DocumentReference",
        "id": str(row["ELTID"]),
        "subject": {"reference": patient_ref(row["PATID"])},
        "context": {
            "encounter": [{"reference": encounter_ref(row["EVTID"])}]
        },
        "content": [{
            "attachment": {
                "contentType": "text/plain",
                "data": encode_base64(row["RECTXT"]),
                "creation": row["RECDATE"]
            }
        }]
    }


# -----------------------------
# Bundle builder
# -----------------------------

def build_bundle(resources: list[dict], bundle_id: str) -> dict:
    return {
        "resourceType": "Bundle",
        "type": "collection",
        "id": bundle_id,
        "entry": [{"resource": r} for r in resources]
    }


# -----------------------------
# Pipeline principal
# -----------------------------

def export_eds_to_fhir(
    eds_dir: str,
    output_dir: str,
    bundle_strategy: str = "patient"  # "patient" ou "encounter"
):
    """
    Export EDS -> FHIR R5 strict
    bundle_strategy : patient | encounter
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Lecture des tables EDS
    patient_df = pd.read_parquet(Path(eds_dir) / "patient.parquet")
    mvt_df = pd.read_parquet(Path(eds_dir) / "mvt.parquet")
    biol_df = pd.read_parquet(Path(eds_dir) / "biol.parquet")
    doc_df = pd.read_parquet(Path(eds_dir) / "doceds.parquet")

    # Construction des ressources
    patients = {row.PATID: build_patient(row) for _, row in patient_df.iterrows()}
    encounters = {row.EVTID: build_encounter(row) for _, row in mvt_df.iterrows()}
    observations = [build_observation(row) for _, row in biol_df.iterrows()]
    documents = [build_document_reference(row) for _, row in doc_df.iterrows()]

    # Regroupement
    if bundle_strategy == "patient":
        bundles = {}

        for pid, patient in patients.items():
            resources = [patient]

            resources += [
                e for e in encounters.values()
                if e["subject"]["reference"] == patient_ref(pid)
            ]

            resources += [
                o for o in observations
                if o["subject"]["reference"] == patient_ref(pid)
            ]

            resources += [
                d for d in documents
                if d["subject"]["reference"] == patient_ref(pid)
            ]

            bundles[pid] = build_bundle(resources, bundle_id=f"patient-{pid}")

    else:  # bundle par encounter
        bundles = {}

        for evtid, encounter in encounters.items():
            pid = encounter["subject"]["reference"].split("/")[-1]

            resources = [
                patients[pid],
                encounter
            ]

            resources += [
                o for o in observations
                if o["encounter"]["reference"] == encounter_ref(evtid)
            ]

            resources += [
                d for d in documents
                if encounter_ref(evtid) in
                [e["reference"] for e in d["context"]["encounter"]]
            ]

            bundles[evtid] = build_bundle(resources, bundle_id=f"encounter-{evtid}")

    # Écriture
    for key, bundle in bundles.items():
        with open(output_dir / f"{bundle['id']}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2, ensure_ascii=False)
