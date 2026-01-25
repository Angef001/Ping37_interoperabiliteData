from __future__ import annotations

import json
import base64
import hashlib
import io, zipfile, tempfile
from pathlib import Path
from typing import Any
import requests
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

def load_mapping(mapping_path: str | Path) -> dict:
    """
    Charge le mapping EDS → FHIR depuis un fichier JSON
    """
    mapping_path = Path(mapping_path)

    if not mapping_path.exists():
        raise FileNotFoundError(f"Mapping introuvable : {mapping_path}")

    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)


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

def build_transaction_bundle(resources: list[dict], bundle_id: str) -> dict:
    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "id": bundle_id,
        "entry": [
            {
                "resource": r,
                "request": {
                    "method": "PUT",
                    "url": f"{r['resourceType']}/{r['id']}",
                },
            }
            for r in resources
        ],
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

from pathlib import Path
import json

def export_eds_to_fhir(
    eds_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    mapping_path: str | Path | None = None,
    bundle_strategy: str = "patient",
    print_summary: bool = True,
    fhir_base_url: str | None = None,
) -> dict:
    """
    Convertit les données EDS en FHIR et :
    - soit les écrit dans output_dir (debug)
    - soit les pousse dans un entrepôt FHIR (transaction)
    """
    if mapping_path is None:
        mapping_path = DEFAULT_MAPPING_PATH
    eds_dir = Path(eds_dir) if eds_dir else None
    output_dir = Path(output_dir) if output_dir else None

    if not eds_dir:
        raise ValueError("eds_dir est obligatoire")

    if not mapping_path:
        raise ValueError("mapping_path est obligatoire")

    # 1️⃣ Charger le mapping
    mapping = load_mapping(mapping_path)

    # 2️⃣ Lire + convertir les données EDS → ressources FHIR
    # (fonction que tu as déjà)
    bundles_by_id = export_eds_to_fhir(
        eds_dir=eds_dir,
        mapping_path=mapping,
        bundle_strategy=bundle_strategy,
    )

    results = {}

    # 3️⃣ Pour chaque bundle logique (patient / séjour)
    for bundle_id, resources in bundles_by_id.items():

        # 👉 construire un bundle TRANSACTION
        bundle = build_transaction_bundle(
            resources=resources,
            bundle_id=bundle_id,
        )

        # --- OPTION DEBUG : écriture locale ---
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            out_path = output_dir / f"{bundle_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(bundle, f, indent=2, ensure_ascii=False)

        # --- OPTION NORMALE : envoi vers FHIR ---
        if fhir_base_url:
            response = push_bundle_to_fhir(
                bundle=bundle,
                fhir_base_url=fhir_base_url,
            )
            results[bundle_id] = response
        else:
            results[bundle_id] = bundle

    # 4️⃣ Résumé console
    if print_summary:
        print(f"✔ {len(results)} bundle(s) traité(s)")
        if fhir_base_url:
            print(f"✔ Bundles envoyés vers {fhir_base_url}")
        if output_dir:
            print(f"✔ Bundles écrits dans {output_dir}")

    return results


def push_bundle_to_fhir(bundle: dict, fhir_base_url: str) -> dict:
    """
    Envoie un Bundle FHIR transaction vers l'entrepôt FHIR
    """
    headers = {
        "Content-Type": "application/fhir+json"
    }

    resp = requests.post(
        fhir_base_url.rstrip("/"),
        json=bundle,
        headers=headers,
        timeout=30,
    )

    if not resp.ok:
        raise RuntimeError(
            f"Erreur FHIR {resp.status_code}: {resp.text}"
        )

    return resp.json()

def build_transaction_bundle(resources: list[dict], bundle_id: str) -> dict:
    """
    Bundle transaction (pour charger dans un serveur FHIR)
    """
    entries = []
    for r in resources:
        rid = r.get("id")
        rtype = r.get("resourceType")
        if not rid or not rtype:
            # on ignore les ressources mal formées
            continue

        entries.append({
            "resource": r,
            "request": {
                "method": "PUT",
                "url": f"{rtype}/{rid}"   # upsert
            }
        })

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "id": bundle_id,
        "entry": entries,
    }


def push_bundle_to_fhir(bundle: dict, fhir_base_url: str) -> dict:
    """
    POST un transaction bundle vers l'entrepôt FHIR (ex: http://localhost:8080/fhir)
    """
    resp = requests.post(
        fhir_base_url.rstrip("/"),
        json=bundle,
        headers={"Content-Type": "application/fhir+json"},
        timeout=60,
    )

    if not resp.ok:
        raise RuntimeError(f"FHIR {resp.status_code}: {resp.text[:500]}")

    return resp.json()

def make_location_stub(location_id: str, name: str | None = None) -> dict:
    return {
        "resourceType": "Location",
        "id": location_id,
        "name": name or location_id,
        "identifier": [{
            "system": "urn:eds:eltid",
            "value": location_id
        }]
    }

def ensure_locations_in_transaction_bundle(bundle: dict) -> dict:
    """
    Ajoute des Location minimalistes (stub) pour toutes les Encounter.location.reference
    afin d'éviter les 400 'Location not found'.
    """
    if bundle.get("resourceType") != "Bundle" or bundle.get("type") != "transaction":
        return bundle

    existing_location_ids: set[str] = set()
    for e in bundle.get("entry", []):
        r = (e or {}).get("resource") or {}
        if r.get("resourceType") == "Location" and r.get("id"):
            existing_location_ids.add(r["id"])

    to_add: list[dict] = []
    for e in bundle.get("entry", []):
        r = (e or {}).get("resource") or {}
        if r.get("resourceType") != "Encounter":
            continue

        locs = r.get("location") or []
        if not isinstance(locs, list) or not locs:
            continue

        # On prend le premier (ton mapping met [0])
        loc_ref = ((locs[0] or {}).get("location") or {}).get("reference")
        loc_name = ((locs[0] or {}).get("location") or {}).get("display")  # SEJUF chez toi

        if isinstance(loc_ref, str) and loc_ref.startswith("Location/"):
            lid = loc_ref.split("/", 1)[1].strip()
            if lid and lid not in existing_location_ids:
                existing_location_ids.add(lid)
                loc_res = make_location_stub(lid, loc_name)
                to_add.append({
                    "resource": loc_res,
                    "request": {"method": "PUT", "url": f"Location/{lid}"}
                })

    if to_add:
        # IMPORTANT: on met les Location au début (plus tolérant côté HAPI)
        bundle["entry"] = to_add + (bundle.get("entry") or [])

    return bundle


def export_eds_to_fhir(
    eds_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    mapping_path: str | Path | None = None,
    bundle_strategy: str = "patient",
    print_summary: bool = True,
    fhir_base_url: str | None = None,
) -> dict:
    eds_dir = Path(eds_dir) if eds_dir is not None else DEFAULT_EDS_DIR
    out_dir = Path(output_dir) if output_dir is not None else None
    mapping_path = Path(mapping_path) if mapping_path is not None else DEFAULT_MAPPING_PATH

    if not eds_dir.exists():
        raise FileNotFoundError(f"EDS_DIR introuvable: {eds_dir}")

    if not mapping_path.exists():
        raise FileNotFoundError(f"mapping.json introuvable: {mapping_path}")

    if out_dir is None and fhir_base_url is None:
        raise ValueError("Il faut soit output_dir (debug) soit fhir_base_url (push vers entrepôt FHIR).")

    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)

    # Charger mapping
    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    all_resources: list[dict] = []
    by_type: dict[str, list[dict]] = {}

    # Lire parquets + construire ressources
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

    # Grouper -> bundles
    bundles: dict[str, dict] = {}

    if bundle_strategy == "patient":
        grouped: dict[str, list[dict]] = {}
     
        for r in all_resources:
            # CAS 1 : ressource Patient → clé = son propre id
            if r.get("resourceType") == "Patient" and r.get("id"):
                grouped.setdefault(r["id"], []).append(r)
                continue

            # CAS 2 : autres ressources → via subject.reference
            pid = get_patient_id(r)
            if pid:
                grouped.setdefault(pid, []).append(r)

        for pid, resources in grouped.items():
            bid = f"patient-{pid}"
            bundles[bid] = build_transaction_bundle(resources, bid)


    elif bundle_strategy == "encounter":
        grouped: dict[str, list[dict]] = {}
        for r in all_resources:
            eid = get_encounter_id(r)
            if eid:
                grouped.setdefault(eid, []).append(r)

        for eid, resources in grouped.items():
            bid = f"encounter-{eid}"
            bundles[bid] = build_transaction_bundle(resources, bid)

    else:
        raise ValueError('bundle_strategy doit être "patient" ou "encounter"')

    # Sorties : fichier (debug) et/ou push vers FHIR
    push_results: dict[str, Any] = {}

    for bid, bundle in bundles.items():
        if out_dir is not None:
            with open(out_dir / f"{bid}.json", "w", encoding="utf-8") as f:
                json.dump(bundle, f, indent=2, ensure_ascii=False)

        if fhir_base_url is not None:
            for bid, bundle in bundles.items():
                bundle = ensure_locations_in_transaction_bundle(bundle)

            if out_dir is not None:
                with open(out_dir / f"{bid}.json", "w", encoding="utf-8") as f:
                    json.dump(bundle, f, indent=2, ensure_ascii=False)

            if fhir_base_url is not None:
                push_results[bid] = push_bundle_to_fhir(bundle, fhir_base_url)

    summary = {
        "eds_dir": str(eds_dir),
        "output_dir": str(out_dir) if out_dir else None,
        "mapping": str(mapping_path),
        "bundle_strategy": bundle_strategy,
        "resources_per_type": {k: len(v) for k, v in by_type.items()},
        "bundles_generated": len(bundles),
        "pushed_to_fhir": fhir_base_url is not None,
        "fhir_base_url": fhir_base_url,
    }

    if print_summary:
        print(json.dumps(summary, indent=2, ensure_ascii=False))

    # garder ton report si tu veux
    if out_dir is not None:
        write_last_run_report(summary, str(out_dir), "last_run.json")

    return {"summary": summary, "push_results": push_results}


# ==============================================================================
# Script entry point
# =============================================================================

#if __name__ == "__main__":
    export_eds_to_fhir()

if __name__ == "__main__":
    export_eds_to_fhir(
        eds_dir="eds",
        mapping_path = DEFAULT_MAPPING_PATH,
        output_dir="exports_eds_fhir",
        fhir_base_url="http://localhost:8080/fhir"

    )
