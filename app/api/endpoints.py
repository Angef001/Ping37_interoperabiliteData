from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from app.core.converters import edsan_to_fhir_1
<<<<<<< HEAD
=======
from collections import Counter

>>>>>>> f6b9c3fd1ec4073542d00a328907424560b70ef7
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.core.converters import fhir_to_edsan
from typing import List
import os
import polars as pl
from app.core.converters.build_eds_with_fhir import EDS_DIR,REPORTS_DIR

import json
from fastapi.responses import FileResponse, HTMLResponse
import zipfile
import tempfile
from pathlib import Path
import shutil
from dotenv import load_dotenv
import os
import requests
from datetime import datetime


load_dotenv()  # charge le .env
router = APIRouter()



# --- ENDPOINT : EDS -> FHIR ---
@router.post("/export/edsan-to-fhir-zip", tags=["Export"])
async def export_edsan_to_fhir_zip():
    # Read the bundle strategy from environment variables
    # Determines how FHIR Bundles are built (by patient or by encounter)
    bundle_strategy = os.getenv("FHIR_BUNDLE_STRATEGY", "patient")

    # Validate the strategy to avoid invalid conversion behavior
    if bundle_strategy not in ("patient", "encounter"):
        raise HTTPException(
            status_code=500,
            detail="FHIR_BUNDLE_STRATEGY must be 'patient' or 'encounter'"
        )

    # Determine the project root directory based on the EDS directory
    project_root = Path(EDS_DIR).resolve().parent

    # Define the FHIR output directory (from .env or default)
    out_dir = Path(os.getenv("FHIR_OUTPUT_DIR", "fhir_output"))

    # If the output path is relative, attach it to the project root
    if not out_dir.is_absolute():
        out_dir = project_root / out_dir

    # Remove any previous export to ensure a clean output
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Convert EDS parquet data into FHIR JSON bundles
    try:
        edsan_to_fhir_1.export_eds_to_fhir(
            eds_dir=EDS_DIR,
            output_dir=str(out_dir),
            bundle_strategy=bundle_strategy,
        )
    except Exception as e:
        # Return an HTTP 500 error if the conversion fails
        raise HTTPException(status_code=500, detail=str(e))

    # Create a ZIP archive containing all generated FHIR JSON files
    zip_path = out_dir / "fhir_export.zip"
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in out_dir.glob("*.json"):
            z.write(p, arcname=p.name)

    # Return the ZIP file as a downloadable response
    return FileResponse(
        path=str(zip_path),
        filename="fhir_export.zip",
        media_type="application/zip",
    )


@router.get("/ui/export/fhir", response_class=HTMLResponse)
async def ui_export_fhir(request: Request):
    # Serve a simple HTML interface to trigger or visualize the FHIR export
    return tempfile.template.TemplateResponse("export_fhir.html", {"request": request})





# --- ENDPOINT : FHIR (dossier) -> EDS ---
@router.post("/convert/fhir-dir-to-edsan", tags=["Conversion"])
async def convert_fhir_dir_to_edsan(payload: dict | None = None):
    """
    Déclenche la conversion FHIR -> EDS sur un dossier.
    - Si payload est None, utilise le dossier par défaut (synthea/output/fhir).
    - Sinon payload peut contenir: {"fhir_dir": "chemin/vers/dossier"}
    """
    try:
        fhir_dir = None
        if payload:
            fhir_dir = payload.get("fhir_dir")  # optionnel
        result = fhir_to_edsan.process_dir(fhir_dir=fhir_dir)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur de conversion dossier : {str(e)}")


# --- ENDPOINTS : Consultation EDS (Parquet) ---
@router.get("/eds/tables", tags=["EDS"])
async def list_eds_tables():
    """
    Liste les fichiers .parquet disponibles dans le dossier eds/
    (on masque patient.parquet car ce n'est pas un module EDSaN dans la figure)
    """
    if not os.path.isdir(EDS_DIR):
        raise HTTPException(status_code=404, detail=f"Dossier EDS introuvable: {EDS_DIR}")

    tables = sorted([f for f in os.listdir(EDS_DIR) if f.endswith(".parquet")])
    tables = [t for t in tables if t != "patient.parquet"]  # garder patient interne
    return tables



@router.get("/eds/table/{name}", tags=["EDS"])
async def read_eds_table(name: str, limit: int = 50):
    """
    Retourne un aperçu (head) d'une table parquet.
    - name: ex "patient.parquet" (si tu passes "patient", on ajoute .parquet)
    """
    if not name.endswith(".parquet"):
        name = f"{name}.parquet"

    path = os.path.join(EDS_DIR, name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Table introuvable: {name}")

    df = pl.read_parquet(path)
    return {
        "table": name,
        "rows": df.height,
        "cols": df.width,
        "preview": df.head(limit).to_dicts()
    }


@router.post("/import/fhir-file", tags=["Import"])
async def import_fhir_file(file: UploadFile = File(...)):
    """
    Upload d'un fichier Bundle FHIR (.json) puis conversion FHIR -> EDS (Parquet).
    """
    try:
        raw = await file.read()
        bundle = json.loads(raw.decode("utf-8"))
        result = fhir_to_edsan.process_bundle(bundle)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur import fichier FHIR : {str(e)}")
    


@router.get("/export/eds-zip", tags=["Export"])
async def export_eds_zip():
    """
    Exporte les 5 modules EDSaN (sans patient.parquet) en un fichier ZIP téléchargeable.
    """
    if not os.path.isdir(EDS_DIR):
        raise HTTPException(status_code=404, detail=f"Dossier EDS introuvable: {EDS_DIR}")

    # 5 modules attendus (alignés figure EDSaN)
    files = ["mvt.parquet", "biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]

    missing = [f for f in files if not os.path.exists(os.path.join(EDS_DIR, f))]
    if missing:
        raise HTTPException(status_code=404, detail=f"Fichiers manquants dans EDS: {missing}")

    # Crée un zip temporaire
    tmp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(tmp_dir, "eds_export.zip")

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in files:
            full_path = os.path.join(EDS_DIR, f)
            z.write(full_path, arcname=f)

    return FileResponse(zip_path, filename="eds_export.zip", media_type="application/zip")

@router.get("/report/last-run", tags=["Report"])
async def get_last_run_report():
    """
    Retourne le dernier rapport de run (report/last_run.json) généré par process_dir/process_bundle.
    """
    report_path = os.path.join(REPORTS_DIR, "last_run.json")
    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Aucun rapport disponible (last_run.json introuvable).")

    with open(report_path, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/stats", tags=["Report"])
async def get_stats():
    """
    Statistiques rapides sur les parquets EDS (rows/cols par table).
    """
    if not os.path.isdir(EDS_DIR):
        raise HTTPException(status_code=404, detail=f"Dossier EDS introuvable: {EDS_DIR}")

    tables = sorted([f for f in os.listdir(EDS_DIR) if f.endswith(".parquet")])
    tables = [t for t in tables if t != "patient.parquet"]

    stats = {}
    for t in tables:
        path = os.path.join(EDS_DIR, t)
        lf = pl.scan_parquet(path)
        rows = lf.select(pl.len()).collect().item()
        cols = len(lf.columns)
        stats[t] = {"rows": rows, "cols": cols}

    # si report existe, on le renvoie aussi (pratique en démo)
    report_path = os.path.join(REPORTS_DIR, "last_run.json")
    last_run = None
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            last_run = json.load(f)

    return {"report_dir": REPORTS_DIR, "tables": stats, "last_run": last_run}






FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")
FHIR_ACCEPT_HEADERS = {"Accept": "application/fhir+json"}


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

@router.post("/convert/fhir-warehouse-to-edsan", tags=["Conversion"])
async def convert_fhir_warehouse_to_edsan(payload: dict | None = None):
    """
    Equivalent "convert dir" mais depuis l’entrepôt FHIR (HAPI).
    payload optionnel:
      - patient_limit (int): nb de patients à convertir (par défaut 50)
        * si patient_limit = 0 => convertit TOUS les patients (attention lourd)
      - page_size (int): _count utilisé pour pagination (par défaut 100)
    """
    patient_limit = 50
    page_size = 100
    if payload:
        patient_limit = int(payload.get("patient_limit", patient_limit))
        page_size = int(payload.get("page_size", page_size))

    # 1) récupérer les IDs de patients
    try:
        patients_bundle = _fetch_bundle_all_pages(
            f"{FHIR_SERVER_URL}/Patient",
            params={"_count": page_size}
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur accès entrepôt FHIR: {str(e)}")

    patient_ids = []
    for entry in patients_bundle.get("entry", []) or []:
        res = entry.get("resource", {})
        if res.get("resourceType") == "Patient" and res.get("id"):
            patient_ids.append(res["id"])

    if not patient_ids:
        raise HTTPException(status_code=404, detail="Aucun Patient dans l'entrepôt FHIR.")

    try:
        patient_ids = _collect_patient_ids(patient_limit, page_size)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur accès entrepôt FHIR: {str(e)}")

    if not patient_ids:
        raise HTTPException(status_code=404, detail="Aucun Patient dans l'entrepôt FHIR.")


    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    per_patient = []
    ok = 0
    ko = 0

    # 2) Pour chaque patient : Patient/{id}/$everything -> Bundle patient complet -> process_bundle
    for pid in patient_ids:
        try:
            everything_url = f"{FHIR_SERVER_URL}/Patient/{pid}/$everything"
            bundle = _fetch_bundle_all_pages(everything_url, params={"_count": page_size})

            # IMPORTANT: on évite d’écrire last_run à chaque patient
            _ = fhir_to_edsan.process_bundle(bundle, write_report=False)

            summary = summarize_bundle(bundle)

            per_patient.append({
                "patient_id": pid,
                "status": "success",
                "entries_total": summary["entries_total"],        # total entries
                "resources_per_type": summary["resources_per_type"],  # détail par type
            })

            ok += 1
        except Exception as e:
            per_patient.append({
                "patient_id": pid,
                "status": "failed",
                "error": str(e),
            })
            ko += 1

    ended_at = datetime.now().isoformat()

    # 3) Report GLOBAL (celui que tu veux: OK/KO, erreurs, traces)
    report = {
        "run_id": run_id,
        "mode": "warehouse_all",
        "warehouse_url": FHIR_SERVER_URL,
        "patient_limit": patient_limit,
        "page_size": page_size,
        "started_at": started_at,
        "ended_at": ended_at,
        "patients_total": len(patient_ids),
        "patients_success": ok,
        "patients_failed": ko,
        "patients": per_patient,
    }

    # archive + last_run.json (historique)
    from app.utils.helpers import write_last_run_report
    write_last_run_report(report, REPORTS_DIR)

    return {"status": "success", "data": report}


@router.post("/convert/fhir-warehouse-patient-to-edsan", tags=["Conversion"])
async def convert_one_patient_from_warehouse(payload: dict):
    """
    Equivalent "1 fichier patient Synthea" mais depuis l’entrepôt.
    payload: {"patient_id": "..."}
    """
    pid = payload.get("patient_id")
    if not pid:
        raise HTTPException(status_code=400, detail="patient_id requis.")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    try:
        everything_url = f"{FHIR_SERVER_URL}/Patient/{pid}/$everything"
        bundle = _fetch_bundle_all_pages(everything_url, params={"_count": 200})

        _ = fhir_to_edsan.process_bundle(bundle, write_report=False)

        summary = summarize_bundle(bundle)

        report = {
            "run_id": run_id,
            "mode": "warehouse_one",
            "warehouse_url": FHIR_SERVER_URL,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "patient_id": pid,
            "entries_total": summary["entries_total"],
            "resources_per_type": summary["resources_per_type"],
        }

        from app.utils.helpers import write_last_run_report
        write_last_run_report(report, REPORTS_DIR)

        return {"status": "success", "data": report}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur conversion patient {pid}: {str(e)}")


@router.get("/report/runs", tags=["Report"])
async def list_runs():
    """
    Liste l'historique des runs (archives).
    """
    runs_dir = Path(REPORTS_DIR) / "runs"
    if not runs_dir.exists():
        return []
    files = sorted(runs_dir.glob("last_run_*.json"), reverse=True)
    return [{"name": f.name, "size": f.stat().st_size} for f in files]


@router.get("/report/run/{name}", tags=["Report"])
async def download_run(name: str):
    """
    Télécharge un run archivé.
    """
    if ".." in name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Nom invalide.")
    path = Path(REPORTS_DIR) / "runs" / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Run introuvable.")
    return FileResponse(str(path), filename=name, media_type="application/json")
