from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from app.core.converters.edsan_to_fhir import export_eds_to_fhir
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.core.converters import fhir_to_edsan
from typing import List
import os
import polars as pl
from app.core.converters.build_eds_with_fhir import EDS_DIR, REPORTS_DIR
from app.utils.helpers import _fetch_bundle_all_pages, _collect_patient_ids, _zip_folder
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
from zipfile import ZipFile, ZIP_DEFLATED



load_dotenv()  # charge les variables du .env
router = APIRouter()


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

            per_patient.append({
                "patient_id": pid,
                "status": "success",
                "entries": len(bundle.get("entry", []) or []),
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

        report = {
            "run_id": run_id,
            "mode": "warehouse_one",
            "warehouse_url": FHIR_SERVER_URL,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "patient_id": pid,
            "entries": len(bundle.get("entry", []) or []),
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




# ===============================EDSAN  TO   FHIR==============================================

@router.post("/export/edsan-to-fhir-zip", tags=["Export"])
def edsan_to_fhir_zip():
    """
    Convertit EDSAN -> FHIR, génère les bundles JSON puis renvoie un ZIP.
    """
    try:
        tmpdir = Path(tempfile.mkdtemp(prefix="edsan_fhir_"))
        out_dir = tmpdir / "exports_eds_fhir"

        export_eds_to_fhir(
            eds_dir=None,            # => DEFAULT_EDS_DIR (eds/)
            output_dir=out_dir,      # écrit les JSON ici
            mapping_path=None,       # => mapping.json par défaut
            fhir_base_url=None,      # pas de push
            print_summary=False,
        )

        zip_path = tmpdir / "edsan_to_fhir.zip"
        _zip_folder(out_dir, zip_path)

        return FileResponse(
            path=str(zip_path),
            filename="edsan_to_fhir.zip",
            media_type="application/zip",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/export/edsan-to-fhir-warehouse", tags=["Export"])
def edsan_to_fhir_warehouse():
    """
    Convertit EDSAN -> FHIR puis pousse les bundles vers le serveur FHIR.
    """
    try:
        result = export_eds_to_fhir(
            eds_dir=None,
            output_dir=None,  # optionnel : mets un dossier si tu veux aussi garder les JSON
            mapping_path=None,
            fhir_base_url="http://localhost:8080/fhir",  # <-- mets ici l'URL réelle
            print_summary=False,
        )

        return {
            "message": "Push vers FHIR terminé",
            "summary": result.get("summary"),
            "push_results_keys": list(result.get("push_results", {}).keys()),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
