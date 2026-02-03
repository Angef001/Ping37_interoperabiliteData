from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from app.core.converters.edsan_to_fhir import export_eds_to_fhir
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.utils.helpers import snapshot_eds_counts, build_merge_report

from app.core.converters import fhir_to_edsan
from typing import List
import os
import polars as pl
from app.core.converters.fhir_to_edsan import EDS_DIR, REPORTS_DIR, REPORTS_DIR_EXPORT
from app.utils.helpers import _fetch_bundle_all_pages, _collect_patient_ids,summarize_bundle, _zip_folder
import json
from fastapi.responses import FileResponse, HTMLResponse
import tempfile
from pathlib import Path
import shutil
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
import zipfile
 
 
 
load_dotenv()  # charge les variables du .env
router = APIRouter()
 
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")
FHIR_ACCEPT_HEADERS = {"Accept": "application/fhir+json"}
REPORTS_DIR_EXPORT_PATH = Path(os.getenv("REPORTS_DIR_EXPORT", REPORTS_DIR_EXPORT))
EDS_DIR = Path(os.getenv("EDS_DIR", EDS_DIR))
EDS_DIR_CONV = Path(os.getenv("EDS_DIR_conv", EDS_DIR))  # fallback


#                --- ENDPOINT : FHIR (ENTREPOT) -> EDS ---


     # - patient_limit (int): nb de patients à convertir (par défaut sa convertir tout l'entrepot)
      #  * si patient_limit = 0 => convertit TOUS les patients (attention lourd)
      #- page_size (int): _count utilisé pour pagination (par défaut 100)
    
@router.post("/convert/fhir-warehouse-to-edsan", tags=["Conversion"])
async def convert_fhir_warehouse_to_edsan(payload: dict | None = None):

    patient_limit = 0
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

    # ✅ tables suivies (EDS conversion)
    tracked_tables = ["biol.parquet", "doceds.parquet", "mvt.parquet", "pharma.parquet", "pmsi.parquet"]

    # ✅ snapshot global AVANT conversion (dans EDS_DIR_CONV)
    before_global = snapshot_eds_counts(EDS_DIR_CONV, tracked_tables)

    # ✅ accumulateur incoming (candidats traités)
    incoming_acc = {t: 0 for t in tracked_tables}

    # 2) conversion patient par patient
    for pid in patient_ids:
        try:
            everything_url = f"{FHIR_SERVER_URL}/Patient/{pid}/$everything"
            bundle = _fetch_bundle_all_pages(everything_url, params={"_count": page_size})

            # ✅ conversion écrite dans EDS_DIR_CONV (data/eds)
            conv = fhir_to_edsan.process_bundle(bundle, eds_dir=str(EDS_DIR_CONV), write_report=False)

            # ✅ addition incoming_rows uniquement
            for r in (conv.get("merge") or conv.get("merge_report") or []):
                t = r.get("table")
                if not t:
                    continue

                # si une table apparaît et n'était pas trackée
                if t not in incoming_acc:
                    incoming_acc[t] = 0
                    tracked_tables.append(t)
                    before_global[t] = snapshot_eds_counts(EDS_DIR_CONV, [t]).get(t, 0)


                incoming_acc[t] += int(r.get("incoming_rows", 0) or 0)

            summary = summarize_bundle(bundle)

            per_patient.append({
                "patient_id": pid,
                "status": "success",
                "entries_total": summary["entries_total"],
                "resources_per_type": summary["resources_per_type"],
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

    # ✅ snapshot global APRÈS conversion (dans EDS_DIR_CONV)
    after_global = snapshot_eds_counts(EDS_DIR_CONV, tracked_tables)

    # ✅ merge_report final
    merge_report = []
    for t in sorted(set(tracked_tables)):
        before_rows = int(before_global.get(t, 0) or 0)
        after_rows = int(after_global.get(t, 0) or 0)
        incoming_rows = int(incoming_acc.get(t, 0) or 0)
        added_rows = after_rows - before_rows

        merge_report.append({
            "table": t,
            "before_rows": before_rows,
            "incoming_rows": incoming_rows,
            "after_rows": after_rows,
            "added_rows": added_rows,
        })

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
        "merge_report": merge_report,
    }

    from app.utils.helpers import write_last_run_report
    write_last_run_report(report, REPORTS_DIR)

    return {"status": "success", "data": report}


@router.post("/convert/fhir-warehouse-patients-to-edsan", tags=["Conversion"])
async def convert_list_patients_from_warehouse(payload: dict):
    patient_ids = payload.get("patient_ids") or payload.get("patients") or payload.get("ids")
    if not patient_ids or not isinstance(patient_ids, list):
        raise HTTPException(
            status_code=400,
            detail="patient_ids (liste) requis. Exemple: {'patient_ids': ['id1','id2']}"
        )

    page_size = int(payload.get("page_size", 200))

    # Optionnel: reset (si tu veux un run propre)
    reset = bool(payload.get("reset", False))
    tables = ["mvt.parquet", "biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]

    if reset:
        from pathlib import Path
        for t in tables:
            p = Path(EDS_DIR_CONV) / t
            if p.exists():
                p.unlink()

    from app.utils.helpers import snapshot_eds_counts, build_merge_report

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    # ✅ Snapshot AVANT
    before_counts = snapshot_eds_counts(EDS_DIR_CONV, tables)

    per_patient = []
    ok = 0
    ko = 0

    # ✅ incoming accumulator
    incoming_acc = {t: 0 for t in tables}

    for pid in patient_ids:
        try:
            everything_url = f"{FHIR_SERVER_URL}/Patient/{pid}/$everything"
            bundle = _fetch_bundle_all_pages(everything_url, params={"_count": page_size})

            conv = fhir_to_edsan.process_bundle(
                bundle,
                eds_dir=str(EDS_DIR_CONV),
                write_report=False
            )

            # ✅ On accumule incoming_rows (tel que process_bundle le renvoie)
            for r in (conv.get("merge") or conv.get("merge_report") or []):
                t = r.get("table")
                if t in incoming_acc:
                    incoming_acc[t] += int(r.get("incoming_rows", 0) or 0)

            summary = summarize_bundle(bundle)
            per_patient.append({
                "patient_id": pid,
                "status": "success",
                "entries_total": summary["entries_total"],
                "resources_per_type": summary["resources_per_type"],
            })
            ok += 1

        except Exception as e:
            per_patient.append({"patient_id": pid, "status": "failed", "error": str(e)})
            ko += 1

    ended_at = datetime.now().isoformat()

    # ✅ Snapshot APRÈS
    after_counts = snapshot_eds_counts(EDS_DIR_CONV, tables)

    # ✅ merge_report final cohérent
    merge_report = build_merge_report(before_counts, after_counts, incoming_acc)

    report = {
        "run_id": run_id,
        "mode": "warehouse_list",
        "warehouse_url": FHIR_SERVER_URL,
        "page_size": page_size,
        "started_at": started_at,
        "ended_at": ended_at,
        "patients_total": len(patient_ids),
        "patients_success": ok,
        "patients_failed": ko,
        "patients": per_patient,
        "merge_report": merge_report,
    }

    from app.utils.helpers import write_last_run_report
    write_last_run_report(report, REPORTS_DIR)

    return {"status": "success", "data": report}


@router.post("/convert/fhir-warehouse-patient-to-edsan", tags=["Conversion"])
async def convert_one_patient_from_warehouse(payload: dict):
    pid = payload.get("patient_id")
    if not pid:
        raise HTTPException(status_code=400, detail="patient_id requis.")

    page_size = int(payload.get("page_size", 200))
    reset = bool(payload.get("reset", False))

    tables = ["mvt.parquet", "biol.parquet", "pharma.parquet", "doceds.parquet", "pmsi.parquet"]

    if reset:
        from pathlib import Path
        for t in tables:
            p = Path(EDS_DIR_CONV) / t
            if p.exists():
                p.unlink()

    from app.utils.helpers import snapshot_eds_counts, build_merge_report

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    before_counts = snapshot_eds_counts(EDS_DIR_CONV, tables)
    incoming_acc = {t: 0 for t in tables}

    try:
        everything_url = f"{FHIR_SERVER_URL}/Patient/{pid}/$everything"
        bundle = _fetch_bundle_all_pages(everything_url, params={"_count": page_size})

        conv = fhir_to_edsan.process_bundle(bundle, eds_dir=str(EDS_DIR_CONV), write_report=False)

        for r in (conv.get("merge") or conv.get("merge_report") or []):
            t = r.get("table")
            if t in incoming_acc:
                incoming_acc[t] += int(r.get("incoming_rows", 0) or 0)

        summary = summarize_bundle(bundle)

        after_counts = snapshot_eds_counts(EDS_DIR_CONV, tables)
        merge_report = build_merge_report(before_counts, after_counts, incoming_acc)

        report = {
            "run_id": run_id,
            "mode": "warehouse_one",
            "warehouse_url": FHIR_SERVER_URL,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "patient_id": pid,
            "entries_total": summary["entries_total"],
            "resources_per_type": summary["resources_per_type"],
            "merge_report": merge_report,
        }

        from app.utils.helpers import write_last_run_report
        write_last_run_report(report, REPORTS_DIR)

        return {"status": "success", "data": report}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur conversion patient {pid}: {str(e)}")

 
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
 
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as z:
 
        for f in files:
            full_path = os.path.join(EDS_DIR, f)
            z.write(full_path, arcname=f)
 
    return FileResponse(zip_path, filename="eds_export.zip", media_type="application/zip")
 
@router.get("/report/last-run", tags=["Report"])
async def get_last_run_report():
    """
    Retourne le dernier rapport de run (report/last_run.json) généré par process_dir/process_bundle.
    Retourne le dernier rapport de run (report/last_run.json) généré par process_dir/process_bundle.
    """
    report_path = os.path.join(REPORTS_DIR, "last_run.json")
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
    report_path = os.path.join(REPORTS_DIR, "last_run.json")
    last_run = None
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            last_run = json.load(f)
 
    return {"report_dir": REPORTS_DIR, "tables": stats, "last_run": last_run}
 
 

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
            eds_dir=os.getenv("EDS_DIR", "data/eds"),
            output_dir=out_dir,
            mapping_path=None,
            fhir_base_url=None,  
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
            eds_dir=os.getenv("EDS_DIR", "eds"),
            output_dir=os.getenv("FHIR_OUTPUT_DIR", "exports_eds_fhir"),  # optionnel 
            mapping_path=os.getenv("FHIR_MAPPING_PATH"),
            fhir_base_url="http://localhost:8080/fhir",  
            print_summary=False,
        )
 
        return {
            "message": "Push vers FHIR terminé",
            "summary": result.get("summary"),
            "push_results_keys": list(result.get("push_results", {}).keys()),
        }
 
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# --- ENDPOINTS : Rapports d'Export (EDSan -> FHIR) ---

@router.get("/report/last-export", tags=["Report"])
async def get_last_export_report():
    """
    Retourne le dernier rapport d'exportation (EDSan -> FHIR) 
    stocké dans reports_export/exports/last_export_fhir.json.
    """
    # On cible le sous-dossier 'exports' créé par save_export_report
    report_path = Path(REPORTS_DIR_EXPORT_PATH) / "last_export_fhir.json"
    
    if not report_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"Fichier last_export_fhir.json introuvable. L'API a cherché ici : {report_path}"
        )

    with open(report_path, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/report/export-runs", tags=["Report"])
async def list_export_runs():
    """
    Liste l'historique des exports archivés dans le sous-dossier exports/.
    """
    export_runs_dir = Path(REPORTS_DIR_EXPORT_PATH) / "exports"
    
    if not export_runs_dir.exists():
        return []

    # On cherche les fichiers datés export_YYYYMMDD_HHMMSS.json
    files = sorted(export_runs_dir.glob("export_*.json"), reverse=True)
    
    # On filtre pour ne pas inclure le 'last_export_fhir.json' dans la liste d'historique
    return [
        {"name": f.name, "size": f.stat().st_size} 
        for f in files if f.name != "last_export_fhir.json"
    ]


@router.get("/report/export-run/{name}", tags=["Report"])
async def download_export_run(name: str):
    """
    Télécharge un rapport d'export archivé spécifique.
    """
    # Sécurité anti-traversal
    if ".." in name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Nom de fichier invalide.")
        
    path = Path(REPORTS_DIR_EXPORT_PATH) / "exports" / name
    
    if not path.exists():
        raise HTTPException(status_code=404, detail="Rapport d'export introuvable.")
        
    return FileResponse(str(path), filename=name, media_type="application/json")
