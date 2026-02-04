from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
from typing import List
from pathlib import Path
from dotenv import load_dotenv

import os
import json
import tempfile
import polars as pl
import requests
import shutil
import zipfile
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
from contextlib import contextmanager

from app.core.converters.edsan_to_fhir import export_eds_to_fhir
from app.core.models.edsan_models import PmsiModel, PatientModel

from app.core.converters import fhir_to_edsan
from app.core.converters.fhir_to_edsan import EDS_DIR, REPORTS_DIR
from app.utils.helpers import (
    _fetch_bundle_all_pages,
    _collect_patient_ids,
    summarize_bundle,
    _zip_folder,
)

load_dotenv()  # charge les variables du .env
router = APIRouter()

FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")
FHIR_ACCEPT_HEADERS = {"Accept": "application/fhir+json"}


# ---------------------------------------------------------------------
# Helpers config : payload > env > défaut
# Objectif : mêmes paramètres CLI / API / UI, sans casser les defaults.
# ---------------------------------------------------------------------
def _pick(payload: dict | None, key: str, default=None):
    """
    Récupère un param :
    - payload[key] si fourni et non vide
    - sinon variable d'environnement
    - sinon default
    """
    if payload and key in payload and payload[key] not in (None, "", " "):
        return payload[key]
    return os.getenv(key, default)


@contextmanager
def _override_module_attrs(module, **overrides):
    """
    Override temporaire d'attributs de module (EDS_DIR, REPORTS_DIR, etc.)
    Utile tant que process_bundle/process_dir ne prennent pas encore eds_dir/reports_dir
    en argument. On restaure toujours ensuite.
    """
    saved = {}
    try:
        for k, v in overrides.items():
            if v is None:
                continue
            if hasattr(module, k):
                saved[k] = getattr(module, k)
                setattr(module, k, v)
        yield
    finally:
        for k, old in saved.items():
            setattr(module, k, old)


#                --- ENDPOINTS : FHIR (ENTREPOT) -> EDS ---
#
# Paramètres existants:
# - patient_limit (int): nb de patients à convertir (par défaut convertit tout l'entrepot)
#   * si patient_limit = 0 => convertit TOUS les patients (attention lourd)
# - page_size (int): _count utilisé pour pagination (par défaut 100)
#
# Paramètres ajoutés (optionnels) pour alignement commanditaires:
# - eds_dir (str): dossier EDS destination (sinon défaut EDS_DIR)
# - reports_dir (str): dossier report import (sinon défaut REPORTS_DIR)
# - fhir_server_url (str): override URL base de l'entrepôt FHIR (sinon env/défaut)
# - query_url / fhir_query_url: URL complète de requête FHIR (mode commanditaire principal)
# ---------------------------------------------------------------------


@router.post("/convert/fhir-query-to-edsan", tags=["Conversion"])
async def convert_fhir_query_to_edsan(payload: dict):
    """
    Import principal demandé par les commanditaires :
    - payload["query_url"] : URL complète de requête FHIR (obligatoire)
      ex: http://.../fhir/Patient?_count=200
    - eds_dir (optionnel) : dossier EDS destination
    - reports_dir (optionnel) : dossier report import (last_run.json + runs/)
    - fhir_server_url (optionnel) : utile si l'utilisateur veut override la base (info seulement ici)
    - page_size (optionnel) : _count pour pagination (défaut 100)

    last_run :
    - écrit reports_dir/last_run.json
    - archive dans reports_dir/runs/last_run_<timestamp>.json
    """
    query_url = payload.get("query_url") or payload.get("fhir_query_url")
    if not query_url or not str(query_url).strip():
        raise HTTPException(status_code=400, detail="query_url requis (URL de requête FHIR).")

    # paramètres optionnels (pratiques)
    eds_dir = _pick(payload, "eds_dir", EDS_DIR)
    reports_dir = _pick(payload, "reports_dir", REPORTS_DIR)
    fhir_server_url = _pick(payload, "fhir_server_url", FHIR_SERVER_URL)  # pour trace dans report
    page_size = int(payload.get("page_size", 100))

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    try:
        # récupère toutes les pages (Bundle searchset paginé)
        # NB: si l'URL contient déjà _count, on le laisse ; sinon on impose page_size.
        params = {}
        if "_count=" not in str(query_url):
            params["_count"] = page_size

        bundle = _fetch_bundle_all_pages(str(query_url).strip(), params=params)

        # conversion + merge (on évite d'écrire last_run ici)
        # et on force EDS_DIR/REPORTS_DIR si override demandé.
        with _override_module_attrs(fhir_to_edsan, EDS_DIR=eds_dir, REPORTS_DIR=reports_dir):
            _ = fhir_to_edsan.process_bundle(bundle, write_report=False)

        summary = summarize_bundle(bundle)

        report = {
            "run_id": run_id,
            "mode": "query_url",
            "warehouse_url": fhir_server_url,
            "query_url": str(query_url).strip(),
            "page_size": page_size,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "eds_dir": eds_dir,
            "reports_dir": reports_dir,
            "entries_total": summary.get("entries_total"),
            "resources_per_type": summary.get("resources_per_type"),
        }

        from app.utils.helpers import write_last_run_report
        write_last_run_report(report, reports_dir)

        return {"status": "success", "data": report}

    except Exception as e:
        # on tente quand même d'archiver un report d'erreur (utile recette)
        error_report = {
            "run_id": run_id,
            "mode": "query_url",
            "warehouse_url": fhir_server_url,
            "query_url": str(query_url).strip(),
            "page_size": page_size,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "eds_dir": eds_dir,
            "reports_dir": reports_dir,
            "status": "failed",
            "error": str(e),
        }
        try:
            from app.utils.helpers import write_last_run_report
            write_last_run_report(error_report, reports_dir)
        except Exception:
            pass

        raise HTTPException(status_code=400, detail=f"Erreur import via query_url: {str(e)}")


@router.post("/convert/fhir-warehouse-to-edsan", tags=["Conversion"])
async def convert_fhir_warehouse_to_edsan(payload: dict | None = None):
    """
    Conversion 'entrepôt complet' : récupère N patients (ou tous) puis $everything
    pour chaque patient, puis convertit vers EDS.
    """
    # 🛑 CORRECTIF : Déclarer global AVANT toute lecture ou pick
    global FHIR_SERVER_URL 

    # Maintenant on peut lire et utiliser les paramètres
    patient_limit = int(_pick(payload, "patient_limit", 0))
    page_size = int(_pick(payload, "page_size", 100))
    eds_dir = _pick(payload, "eds_dir", EDS_DIR)
    reports_dir = _pick(payload, "reports_dir", REPORTS_DIR)
    
    # On récupère l'URL cible (depuis payload ou la valeur actuelle du global)
    fhir_server_url = _pick(payload, "fhir_server_url", FHIR_SERVER_URL)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    per_patient = []
    ok = 0
    ko = 0

    # Sauvegarde pour restauration
    old_url = FHIR_SERVER_URL
    try:
        FHIR_SERVER_URL = fhir_server_url
        patient_ids = _collect_patient_ids(patient_limit, page_size)
        
        if not patient_ids:
            raise HTTPException(status_code=404, detail="Aucun Patient dans l'entrepôt FHIR.")

        # ... (le reste de ton code ne change pas)
        for pid in patient_ids:
            try:
                everything_url = f"{fhir_server_url}/Patient/{pid}/$everything"
                bundle = _fetch_bundle_all_pages(everything_url, params={"_count": page_size})

                with _override_module_attrs(fhir_to_edsan, EDS_DIR=eds_dir, REPORTS_DIR=reports_dir):
                    _ = fhir_to_edsan.process_bundle(bundle, write_report=False)

                summary = summarize_bundle(bundle)
                per_patient.append({
                    "patient_id": pid,
                    "status": "success",
                    "entries_total": summary.get("entries_total"),
                    "resources_per_type": summary.get("resources_per_type"),
                })
                ok += 1
            except Exception as e:
                per_patient.append({"patient_id": pid, "status": "failed", "error": str(e)})
                ko += 1

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur accès entrepôt FHIR: {str(e)}")
    finally:
        # ✅ On restaure toujours la valeur initiale
        FHIR_SERVER_URL = old_url

 

    ended_at = datetime.now().isoformat()

    # 3) Report GLOBAL
    report = {
        "run_id": run_id,
        "mode": "warehouse_all",
        "warehouse_url": fhir_server_url,
        "patient_limit": patient_limit,
        "page_size": page_size,
        "started_at": started_at,
        "ended_at": ended_at,
        "patients_total": len(patient_ids),
        "patients_success": ok,
        "patients_failed": ko,
        "eds_dir": eds_dir,
        "reports_dir": reports_dir,
        "patients": per_patient,
    }

    # archive + last_run.json (historique)
    from app.utils.helpers import write_last_run_report
    write_last_run_report(report, reports_dir)

    return {"status": "success", "data": report}


@router.post("/convert/fhir-warehouse-patient-to-edsan", tags=["Conversion"])
async def convert_one_patient_from_warehouse(payload: dict):
    """
    Equivalent "1 fichier patient Synthea" mais depuis l’entrepôt.
    payload: {"patient_id": "...", "eds_dir": "...", "reports_dir": "...", "fhir_server_url": "..."}
    """
    pid = payload.get("patient_id")
    if not pid:
        raise HTTPException(status_code=400, detail="patient_id requis.")

    eds_dir = _pick(payload, "eds_dir", EDS_DIR)
    reports_dir = _pick(payload, "reports_dir", REPORTS_DIR)
    fhir_server_url = _pick(payload, "fhir_server_url", FHIR_SERVER_URL)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now().isoformat()

    try:
        everything_url = f"{fhir_server_url}/Patient/{pid}/$everything"
        bundle = _fetch_bundle_all_pages(everything_url, params={"_count": int(payload.get("page_size", 200))})

        with _override_module_attrs(fhir_to_edsan, EDS_DIR=eds_dir, REPORTS_DIR=reports_dir):
            _ = fhir_to_edsan.process_bundle(bundle, write_report=False)

        summary = summarize_bundle(bundle)

        report = {
            "run_id": run_id,
            "mode": "warehouse_one",
            "warehouse_url": fhir_server_url,
            "started_at": started_at,
            "ended_at": datetime.now().isoformat(),
            "patient_id": pid,
            "eds_dir": eds_dir,
            "reports_dir": reports_dir,
            "entries_total": summary.get("entries_total"),
            "resources_per_type": summary.get("resources_per_type"),
        }

        from app.utils.helpers import write_last_run_report
        write_last_run_report(report, reports_dir)

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
    Retourne le dernier rapport import (reports_dir/last_run.json).
    """
    report_path = os.path.join(REPORTS_DIR, "last_run.json")
    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Aucun rapport disponible (last_run.json introuvable).")

    with open(report_path, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/stats", tags=["Report"])
async def get_stats(eds_dir: str | None = None, reports_dir: str | None = None):
    """
    Statistiques rapides sur les parquets EDS (rows/cols par table).

    ✅ Important (cohérence CLI / API / UI) :
    - si eds_dir n'est pas fourni, on essaye de le déduire via le last_run.json
      (utile quand un run CLI/UI a été lancé avec --eds-dir).
    - reports_dir peut être passé pour pointer vers un autre dossier de rapports.
    """

    # 1) base reports_dir (optionnel)
    base_reports_dir = (reports_dir or "").strip() or REPORTS_DIR
    report_path = os.path.join(base_reports_dir, "last_run.json")

    # 2) si eds_dir non fourni, on tente de le récupérer dans last_run.json
    effective_eds_dir = (eds_dir or "").strip()
    last_run = None

    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                last_run = json.load(f)
        except Exception:
            # l'API ne doit pas planter si le report est corrompu / partiel
            last_run = None

    if not effective_eds_dir:
        if isinstance(last_run, dict):
            lr_eds = (last_run.get("eds_dir") or "").strip()
            if lr_eds:
                effective_eds_dir = lr_eds

    # 3) fallback final (compat ancienne logique)
    if not effective_eds_dir:
        effective_eds_dir = EDS_DIR

    # 4) calcul stats (rapide)
    if not os.path.isdir(effective_eds_dir):
        raise HTTPException(status_code=404, detail=f"Dossier EDS introuvable: {effective_eds_dir}")

    tables = sorted([f for f in os.listdir(effective_eds_dir) if f.endswith(".parquet")])
    tables = [t for t in tables if t != "patient.parquet"]

    stats = {}
    for t in tables:
        path = os.path.join(effective_eds_dir, t)
        lf = pl.scan_parquet(path)
        rows = lf.select(pl.len()).collect().item()
        cols = len(lf.columns)
        stats[t] = {"rows": rows, "cols": cols}

    return {
        "report_dir": base_reports_dir,
        "eds_dir": effective_eds_dir,
        "tables": stats,
        "last_run": last_run,
    }


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


# =============================== EDSAN  TO  FHIR ==============================================

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
            eds_dir=os.getenv("EDS_DIR", "data/eds"),
            output_dir=os.getenv("FHIR_OUTPUT_DIR", "exports_eds_fhir"),
            mapping_path=None,
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
    
