from fastapi import APIRouter, HTTPException, UploadFile, File
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.core.converters import fhir_to_edsan, edsan_to_fhir
from typing import List
import os
import polars as pl
from app.core.converters.build_eds_with_fhir import EDS_DIR
import json
from fastapi.responses import FileResponse
import zipfile
import tempfile
import json





router = APIRouter()



# --- ENDPOINT : EDS -> FHIR ---
@router.post("/convert/edsan-to-fhir", tags=["Conversion"])
async def convert_edsan_to_fhir(data: List[PmsiModel]):
    """
    Reçoit une liste de données EDS (ex: lignes PMSI) et reconstruit des ressources FHIR.
    """
    try:
        # On délègue au binôme 2
        bundle = edsan_to_fhir.reconstruct_bundle(data)
        return bundle
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur de reconstruction : {str(e)}")
    
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
    Retourne le dernier rapport de run (eds/last_run.json) généré par process_dir/process_bundle.
    """
    report_path = os.path.join(EDS_DIR, "last_run.json")
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
    report_path = os.path.join(EDS_DIR, "last_run.json")
    last_run = None
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            last_run = json.load(f)

    return {"eds_dir": EDS_DIR, "tables": stats, "last_run": last_run}

