from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.core.converters import fhir_to_edsan, edsan_to_fhir
from typing import List
import os
import polars as pl
from app.core.converters.build_eds_with_fhir import EDS_DIR
import json
from fastapi.responses import FileResponse, HTMLResponse
import zipfile
import tempfile
from pathlib import Path
import shutil
from dotenv import load_dotenv
import os

# Creez dans votre .env les variables suivantes :
# FHIR_BUNDLE_STRATEGY = patient ou encounter
# FHIR_EXPORT_DIR = dossier de sortie des exports
# FHIR_OUTPUT_DIR = dossier de sortie FHIR
load_dotenv()  # charge le .env
router = APIRouter()



# --- ENDPOINT : EDS -> FHIR ---
@router.post("/export/edsan-to-fhir-zip", tags=["Export"])
async def export_edsan_to_fhir_zip():
    # charger la variable d’environnement
    bundle_strategy = os.getenv("FHIR_BUNDLE_STRATEGY", "patient")

    if bundle_strategy not in ("patient", "encounter"):
        raise HTTPException(
            status_code=500,
            detail="FHIR_BUNDLE_STRATEGY doit être 'patient' ou 'encounter'"
        )

    # aller à laracine du projet
    project_root = Path(EDS_DIR).resolve().parent

    # dossier de sortie (depuis .env ou défaut)
    out_dir = Path(os.getenv("FHIR_OUTPUT_DIR", "fhir_output"))

    # si chemin relatif → on le met sous la racine projet
    if not out_dir.is_absolute():
        out_dir = project_root / out_dir

    # nettoyage ancien export
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # génération des bundles FHIR
    try:
        edsan_to_fhir.export_eds_to_fhir(
            eds_dir=EDS_DIR,
            output_dir=str(out_dir),
            bundle_strategy=bundle_strategy,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # création du ZIP
    zip_path = out_dir / "fhir_export.zip"
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in out_dir.glob("*.json"):
            z.write(p, arcname=p.name)

    # retour du ZIP
    return FileResponse(
        path=str(zip_path),
        filename="fhir_export.zip",
        media_type="application/zip",
    )

@router.get("/ui/export/fhir", response_class=HTMLResponse)
async def ui_export_fhir(request: Request):
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

