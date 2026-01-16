from fastapi import APIRouter, HTTPException, UploadFile, File
from app.core.models.edsan_models import PmsiModel, PatientModel
from app.core.converters import fhir_to_edsan, edsan_to_fhir
from typing import List

router = APIRouter()

# --- ENDPOINT : FHIR -> EDS ---
@router.post("/convert/fhir-to-edsan", tags=["Conversion"])
async def convert_fhir_to_edsan(bundle: dict):
    """
    Reçoit un Bundle FHIR (JSON) et le transforme en structure EDSaN.
    """
    try:
        # On délègue au binôme 1
        result = fhir_to_edsan.process_bundle(bundle)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur de conversion : {str(e)}")

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