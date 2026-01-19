import os
from dotenv import load_dotenv
from app.core.converters.edsan_to_fhir import export_eds_to_fhir

load_dotenv()  # charge le .env

export_eds_to_fhir(
    eds_dir=os.getenv("EDS_DIR"),
    output_dir=os.getenv("FHIR_OUTPUT_DIR"),
    bundle_strategy=os.getenv("FHIR_BUNDLE_STRATEGY", "patient")
)

if not os.getenv("EDS_DIR") or not os.getenv("FHIR_OUTPUT_DIR"):
    raise RuntimeError("Variables d'environnement manquantes (EDS_DIR / FHIR_OUTPUT_DIR)")

