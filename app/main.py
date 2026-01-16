from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import endpoints

app = FastAPI(
    title="FHIR-EDS Transformer API (Projet PING - CHU Rouen)",
    description="API de transformation bidirectionnelle de données de santé",
    version="1.0.0"
)

# Configuration CORS (important pour que le front-end puisse appeler l'API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclusion des routes
app.include_router(endpoints.router, prefix="/api/v1")

@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Bienvenue sur l'API de transformation FHIR-EDS",
        "docs": "/docs" # Lien direct vers la doc Swagger
    }