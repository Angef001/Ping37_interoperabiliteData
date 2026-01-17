from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api import endpoints
from app.web.routes import router as web_router

app = FastAPI(
    title="FHIR-EDS Transformer API (Projet PING - CHU Rouen)",
    description="API de transformation bidirectionnelle de données de santé",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API
app.include_router(endpoints.router, prefix="/api/v1")

# UI HTMX
app.include_router(web_router)
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")


@app.get("/")
async def root():
    return {
        "message": "Bienvenue sur l'API de transformation FHIR-EDS",
        "docs": "/docs",
        "ui": "/ui"
    }
