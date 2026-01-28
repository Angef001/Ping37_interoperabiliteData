from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# On importe les routes de l'API (JSON) et du Site Web (HTML/HTMX)
from app.api import endpoints
from app.web.routes import router as web_router

# =============================================================================
# 1. INITIALISATION DE L'APPLICATION
# =============================================================================
# Création de l'instance "app". C'est le cœur du serveur.
# Les infos title/description/version apparaissent automatiquement dans Swagger (/docs).
app = FastAPI(
    title="FHIR-EDS Transformer API (Projet PING - CHU Rouen)",
    description="API de transformation bidirectionnelle de données de santé (EDS <-> FHIR).",
    version="1.0.0"
)

# =============================================================================
# 2. SÉCURITÉ & MIDDLEWARE (CORS)
# =============================================================================
# Le CORS (Cross-Origin Resource Sharing) définit qui a le droit d'appeler ton API.
# Ici, allow_origins=["*"] signifie "Tout le monde".
# En prod, on remplacerait "*" par l'URL du frontend (ex: "http://chu-rouen.fr").
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],     # Autorise toutes les sources
    allow_credentials=True,  # Autorise les cookies/auth headers
    allow_methods=["*"],     # Autorise GET, POST, PUT, DELETE...
    allow_headers=["*"],     # Autorise tous les types d'en-têtes
)

# =============================================================================
# 3. ROUTAGE 
# =============================================================================

# A. L'API REST (Pour les machines / Swagger)
# On inclut toutes les routes définies dans endpoints.py.
# Le prefix "/api/v1" permet de versionner l'API. Si demain on change tout,
# on créera "/api/v2" sans casser l'ancienne version.
app.include_router(endpoints.router, prefix="/api/v1")

# B. L'Interface Graphique 
# On inclut les routes qui renvoient du HTML.
app.include_router(web_router)

# C. Fichiers Statiques (CSS, JS, Images, Logos)
# Permet au navigateur d'accéder aux fichiers du dossier "app/web/static"
# via l'URL "http://localhost:8000/static/..."
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")


# =============================================================================
# 4. ROUTE RACINE
# =============================================================================
@app.get("/")
async def root():
    """
    Message d'accueil simple pour vérifier que le serveur tourne.
    Redirige mentalement l'utilisateur vers la doc ou l'interface.
    """
    return {
        "message": "Bienvenue sur l'API de transformation FHIR-EDS",
        "docs_url": "Allez sur /docs pour tester l'API",
        "ui_url": "Allez sur /ui pour l'interface graphique"
    }