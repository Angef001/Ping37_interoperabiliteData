# Utiliser une image Python légère basée sur Debian Bookworm
FROM python:3.12-slim-bookworm

# Définir des variables d'environnement pour Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Installer les dépendances système nécessaires pour Polars et PyArrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier des dépendances
COPY requirements.txt .

# Installer les bibliothèques Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copier le reste du code source
COPY app ./app

RUN mkdir -p eds 

# Créer le dossiers de stockage des fichiers edsan au format parquet
RUN mkdir -p /data/eds 

RUN mkdir -p /data/reports

RUN mkdir -p /data/reports_export

RUN mkdir -p /app/output

# Créer le dossiers de stockage des fichiers edsan au format fhir
RUN mkdir -p /data/exports_eds_fhir

# Exposer le port sur lequel FastAPI va tourner
EXPOSE 8000

# Commande pour démarrer l'API avec Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
