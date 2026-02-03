FROM python:3.12-slim
WORKDIR /app
COPY . .
# Installation du package en mode normal (pas éditable en prod)
RUN pip install .
# Définit le PYTHONPATH pour que "import src" fonctionne
ENV PYTHONPATH=/app

# Créer le dossier de stockage des fichiers edsan au format parquet
RUN mkdir -p /data/eds 

# Créer le dossier de stockage des fichiers de logs d'import
RUN mkdir -p /data/reports

# Créer le dossier de stockage des fichiers de logs d'export
RUN mkdir -p /data/reports_export

# Créer le dossier de stockage des fichiers au format fhir
RUN mkdir -p /app/output_export

# Créer le dossier de stockage de dossiers format zip contenant des données au format fhir
RUN mkdir -p /app/output_export_zip

# On garde le conteneur ouvert pour pouvoir taper des commandes
CMD ["tail", "-f", "/dev/null"]