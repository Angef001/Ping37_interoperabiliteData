FROM python:3.12-slim
WORKDIR /app
COPY . .
# Installation du package en mode normal (pas éditable en prod)
RUN pip install .
# Définit le PYTHONPATH pour que "import src" fonctionne
ENV PYTHONPATH=/app

# Créer le dossier de stockage de dossiers format zip contenant des données au format fhir
RUN mkdir -p /app/output_zip

# On garde le conteneur ouvert pour pouvoir taper des commandes
CMD ["tail", "-f", "/dev/null"]