FROM python:3.12-slim
WORKDIR /app
COPY . .
# Installation du package en mode normal (pas éditable en prod)
RUN pip install .
# On garde le conteneur ouvert pour pouvoir taper des commandes
CMD ["tail", "-f", "/dev/null"]