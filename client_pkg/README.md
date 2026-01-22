
# 🏥 Client CLI - Interopérabilité FHIR CHU Rouen

Ce package Python fournit une interface en ligne de commande (CLI) pour interagir facilement avec l'entrepôt de données de santé FHIR du projet PING.

## 📋 Fonctionnalités

* **Vérification du statut** : Contrôler la connectivité avec le serveur FHIR.
* **Recherche de patients** : Rechercher des patients par nom et afficher les résultats sous forme de tableau.
* **Extraction de ressources** : Récupérer le contenu JSON brut de n'importe quelle ressource FHIR via son ID.

---

## 🚀 Installation (Mode Développement)

Si tu souhaites utiliser le client directement sur ta machine (hors conteneur) :

1. **Créer l'environnement virtuel** :
```bash
python3 -m venv .venv
source .venv/bin/activate

```


2. **Installer le package en mode éditable** :
```bash
pip install -e .

```


*Cette commande installe automatiquement les dépendances : `typer`, `requests` et `rich*`.

---

## 🛠️ Utilisation

Une fois installé, la commande `chu-fhir` est disponible partout dans ton terminal.

### 1. Vérifier la connexion

```bash
chu-fhir info

```

*Vérifie si le serveur est en ligne et affiche la version FHIR supportée*.

### 2. Rechercher un patient

```bash
chu-fhir search-patient --name "M Dupont"

```

*Affiche un tableau avec l'ID, le nom et la date de naissance des patients correspondants*.

### 3. Récupérer une ressource brute

```bash
chu-fhir get-resource Patient 123

```

*Affiche le JSON formaté de la ressource demandée*.

---

## 🐳 Utilisation avec Podman

Si le projet est lancé via `podman-compose`, tu peux utiliser le client sans rien installer sur ton système :

```bash
# Exécuter une commande à l'intérieur du conteneur
podman exec -it cli-client chu-fhir info

```

---

## ⚙️ Configuration

Le client utilise par défaut l'URL `http://localhost:8080/fhir`.
En environnement conteneurisé, il utilise automatiquement l'URL du service défini par la variable d'environnement `FHIR_URL`.

---

## 📁 Structure du code

* `pyproject.toml` : Configuration du package et des scripts.
* `src/main.py` : Logique principale de l'application et définition des commandes.
