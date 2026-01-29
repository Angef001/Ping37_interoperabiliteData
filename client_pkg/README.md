# 🏥 FHIR Client CLI — CHU Rouen (Projet PING)

Ce package Python fournit une interface en ligne de commande (CLI) robuste pour interagir avec l'entrepôt de données de santé FHIR et l'API de conversion EDS.

## 📋 Prérequis

Avant d'installer le client, assurez-vous d'avoir les éléments suivants sur votre machine :

* **Python 3.10 3.11 ou 3.12** 
* **Accès réseau** aux services suivants :
* Serveur FHIR (HAPI) : par défaut sur `http://localhost:8080/fhir`
* API Converter (FastAPI) : par défaut sur `http://localhost:8000/api/v1`


* **Environnement Linux/WSL** (recommandé pour la gestion des variables d'environnement)

---

## 🚀 Installation

### 1. Installation via Podman (Recommandé)

Le client est déjà conteneurisé pour éviter les conflits de dépendances sur votre machine hôte.

```bash
# Lancement des conteneurs
podman-compose up --build -d

#Remplir l'entrepôt de données fhir (si ce n'est pas déjà fait)
podman exec -it ping37_interoperabilitedata_api-converter_1 python3 -m app.core.converters.edsan_to_fhir
 
# Entrer dans le conteneur client
podman exec -it ping37_interoperabilitedata_cli-client_1 bash

# Une fois à l'intérieur, la commande est directement disponible
chu-fhir --help

```

Note: Taper "exit" pour sortir d'un conteneur 

### 2. Installation locale (Mode Développement)

Si vous souhaitez développer ou tester le client directement sur votre machine :

```bash
# 1. Créer et activer un environnement virtuel
python3 -m venv .venv
source .venv/bin/activate

# 2. Installer le package en mode éditable en vous plaçant dans le dossier client_pkg
pip install -e .

```
Cette commande installe automatiquement `typer`, `requests` et `rich`.

**Astuce dépannage** : Si vous installez le client localement et recevez une erreur `ModuleNotFoundError: No module named 'src'`, assurez-vous de définir votre chemin source en tapant:
`export PYTHONPATH=$PYTHONPATH:.`


---

## ⚙️ Configuration

Le client utilise des variables d'environnement pour localiser les services. Vous pouvez les modifier si vos ports diffèrent :

| Variable | Description | Valeur par défaut |
| --- | --- | --- |
| `FHIR_URL` | URL de l'entrepôt HAPI FHIR | `http://localhost:8080/fhir` |
| `CONVERTER_API_URL` | URL de l'API de conversion | `http://localhost:8000/api/v1` |

---

## 🛠️ Guide d'utilisation

Le client `chu-fhir` est divisé en plusieurs groupes de commandes.

### 🔍 1. Exploration FHIR

Interrogez directement l'entrepôt HAPI.

* **Vérifier la connexion** :
```bash
chu-fhir info

```


* **Chercher des patients (par IDs)** :
```bash
chu-fhir get-patients <ID1> <ID2>

```


* **Voir une ressource brute (JSON)** :
```bash
chu-fhir get-resource Patient 123

```



### ⚙️ 2. Conversion d'Entrepôt FHIR

Pilotez la conversion de l'entrepôt FHIR vers le format EDS (Parquet).

* **Convertir tout l'entrepôt** (limité à 50 patients par défaut) :
```bash
chu-fhir warehouse-convert --patient-limit 100

```


* **Convertir un patient spécifique** :
```bash
chu-fhir warehouse-convert-patient --id <FHIR_ID>

```

### ⚙️ 3. Conversion d'EDSan vers l'entrepot FHIR

* **Export ZIP**
Convertit les données EDSan en bundles FHIR et génère un fichier ZIP :
```bash
chu-fhir edsan-to-fhir-zip --output /chemin/vers/export.zip
```

* **Push vers l'entrepôt FHIR**
Convertit et envoie directement les bundles vers le serveur FHIR :
```bash
chu-fhir edsan-to-fhir-push
```

**Note :** L'API doit être démarrée (`uvicorn app.main:app --reload`) avant d'utiliser ces commandes.


### 📊 4. Gestion de l'EDS

Explorez les données converties au format `.parquet`.

* **Lister les tables EDS** :
```bash
chu-fhir eds-tables

```


* **Aperçu des données** :
```bash
chu-fhir eds-preview <Nom_Table> --limit 10

```


* **Statistiques de stockage** :
```bash
chu-fhir stats

```



### 📁 5. Rapports et Archives

Gérez l'historique des exécutions.

* **Voir le dernier rapport de run** :
```bash
chu-fhir last-run

```


* **Télécharger un run archivé (ZIP)** :
```bash
chu-fhir download-run <nom_du_zip> --out ./ma_destination/

```

---

## 📁 Structure du Projet

* `src/main.py` : Logique principale utilisant **Typer** pour le CLI et **Rich** pour les affichages en tableau.
* `pyproject.toml` : Configuration du package et définition du point d'entrée `chu-fhir`.
* `Dockerfile` : Image basée sur `python:3.12-slim` pour un déploiement léger.
 

## Commande pour tester la conversion fhir-eds

* Convertir N patient(N peut etre egale à 50, 3, 10, etc selon votre envie)
    'chu-fhir warehouse-convert --patient-limit N'

* Convertir tout l'entrepot
    'chu-fhir warehouse-convert ou chu-fhir warehouse-convert --patient-limit 0'

* Convertir un patient 
    'chu-fhir warehouse-convert-patient --id <id>'

* Convertir plusieurs patients en parquet
    ' chu-fhir --ids <id> --ids <id> --ids <id> (vous pouvez mettre autant d'ids que vous souhaitez)

* Afficher l'historique 
    'chu-fhir runs'

* Telecharger un fichier last_run
    'chu-fhir download-run  <nom du fichier>'


* Afficher les stats de l'eds
    'chu-fhir stats'


* Afficher les tables de l'eds
    'chu-fhir eds-tables'


Pour consulter les nouvelles tables parquet, consluter le dossier data/eds
Pour consulter l'historique des runs, consulter le dossier data/reports/runs
