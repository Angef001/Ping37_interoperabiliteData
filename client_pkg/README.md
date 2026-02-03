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

```bash
# 1. Créer et activer un environnement virtuel (si ce n'est pas déjà fait)
python3 -m venv .venv
source .venv/bin/activate

# 2. Installer le package en mode éditable en vous plaçant dans le dossier client_pkg
cd client_pkg
pip install -e .

```

**Astuce dépannage** : Si vous installez le client localement et recevez une erreur `ModuleNotFoundError: No module named 'src'`, assurez-vous de définir votre chemin source en tapant:
`export PYTHONPATH=$PYTHONPATH:.`


---

## ⚙️ Configuration

Le client utilise des variables d'environnement pour localiser les services. Vous pouvez les modifier si vos ports diffèrent :

| Variable | Description | Valeur par défaut |
| --- | --- | --- |
| `FHIR_URL` | URL de l'entrepôt HAPI FHIR | `http://localhost:8080/fhir` |
| `CONVERTER_API_URL` | URL de l'API de conversion | `http://localhost:8000` |

---

## 🛠️ Guide d'utilisation

Le client `chu-fhir` est divisé en plusieurs groupes de commandes.

Taper `chu-fhir --help` pour avoir la liste des commandes et comment les utiliser

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

* **Convertir plusieurs patients en parquet** :
```bash
    chu-fhir warehouse-convert-patients --ids <id> --ids <id> --ids <id> 
```

### ⚙️ 3. Conversion d'EDSan vers l'entrepot FHIR

* **Export ZIP**
Convertit les données EDSan en bundles FHIR et génère un fichier ZIP :
```bash
chu-fhir edsan-to-fhir-zip --output chemin/vers/export.zip
```

* **Push vers l'entrepôt FHIR**
Convertit et envoie directement les bundles vers le serveur FHIR :
```bash
chu-fhir edsan-to-fhir-push
```
* **Push vers l'entrepôt FHIR d'un fichier fhir non contenu dans l'edsan**
Envoie directement le bundle vers le serveur FHIR :
```bash
 chu-fhir upload-bundle chemin_vers/le/fichier.json
```

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
* **Supprimer des données d'une table par id** :
```bash
chu-fhir eds-delete patient --id 123 --id 456

```

* **Statistiques de stockage** :
```bash
chu-fhir stats

```



### 📁 5. Rapports et Archives

Gérez l'historique des exécutions.

* **Consulter le dernier rapport d'importation** :
```bash
chu-fhir last-run

```


* **Télécharger un rapport d'import spécifique** :
```bash
chu-fhir download-run <nom_du_rapport> --out ma_destination/log_import.json

```
* **Afficher l'historique des rapports d'import** 
```bash
chu-fhir runs

```

* **Consulter le dernier rapport d'exportation** 
Affiche les statistiques sur les bundles générés et les types de ressources poussés vers FHIR :

```bash
chu-fhir last-export

```

* **Lister l'historique des exports** 
Affiche la liste des anciens exports archivés dans le dossier de rapports :

```bash
chu-fhir export-runs

```

* **Télécharger un rapport d'export spécifique** 
Récupère un fichier de rapport archivé sur votre machine locale :

```bash
chu-fhir download-export-run [NOM_DU_FICHIER] --out download/bilan.json

```

* **Télécharger le dernier rapport d'export (Auto-daté)** 
Télécharge une copie locale du rapport le plus récent avec un timestamp automatique :

```bash
chu-fhir download-last-export

```
---

## 📁 Structure du Projet

* `src/main.py` : Logique principale utilisant **Typer** pour le CLI et **Rich** pour les affichages en tableau.
* `pyproject.toml` : Configuration du package et définition du point d'entrée `chu-fhir`.
* `Dockerfile` : Image basée sur `python:3.12-slim` pour un déploiement léger.