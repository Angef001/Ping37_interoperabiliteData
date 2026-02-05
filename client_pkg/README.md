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

Taper `chu-fhir --help` pour afficher l'aide interactive dans le terminal.

### 🔍 1. Exploration FHIR

Interrogez directement l'entrepôt HAPI pour vérifier la connexion ou inspecter des données brutes.

* **Vérifier la connexion** :
```bash
chu-fhir info

```


* **Voir un patient unique (détails formatés)** :
```bash
chu-fhir get-patient <ID>

```


* **Chercher plusieurs patients (tableau récapitulatif)** :
```bash
chu-fhir get-patients <ID1> <ID2>

```


* **Voir une ressource brute (JSON)** :
```bash
chu-fhir get-resource <ResourceType> <ResourceID>
# Ex: chu-fhir get-resource Patient 123

```



### 📥 2. Import et Conversion (FHIR ➔ EDS)

Pilotez la conversion de l'entrepôt FHIR vers le format EDS (fichiers Parquet).

#### A. Import via URL de requête (Recommandé)

Idéal pour importer un sous-ensemble précis défini par une requête FHIR standard.

* **Importer via une requête FHIR complète** :
```bash
chu-fhir import-url --url "http://localhost:8080/fhir/Encounter?_count=100"

```


* **Spécifier le dossier de destination et afficher les stats** :
```bash
chu-fhir import-url \
  --url "http://localhost:8080/fhir/Patient?gender=female" \
  --eds-dir data/eds_custom \
  --stats

```


> **Note :** Le paramètre `--eds-dir` permet de cibler un dossier spécifique. Les rapports sont centralisés dans le dossier `reports` par défaut.



#### B. Import via commandes "Warehouse" (API Converter)

Commandes pour déclencher les conversions prédéfinies côté serveur.

* **Convertir tout l'entrepôt** (avec limite optionnelle) :
```bash
chu-fhir warehouse-convert --patient-limit 100 --page-size 200

```


* **Convertir un patient spécifique** :
```bash
chu-fhir warehouse-convert-patient --id <FHIR_ID>

```


* **Convertir une liste de patients** :
```bash
chu-fhir warehouse-convert-patients --ids <ID1> --ids <ID2>

```



### 📊 3. Gestion et Manipulation de l'EDS

Explorez, modifiez ou supprimez des données dans les fichiers Parquet générés.

* **Lister les tables EDS disponibles** :
```bash
chu-fhir eds-tables

```


* **Aperçu des données d'une table (via API)** :
```bash
chu-fhir eds-preview <Nom_Table> --limit 10

```


* **Affichage local des tables (Rendu riche)** :
Affiche le contenu directement depuis le disque local (similaire à preview mais côté client).
```bash
chu-fhir display-eds --eds-dir data/eds --limit 5

```


* **Supprimer des lignes d'une table** :
Supprime des enregistrements spécifiques par leur ID technique.
```bash
chu-fhir eds-delete <Nom_Table> --id <ID1> --id <ID2>
# Ex: chu-fhir eds-delete patient --id 123 --id 456

```


* **Afficher les statistiques de volume (Lignes/Colonnes)** :
```bash
chu-fhir stats

```



### ⚙️ 4. Filtrage et Export (EDS ➔ FHIR)

Transformez un EDS (Parquet) en ressources FHIR, filtrez-le ou envoyez-le vers un serveur.

#### A. Filtrage Avancé & Export (edsan-filter-to-fhir)

C'est la commande "couteau suisse" pour filtrer, convertir et pousser les données.

**Paramètres clés :**

* `--input-dir` : Dossier EDS source.
* `--fhir-output-dir` : Dossier de destination des JSON.
* `--where "table:condition"` : Filtre (ex: `patient:PATAGE<10`).
* `--propagate "CLE:table"` : Propage le filtre (ex: `PATID:patient`).
* `--push` : Envoie directement au serveur FHIR.

**Exemples :**

* **Export simple (tout l'EDS)** :
```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir data/eds \
  --fhir-output-dir data/output_fhir \
  --stats

```


* **Export filtré (Femmes > 40 ans) avec Push FHIR** :
```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir data/eds \
  --fhir-output-dir data/output_fhir \
  --where "patient:PATAGE>40" \
  --where "patient:PATSEX==F" \
  --propagate "PATID:patient" \
  --push \
  --fhir-url http://localhost:8080/fhir

```



#### B. Filtrage seul (EDS ➔ EDS)

Génère un sous-ensemble de fichiers Parquet sans conversion FHIR.

```bash
chu-fhir edsan-filter \
  --input-dir data/eds \
  --output-dir data/eds_filtre \
  --where "patient:PATAGE<6" \
  --propagate "PATID:patient"

```

#### C. Exports Rapides & Utilitaires

Commandes raccourcies pour des tâches d'export spécifiques.

* **Export ZIP (Tout l'EDS vers FHIR)** :
```bash
chu-fhir edsan-to-fhir-zip --output exports/data.zip

```


* **Push complet (Tout l'EDS vers serveur FHIR)** :
```bash
chu-fhir edsan-to-fhir-push

```


* **Upload manuel d'un Bundle JSON** :
Envoie n'importe quel fichier JSON (Transaction/Batch) local vers le serveur FHIR.
```bash
chu-fhir upload-bundle ./mon_bundle_custom.json

```



### 📁 5. Historique et Rapports

Gérez la traçabilité des imports et des exports.

#### Rapports d'Import (Conversion FHIR ➔ EDS)

* **Consulter le dernier rapport** :
```bash
chu-fhir last-run

```


* **Télécharger le dernier rapport (JSON)** :
```bash
chu-fhir download-last-run --out logs/dernier_import.json

```


* **Lister l'historique des imports** :
```bash
chu-fhir runs

```


* **Télécharger un rapport spécifique** :
```bash
chu-fhir download-run <nom_du_fichier> --out logs/vieux_log.json

```



#### Rapports d'Export (Conversion EDS ➔ FHIR)

* **Consulter le dernier rapport d'export** :
```bash
chu-fhir last-export

```


* **Télécharger le dernier rapport d'export** :
```bash
chu-fhir download-last-export --out logs/dernier_export.json

```


* **Lister l'historique des exports** :
```bash
chu-fhir export-runs

```


* **Télécharger un rapport d'export spécifique** :
```bash
chu-fhir download-export-run <nom_du_fichier> --out logs/export_specifique.json

```
---

## 📁 Structure du Projet

* `src/main.py` : Logique principale utilisant **Typer** pour le CLI et **Rich** pour les affichages en tableau.
* `pyproject.toml` : Configuration du package et définition du point d'entrée `chu-fhir`.

* `Dockerfile` : Image basée sur `python:3.12-slim` pour un déploiement léger.
