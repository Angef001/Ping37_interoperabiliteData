# 🚀 FHIR-EDS Transformer API

Cette API FastAPI est le moteur de transformation du projet PING. Elle assure la conversion bidirectionnelle des données de santé entre l'Entrepôt de Données de Santé (**EDSaN/Parquet**) et le standard **FHIR (JSON/HAPI)**.

## 📑 Sommaire

* Installation & Lancement
* Catalogue Complet des Endpoints
* Flux de Conversion
* Configuration (Mapping)

---

## 🚀 Installation & Lancement

L'utilisation de `podman-compose` est la méthode recommandée pour garantir l'interopérabilité réseau entre l'API et le serveur FHIR.

### Lancement avec Podman-compose

Depuis la racine du projet (contenant le fichier `podman-compose.yml`):

```bash
# 1. Construire et lancer l'API en arrière-plan
podman-compose up -d api-converter

# 2. Vérifier l'état des conteneurs
podman ps

# 3. Consulter les logs en temps réel
podman logs -f api-converter

```

*L'API est accessible par défaut sur `http://localhost:8000`.*

---

### Lancement en local dans un environnement virtuel (VENV)

Pour modifier le code ou exécuter les scripts de conversion manuellement, il est impératif d'utiliser un environnement virtuel Python.

### 1. Création de l'environnement virtuel

```bash
# Créer le dossier .venv à la racine
python3 -m venv .venv

```

### 2. Activation

* **Sur Linux / WSL / macOS :**
```bash
source .venv/bin/activate

```


### 3. Installation des dépendances

```bash
pip install --upgrade pip
pip install -r requirements.txt

```

### 4. Lancement de l'API de conversion
Une fois les données chargées, vous pouvez démarrer le serveur FastAPI en utilisant Uvicorn :

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

```
--reload : Permet de redémarrer automatiquement le serveur à chaque modification du code source.

Accès : L'interface Swagger sera disponible sur http://localhost:8000/docs
---

## 📡 Catalogue Complet des Endpoints

### 🔄 Conversion & Import (FHIR → EDS)

* **`POST /api/v1/convert/fhir-query-to-edsan`** : Importe des données en exécutant une requête FHIR spécifique (URL fournie dans le payload). Génère un rapport de run standard.
* **`POST /api/v1/convert/fhir-warehouse-to-edsan`** : Déclenche l'ETL complet depuis l'entrepôt HAPI FHIR vers les fichiers Parquet. Supporte la pagination et une limite de patients via le payload.
* **`POST /api/v1/convert/fhir-warehouse-patients-to-edsan`** : Convertit une liste spécifique d'identifiants patients (`patient_ids`) depuis l'entrepôt.
* **`POST /api/v1/convert/fhir-warehouse-patient-to-edsan`** : Convertit un patient unique de l'entrepôt via son `patient_id`.

### 📤 Export & Envoi (EDS → FHIR)

* **`POST /api/v1/export/edsan-to-fhir-warehouse`** : Convertit l'EDS local en bundles FHIR et les pousse directement vers le serveur FHIR configuré.
* **`POST /api/v1/export/edsan-to-fhir-zip`** : Convertit l'EDS local en bundles FHIR et génère une archive ZIP téléchargeable.
* **`GET /api/v1/export/eds-zip`** : Exporte les modules EDSaN bruts (mvt, biol, pharma, doceds, pmsi) en un fichier ZIP de parquets.

### 📊 Consultation & Statistiques

* **`GET /api/v1/eds/tables`** : Liste les fichiers `.parquet` disponibles dans le stockage EDS.
* **`GET /api/v1/eds/table/{name}`** : Affiche un aperçu (lignes et colonnes) d'une table spécifique.
* **`GET /api/v1/stats`** : Statistiques sur le volume de données par table. Accepte désormais un paramètre optionnel `eds_dir` pour cibler un dossier spécifique.

### 📝 Rapports de Run (Imports)

* **`GET /api/v1/report/last-run`** : Récupère le rapport détaillé de la dernière conversion (Import FHIR → EDS) effectuée.
* **`GET /api/v1/report/runs`** : Liste l'historique de tous les rapports d'import archivés.
* **`GET /api/v1/report/run/{name}`** : Télécharge un fichier de rapport d'archive spécifique.

### 📝 Rapports d'Export (EDS → FHIR)

* **`GET /api/v1/report/last-export`** : Récupère le dernier rapport d'exportation généré.
* **`GET /api/v1/report/export-runs`** : Liste l'historique des exports archivés.
* **`GET /api/v1/report/export-run/{name}`** : Télécharge un rapport d'export spécifique.

### 🖥️ Utilitaires

* **`GET /`** : Page d'accueil et statut du service.
* **`GET /docs`** : Documentation interactive Swagger UI.
* **`GET /ui/export/fhir`** : Interface HTML pour visualiser ou déclencher l'export FHIR.

---

## 🔄 Flux de Conversion

### 1. FHIR → EDS (Import)

Le module `fhir_to_edsan.py` réalise l'extraction depuis HAPI ou des fichiers locaux.

* **Nettoyage** : Les identifiants sont normalisés (suppression des préfixes `urn:uuid:`, etc.).
* **Fusion (Merge)** : Le script `eds_merge.py` compare les nouvelles données avec l'existant pour éviter les doublons via des clés d'unicité.

### 2. EDS → FHIR (Export)

Le module `edsan_to_fhir.py` transforme les tables Parquet en bundles transactionnels.

* **Intégrité** : Le système génère automatiquement des ressources "Stubs" (ex: `Location`) si elles sont référencées dans un séjour mais absentes de la source EDS.

---

## ⚙️ Configuration (Mapping)

Le fichier `app/core/config/mapping.json` définit la correspondance entre les ressources FHIR et les colonnes des tables Parquet. Il contient également les schémas attendus pour garantir la qualité des données lors de la génération des fichiers.
