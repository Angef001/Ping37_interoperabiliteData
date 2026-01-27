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

---

## 📡 Catalogue Complet des Endpoints

### 🔄 Conversion & Import (FHIR → EDS)

* **`POST /api/v1/convert/fhir-warehouse-to-edsan`** : Déclenche l'ETL complet depuis l'entrepôt HAPI FHIR vers les fichiers Parquet. Supporte la pagination et une limite de patients via le payload.
* **`POST /api/v1/convert/fhir-warehouse-patient-to-edsan`** : Convertit un patient spécifique de l'entrepôt via son `patient_id`.
* **`POST /api/v1/convert/fhir-dir-to-edsan`** : Scanne un dossier local de bundles FHIR pour les convertir en EDS.
* **`POST /api/v1/import/fhir-file`** : Upload manuel d'un fichier JSON Bundle FHIR pour une conversion immédiate.

### 📤 Export & Envoi (EDS → FHIR)

* **`POST /api/v1/export/edsan-to-fhir-zip`** : Convertit l'EDS local en bundles FHIR et génère une archive ZIP.
* **`GET /api/v1/export/eds-zip`** : Exporte les modules EDSaN (mvt, biol, pharma, doceds, pmsi) en un fichier ZIP de parquets.

### 📊 Consultation & Statistiques

* **`GET /api/v1/eds/tables`** : Liste les fichiers `.parquet` disponibles dans le stockage EDS.
* **`GET /api/v1/eds/table/{name}`** : Affiche un aperçu (lignes et colonnes) d'une table spécifique.
* **`GET /api/v1/stats`** : Statistiques sur le volume de données par table (nombre de lignes/colonnes).

### 📝 Rapports de Run

* **`GET /api/v1/report/last-run`** : Récupère le rapport détaillé de la dernière conversion effectuée.
* **`GET /api/v1/report/runs`** : Liste l'historique de tous les rapports archivés.
* **`GET /api/v1/report/run/{name}`** : Télécharge un fichier de rapport d'archive spécifique.

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
