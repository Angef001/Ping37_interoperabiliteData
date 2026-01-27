# 🏥 Projet PING : Interopérabilité FHIR-EDS (CHU Rouen)

Ce projet implémente une solution complète de transformation et d'échange de données de santé entre un entrepôt standardisé **FHIR** (Fast Healthcare Interoperability Resources) et un format analytique **EDS** (Entrepôt de Données de Santé) basé sur des fichiers **Parquet**.

## 🏗️ Architecture et Interactions

Le projet est divisé en trois composants majeurs qui communiquent au sein d'un réseau conteneurisé via **Podman**:

1. **L'Entrepôt FHIR (Serveur HAPI)** : Stocke les données cliniques au format standard. Il sert de source pour l'EDS et de destination pour les exports.
2. **L'API de Conversion (Transformer)** : Le "cerveau" du projet. Elle contient la logique métier (mapping, nettoyage, fusion de données) pour transformer le FHIR en Parquet et inversement.
3. **Le Client CLI (chu-fhir)** : L'interface utilisateur permettant de piloter les conversions, de consulter les statistiques et d'interroger les ressources depuis un terminal.

**Flux de données type :**

* **FHIR → EDS** : L'API extrait les ressources du serveur HAPI, les normalise (via `helpers.py`), les transforme en tables (via `fhir_to_edsan.py`) et les fusionne dans le stockage local (via `eds_merge.py`).
* **EDS → FHIR** : L'API lit les fichiers Parquet, reconstruit des bundles transactionnels (via `edsan_to_fhir.py`) et les renvoie vers le serveur HAPI en assurant l'intégrité référentielle.

---

## 📚 Guide de lecture des documentations

Pour une compréhension optimale, il est recommandé de lire les README dans l'ordre suivant :

1. **`README.md`** dans le dossier `entrepot_fhir`: Comprendre le stockage de base et l'infrastructure HAPI FHIR.
2. **`README.md`** dans le dossier `app`: Découvrir le moteur de transformation, les mappings et les endpoints de conversion.
3. **`README_CLIENT.md`** dans le dossier `client_pkg`: Apprendre à utiliser les commandes pour piloter l'ensemble du système.

---

## 🚀 Installation Rapide (Full Stack)

### 1. Prérequis Système

* **Linux / WSL** (recommandé).
* **Podman** et **Podman-compose** installés.

### 2. Lancement Global

Depuis la racine du projet, lancez l'intégralité de la pile :

```bash
podman-compose up -d

```

Cela démarrera automatiquement le serveur FHIR (port 8080), l'API (port 8000) et préparera le conteneur client.

### 3. Configuration du Développement

Si vous souhaitez travailler sur le code de l'API hors conteneur, créez votre environnement virtuel :

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

```

*(Voir le README de l'API pour les détails spécifiques au développement local)*

---

## 🛠️ Composants Techniques

* **Format EDS** : Fichiers Apache Parquet traités avec la bibliothèque **Polars** pour des performances optimales.
* **Moteur API** : **FastAPI** pour une documentation automatique via Swagger (`/docs`).
* **Mapping** : Piloté par le fichier `mapping.json` qui définit la correspondance entre les JSON-Path FHIR et les colonnes EDS.