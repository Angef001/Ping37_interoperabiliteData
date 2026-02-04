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

* **Importer des données FHIR vers l’EDS à partir d’une requête FHIR complète** 
```bash
chu-fhir import-url --url "<URL_DE_REQUETE_FHIR>"

Ex: chu-fhir import-url --url "http://localhost:8080/fhir/Encounter?_count=100"

```

* **Importer des données FHIR vers un dossier EDS spécifique**  

```bash
chu-fhir import-url --url "<URL_DE_REQUETE_FHIR>" --eds-dir <CHEMIN_DOSSIER_EDS>


Ex: chu-fhir import-url  --url "http://localhost:8080/fhir/Encounter?_count=100" --eds-dir data/eds_custom

```


* **Importer des données FHIR et afficher les statistiques complètes du run** 

```bash
chu-fhir import-url --url "<URL_DE_REQUETE_FHIR>" --stats


Ex: chu-fhir import-url --url "http://localhost:8080/fhir/Encounter?_count=100" --stats
```

* **Combiner tous les paramètres dans une seule commande**

```bash
chu-fhir import-url --url "<URL_DE_REQUETE_FHIR>" --eds-dir <CHEMIN_DOSSIER_EDS>  --stats


Exemple :

chu-fhir import-url \
  --url "http://localhost:8080/fhir/Encounter?_count=100" \
  --eds-dir data/eds_custom \
  --stats
```
* **NB**

Le paramètre --eds-dir permet de choisir dynamiquement le dossier EDS dans lequel les fichiers parquet sont générés.

Les rapports d’exécution (last_run.json et l’historique des runs) sont toujours stockés dans le dossier de reports par défaut du projet, indépendamment du dossier EDS choisi.

Cela garantit une centralisation cohérente des rapports, même lorsqu’on travaille avec plusieurs EDS en parallèle.

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

Cette partie transforme un **EDS (fichiers Parquet)** en **ressources FHIR**.

Les commandes permettent de :

* filtrer l’EDS via des conditions (`--where`)
* propager une clé pour garder la cohérence entre tables (`--propagate`)
* mesurer l’impact avec des statistiques (`--stats`)
* exporter en fichiers FHIR et éventuellement pousser vers HAPI (`--push`)

---

#### 🔧 Paramètres communs (à connaître)

* **`--input-dir <chemin>`**
  Dossier contenant l’EDS source (fichiers `.parquet`).

* **`--fhir-output-dir <chemin>`**
  Dossier où seront écrits les bundles / fichiers FHIR générés.

* **`--filtered-output-dir <chemin>`** *(optionnel)*
  Dossier où seront écrits les parquets filtrés (audit EDS ↔ FHIR).

* **`--where "<table>:<expression>"`**
  Condition de filtrage.
  Format : `table:expression`
  Exemple :

  ```text
  patient:PATAGE<10
  ```

  Interprétation : garder uniquement les lignes de la table `patient` dont `PATAGE < 10`.

   Le paramètre `--where` peut être répété : c’est un **ET logique (AND)**.

* **`--propagate "<CLE>:<table_source>"`**
  Propage la sélection via une clé.
  Exemple :

  ```text
  PATID:patient
  ```

  Interprétation : on garde les `PATID` sélectionnés dans `patient`, puis on filtre toutes les autres tables sur ces `PATID`.

* **`--stats`**
  Affiche un tableau de statistiques (lignes avant/après côté EDS, volumes de ressources FHIR générées).

* **`--push`** et **`--fhir-url <url>`**

  * `--push` : envoie les ressources générées vers le serveur FHIR
  * `--fhir-url` : URL de base du serveur (ex. `http://localhost:8080/fhir`)

---

#### 3.1 Export FHIR sans filtre (référence de volume)

```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --fhir-output-dir /mnt/c/Users/User/Downloads/fhir_ref_all \
  --stats
```

**Ce que fait la requête**

* Absence de `--where` ⇒ conversion de **tout l’EDS**
* Écriture des bundles FHIR dans `fhir_ref_all`
* Affichage des statistiques

**Résultat attendu**

* Volume FHIR maximal (référence)
* Lignes filtrées ≃ lignes d’entrée

---

#### 3.2 Export filtré “patients < 10 ans” + propagation (cohérence globale)

```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --fhir-output-dir /mnt/c/Users/User/Downloads/fhir_age_lt_10 \
  --where "patient:PATAGE<10" \
  --propagate "PATID:patient" \
  --stats
```

**Explication des paramètres**

* **`--where "patient:PATAGE<10"`** : sélectionne uniquement les patients dont `PATAGE < 10`
* **`--propagate "PATID:patient"`** : filtre toutes les tables à partir des `PATID` sélectionnés

**Pourquoi c’est important**

* Sans propagation, certaines tables resteraient incohérentes
* Avec propagation, on obtient un **sous-EDS cohérent**

**Résultat attendu**

* Baisse sur `patient` **et** sur les autres tables
* Moins de ressources FHIR générées

---

#### 3.3 Export multi-conditions (AND logique) : “femmes > 40 ans”

```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --fhir-output-dir /mnt/c/Users/User/Downloads/fhir_age_gt_40_female \
  --where "patient:PATAGE>40" \
  --where "patient:PATSEX==F" \
  --propagate "PATID:patient" \
  --stats
```

**Explication des paramètres**

* Premier `--where` : patients d’âge > 40
* Deuxième `--where` : patients de sexe féminin
* Deux `--where` ⇒ **AND logique**
* `--propagate` garantit la cohérence inter-tables

**Résultat attendu**

* Sous-ensemble plus restreint
* Diminution cohérente des volumes FHIR

---

#### 3.4 Export avec copie des parquets filtrés (audit EDS ↔ FHIR)

```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --filtered-output-dir /mnt/c/Users/User/Downloads/eds_filtered_check \
  --fhir-output-dir /mnt/c/Users/User/Downloads/fhir_with_check \
  --where "patient:PATAGE<10" \
  --propagate "PATID:patient" \
  --stats
```

**Explication des paramètres**

* **`--filtered-output-dir`** : écrit le nouvel EDS filtré (parquets)
* **`--fhir-output-dir`** : écrit les bundles FHIR correspondants

**Résultat attendu**

* Vérification possible de la cohérence EDS ↔ FHIR
* Correspondance directe volumes / ressources

---

#### 3.5 Export + push vers HAPI (intégration directe)

```bash
chu-fhir edsan-filter-to-fhir \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --fhir-output-dir /mnt/c/Users/User/Downloads/fhir_push_test \
  --where "patient:PATAGE<10" \
  --propagate "PATID:patient" \
  --push \
  --fhir-url http://localhost:8080/fhir \
  --stats
```

**Explication des paramètres**

* **`--push`** : envoi des ressources vers l’entrepôt FHIR
* **`--fhir-url`** : URL du serveur HAPI

**Résultat attendu**

* Push réussi
* Ressources visibles côté serveur FHIR


## 3.6 Filtrage EDS et visualisation des résultats

Cette section illustre le **filtrage d’un EDS (fichiers Parquet)** à partir d’un critère clinique patient, puis la **visualisation des données filtrées**, afin de vérifier l’impact du filtrage et la cohérence des tables produites.

---

### 3.6.1 Filtrage de l’EDS avec critère patient

Filtrage des patients de moins de 6 ans, avec propagation de la clé patient (`PATID`) pour garantir la cohérence entre toutes les tables de l’EDS.

```bash
chu-fhir edsan-filter \
  --input-dir /mnt/c/Projets/Ping37_interoperabiliteData/eds \
  --output-dir /mnt/c/Users/User/Downloads/eds_no_propag \
  --where "patient:PATAGE<6" \
  --propagate "PATID:patient" \
  --stats
```

**Explication des paramètres :**

* `--where "patient:PATAGE<6"` : sélectionne uniquement les patients âgés de moins de 6 ans
* `--propagate "PATID:patient"` : propage la sélection patient à l’ensemble des tables liées
* `--stats` : affiche les statistiques avant/après filtrage

**Résultat attendu :**

* Réduction cohérente de toutes les tables de l’EDS
* Données restantes uniquement liées aux patients sélectionnés

---

### 3.6.2 Visualisation de l’EDS filtré

Affichage du contenu de l’EDS filtré afin de vérifier visuellement la cohérence des tables générées.

```bash
chu-fhir display-eds \
  --eds-dir /mnt/c/Users/User/Downloads/eds_no_propag
```

**Objectif :**

* Visualiser les tables Parquet produites après filtrage
* Vérifier que toutes les tables sont cohérentes avec le critère clinique appliqué

---

### 💡 Intérêt métier et technique

Cette séquence permet de :

* démontrer l’impact réel d’un filtre clinique sur un EDS
* valider la **cohérence référentielle inter-tables**
* contrôler les résultats avant une conversion ultérieure vers FHIR



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
