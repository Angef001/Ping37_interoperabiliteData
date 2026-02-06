# app/api

Ce dossier fait partie du projet **PING 37 – Interopérabilité (FHIR ↔ EDSaN)**.

## Rôle

Routes FastAPI (endpoints HTTP) exposées par le service de conversion.

## Contenu

### `__init__.py`

- Marqueur de package.



### `endpoints.py`

- Définit le `APIRouter` FastAPI et les endpoints de conversion/import/export + consultation EDS.

#### Fonctions / classes principales

- `_list_existing_tables()` — Liste tous les fichiers .parquet déjà présents dans le dossier EDS.

- `_pick()` — Récupère un param :

- `_override_module_attrs()` — Override temporaire d'attributs de module (EDS_DIR, REPORTS_DIR, etc.)

- `convert_fhir_query_to_edsan()` — Import principal demandé par les commanditaires :

- `convert_fhir_warehouse_to_edsan()`

- `convert_list_patients_from_warehouse()`

- `convert_one_patient_from_warehouse()`

- `list_eds_tables()` — Liste les fichiers .parquet disponibles dans le dossier eds/

- `read_eds_table()` — Retourne un aperçu (head) d'une table parquet.

- `export_eds_zip()` — Exporte les 5 modules EDSaN (sans patient.parquet) en un fichier ZIP téléchargeable.

- `get_last_run_report()` — Retourne le dernier rapport de run (report/last_run.json) généré par process_dir/process_bundle.

- `get_stats()` — Statistiques rapides sur les parquets EDS (rows/cols par table).

- `list_runs()` — Liste l'historique des runs (archives).

- `download_run()` — Télécharge un run archivé.

- `edsan_to_fhir_zip()` — Convertit EDSAN -> FHIR, génère les bundles JSON puis renvoie un ZIP.

- `edsan_to_fhir_warehouse()` — Convertit EDSAN -> FHIR puis pousse les bundles vers le serveur FHIR.

- `get_last_export_report()` — Retourne le dernier rapport d'exportation (EDSan -> FHIR) 

- `list_export_runs()` — Liste l'historique des exports archivés dans le sous-dossier exports/.

- `download_export_run()` — Télécharge un rapport d'export archivé spécifique.


#### Utilisé par

- `app/main.py`


#### Références (fonctions/classes) dans le projet

- `download_export_run` → `client_pkg/src/main.py`

- `download_run` → `client_pkg/src/main.py`

- `edsan_to_fhir_zip` → `client_pkg/src/main.py`



## Notes

- Les routes sont généralement montées par l’application FastAPI principale (voir `app/main.py` ou équivalent).

- Les endpoints d’import s’appuient sur les fonctions de `app.utils.helpers` et sur `app.core.converters`.
