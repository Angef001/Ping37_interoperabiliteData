# client_pkg/src

Ce dossier fait partie du projet **PING 37 – Interopérabilité (FHIR ↔ EDSaN)**.

## Rôle

Code source du client CLI (Typer/Rich) utilisé pour piloter l’entrepôt FHIR et l’API Converter.

## Contenu

### `__init__.py`

- Marqueur de package.



### `display_edsan.py`

#### Fonctions / classes principales

- `_clip()` — Coupe une valeur pour éviter les cellules immenses.

- `_print_preview()`

- `display_eds()`


#### Utilisé par

- `client_pkg/src/main.py`



### `edsan_filter.py`

#### Fonctions / classes principales

- `_count_rows_parquet_dir()`

- `edsan_filter()` — Filtre un EDS (Parquet) avec des conditions dynamiques (--where)


#### Utilisé par

- `client_pkg/src/main.py`


#### Références (fonctions/classes) dans le projet

- `_count_rows_parquet_dir` → `client_pkg/src/edsan_filter_to_fhir.py`

- `edsan_filter` → `client_pkg/src/main.py`



### `edsan_filter_to_fhir.py`

#### Fonctions / classes principales

- `_push_bundles_to_fhir()`

- `_count_rows_parquet_dir()`

- `edsan_filter_to_fhir()` — Pipeline:


#### Utilisé par

- `client_pkg/src/main.py`


#### Références (fonctions/classes) dans le projet

- `_count_rows_parquet_dir` → `client_pkg/src/edsan_filter.py`

- `edsan_filter_to_fhir` → `client_pkg/src/main.py`



### `edsan_to_fhir_cli.py`

#### Fonctions / classes principales

- `cli()` — CLI pour la conversion EDSan → FHIR

- `export_zip()` — Convertir EDSan → FHIR et télécharger un ZIP

- `push_warehouse()` — Convertir EDSan → FHIR et pousser vers l'entrepôt FHIR



### `import_url.py`

- Commande CLI `import-url` : import ciblé via URL de requête FHIR → EDSaN.

#### Fonctions / classes principales

- `import_url()` — Import FHIR (entrepôt) -> EDS via URL de requête.


#### Utilisé par

- `client_pkg/src/main.py`


#### Références (fonctions/classes) dans le projet

- `import_url` → `client_pkg/src/main.py`



### `main.py`

- Point d’entrée CLI `chu-fhir` : enregistre toutes les commandes (FHIR, conversion, EDS, reports, export).

#### Fonctions / classes principales

- `_raise_if_error()`

- `info()` — Vérifie si le serveur FHIR est en ligne (metadata).

- `_patient_row()`

- `get_patient()` — Récupère un patient unique par ID.

- `get_patients()` — Récupère plusieurs patients par IDs.

- `get_resource()` — Affiche le JSON brut d'une ressource.

- `warehouse_convert()` — Equivalent 'convert dossier' mais depuis l'entrepôt HAPI.

- `warehouse_convert_patient()` — Equivalent '1 fichier Synthea patient' mais depuis l'entrepôt.

- `warehouse_convert_patients()` — Convertit une LISTE de patients depuis l'entrepôt (HAPI) vers EDSan (parquet).

- `eds_tables()` — Liste les tables EDS disponibles.

- `eds_preview()` — Preview d’une table parquet avec limite choisie par l’utilisateur.

- `stats()` — Affiche les stats EDS.

- `last_run()` — Affiche le last_run.json.

- `runs()` — Liste l’historique des runs (archives).

- `download_run()` — Télécharge un run archivé.

- `download_last_run()` — Télécharge le last_run.json le plus récent.

- `edsan_to_fhir_zip()` — Convertir EDSan → FHIR et télécharger un ZIP.

- `edsan_to_fhir_push()` — Convertir EDSan → FHIR et pousser vers l'entrepôt FHIR.

- `last_export()` — Affiche le dernier rapport d'exportation (EDSan -> FHIR).

- `export_runs()` — Liste l’historique des exports archivés (EDSan -> FHIR).

- `download_export_run()` — Télécharge un rapport d'export archivé spécifique.

- `download_last_export()` — Télécharge le rapport d'export le plus récent (last_export_fhir.json).

- `eds_delete()` — Supprime des enregistrements d'une table EDS par ID.

- `upload_bundle()` — Envoie un Bundle FHIR (transaction/batch) au serveur.


#### Références (fonctions/classes) dans le projet

- `download_export_run` → `app/api/endpoints.py`

- `download_run` → `app/api/endpoints.py`

- `eds_preview` → `app/web/routes.py`

- `edsan_to_fhir_zip` → `app/api/endpoints.py`

- `info` → `app/api/endpoints.py`, `app/core/converters/edsan_to_fhir.py`, `client_pkg/src/display_edsan.py`

- `last_run` → `app/api/endpoints.py`, `app/utils/helpers.py`, `app/web/routes.py`, `client_pkg/src/edsan_filter_to_fhir.py`, `client_pkg/src/import_url.py`

- `runs` → `app/api/endpoints.py`, `app/utils/helpers.py`, `app/web/routes.py`

- `stats` → `app/api/endpoints.py`, `app/web/routes.py`, `client_pkg/src/display_edsan.py`, `client_pkg/src/edsan_filter.py`, `client_pkg/src/edsan_filter_to_fhir.py` (+1 autres)



## Notes

- Le CLI dépend des variables d’environnement `FHIR_URL` (entrepôt HAPI) et `CONVERTER_API_URL` (API Converter).

- Si une commande n’apparaît pas, vérifier que `main.py` l’enregistre bien (decorator `@app.command()` ou `app.command(...)(func)`).
