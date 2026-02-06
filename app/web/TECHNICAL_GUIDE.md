# app/web

Ce dossier fait partie du projet **PING 37 – Interopérabilité (FHIR ↔ EDSaN)**.

## Rôle

Couche Web (routes HTML / UI simple) : routes FastAPI/Starlette pour servir des pages/templates et assets statiques.

## Contenu

### `routes.py`

#### Fonctions / classes principales

- `_load_json_if_exists()` — Petit helper pour éviter de répéter try/except partout.

- `load_last_run()` — Charge le dernier rapport d'import généré par la conversion.

- `load_last_export()` — Charge le dernier rapport d'export généré par l'export EDS->FHIR.

- `_effective_eds_dir()` — Détermine le dossier EDS "effectif" pour l'UI.

- `merged_cfg()` — Fusionne la config issue d'un payload (UI/POST) avec l'environnement.

- `list_parquets()` — Liste les .parquet du dossier EDS.

- `ui_home()`

- `import_page()`

- `import_run()` — Import UI :

- `eds_page()`

- `eds_preview()`

- `stats_page()`

- `stats_data()`

- `ui_home_data()`

- `ui_convert()`

- `ui_convert_run()`

- `ui_export()`

- `ui_export_download()`

- `eds_meta()`


#### Utilisé par

- `app/main.py`


#### Références (fonctions/classes) dans le projet

- `eds_preview` → `client_pkg/src/main.py`



## Notes
