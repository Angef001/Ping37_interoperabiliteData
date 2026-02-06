# app/core/converters

Ce dossier fait partie du projet **PING 37 – Interopérabilité (FHIR ↔ EDSaN)**.

## Rôle

Conversion des données entre formats (FHIR ↔ EDSaN) et logique de merge des Parquet.

## Contenu

### `__init__.py`

- Marqueur de package.



### `eds_merge.py`

- Fusion (merge) des Parquet EDSaN (déduplication via clés, ajout de lignes, reporting).

#### Fonctions / classes principales

- `_read_parquet_if_exists()`

- `_safe_concat()` — Concat vertical robuste:

- `_fill_null_keys()` — Remplace les null sur les colonnes de clé par une valeur neutre.

- `merge_table()`

- `merge_run_into_eds()`

- `class MergeReport`


#### Utilisé par

- `app/core/converters/fhir_to_edsan.py`


#### Références (fonctions/classes) dans le projet

- `merge_run_into_eds` → `app/core/converters/fhir_to_edsan.py`



### `edsan_to_fhir.py`

- Conversion EDSaN → FHIR (génération de Bundles JSON + push optionnel vers serveur FHIR).

#### Fonctions / classes principales

- `encode_base64()`

- `is_missing()`

- `stable_id()`

- `normalize_fhir_id()`

- `normalize_gender()`

- `ensure_xhtml_div()`

- `_parse_path()`

- `set_path()`

- `build_resource()`

- `coerce_value()`

- `make_location_stub()`

- `build_transaction_bundle()`

- `push_bundle_to_fhir()`

- `get_patient_id()`

- `save_export_report()`

- `export_eds_to_fhir()`


#### Utilisé par

- `app/api/endpoints.py`

- `app/utils/filter_then_export_edsan_to_fhir.py`

- `app/web/routes.py`

- `client_pkg/src/edsan_filter_to_fhir.py`


#### Références (fonctions/classes) dans le projet

- `export_eds_to_fhir` → `app/api/endpoints.py`, `app/utils/filter_then_export_edsan_to_fhir.py`, `app/web/routes.py`, `client_pkg/src/edsan_filter_to_fhir.py`

- `save_export_report` → `app/api/endpoints.py`



### `fhir_to_edsan.py`

- Conversion FHIR → EDSaN (parsing Bundle, extraction ressources, écriture/merge Parquet).

#### Fonctions / classes principales

- `_dtype_from_str()` — Convertit une string de mapping.json (_schemas) en dtype Polars.

- `_normalize_value()` — Normalise une valeur brute extraite d'un JSON FHIR selon le type attendu.

- `build_eds()` — Construit les tables Parquet de l'EDS a partir des bundles FHIR.

- `process_dir()` — Phase 3 (FHIR -> EDS) : traite un dossier de bundles FHIR,

- `process_bundle()` — Phase 3 (FHIR -> EDS) : traite un bundle FHIR (dict),


#### Utilisé par

- `app/api/endpoints.py`

- `app/web/routes.py`


#### Références (fonctions/classes) dans le projet

- `_normalize_value` → `app/utils/helpers.py`

- `process_bundle` → `app/api/endpoints.py`, `app/web/routes.py`

- `process_dir` → `app/api/endpoints.py`, `app/web/routes.py`



## Notes
