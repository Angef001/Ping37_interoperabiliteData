# app/utils

Ce dossier fait partie du projet **PING 37 – Interopérabilité (FHIR ↔ EDSaN)**.

## Rôle

Fonctions utilitaires partagées (fetch FHIR paginé, zip, stats, reporting).

## Contenu

### `__init__.py`

- Marqueur de package.



### `display_eds.py`

#### Fonctions / classes principales

- `display_tables()` — Parcourt les fichiers Parquet attendus dans le dossier EDS

- `main()`


#### Utilisé par

- `client_pkg/src/main.py`


#### Références (fonctions/classes) dans le projet

- `main` → `app/utils/filter_dataset.py`, `app/utils/filter_then_export_edsan_to_fhir.py`



### `filter_dataset.py`

#### Fonctions / classes principales

- `_split_csv()`

- `_strip_quotes()`

- `_parse_table_pattern()`

- `_read_list_file()`

- `_parse_value_token()`

- `parse_where()`

- `parse_propagate()`

- `_to_expr()`

- `filter_folder()`

- `filter_dataset()` — - Filtre TOUJOURS dans un dossier temporaire

- `main()`

- `class WhereClause`

- `class PropagateSpec`


#### Utilisé par

- `app/utils/filter_then_export_edsan_to_fhir.py`

- `client_pkg/src/edsan_filter.py`

- `client_pkg/src/edsan_filter_to_fhir.py`


#### Références (fonctions/classes) dans le projet

- `_split_csv` → `app/utils/filter_then_export_edsan_to_fhir.py`

- `filter_dataset` → `app/utils/filter_then_export_edsan_to_fhir.py`, `client_pkg/src/edsan_filter.py`, `client_pkg/src/edsan_filter_to_fhir.py`

- `filter_folder` → `app/utils/filter_then_export_edsan_to_fhir.py`, `client_pkg/src/edsan_filter.py`, `client_pkg/src/edsan_filter_to_fhir.py`

- `main` → `app/utils/display_eds.py`, `app/utils/filter_then_export_edsan_to_fhir.py`



### `filter_then_export_edsan_to_fhir.py`

#### Fonctions / classes principales

- `main()`


#### Références (fonctions/classes) dans le projet

- `main` → `app/utils/display_eds.py`, `app/utils/filter_dataset.py`



### `helpers.py`

- Fonctions utilitaires (pagination FHIR, statistiques parquet, zip, génération/archivage de reports).

#### Fonctions / classes principales

- `clean_id()` — Nettoie les identifiants FHIR pour ne garder que la partie unique.

- `_normalize_value()` — Normalise une valeur brute issue du JSON FHIR selon le type attendu (_schemas).

- `format_fhir_date()` — Normalise les dates pour l'affichage ou le stockage.

- `get_coding_value()` — Extrait un code d'un CodeableConcept FHIR selon un système (ex: CIM-10).

- `compute_age()` — Calcule l'âge à une date de référence.

- `get_value_from_path()` — Navigue dans un JSON via un chemin type 'a.b[0].c'.

- `load_json_flexible()` — Charge un JSON robuste (mapping.json) même si le fichier est "sale".

- `_compute_expected_columns()` — Construit les colonnes attendues par table (ordre stable)..

- `enforce_schema()` — Garde exactement les colonnes attendues (et leur ordre) selon expected_columns.

- `_coalesce_from()` — Remplit target avec src quand target est null, puis supprime src.

- `write_last_run_report()` — Ecrit le dernier report (import / export).

- `_fetch_bundle_all_pages()` — Récupère un Bundle FHIR (searchset / $everything) en suivant la pagination (link[next]).

- `_collect_patient_ids()` — Récupère les IDs Patient depuis l'entrepôt en paginant.

- `summarize_bundle()` — Retourne:

- `_zip_folder()`

- `_coalesce_from_path()` — Remplit target avec src quand target est null, puis supprime src.

- `parquet_row_count()` — Retourne le nombre de lignes d'un parquet, 0 si fichier absent.

- `snapshot_eds_counts()` — Prend un snapshot {table: nb_lignes} dans eds_dir.

- `build_merge_report()` — Construit un merge_report final cohérent :


#### Utilisé par

- `app/api/endpoints.py`

- `app/core/converters/edsan_to_fhir.py`

- `app/core/converters/fhir_to_edsan.py`


#### Références (fonctions/classes) dans le projet

- `_coalesce_from` → `app/core/converters/fhir_to_edsan.py`

- `_coalesce_from_path` → `app/core/converters/fhir_to_edsan.py`

- `_collect_patient_ids` → `app/api/endpoints.py`

- `_compute_expected_columns` → `app/core/converters/fhir_to_edsan.py`

- `_fetch_bundle_all_pages` → `app/api/endpoints.py`

- `_normalize_value` → `app/core/converters/fhir_to_edsan.py`

- `_zip_folder` → `app/api/endpoints.py`

- `build_merge_report` → `app/api/endpoints.py`

- `clean_id` → `app/core/converters/edsan_to_fhir.py`

- `compute_age` → `app/core/converters/fhir_to_edsan.py`

- `enforce_schema` → `app/core/converters/fhir_to_edsan.py`

- `format_fhir_date` → `app/core/converters/edsan_to_fhir.py`

- `get_value_from_path` → `app/core/converters/fhir_to_edsan.py`

- `load_json_flexible` → `app/core/converters/fhir_to_edsan.py`

- `snapshot_eds_counts` → `app/api/endpoints.py`

- `summarize_bundle` → `app/api/endpoints.py`

- `write_last_run_report` → `app/api/endpoints.py`



## Notes

- Les helpers sont volontairement « transverses » : une modification ici peut impacter plusieurs endpoints/converters.
