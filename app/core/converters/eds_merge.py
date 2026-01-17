# app/core/converters/eds_merge.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import polars as pl


@dataclass
class MergeReport:
    table: str
    before_rows: int
    incoming_rows: int
    after_rows: int
    added_rows: int


def _read_parquet_if_exists(path: Path) -> pl.DataFrame | None:
    if path.exists():
        return pl.read_parquet(path)
    return None


def _safe_concat(base: pl.DataFrame | None, incoming: pl.DataFrame) -> pl.DataFrame:
    if base is None:
        return incoming
    # aligne les colonnes si besoin (colonnes manquantes => null)
    all_cols = sorted(set(base.columns) | set(incoming.columns))
    base2 = base.with_columns([pl.lit(None).alias(c) for c in all_cols if c not in base.columns]).select(all_cols)
    inc2 = incoming.with_columns([pl.lit(None).alias(c) for c in all_cols if c not in incoming.columns]).select(all_cols)
    return pl.concat([base2, inc2], how="vertical_relaxed")


def _fill_null_keys(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """
    Remplace les null sur les colonnes de clé par une valeur neutre.
    Utile pour les joins/anti-joins (Polars n'aime pas les clés null en hash join).
    """
    exprs = []
    for k in keys:
        if k in df.columns:
            # cast en Utf8 pour éviter des soucis (ex: Date/Int/Null mix)
            exprs.append(pl.col(k).cast(pl.Utf8, strict=False).fill_null("").alias(k))
    if exprs:
        return df.with_columns(exprs)
    return df


def merge_table(
    eds_dir: str | Path,
    incoming_dir: str | Path,
    table_name: str,
    unique_keys: list[str],
) -> MergeReport:
    eds_dir = Path(eds_dir)
    incoming_dir = Path(incoming_dir)

    base_path = eds_dir / table_name
    inc_path = incoming_dir / table_name

    base = _read_parquet_if_exists(base_path)
    incoming = pl.read_parquet(inc_path)

    before_rows = 0 if base is None else base.height
    incoming_rows = incoming.height

    # si aucune base, on écrit direct
    if base is None:
        incoming.write_parquet(base_path)
        return MergeReport(
            table=table_name,
            before_rows=0,
            incoming_rows=incoming_rows,
            after_rows=incoming_rows,
            added_rows=incoming_rows,
        )

    # aligner colonnes (utile pour avoir des colonnes compatibles)
    merged_full = _safe_concat(base, incoming)

    # MERGE SAFE: on n'enlève jamais des lignes
    if unique_keys:
        # Remplace nulls sur les colonnes de clé (important pour DOCEDS & co)
        base_norm = _fill_null_keys(base, unique_keys)
        inc_norm = _fill_null_keys(incoming, unique_keys)

        # On garde toutes les lignes de base
        # Et on ajoute seulement les lignes incoming dont la clé n'existe pas dans base
        base_keys = base_norm.select(unique_keys).unique()

        # anti-join: lignes incoming dont les keys ne sont pas dans base
        inc_new = inc_norm.join(base_keys, on=unique_keys, how="anti")

        final_df = _safe_concat(base, inc_new)
    else:
        # pas de clés => on concatène tout (append)
        final_df = merged_full

    after_rows = final_df.height
    added_rows = after_rows - before_rows

    final_df.write_parquet(base_path)

    return MergeReport(
        table=table_name,
        before_rows=before_rows,
        incoming_rows=incoming_rows,
        after_rows=after_rows,
        added_rows=added_rows,
    )


def merge_run_into_eds(
    eds_dir: str | Path,
    run_dir: str | Path,
    table_names: list[str],
    keys_by_table: dict[str, list[str]],
) -> list[MergeReport]:
    reports: list[MergeReport] = []
    for t in table_names:
        # on ignore patient.parquet si vous le gardez interne
        if t == "patient.parquet":
            continue
        keys = keys_by_table.get(t, [])
        reports.append(merge_table(eds_dir, run_dir, t, keys))
    return reports
