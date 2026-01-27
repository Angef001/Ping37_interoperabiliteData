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


def _safe_concat(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """
    Concat vertical robuste:
    - aligne les colonnes
    - si un même nom de colonne a des types différents entre df1/df2,
      on caste les deux en Utf8 (pour éviter les crashes Polars).
    """
    if df1 is None or df1.height == 0:
        return df2
    if df2 is None or df2.height == 0:
        return df1

    # 1) aligner les colonnes (ajouter les manquantes en null)
    cols = list(dict.fromkeys(list(df1.columns) + list(df2.columns)))  # union en gardant l'ordre

    for c in cols:
        if c not in df1.columns:
            df1 = df1.with_columns(pl.lit(None).alias(c))
        if c not in df2.columns:
            df2 = df2.with_columns(pl.lit(None).alias(c))

    df1 = df1.select(cols)
    df2 = df2.select(cols)

    # 2) harmoniser les types (si mismatch -> cast en Utf8)
    for c in cols:
        t1 = df1.schema.get(c)
        t2 = df2.schema.get(c)
        if t1 != t2:
            df1 = df1.with_columns(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
            df2 = df2.with_columns(pl.col(c).cast(pl.Utf8, strict=False).alias(c))

    # 3) concat
    return pl.concat([df1, df2], how="vertical_relaxed")


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

    # ✅ NEW: si le parquet incoming n’existe pas, on skip proprement
    if not inc_path.exists():
        before_rows = 0
        if base_path.exists():
            base_df = _read_parquet_if_exists(base_path)
            before_rows = 0 if base_df is None else base_df.height

        return MergeReport(
            table=table_name,
            before_rows=before_rows,
            incoming_rows=0,
            after_rows=before_rows,
            added_rows=0,
        )

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

    eds_dir = Path(eds_dir)
    run_dir = Path(run_dir)

    for t in table_names:
        # on ignore patient.parquet si vous le gardez interne
        if t == "patient.parquet":
            continue

        # ✅ NEW: skip si le parquet n’existe pas dans le run
        if not (run_dir / t).exists():
            # on renvoie un report "neutre" (pas d'ajout)
            before_rows = 0
            base_path = eds_dir / t
            if base_path.exists():
                base_df = _read_parquet_if_exists(base_path)
                before_rows = 0 if base_df is None else base_df.height

            reports.append(
                MergeReport(
                    table=t,
                    before_rows=before_rows,
                    incoming_rows=0,
                    after_rows=before_rows,
                    added_rows=0,
                )
            )
            continue

        keys = keys_by_table.get(t, [])
        reports.append(merge_table(eds_dir, run_dir, t, keys))

    return reports