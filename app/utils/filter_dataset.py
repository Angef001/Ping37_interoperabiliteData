#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl


# =============================================================================
# Parsing helpers
# =============================================================================

@dataclass
class WhereClause:
    table_pat: str
    col: str
    op: str
    raw_value: str


@dataclass
class PropagateSpec:
    key_col: str
    source_table_pat: str  # stem, "*" or "/regex/"


def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s


def _parse_table_pattern(pat: str) -> Callable[[str], bool]:
    pat = (pat or "").strip()
    if pat in ("", "*"):
        return lambda stem: True
    if pat.startswith("/") and pat.endswith("/") and len(pat) >= 2:
        rx = re.compile(pat[1:-1])
        return lambda stem: bool(rx.search(stem))
    return lambda stem: stem == pat


def _read_list_file(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        raise ValueError(f"Fichier introuvable: {path}")
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def _parse_value_token(v: str):
    v = v.strip()

    m = re.fullmatch(r"in_file\((.+)\)", v)
    if m:
        return _read_list_file(_strip_quotes(m.group(1)))

    if v.startswith("(") and v.endswith(")"):
        return [_strip_quotes(x) for x in _split_csv(v[1:-1])]

    if v.lower() == "null":
        return None
    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False

    if re.fullmatch(r"-?\d+", v):
        return int(v)
    if re.fullmatch(r"-?\d*\.\d+", v):
        return float(v)

    return _strip_quotes(v)


def parse_where(expr: str) -> WhereClause:
    expr = expr.strip()
    if ":" not in expr:
        raise ValueError(expr)

    table_pat, rest = expr.split(":", 1)

    for rx, op in [
        (r"(.+)\s+(is_null|not_null)$", None),
        (r"(.+)\s+(contains|regex|in|between)\s+(.+)", None),
        (r"(.+)\s*(==|!=|>=|<=|>|<)\s*(.+)", None),
    ]:
        m = re.fullmatch(rx, rest.strip())
        if m:
            return WhereClause(
                table_pat.strip(),
                m.group(1).strip(),
                m.group(2),
                m.group(3).strip() if m.lastindex == 3 else "",
            )

    raise ValueError(expr)


def parse_propagate(expr: str) -> PropagateSpec:
    key, src = expr.split(":", 1)
    return PropagateSpec(key.strip(), src.strip() or "*")


# =============================================================================
# Polars expressions
# =============================================================================

def _to_expr(col: str, op: str, raw: str) -> pl.Expr:
    c = pl.col(col)
    val = _parse_value_token(raw)

    if op == "is_null":
        return c.is_null()
    if op == "not_null":
        return c.is_not_null()
    if op == "contains":
        return c.cast(pl.Utf8, strict=False).str.contains(str(val))
    if op == "regex":
        rx = str(val)
        if rx.startswith("/") and rx.endswith("/"):
            rx = rx[1:-1]
        return c.cast(pl.Utf8, strict=False).str.contains(rx)
    if op == "in":
        return c.is_in(val if isinstance(val, list) else [val])
    if op == "between":
        return (c >= val[0]) & (c <= val[1])
    if op == "==":
        return c == val
    if op == "!=":
        return c != val
    if op == ">":
        return c > val
    if op == "<":
        return c < val
    if op == ">=":
        return c >= val
    if op == "<=":
        return c <= val

    raise ValueError(op)


# =============================================================================
# Core filtering logic (EXISTANT)
# =============================================================================

def filter_folder(
    input_dir: str,
    output_dir: str,
    *,
    only: list[str] | None,
    exclude: list[str] | None,
    where: list[str] | None,
    propagate: list[str] | None,
    propagate_drop_nulls: bool = True,
) -> None:
    in_dir = Path(input_dir)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob("*.parquet"))
    clauses = [parse_where(w) for w in (where or [])]
    prop_specs = [parse_propagate(p) for p in (propagate or [])]

    # PASS 1 — propagation keys
    propagated_sets: dict[str, set] = {}

    for spec in prop_specs:
        matcher = _parse_table_pattern(spec.source_table_pat)
        unions = []

        for f in files:
            if not matcher(f.stem):
                continue

            lf = pl.scan_parquet(str(f))
            if spec.key_col not in lf.schema:
                continue

            for cl in clauses:
                if _parse_table_pattern(cl.table_pat)(f.stem) and cl.col in lf.schema:
                    lf = lf.filter(_to_expr(cl.col, cl.op, cl.raw_value))

            if propagate_drop_nulls:
                lf = lf.filter(pl.col(spec.key_col).is_not_null())

            unions.append(lf.select(spec.key_col))

        if unions:
            propagated_sets[spec.key_col] = set(
                pl.concat(unions).unique().collect()[spec.key_col].to_list()
            )

    # PASS 2 — write filtered tables
    for f in files:
        lf = pl.scan_parquet(str(f))

        for cl in clauses:
            if _parse_table_pattern(cl.table_pat)(f.stem) and cl.col in lf.schema:
                lf = lf.filter(_to_expr(cl.col, cl.op, cl.raw_value))

        for key, vals in propagated_sets.items():
            if key in lf.schema:
                lf = lf.filter(pl.col(key).is_in(list(vals)))

        lf.collect(streaming=True).write_parquet(out_dir / f.name)


# =============================================================================
# NEW — wrapper: ALWAYS temp + optional persistent copy
# =============================================================================

def filter_dataset(
    *,
    input_dir: str,
    where: list[str],
    propagate: list[str] | None = None,
    only: list[str] | None = None,
    exclude: list[str] | None = None,
    output_dir: str | None = None,  # <- optionnel (vérif)
) -> tuple[Path, tempfile.TemporaryDirectory]:
    """
    - Filtre TOUJOURS dans un dossier temporaire
    - Copie optionnelle vers output_dir (si fourni)
    - Retourne (temp_dir, temp_handle)
    """
    tmp = tempfile.TemporaryDirectory(prefix="eds_filtered_")
    tmp_dir = Path(tmp.name)

    filter_folder(
        input_dir=input_dir,
        output_dir=str(tmp_dir),
        only=only,
        exclude=exclude,
        where=where,
        propagate=propagate,
    )

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        for p in tmp_dir.glob("*.parquet"):
            shutil.copy2(p, out / p.name)

    return tmp_dir, tmp


# =============================================================================
# CLI (inchangé fonctionnellement)
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--where", action="append", default=[])
    ap.add_argument("--propagate", action="append", default=[])

    args = ap.parse_args()

    filter_folder(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        only=None,
        exclude=None,
        where=args.where,
        propagate=args.propagate,
    )


if __name__ == "__main__":
    main()
