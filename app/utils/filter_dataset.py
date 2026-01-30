#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl


# ----------------------------
# Parsing helpers
# ----------------------------

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
    """
    pat:
      - "*" matches all
      - "pharma" exact stem match
      - "/regex/" -> regex match on stem
    """
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
    """
    Supported:
      - null / true / false
      - numbers
      - quoted strings
      - (a,b,c) lists
      - in_file(path)
    """
    v = v.strip()

    m = re.fullmatch(r"in_file\((.+)\)", v)
    if m:
        path = _strip_quotes(m.group(1).strip())
        return _read_list_file(path)

    if v.startswith("(") and v.endswith(")"):
        inner = v[1:-1].strip()
        if not inner:
            return []
        return [_strip_quotes(x) for x in _split_csv(inner)]

    vl = v.lower()
    if vl == "null":
        return None
    if vl == "true":
        return True
    if vl == "false":
        return False

    if re.fullmatch(r"[-+]?\d+", v):
        return int(v)
    if re.fullmatch(r"[-+]?\d*\.\d+", v):
        return float(v)

    return _strip_quotes(v)


def parse_where(expr: str) -> WhereClause:
    """
    Examples:
      pharma:status==active
      biol:code in (HB,GLU)
      *:patient_id in_file(patients.txt)
      mvt:date>=2024-01-01
      doceds:note contains "cancer"
      pmsi:diag regex "/^C[0-9]/"
      biol:value between (3.5,5.0)
      pharma:deleted is_null
    """
    expr = expr.strip()
    if ":" not in expr:
        raise ValueError(f"Clause invalide (manque ':'): {expr}")

    table_pat, rest = expr.split(":", 1)
    table_pat = table_pat.strip()
    rest = rest.strip()

    m = re.fullmatch(r"([A-Za-z0-9_\.]+)\s+(is_null|not_null)\s*", rest)
    if m:
        return WhereClause(table_pat, m.group(1), m.group(2), "")

    m = re.fullmatch(r"([A-Za-z0-9_\.]+)\s+(contains|regex|in|between)\s+(.+)", rest)
    if m:
        return WhereClause(table_pat, m.group(1), m.group(2), m.group(3).strip())

    m = re.fullmatch(r"([A-Za-z0-9_\.]+)\s*(==|!=|>=|<=|>|<)\s*(.+)", rest)
    if m:
        return WhereClause(table_pat, m.group(1), m.group(2), m.group(3).strip())

    raise ValueError(f"Clause WHERE non reconnue: {expr}")


def parse_propagate(expr: str) -> PropagateSpec:
    """
    Syntax:
      key_col:source_table_pat

    Examples:
      patient_id:patient
      encounter_id:mvt
      stay_id:/^(mvt|pmsi)$/
      patient_id:*            (source = toutes tables)
    """
    expr = expr.strip()
    if ":" not in expr:
        raise ValueError(f"Propagate invalide (attendu key:source): {expr}")
    key, src = expr.split(":", 1)
    key = key.strip()
    src = (src or "*").strip()
    if not key:
        raise ValueError(f"Propagate invalide (key vide): {expr}")
    return PropagateSpec(key_col=key, source_table_pat=src)


# ----------------------------
# Polars expression builder
# ----------------------------

def _to_expr(col: str, op: str, value_raw: str) -> pl.Expr:
    c = pl.col(col)

    if op == "is_null":
        return c.is_null()
    if op == "not_null":
        return c.is_not_null()

    val = _parse_value_token(value_raw)

    if op == "contains":
        if not isinstance(val, str):
            val = str(val)
        return c.cast(pl.Utf8, strict=False).str.contains(re.escape(val))

    if op == "regex":
        rx = val if isinstance(val, str) else str(val)
        rx = rx.strip()
        if rx.startswith("/") and rx.endswith("/") and len(rx) >= 2:
            rx = rx[1:-1]
        return c.cast(pl.Utf8, strict=False).str.contains(rx)

    if op == "in":
        if not isinstance(val, list):
            val = [val]
        return c.is_in(val)

    if op == "between":
        if not isinstance(val, list) or len(val) != 2:
            raise ValueError(f"between attend (a,b). Reçu: {value_raw}")
        lo, hi = val[0], val[1]
        return (c >= lo) & (c <= hi)

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

    raise ValueError(f"Opérateur inconnu: {op}")


# ----------------------------
# Main filtering
# ----------------------------

def list_parquets(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("*.parquet"))


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _apply_where_for_table(lf: pl.LazyFrame, stem: str, clauses: list[WhereClause]) -> pl.LazyFrame:
    schema = lf.schema
    matchers = [(cl, _parse_table_pattern(cl.table_pat)) for cl in clauses]

    filters: list[pl.Expr] = []
    for cl, match in matchers:
        if not match(stem):
            continue
        if cl.col not in schema:
            continue
        filters.append(_to_expr(cl.col, cl.op, cl.raw_value))

    if filters:
        lf = lf.filter(pl.all_horizontal(filters))
    return lf


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
    ensure_dir(out_dir)

    files = list_parquets(in_dir)
    if not files:
        raise SystemExit(f"Aucun .parquet trouvé dans {in_dir}")

    only = only or []
    exclude = exclude or []

    if only:
        allow = set(only)
        files = [f for f in files if f.name in allow or f.stem in allow]
    if exclude:
        ban = set(exclude)
        files = [f for f in files if (f.name not in ban and f.stem not in ban)]

    clauses = [parse_where(w) for w in (where or [])]
    prop_specs = [parse_propagate(p) for p in (propagate or [])]

    # PASS 1: compute propagated key sets from SOURCE tables after WHERE filters
    propagated_sets: dict[str, set] = {}

    if prop_specs:
        for spec in prop_specs:
            src_match = _parse_table_pattern(spec.source_table_pat)

            unions: list[pl.LazyFrame] = []
            for f in files:
                stem = f.stem
                if not src_match(stem):
                    continue

                lf_src = pl.scan_parquet(str(f))
                if spec.key_col not in lf_src.schema:
                    continue

                lf_src = _apply_where_for_table(lf_src, stem, clauses)
                col_expr = pl.col(spec.key_col)

                if propagate_drop_nulls:
                    lf_src = lf_src.filter(col_expr.is_not_null())

                unions.append(lf_src.select(col_expr.alias(spec.key_col)))

            if unions:
                keys_df = pl.concat(unions).unique().collect(streaming=True)
                propagated_sets[spec.key_col] = set(keys_df[spec.key_col].to_list())
            else:
                propagated_sets[spec.key_col] = set()

    # PASS 2: for each table, apply WHERE + propagated filters, then write
    for f in files:
        stem = f.stem
        lf = pl.scan_parquet(str(f))

        # First apply WHERE
        lf = _apply_where_for_table(lf, stem, clauses)

        # Then apply propagated filters for columns present
        for key_col, key_set in propagated_sets.items():
            if key_col in lf.schema:
                if propagate_drop_nulls:
                    lf = lf.filter(pl.col(key_col).is_not_null())
                lf = lf.filter(pl.col(key_col).is_in(list(key_set)))

        out_path = out_dir / f.name
        lf.collect(streaming=True).write_parquet(str(out_path))

    print(f"✅ Filtrage terminé. Sortie: {out_dir.resolve()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Filtre dynamique de Parquet (Polars) + propagation de clés.")
    ap.add_argument("--input-dir", required=True, help="Dossier contenant les .parquet")
    ap.add_argument("--output-dir", required=True, help="Dossier de sortie")

    ap.add_argument("--only", default="", help="Tables à inclure (csv): stem ou fichier. Ex: pharma,biol")
    ap.add_argument("--exclude", default="", help="Tables à exclure (csv).")

    ap.add_argument(
        "--where",
        action="append",
        default=[],
        help=(
            "Clause WHERE (répétable). Ex: "
            "\"pharma:status==active\" "
            "\"biol:code in (HB,GLU)\" "
            "\"*:patient_id in_file(patients.txt)\" "
            "\"mvt:date>=2024-01-01\" "
            "\"pmsi:diag regex '/^C[0-9]/'\" "
            "\"biol:value between (3.5,5.0)\" "
            "\"pharma:deleted is_null\""
        ),
    )

    ap.add_argument(
        "--propagate",
        action="append",
        default=[],
        help=(
            "Propagation (répétable) au format key_col:source_table_pat. Ex: "
            "\"patient_id:patient\" "
            "\"encounter_id:mvt\" "
            "\"stay_id:/^(mvt|pmsi)$/\" "
            "\"patient_id:*\""
        ),
    )

    ap.add_argument(
        "--no-propagate-nulls",
        action="store_true",
        help="Si présent, n'élimine pas les valeurs nulles lors de la propagation (par défaut: on drop les nulls).",
    )

    args = ap.parse_args()
    only = _split_csv(args.only) if args.only else []
    exclude = _split_csv(args.exclude) if args.exclude else []

    filter_folder(
        args.input_dir,
        args.output_dir,
        only=only,
        exclude=exclude,
        where=args.where,
        propagate=args.propagate,
        propagate_drop_nulls=not args.no_propagate_nulls,
    )


if __name__ == "__main__":
    main()
