from __future__ import annotations

from pathlib import Path
from typing import Optional

import polars as pl
import typer
from rich.console import Console
from rich.table import Table
from rich import box

app = typer.Typer(help="Affichage local des tables EDSan (parquet)")
console = Console()

TABLES = [
    "patient.parquet",
    "mvt.parquet",
    "biol.parquet",
    "pharma.parquet",
    "pmsi.parquet",
    "doceds.parquet",
]


def _clip(s: object, max_len: int) -> str:
    """Coupe une valeur pour éviter les cellules immenses."""
    if s is None:
        return ""
    txt = str(s)
    txt = txt.replace("\n", " ").replace("\r", " ")
    if max_len > 0 and len(txt) > max_len:
        return txt[: max_len - 1] + "…"
    return txt


def _print_preview(df: pl.DataFrame, *, limit: int, cols: list[str], max_cell: int) -> None:
    head = df.select(cols).head(limit).to_dicts()

    t = Table(
        title=f"Preview (head {min(limit, df.height)})",
        box=box.SQUARE,          # bien quadrillé
        show_lines=True,         # lignes entre chaque ligne
        pad_edge=False,
    )

    for c in cols:
        # no_wrap=True -> pas de texte multi-lignes (beaucoup plus lisible)
        # overflow="ellipsis" -> Rich met … si ça dépasse (mais dépend de la largeur terminal)
        t.add_column(str(c), no_wrap=True, overflow="ellipsis")

    for row in head:
        t.add_row(*[_clip(row.get(c, ""), max_cell) for c in cols])

    console.print(t)


@app.command("display-eds")
def display_eds(
    eds_dir: Path = typer.Option(..., "--eds-dir", help="Dossier EDSan (obligatoire)"),
    limit: int = typer.Option(5, "--limit", "-l", help="Nb lignes à afficher par table"),
    cols: Optional[str] = typer.Option(None, "--cols", help="Colonnes à afficher (csv). Ex: PATID,EVTID,DATENT"),
    max_cols: int = typer.Option(8, "--max-cols", help="Nb max de colonnes si --cols n'est pas fourni"),
    max_cell: int = typer.Option(40, "--max-cell", help="Taille max d'une cellule (0=pas de coupe)"),
    no_preview: bool = typer.Option(False, "--no-preview", help="Affiche seulement les stats, pas le head"),
):
    if not eds_dir.exists():
        raise typer.BadParameter(f"Dossier introuvable : {eds_dir}")

    for name in TABLES:
        path = eds_dir / name

        info = Table(title=name, box=box.SQUARE, show_lines=True)
        info.add_column("Info", style="cyan", no_wrap=True)
        info.add_column("Valeur", style="magenta")

        if not path.exists():
            info.add_row("Statut", "ABSENT")
            console.print(info)
            console.print()
            continue

        try:
            df = pl.read_parquet(path)

            info.add_row("Statut", "OK")
            info.add_row("Rows", str(df.height))
            info.add_row("Cols", str(df.width))
            console.print(info)

            if no_preview:
                console.print()
                continue

            # Colonnes à afficher
            if cols:
                wanted = [c.strip() for c in cols.split(",") if c.strip()]
                missing = [c for c in wanted if c not in df.columns]
                if missing:
                    raise typer.BadParameter(f"{name}: colonnes inconnues: {missing}")
                preview_cols = wanted
            else:
                preview_cols = df.columns[: max_cols]

            if df.height > 0 and preview_cols:
                _print_preview(df, limit=limit, cols=preview_cols, max_cell=max_cell)

        except Exception as e:
            info_err = Table(title=f"{name} — erreur", box=box.SQUARE, show_lines=True)
            info_err.add_column("Erreur", style="red")
            info_err.add_row(str(e))
            console.print(info_err)

        console.print()
