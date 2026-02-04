from __future__ import annotations

import tempfile
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import box

from app.utils.filter_dataset import filter_folder

app = typer.Typer(help="Filtrage de l'EDS (Parquet) avec WHERE et PROPAGATE")
console = Console()



def _count_rows_parquet_dir(dir_path: Path) -> dict[str, int]:
    import pandas as pd

    counts: dict[str, int] = {}
    for p in sorted(dir_path.glob("*.parquet")):
        try:
            counts[p.name] = len(pd.read_parquet(str(p)))
        except Exception:
            counts[p.name] = -1
    return counts




@app.command("edsan-filter")
def edsan_filter(
    input_dir: str = typer.Option(
        ...,
        "--input-dir",
        help="Dossier EDS source (contenant les fichiers .parquet)",
    ),
    output_dir: str = typer.Option(
        ...,
        "--output-dir",
        help="Dossier de sortie pour l'EDS filtré (parquets)",
    ),
    where: List[str] = typer.Option(
        [],
        "--where",
        help='Clause WHERE (répétable), ex: "patient:PATAGE>50"',
    ),
    propagate: List[str] = typer.Option(
        [],
        "--propagate",
        help='Propagation clé:source, ex: "PATID:patient"',
    ),
    only: Optional[str] = typer.Option(
        None,
        "--only",
        help="Tables à inclure (csv), ex: patient,mvt,biol",
    ),
    exclude: Optional[str] = typer.Option(
        None,
        "--exclude",
        help="Tables à exclure (csv)",
    ),
    no_propagate_nulls: bool = typer.Option(
        False,
        "--no-propagate-nulls",
        help="Ne pas supprimer les valeurs nulles lors de la propagation",
    ),
    stats: bool = typer.Option(
        False,
        "--stats",
        help="Affiche les stats (lignes) après filtrage",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="N'écrit aucun parquet (simulation)",
    ),
):
    """
    Filtre un EDS (Parquet) avec des conditions dynamiques (--where)
    et propage les clés (--propagate) vers les autres tables.
    """

    only_list = [x.strip() for x in only.split(",") if x.strip()] if only else None
    exclude_list = [x.strip() for x in exclude.split(",") if x.strip()] if exclude else None

    target_dir = output_dir

    # Dry-run => écrire dans un dossier temporaire (et ne rien laisser sur disque)
    tmp_ctx = None
    if dry_run:
        tmp_ctx = tempfile.TemporaryDirectory(prefix="eds_dryrun_")
        target_dir = tmp_ctx.name

    filter_folder(
        input_dir=input_dir,
        output_dir=target_dir,
        only=only_list,
        exclude=exclude_list,
        where=where,
        propagate=propagate,
        propagate_drop_nulls=not no_propagate_nulls,
    )

    if stats:
        in_counts = _count_rows_parquet_dir(Path(input_dir))
        out_counts = _count_rows_parquet_dir(Path(target_dir))

        all_tables = sorted(set(in_counts) | set(out_counts))

        t = Table(
            title="Impact du filtre (lignes)",
            box=box.SIMPLE_HEAVY,
        )
        t.add_column("Table", style="cyan")
        t.add_column("Input rows", justify="right")
        t.add_column("Output rows", justify="right")
        t.add_column("Δ rows", justify="right")
        t.add_column("Δ %", justify="right")

        for name in all_tables:
            a = in_counts.get(name, 0)
            b = out_counts.get(name, 0)

            # -1 => erreur de lecture
            if a == -1 or b == -1:
                t.add_row(name, "?", "?", "?", "?")
                continue

            delta = b - a
            pct = (delta / a * 100.0) if a else 0.0
            t.add_row(name, str(a), str(b), str(delta), f"{pct:.1f}%")

        console.print(t)


    if dry_run and tmp_ctx is not None:
        tmp_ctx.cleanup()
        typer.echo("✅ Dry-run terminé (aucun fichier conservé)")
        raise typer.Exit(code=0)

    typer.echo("✅ Filtrage EDS terminé")
    typer.echo(f"📂 EDS filtré écrit dans : {output_dir}")
