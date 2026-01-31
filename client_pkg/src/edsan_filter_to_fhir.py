from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import List, Optional

import typer

from app.utils.filter_dataset import filter_folder
from app.core.converters.edsan_to_fhir import export_eds_to_fhir


app = typer.Typer(help="Filtrer EDSan (temp) puis exporter en bundles FHIR (JSON).")


@app.command("edsan-filter-to-fhir")
def edsan_filter_to_fhir(
    input_dir: str = typer.Option(..., "--input-dir", help="Dossier EDS source (*.parquet)"),
    fhir_output_dir: str = typer.Option(..., "--fhir-output-dir", help="Dossier sortie FHIR (bundles JSON)"),
    filtered_output_dir: Optional[str] = typer.Option(
        None,
        "--filtered-output-dir",
        help="(Optionnel) Dossier de vérification: copie des parquets filtrés",
    ),
    bundle_strategy: str = typer.Option(
        "patient",
        "--bundle-strategy",
        help="Stratégie de bundle: patient ou encounter",
    ),
    where: List[str] = typer.Option([], "--where", help='Clause WHERE (répétable), ex: "patient:PATAGE>50"'),
    propagate: List[str] = typer.Option([], "--propagate", help='Propagation (répétable), ex: "PATID:patient"'),
    only: Optional[str] = typer.Option(None, "--only", help="Tables à inclure (csv), ex: patient,mvt,biol"),
    exclude: Optional[str] = typer.Option(None, "--exclude", help="Tables à exclure (csv)"),
    no_propagate_nulls: bool = typer.Option(False, "--no-propagate-nulls", help="Ne pas drop les nulls"),
):
    """
    Pipeline:
      1) Filtrage EDS -> TEMP (toujours)
      2) Copie optionnelle des parquets filtrés -> --filtered-output-dir
      3) Export FHIR (bundles JSON) depuis le TEMP -> --fhir-output-dir
    """

    if bundle_strategy not in ("patient", "encounter"):
        raise typer.BadParameter("bundle-strategy doit être 'patient' ou 'encounter'")

    only_list = [x.strip() for x in only.split(",") if x.strip()] if only else None
    exclude_list = [x.strip() for x in exclude.split(",") if x.strip()] if exclude else None

    with tempfile.TemporaryDirectory(prefix="eds_filtered_") as tmp:
        tmp_dir = Path(tmp)

        # 1) Filtrer dans TEMP
        filter_folder(
            input_dir=input_dir,
            output_dir=str(tmp_dir),
            only=only_list,
            exclude=exclude_list,
            where=where,
            propagate=propagate,
            propagate_drop_nulls=not no_propagate_nulls,
        )

        # 2) Copie optionnelle vers dossier utilisateur (vérif)
        if filtered_output_dir:
            dst = Path(filtered_output_dir)
            dst.mkdir(parents=True, exist_ok=True)
            for p in tmp_dir.glob("*.parquet"):
                shutil.copy2(p, dst / p.name)

        # 3) Export FHIR depuis TEMP
        summary = export_eds_to_fhir(
            eds_dir=str(tmp_dir),
            output_dir=str(Path(fhir_output_dir)),
            bundle_strategy=bundle_strategy,
        )

        typer.echo("✅ Filtre + export FHIR terminé")
        typer.echo(f"📦 FHIR écrit dans : {Path(fhir_output_dir).resolve()}")
        if filtered_output_dir:
            typer.echo(f"📂 EDS filtré (copie) : {Path(filtered_output_dir).resolve()}")

        # Affiche le résumé utile
        typer.echo(str(summary))
