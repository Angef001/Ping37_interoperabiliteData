from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import List, Optional
from rich.console import Console
from rich.table import Table
from rich import box


import json
import requests


import typer
import os


from app.utils.filter_dataset import filter_folder
from app.core.converters.edsan_to_fhir import export_eds_to_fhir


app = typer.Typer(help="Filtrer EDSan (temp) puis exporter en bundles FHIR (JSON).")
console = Console()


def _push_bundles_to_fhir(
    *,
    fhir_url: str,
    bundle_files: list[Path],
    timeout: int = 30,
) -> dict:
    headers = {
        "Accept": "application/fhir+json",
        "Content-Type": "application/fhir+json",
    }

    pushed = 0
    failed = 0
    errors: list[str] = []

    for bf in bundle_files:
        data = json.loads(bf.read_text(encoding="utf-8"))
        entries = data.get("entry", []) or []

        for e in entries:
            res = (e or {}).get("resource") or {}
            rtype = res.get("resourceType")
            rid = res.get("id")

            if not rtype:
                continue

            try:
                if rid:
                    url = f"{fhir_url.rstrip('/')}/{rtype}/{rid}"
                    r = requests.put(url, headers=headers, json=res, timeout=timeout)
                else:
                    url = f"{fhir_url.rstrip('/')}/{rtype}"
                    r = requests.post(url, headers=headers, json=res, timeout=timeout)

                if 200 <= r.status_code < 300:
                    pushed += 1
                else:
                    failed += 1
                    errors.append(f"{rtype}/{rid or '?'} -> HTTP {r.status_code} ({bf.name})")

            except Exception as ex:
                failed += 1
                errors.append(f"{rtype}/{rid or '?'} -> EXC {ex} ({bf.name})")

    return {
        "resources_pushed_ok": pushed,
        "resources_pushed_failed": failed,
        "errors_preview": errors[:10],
    }



def _count_rows_parquet_dir(dir_path: Path) -> dict[str, int]:
    import pandas as pd

    counts: dict[str, int] = {}
    for p in sorted(dir_path.glob("*.parquet")):
        try:
            counts[p.name] = len(pd.read_parquet(str(p)))
        except Exception:
            counts[p.name] = -1
    return counts



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
    stats: bool = typer.Option(False, "--stats", help="Affiche un tableau Input EDS vs EDS filtré (temp), affiches les stats sur chaque resource fhir générée et sur le nombre de bundle généré"),

    push: bool = typer.Option(False, "--push", help="Pousse les ressources vers le serveur FHIR après export"),
    fhir_url: str = typer.Option("http://localhost:8080/fhir", "--fhir-url", help="URL base du serveur FHIR"),

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

        if stats:
            in_counts = _count_rows_parquet_dir(Path(input_dir))
            out_counts = _count_rows_parquet_dir(tmp_dir)

            all_tables = sorted(set(in_counts) | set(out_counts))

            t = Table(title="Impact du filtre (lignes) — Input vs EDS filtré (temp)", box=box.SIMPLE_HEAVY)
            t.add_column("Table", style="cyan")
            t.add_column("Input rows", justify="right")
            t.add_column("Filtered (temp) rows", justify="right")
            t.add_column("Δ rows", justify="right")
            t.add_column("Δ %", justify="right")

            for name in all_tables:
                a = in_counts.get(name, 0)
                b = out_counts.get(name, 0)

                if a == -1 or b == -1:
                    t.add_row(name, "?", "?", "?", "?")
                    continue

                delta = b - a
                pct = (delta / a * 100.0) if a else 0.0
                t.add_row(name, str(a), str(b), str(delta), f"{pct:.1f}%")

            console.print()  
            console.print()  
            console.print(t)
            console.print()  


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
            print_summary=False,
        )


        if stats:
            # ---- Tableau stats FHIR ----
            summ = summary.get("summary", summary)  # selon ton format de retour
            bundles = summ.get("bundles_generated", "?")
            rpt = summ.get("resources_per_type", {}) or {}

            tf = Table(title="Stats FHIR exportées", box=box.SIMPLE_HEAVY)
            tf.add_column("Type", style="cyan")
            tf.add_column("Count", justify="right")

            tf.add_row("Bundle", str(bundles))
            for k in sorted(rpt.keys()):
                tf.add_row(k, str(rpt.get(k, 0)))

            console.print()
            console.print()    
            console.print(tf)
            console.print()  

        if stats:
            out_dir = Path(fhir_output_dir)

            json_files = sorted(out_dir.glob("*.json"))

            # Si tu as un last_run.json ou autre, on ne compte que les bundles patient-/encounter-
            bundle_files = [
                p for p in json_files
                if p.name.startswith("patient-") or p.name.startswith("encounter-")
            ]

            sizes = [p.stat().st_size for p in bundle_files]
            total_size = sum(sizes)

            avg_size = int(total_size / len(sizes)) if sizes else 0
            min_size = min(sizes) if sizes else 0
            max_size = max(sizes) if sizes else 0

            tfiles = Table(title="Fichiers écrits (bundles JSON)", box=box.SIMPLE_HEAVY)
            tfiles.add_column("Métrique", style="cyan")
            tfiles.add_column("Valeur", justify="right")

            tfiles.add_row("Bundles (JSON)", str(len(bundle_files)))
            tfiles.add_row("Taille totale", f"{total_size:,} octets")
            tfiles.add_row("Taille moyenne / bundle", f"{avg_size:,} octets")
            tfiles.add_row("Taille min / bundle", f"{min_size:,} octets")
            tfiles.add_row("Taille max / bundle", f"{max_size:,} octets")

            console.print(tfiles)

        if push:
            out_dir = Path(fhir_output_dir)
            bundle_files = sorted(
                [p for p in out_dir.glob("*.json") if p.name.startswith("patient-") or p.name.startswith("encounter-")]
            )

            console.print(f"🔄 Push vers FHIR: {fhir_url} — bundles: {len(bundle_files)}")

            push_report = _push_bundles_to_fhir(
                fhir_url=fhir_url,
                bundle_files=bundle_files,
            )

            tpush = Table(title="Push FHIR", box=box.SIMPLE_HEAVY)
            tpush.add_column("Métrique", style="cyan")
            tpush.add_column("Valeur", justify="right")
            tpush.add_row("Resources OK", str(push_report["resources_pushed_ok"]))
            tpush.add_row("Resources FAILED", str(push_report["resources_pushed_failed"]))
            console.print(tpush)

            if push_report["resources_pushed_failed"] and push_report["errors_preview"]:
                console.print("[yellow]Exemples d'erreurs (max 10):[/yellow]")
                for msg in push_report["errors_preview"]:
                    console.print()  
                    console.print(f" - {msg}")


        console.print()  
        console.print()  
        typer.echo("✅ Filtre + export FHIR terminé")
        console.print()  
        typer.echo(f"📦 FHIR écrit dans : {Path(fhir_output_dir).resolve()}")
        console.print()  
        if filtered_output_dir:
            typer.echo(f"📂 EDS filtré (copie) : {Path(filtered_output_dir).resolve()}")

    
