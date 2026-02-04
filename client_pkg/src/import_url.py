
from __future__ import annotations

import os
import requests
import typer

CONVERTER_API_URL = os.getenv(
    "CONVERTER_API_URL",
    "http://localhost:8000/api/v1"
)


def import_url(
    url: str = typer.Option(
        ..., "--url",
        help="URL complète de requête FHIR (entrepôt)"
    ),
    eds_dir: str = typer.Option(
        "", "--eds-dir",
        help="Dossier EDS destination (optionnel)"
    ),
    stats: bool = typer.Option(
        False, "--stats",
        help="Afficher les statistiques détaillées du run"
    ),
):
    """
    Import FHIR (entrepôt) -> EDS via URL de requête.

    Principe :
    - La CLI appelle l’API FastAPI
    - L’API génère last_run.json (source de vérité)
    - La CLI affiche une synthèse claire et non ambiguë
    """

    # ---------------------------
    # Payload envoyé à l’API
    # ---------------------------
    payload = {
        "query_url": url,
    }

    if eds_dir.strip():
        payload["eds_dir"] = eds_dir.strip()

    endpoint = f"{CONVERTER_API_URL}/convert/fhir-query-to-edsan"

    try:
        r = requests.post(endpoint, json=payload, timeout=600)
    except Exception as e:
        typer.echo(f"❌ Erreur réseau vers l’API : {e}")
        raise typer.Exit(1)

    if r.status_code >= 400:
        typer.echo(f"❌ Erreur API ({r.status_code})")
        typer.echo(r.text)
        raise typer.Exit(1)

    # ---------------------------
    # Lecture du last_run (source de vérité)
    # ---------------------------
    try:
        report_resp = requests.get(
            f"{CONVERTER_API_URL}/report/last-run",
            timeout=600,
        )
        report_resp.raise_for_status()
        report = report_resp.json()
    except Exception as e:
        typer.echo(f"❌ Impossible de lire last-run : {e}")
        raise typer.Exit(1)

    # ---------------------------
    # Affichage synthèse
    # ---------------------------
    typer.echo("✅ Import terminé")
    typer.echo(f"- run_id   : {report.get('run_id')}")
    typer.echo(f"- mode     : {report.get('mode')}")
    typer.echo(f"- started  : {report.get('started_at')}")
    typer.echo(f"- ended    : {report.get('ended_at')}")

    summary = report.get("summary", {})
    typer.echo(f"- entries  : {summary.get('entries_total')}")

    typer.echo("")
    typer.echo("📁 Dossier EDS utilisé")
    typer.echo(f"- eds_dir  : {eds_dir or report.get('paths', {}).get('eds_dir')}")

    if not stats:
        return

    # ---------------------------
    # Impact du run (clair, non ambigu)
    # ---------------------------
    typer.echo("\n📦 Impact du run (batch courant)\n")

    merge = report.get("merge_report", [])

    if merge:
        typer.echo(f"{'Table':<18} {'Incoming':>12} {'Added':>10}")
        typer.echo("-" * 42)
        for r in merge:
            typer.echo(
                f"{r.get('table', ''):<18} "
                f"{r.get('incoming_rows', 0):>12} "
                f"{r.get('added_rows', 0):>10}"
            )
    else:
        typer.echo("Aucune donnée de conversion disponible.")

    # ---------------------------
    # État actuel de l’EDS
    # ---------------------------
    typer.echo("\n📊 État actuel de l’EDS\n")

    params = {}
    if eds_dir.strip():
        params["eds_dir"] = eds_dir.strip()

    try:
        stats_resp = requests.get(
            f"{CONVERTER_API_URL}/stats",
            params=params,
            timeout=600,
        )
        stats_resp.raise_for_status()
        stats_payload = stats_resp.json()
    except Exception as e:
        typer.echo(f"❌ Impossible de lire /stats : {e}")
        raise typer.Exit(1)

    tables = stats_payload.get("tables", {})

    typer.echo(f"{'Table':<18} {'Lignes':>10} {'Colonnes':>10}")
    typer.echo("-" * 42)
    for t, v in tables.items():
        typer.echo(
            f"{t:<18} "
            f"{v.get('rows', 0):>10} "
            f"{v.get('cols', 0):>10}"
        )
