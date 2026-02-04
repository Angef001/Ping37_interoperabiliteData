# from __future__ import annotations

# import os
# import requests
# import typer

# CONVERTER_API_URL = os.getenv(
#     "CONVERTER_API_URL",
#     "http://localhost:8000/api/v1"
# )


# def import_url(
#     url: str = typer.Option(..., "--url", help="URL complète de requête FHIR (entrepôt)"),
#     eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
#     reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
#     fhir_server_url: str = typer.Option(
#         "", "--fhir-server-url", help="Override URL base entrepôt (optionnel)"
#     ),
#     page_size: int = typer.Option(100, "--page-size", help="Pagination _count"),
#     timeout: int = typer.Option(600, "--timeout", help="Timeout HTTP (secondes)"),
#     stats: bool = typer.Option(False, "--stats", help="Afficher les statistiques complètes du run"),
# ):
#     """
#     Import FHIR (entrepôt) -> EDS via URL de requête.

#     Principe :
#     - La CLI appelle l'API FastAPI
#     - L'API génère last_run.json (source de vérité)
#     - La CLI affiche les infos du run courant
#     """

#     # ---------------------------
#     # Payload envoyé à l’API
#     # ---------------------------
#     payload = {
#         "query_url": url,
#         "page_size": page_size,
#     }

#     if eds_dir.strip():
#         payload["eds_dir"] = eds_dir.strip()
#     if reports_dir.strip():
#         payload["reports_dir"] = reports_dir.strip()
#     if fhir_server_url.strip():
#         payload["fhir_server_url"] = fhir_server_url.strip()

#     endpoint = f"{CONVERTER_API_URL}/convert/fhir-query-to-edsan"

#     try:
#         r = requests.post(endpoint, json=payload, timeout=timeout)
#     except Exception as e:
#         typer.echo(f"❌ Erreur réseau vers l’API : {e}")
#         raise typer.Exit(1)

#     if r.status_code >= 400:
#         typer.echo(f"❌ Erreur API ({r.status_code})")
#         typer.echo(r.text)
#         raise typer.Exit(1)

#     resp = r.json()
#     run_id = resp.get("run_id")

#     # ---------------------------
#     # Lecture du last_run (source de vérité)
#     # ---------------------------
#     try:
#         report_resp = requests.get(
#             f"{CONVERTER_API_URL}/report/last-run",
#             timeout=timeout,
#         )
#         report_resp.raise_for_status()
#         report = report_resp.json()
#     except Exception as e:
#         typer.echo(f"❌ Impossible de lire last-run : {e}")
#         raise typer.Exit(1)

#     # ---------------------------
#     # Affichage synthèse
#     # ---------------------------
#     typer.echo("✅ Import terminé")
#     typer.echo(f"- run_id   : {report.get('run_id', run_id)}")
#     typer.echo(f"- mode     : {report.get('mode')}")
#     typer.echo(f"- started  : {report.get('started_at')}")
#     typer.echo(f"- ended    : {report.get('ended_at')}")

#     summary = report.get("summary", {})
#     typer.echo(f"- entries  : {summary.get('entries_total')}")

#     #paths = report.get("paths", {})
#     typer.echo("")
#     typer.echo("📁 Paramètres effectifs du run")
#     #typer.echo(f"- eds_dir     : {paths.get('eds_dir', eds_dir)}")
#     #typer.echo(f"- reports_dir : {paths.get('reports_dir', reports_dir)}")
#     typer.echo(f"- eds_dir     : {eds_dir or report.get('paths', {}).get('eds_dir')}")
#     typer.echo(f"- reports_dir : {reports_dir or report.get('paths', {}).get('reports_dir')}")


#     if not stats:
#         return

#     # ---------------------------
#     # Stats de conversion (merge_report)
#     # ---------------------------
#     typer.echo("\n📦 Impact du run (stats de conversion)\n")

#     merge = report.get("merge_report", [])

#     if merge:
#         typer.echo(f"{'Table':<16} {'Before':>8} {'Incoming':>10} {'Added':>8} {'After':>8}")
#         typer.echo("-" * 56)
#         for r in merge:
#             typer.echo(
#                 f"{r.get('table', ''):<16} "
#                 f"{r.get('before_rows', 0):>8} "
#                 f"{r.get('incoming_rows', 0):>10} "
#                 f"{r.get('added_rows', 0):>8} "
#                 f"{r.get('after_rows', 0):>8}"
#             )
#     else:
#         typer.echo("Aucune statistique de merge disponible.")

#     # ---------------------------
#     # État actuel de l’EDS
#     # ---------------------------
#     typer.echo("\n📊 État actuel de l’EDS\n")

#     try:
#         # stats_resp = requests.get(f"{CONVERTER_API_URL}/stats", timeout=timeout)
#         # stats_resp.raise_for_status()
#         # stats_payload = stats_resp.json()

#         # stats sur le bon dossier EDS
#         params = {}
#         if eds_dir.strip():
#             params["eds_dir"] = eds_dir.strip()

#         stats_resp = requests.get(
#             f"{CONVERTER_API_URL}/stats",
#             params=params,
#             timeout=timeout,
#         )
#         stats_resp.raise_for_status()
#         stats_payload = stats_resp.json()

#     except Exception as e:
#         typer.echo(f"❌ Impossible de lire /stats : {e}")
#         raise typer.Exit(1)

#     tables = stats_payload.get("tables", {})

#     typer.echo(f"{'Table':<16} {'Lignes':>10} {'Colonnes':>10}")
#     typer.echo("-" * 40)
#     for t, v in tables.items():
#         typer.echo(f"{t:<16} {v.get('rows', 0):>10} {v.get('cols', 0):>10}")

#     typer.echo(f"\n📁 Dossier EDS : {paths.get('eds_dir', '')}")

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
    page_size: int = typer.Option(
        100, "--page-size",
        help="Pagination _count pour l’entrepôt FHIR"
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
        "page_size": page_size,
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
