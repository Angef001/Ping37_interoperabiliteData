# import os
# import requests
# import typer
# from typing import Optional

# app = typer.Typer(add_completion=False)

# # ⚠️ cohérent avec le serveur FastAPI monté sous /api/v1
# CONVERTER_API_URL = os.getenv("CONVERTER_API_URL", "http://localhost:8000/api/v1")


# @app.command("import-url")
# def import_url(
#     url: str = typer.Option(..., "--url", help="URL complète de requête FHIR (entrepôt)"),
#     eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
#     reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
#     fhir_server_url: str = typer.Option("", "--fhir-server-url", help="Override URL base entrepôt (optionnel, pour trace/report)"),
#     page_size: int = typer.Option(100, "--page-size", help="Pagination _count (optionnel)"),
#     timeout: int = typer.Option(600, "--timeout", help="Timeout HTTP (secondes)"),
# ):
#     """
#     Import FHIR (entrepôt) -> EDS via URL de requête.

#     Principe (important pour cohérence) :
#     - La CLI ne reconstruit pas de logique conversion.
#     - Elle appelle l'API FastAPI, qui génère last_run + runs/ (source de vérité).
#     """
#     payload = {"query_url": url, "page_size": page_size}

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
#         typer.echo(f"❌ Erreur réseau vers l'API: {e}")
#         raise typer.Exit(1)

#     if r.status_code >= 400:
#         typer.echo(f"❌ Erreur API ({r.status_code})")
#         typer.echo(r.text)
#         raise typer.Exit(1)

#     resp = r.json()
#     data = resp.get("data", resp)

#     typer.echo("✅ Import terminé")
#     typer.echo(f"- run_id       : {data.get('run_id')}")
#     typer.echo(f"- mode         : {data.get('mode')}")
#     typer.echo(f"- reports_dir  : {data.get('reports_dir')}")
#     typer.echo(f"- eds_dir      : {data.get('eds_dir')}")
#     typer.echo(f"- entries_total: {data.get('entries_total')}")
#     typer.echo(f"- query_url    : {data.get('query_url')}")
# import os
# import requests
# import typer

# app = typer.Typer(add_completion=False)

# # ✅ Cohérent avec main.py (API versionnée)
# CONVERTER_API_URL = os.getenv("CONVERTER_API_URL", "http://localhost:8000/api/v1")


# @app.command("import-url")
# def import_url(
#     url: str = typer.Option(..., "--url", help="URL complète de requête FHIR (entrepôt)"),
#     eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
#     reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
#     fhir_server_url: str = typer.Option("", "--fhir-server-url", help="Override URL base entrepôt (optionnel, pour trace/report)"),
#     page_size: int = typer.Option(100, "--page-size", help="Pagination _count (optionnel)"),
#     timeout: int = typer.Option(600, "--timeout", help="Timeout HTTP (secondes)"),
# ):
#     """
#     Import FHIR (entrepôt) -> EDS via URL de requête.
#     Méthode uniforme avec l'export : la CLI appelle l'API.
#     """
#     payload = {"query_url": url, "page_size": page_size}

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
#         typer.echo(f"❌ Erreur réseau vers l'API: {e}")
#         raise typer.Exit(1)

#     if r.status_code >= 400:
#         typer.echo(f"❌ Erreur API ({r.status_code})")
#         typer.echo(r.text)
#         raise typer.Exit(1)

#     resp = r.json()
#     data = resp.get("data", resp)

#     typer.echo("✅ Import terminé")
#     typer.echo(f"- run_id       : {data.get('run_id')}")
#     typer.echo(f"- mode         : {data.get('mode')}")
#     typer.echo(f"- reports_dir  : {data.get('reports_dir')}")
#     typer.echo(f"- eds_dir      : {data.get('eds_dir')}")
#     typer.echo(f"- entries_total: {data.get('entries_total')}")
#     typer.echo(f"- query_url    : {data.get('query_url')}")


from __future__ import annotations

import os
import requests
import typer

CONVERTER_API_URL = os.getenv(
    "CONVERTER_API_URL",
    "http://localhost:8000/api/v1"
)



def import_url(
    url: str = typer.Option(..., "--url", help="URL complète de requête FHIR (entrepôt)"),
    eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
    reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
    fhir_server_url: str = typer.Option(
        "", "--fhir-server-url", help="Override URL base entrepôt (optionnel, pour trace/report)"
    ),
    page_size: int = typer.Option(100, "--page-size", help="Pagination _count (optionnel)"),
    timeout: int = typer.Option(600, "--timeout", help="Timeout HTTP (secondes)"),
):
    """
    Import FHIR (entrepôt) -> EDS via URL de requête.
    Méthode uniforme : la CLI appelle l'API /convert/fhir-query-to-edsan
    """
    payload = {"query_url": url, "page_size": page_size}

    if eds_dir.strip():
        payload["eds_dir"] = eds_dir.strip()
    if reports_dir.strip():
        payload["reports_dir"] = reports_dir.strip()
    if fhir_server_url.strip():
        payload["fhir_server_url"] = fhir_server_url.strip()

    endpoint = f"{CONVERTER_API_URL}/convert/fhir-query-to-edsan"

    try:
        r = requests.post(endpoint, json=payload, timeout=timeout)
    except Exception as e:
        typer.echo(f"❌ Erreur réseau vers l'API: {e}")
        raise typer.Exit(1)

    if r.status_code >= 400:
        typer.echo(f"❌ Erreur API ({r.status_code})")
        typer.echo(r.text)
        raise typer.Exit(1)

    resp = r.json()
    data = resp.get("data", resp)

    typer.echo("✅ Import terminé")
    typer.echo(f"- run_id       : {data.get('run_id')}")
    typer.echo(f"- mode         : {data.get('mode')}")
    typer.echo(f"- reports_dir  : {data.get('reports_dir')}")
    typer.echo(f"- eds_dir      : {data.get('eds_dir')}")
    typer.echo(f"- entries_total: {data.get('entries_total')}")
    typer.echo(f"- query_url    : {data.get('query_url')}")
