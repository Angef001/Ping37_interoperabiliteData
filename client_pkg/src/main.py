from __future__ import annotations

import sys
import os
import json
import requests
import typer
from typing import List, Optional
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich import box

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# CLI modules
# ---------------------------------------------------------------------------

from . import edsan_filter
from . import edsan_filter_to_fhir
from . import display_edsan
from .import_url import import_url as import_url_cmd  # ✅ TA FEATURE

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = typer.Typer(help="CLI CHU Rouen — Entrepôt FHIR (HAPI) + Conversion EDS")
console = Console()

app.add_typer(edsan_filter.app)
app.add_typer(edsan_filter_to_fhir.app)
app.add_typer(display_edsan.app)

# ✅ commande ajoutée par toi
app.command("import-url")(import_url_cmd)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FHIR_URL = os.getenv("FHIR_URL", "http://localhost:8080/fhir")
FHIR_HEADERS = {"Accept": "application/fhir+json"}

CONVERTER_API_URL = os.getenv(
    "CONVERTER_API_URL", "http://localhost:8000/api/v1"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raise_if_error(resp: requests.Response, context: str):
    if 200 <= resp.status_code < 300:
        return
    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    raise typer.BadParameter(
        f"{context} — HTTP {resp.status_code} — {detail}"
    )

# ---------------------------------------------------------------------------
# FHIR commands
# ---------------------------------------------------------------------------

@app.command()
def info():
    """Vérifie si le serveur FHIR est en ligne (metadata)."""
    try:
        r = requests.get(
            f"{FHIR_URL}/metadata",
            headers=FHIR_HEADERS,
            timeout=10,
        )
        if r.status_code == 200:
            console.print("[bold green]✅ Serveur FHIR en ligne[/bold green]")
            console.print(f"URL: [cyan]{FHIR_URL}[/cyan]")
            console.print(f"FHIR Version: {r.json().get('fhirVersion', '?')}")
        else:
            console.print("[bold red]❌ Erreur serveur FHIR[/bold red]")
            console.print(r.text)
    except Exception as e:
        console.print(
            f"[bold red]❌ Impossible de contacter le serveur FHIR: {e}[/bold red]"
        )


def _patient_row(res: dict):
    pid = res.get("id", "?")
    family = "N/A"
    given = "N/A"
    if res.get("name"):
        family = res["name"][0].get("family", "N/A")
        given = " ".join(res["name"][0].get("given", []) or ["N/A"])
    birth = res.get("birthDate", "N/A")
    gender = res.get("gender", "N/A")
    return pid, family, given, birth, gender


@app.command()
def get_patient(patient_id: str):
    """Récupère un patient unique par ID."""
    url = f"{FHIR_URL}/Patient/{patient_id}"
    r = requests.get(url, headers=FHIR_HEADERS)
    if r.status_code == 200:
        p = r.json()
        table = Table(
            title=f"Patient {patient_id}",
            box=box.SIMPLE_HEAVY,
        )
        table.add_column("ID", style="cyan")
        table.add_column("Nom", style="magenta")
        table.add_column("Prénom", style="green")
        table.add_column("Naissance")
        table.add_column("Genre")
        table.add_row(*_patient_row(p))
        console.print(table)
    else:
        console.print(
            f"[red]Patient {patient_id} introuvable (HTTP {r.status_code})[/red]"
        )


@app.command()
def get_patients(ids: List[str]):
    """Récupère plusieurs patients par IDs."""
    ids_param = ",".join(ids)
    url = f"{FHIR_URL}/Patient"
    r = requests.get(
        url,
        params={"_id": ids_param},
        headers=FHIR_HEADERS,
    )
    if r.status_code != 200:
        console.print(f"[red]Erreur (HTTP {r.status_code})[/red]")
        return

    bundle = r.json()
    entries = bundle.get("entry", []) or []

    table = Table(
        title=f"Patients demandés: {len(ids)}",
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("ID", style="cyan")
    table.add_column("Nom", style="magenta")
    table.add_column("Prénom", style="green")
    table.add_column("Naissance")
    table.add_column("Genre")

    for e in entries:
        res = e.get("resource", {})
        if res.get("resourceType") == "Patient":
            table.add_row(*_patient_row(res))

    console.print(table)


@app.command()
def get_resource(resource_type: str, resource_id: str):
    """Affiche le JSON brut d'une ressource."""
    url = f"{FHIR_URL}/{resource_type}/{resource_id}"
    r = requests.get(url, headers=FHIR_HEADERS)
    if r.status_code == 200:
        console.print_json(
            json.dumps(r.json(), ensure_ascii=False)
        )
    else:
        console.print(
            f"[red]Ressource introuvable (HTTP {r.status_code})[/red]"
        )

# ---------------------------------------------------------------------------
# EDS / stats / reports
# ---------------------------------------------------------------------------

@app.command()
def stats():
    """Affiche les stats EDS."""
    console.print(
        "🔄 [bold cyan]Chargement des stats EDS en cours...[/bold cyan]"
    )
    url = f"{CONVERTER_API_URL}/stats"
    r = requests.get(url, timeout=15)
    _raise_if_error(r, "Lecture stats")

    data = r.json()
    tables = data.get("tables", {})

    t = Table(
        title=f"Stats EDS — dir={data.get('eds_dir', '')}",
        box=box.SIMPLE_HEAVY,
    )
    t.add_column("Table", style="magenta")
    t.add_column("Rows", justify="right")
    t.add_column("Cols", justify="right")

    for name, st in tables.items():
        t.add_row(
            name,
            str(st.get("rows", "?")),
            str(st.get("cols", "?")),
        )

    console.print(t)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
