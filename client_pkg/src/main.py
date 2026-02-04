
# from __future__ import annotations

# import sys
# from pathlib import Path

# PROJECT_ROOT = Path(__file__).resolve().parents[2]
# sys.path.insert(0, str(PROJECT_ROOT))



# import os
# import json
# import requests
# import typer
# from typing import List, Optional
# from datetime import datetime
# from pathlib import Path

# from rich.console import Console
# from rich.table import Table
# from rich import box

# from . import edsan_filter
# from . import edsan_filter_to_fhir
# from . import display_edsan




# app = typer.Typer(help="CLI CHU Rouen — Entrepôt FHIR (HAPI) + Conversion EDS")
# console = Console()

# app.add_typer(edsan_filter.app)
# app.add_typer(edsan_filter_to_fhir.app)
# app.add_typer(display_edsan.app)


# # Entrepôt FHIR (HAPI)
# FHIR_URL = os.getenv("FHIR_URL", "http://localhost:8080/fhir")
# FHIR_HEADERS = {"Accept": "application/fhir+json"}

# # API Converter (FastAPI)
# CONVERTER_API_URL = os.getenv("CONVERTER_API_URL", "http://localhost:8000/api/v1")


# def _raise_if_error(resp: requests.Response, context: str):
#     if 200 <= resp.status_code < 300:
#         return
#     try:
#         detail = resp.json()
#     except Exception:
#         detail = resp.text
#     raise typer.BadParameter(f"{context} — HTTP {resp.status_code} — {detail}")


# # =============================================================================
# # COMMANDES FHIR (EXISTANTES)
# # =============================================================================

# @app.command()
# def info():
#     """Vérifie si le serveur FHIR est en ligne (metadata)."""
#     try:
#         r = requests.get(f"{FHIR_URL}/metadata", headers=FHIR_HEADERS, timeout=10)
#         if r.status_code == 200:
#             console.print("[bold green]✅ Serveur FHIR en ligne[/bold green]")
#             console.print(f"URL: [cyan]{FHIR_URL}[/cyan]")
#             console.print(f"FHIR Version: {r.json().get('fhirVersion', '?')}")
#         else:
#             console.print("[bold red]❌ Erreur serveur FHIR[/bold red]")
#             console.print(r.text)
#     except Exception as e:
#         console.print(f"[bold red]❌ Impossible de contacter le serveur FHIR: {e}[/bold red]")


# def _patient_row(res: dict):
#     pid = res.get("id", "?")
#     family = "N/A"
#     given = "N/A"
#     if "name" in res and res["name"]:
#         family = res["name"][0].get("family", "N/A")
#         given = " ".join(res["name"][0].get("given", []) or ["N/A"])
#     birth = res.get("birthDate", "N/A")
#     gender = res.get("gender", "N/A")
#     return pid, family, given, birth, gender


# @app.command()
# def get_patient(patient_id: str):
#     """Récupère un patient unique par ID."""
#     url = f"{FHIR_URL}/Patient/{patient_id}"
#     r = requests.get(url, headers=FHIR_HEADERS)
#     if r.status_code == 200:
#         p = r.json()
#         table = Table(title=f"Patient {patient_id}", box=box.SIMPLE_HEAVY)
#         table.add_column("ID", style="cyan")
#         table.add_column("Nom", style="magenta")
#         table.add_column("Prénom", style="green")
#         table.add_column("Naissance")
#         table.add_column("Genre")
#         table.add_row(*_patient_row(p))
#         console.print(table)
#     else:
#         console.print(f"[red]Patient {patient_id} introuvable (HTTP {r.status_code})[/red]")


# @app.command()
# def get_patients(ids: List[str]):
#     """Récupère plusieurs patients par IDs."""
#     ids_param = ",".join(ids)
#     url = f"{FHIR_URL}/Patient"
#     r = requests.get(url, params={"_id": ids_param}, headers=FHIR_HEADERS)
#     if r.status_code != 200:
#         console.print(f"[red]Erreur (HTTP {r.status_code})[/red]")
#         return

#     bundle = r.json()
#     entries = bundle.get("entry", []) or []

#     table = Table(title=f"Patients demandés: {len(ids)}", box=box.SIMPLE_HEAVY)
#     table.add_column("ID", style="cyan")
#     table.add_column("Nom", style="magenta")
#     table.add_column("Prénom", style="green")
#     table.add_column("Naissance")
#     table.add_column("Naissance")

#     for e in entries:
#         res = e.get("resource", {})
#         if res.get("resourceType") == "Patient":
#             table.add_row(*_patient_row(res))

#     console.print(table)


# @app.command()
# def get_resource(resource_type: str, resource_id: str):
#     """Affiche le JSON brut d'une ressource."""
#     url = f"{FHIR_URL}/{resource_type}/{resource_id}"
#     r = requests.get(url, headers=FHIR_HEADERS)
#     if r.status_code == 200:
#         console.print_json(json.dumps(r.json(), ensure_ascii=False))
#     else:
#         console.print(f"[red]Ressource introuvable (HTTP {r.status_code})[/red]")


# # =============================================================================
# # COMMANDES "INTERFACE -> CLI" (Conversion depuis ENTREPÔT)
# # =============================================================================

# @app.command()
# def warehouse_convert(
#     patient_limit: int = typer.Option(0, "--patient-limit", "-n", help="Nb patients à convertir (0 = tout l'entrepôt)"),
#     page_size: int = typer.Option(100, "--page-size", help="Taille de page _count côté FHIR")
# ):
#     """
#     Equivalent 'convert dossier' mais depuis l'entrepôt HAPI.
#     POST /convert/fhir-warehouse-to-edsan
#     """

#     console.print("🔄 [bold cyan]Conversion  en cours...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/convert/fhir-warehouse-to-edsan"
#     payload = {"patient_limit": patient_limit, "page_size": page_size}
#     r = requests.post(url, json=payload, timeout=(10, 900))  # 10s connect, 15min read
#     _raise_if_error(r, "Conversion entrepôt -> EDS")

#     console.print("[bold green]✅ Conversion entrepôt terminée[/bold green]")
#     console.print_json(json.dumps(r.json(), ensure_ascii=False))


# @app.command()
# def warehouse_convert_patient(
#     patient_id: str = typer.Option(..., "--id", help="Patient ID dans l'entrepôt")
# ):
#     """
#     Equivalent '1 fichier Synthea patient' mais depuis l'entrepôt.
#     POST /convert/fhir-warehouse-patient-to-edsan
#     """

#     console.print("🔄 [bold cyan]Conversion en cours..[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/convert/fhir-warehouse-patient-to-edsan"
#     payload = {"patient_id": patient_id}
#     r = requests.post(url, json=payload)
#     _raise_if_error(r, "Conversion patient entrepôt -> EDS")

#     console.print("[bold green]✅ Conversion patient terminée[/bold green]")
#     console.print_json(json.dumps(r.json(), ensure_ascii=False))

# @app.command()
# def warehouse_convert_patients(
#     ids: List[str] = typer.Option(..., "--ids", "-i", help="Liste des Patient IDs (répéter -i ou utiliser --ids id1 --ids id2)"),
# ):
#     """
#     Convertit une LISTE de patients depuis l'entrepôt (HAPI) vers EDSan (parquet).
#     POST /convert/fhir-warehouse-patients-to-edsan
#     """
#     console.print("🔄 [bold cyan]Conversion multi-patients en cours...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/convert/fhir-warehouse-patients-to-edsan"
#     payload = {"patient_ids": ids}

#     r = requests.post(url, json=payload, timeout=(10, 900))
#     _raise_if_error(r, "Conversion liste patients entrepôt -> EDS")

#     console.print("[bold green]✅ Conversion multi-patients terminée[/bold green]")
#     console.print_json(json.dumps(r.json(), ensure_ascii=False))



# # =============================================================================
# # COMMANDES EDS (comme interface)
# # =============================================================================

# @app.command()
# def eds_tables():
#     """Liste les tables EDS disponibles."""

#     console.print("🔄 [bold cyan]Chargement des tables de l'EDS en cours...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/eds/tables"
#     r = requests.get(url, timeout=15)
#     _raise_if_error(r, "Liste tables EDS")

#     tables = r.json()
#     t = Table(title="Tables EDS (.parquet)", box=box.SIMPLE_HEAVY)
#     t.add_column("#", style="cyan", justify="right")
#     t.add_column("Nom", style="magenta")
#     for i, name in enumerate(tables, 1):
#         t.add_row(str(i), name)
#     console.print(t)


# @app.command()
# def eds_preview(
#     name: str,
#     limit: int = typer.Option(50, "--limit", "-l", help="Nombre de lignes à afficher")
# ):
#     """
#     Preview d’une table parquet avec limite choisie par l’utilisateur.
#     GET /eds/table/{name}?limit=...
#     """

#     console.print("🔄 [bold cyan]Veuillez patientez quelques instants....[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/eds/table/{name}"
#     r = requests.get(url, params={"limit": limit})
#     _raise_if_error(r, "Preview table EDS")

#     data = r.json()
#     preview = data.get("preview", [])
#     if not preview:
#         console.print("[yellow]Aucune ligne à afficher.[/yellow]")
#         raise typer.Exit(code=0)

#     cols = list(preview[0].keys())
#     table = Table(
#         title=f"{data.get('table', name)} — rows={data.get('rows')} cols={data.get('cols')} (preview {len(preview)})",
#         box=box.SIMPLE_HEAVY
#     )
#     for c in cols:
#         table.add_column(str(c))

#     for row in preview:
#         table.add_row(*[str(row.get(c, "")) for c in cols])

#     console.print(table)


# @app.command()
# def stats():

#     """Affiche les stats EDS."""

#     console.print("🔄 [bold cyan]Chargement des stats en cours...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/stats"
#     r = requests.get(url, timeout=15)

#     _raise_if_error(r, "Lecture stats")

#     data = r.json()
#     tables = data.get("tables", {})

#     t = Table(title=f"Stats EDS — dir={data.get('eds_dir', '')}", box=box.SIMPLE_HEAVY)
#     t.add_column("Table", style="magenta")
#     t.add_column("Rows", justify="right")
#     t.add_column("Cols", justify="right")

#     for name, st in tables.items():
#         t.add_row(name, str(st.get("rows", "?")), str(st.get("cols", "?")))

#     console.print(t)


# # =============================================================================
# # REPORTS (last_run + historique)
# # =============================================================================

# @app.command()
# def last_run():
#     """Affiche le last_run.json."""
#     url = f"{CONVERTER_API_URL}/report/last-run"
#     r = requests.get(url)
#     _raise_if_error(r, "Lecture last_run")
#     console.print_json(json.dumps(r.json(), ensure_ascii=False))


# @app.command()
# def runs():
#     """Liste l’historique des runs (archives)."""

#     console.print("🔄 [bold cyan]Chargement de l'historique...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/report/runs"
#     r = requests.get(url)
#     _raise_if_error(r, "Liste runs")

#     items = r.json()
#     t = Table(title="Historique des runs", box=box.SIMPLE_HEAVY)
#     t.add_column("Nom", style="magenta")
#     t.add_column("Taille", justify="right")
#     for it in items:
#         t.add_row(it.get("name", "?"), str(it.get("size", "?")))
#     console.print(t)


# @app.command()
# def download_run(name: str, out: Optional[str] = typer.Option(None, "--out", help="Chemin de sortie")):
#     """Télécharge un run archivé."""
#     url = f"{CONVERTER_API_URL}/report/run/{name}"
#     r = requests.get(url, stream=True)
#     _raise_if_error(r, "Téléchargement run")

#     out_path = Path(out) if out else Path(name)
#     out_path.parent.mkdir(parents=True, exist_ok=True)

#     with open(out_path, "wb") as f:
#         for chunk in r.iter_content(chunk_size=1024 * 256):
#             if chunk:
#                 f.write(chunk)

#     console.print(f"[bold green]✅ Run téléchargé -> {out_path.resolve()}[/bold green]")


# @app.command()
# def download_last_run(out: Optional[str] = typer.Option(None, "--out", help="Chemin de sortie")):
#     """Télécharge le last_run.json le plus récent."""

#     console.print("🔄 [bold cyan]Téléchargement du last run en cours...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/report/last-run"
#     r = requests.get(url)
#     _raise_if_error(r, "Téléchargement last_run")

#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     out_path = Path(out) if out else Path(f"last_run_{ts}.json")
#     out_path.parent.mkdir(parents=True, exist_ok=True)

#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(r.json(), f, ensure_ascii=False, indent=2)

#     console.print(f"[bold green]✅ last_run téléchargé -> {out_path.resolve()}[/bold green]")



# @app.command()
# def edsan_to_fhir_zip(
#     output: str = typer.Option("edsan_to_fhir.zip", "--output", "-o", help="Chemin où sauvegarder le ZIP")
# ):
#     """
#     Convertir EDSan → FHIR et télécharger un ZIP.
#     """
#     console.print("🔄 [bold cyan]Conversion EDSan → FHIR en cours...[/bold cyan]")
    
#     url = f"{CONVERTER_API_URL}/export/edsan-to-fhir-zip"
#     r = requests.post(url, stream=True, timeout=(10, 300))
#     _raise_if_error(r, "Export EDSan → FHIR ZIP")
    
#     output_path = Path(output)
#     output_path.parent.mkdir(parents=True, exist_ok=True)
    
#     with open(output_path, 'wb') as f:
#         for chunk in r.iter_content(chunk_size=8192):
#             if chunk:
#                 f.write(chunk)
    
#     size = output_path.stat().st_size
#     console.print(f"[bold green]✅ Export ZIP réussi → {output_path.resolve()}[/bold green]")
#     console.print(f"   Taille : {size:,} octets")


# @app.command()
# def edsan_to_fhir_push():
#     """
#     Convertir EDSan → FHIR et pousser vers l'entrepôt FHIR.
#     """
#     console.print("🔄 [bold cyan]Conversion et push vers FHIR en cours...[/bold cyan]")
    
#     url = f"{CONVERTER_API_URL}/export/edsan-to-fhir-warehouse"
#     r = requests.post(url, timeout=(10, 600))
#     _raise_if_error(r, "Push EDSan → FHIR warehouse")
    
#     result = r.json()
#     console.print("[bold green]✅ Push vers entrepôt FHIR réussi ![/bold green]")
    
#     summary = result.get("summary", {})
#     table = Table(title="Résumé de la conversion", box=box.SIMPLE_HEAVY)
#     table.add_column("Métrique", style="cyan")
#     table.add_column("Valeur", style="magenta", justify="right")
    
#     table.add_row("Bundles générés", str(summary.get("bundles_generated", 0)))
    
#     resources = summary.get("resources_per_type", {})
#     for res_type, count in resources.items():
#         table.add_row(f"  └─ {res_type}", str(count))
    
#     console.print(table)

# @app.command()
# def last_export():
#     """Affiche le dernier rapport d'exportation (EDSan -> FHIR)."""
#     url = f"{CONVERTER_API_URL}/report/last-export"
#     r = requests.get(url)
#     _raise_if_error(r, "Lecture last_export")
#     console.print_json(json.dumps(r.json(), ensure_ascii=False))


# @app.command()
# def export_runs():
#     """Liste l’historique des exports archivés (EDSan -> FHIR)."""
#     console.print("🔄 [bold cyan]Chargement de l'historique des exports...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/report/export-runs"
#     r = requests.get(url)
#     _raise_if_error(r, "Liste export_runs")

#     items = r.json()
#     if not items:
#         console.print("[yellow]Aucun historique d'export trouvé.[/yellow]")
#         return

#     t = Table(title="Historique des exports (EDSan -> FHIR)", box=box.SIMPLE_HEAVY)
#     t.add_column("Nom du fichier", style="magenta")
#     t.add_column("Taille", justify="right")
    
#     for it in items:
#         t.add_row(it.get("name", "?"), f"{it.get('size', 0):,} octets")
    
#     console.print(t)

# @app.command()
# def download_export_run(
#     name: str, 
#     out: Optional[str] = typer.Option(None, "--out", help="Chemin de sortie local")
#     ):
#     """ Télécharge un rapport d'export archivé spécifique."""
#     url = f"{CONVERTER_API_URL}/report/export-run/{name}"
#     r = requests.get(url, stream=True)
#     _raise_if_error(r, f"Téléchargement de l'export {name}")

#     out_path = Path(out) if out else Path(name)
#     out_path.parent.mkdir(parents=True, exist_ok=True)

#     with open(out_path, "wb") as f:
#         for chunk in r.iter_content(chunk_size=1024 * 256):
#             if chunk:
#                 f.write(chunk)

#     console.print(f"[bold green]✅ Rapport d'export téléchargé -> {out_path.resolve()}[/bold green]")


# @app.command()
# def download_last_export(
#     out: Optional[str] = typer.Option(None, "--out", help="Chemin de sortie local")
# ):
#     """Télécharge le rapport d'export le plus récent (last_export_fhir.json)."""
#     console.print("🔄 [bold cyan]Téléchargement du dernier rapport d'export...[/bold cyan]")

#     url = f"{CONVERTER_API_URL}/report/last-export"
#     r = requests.get(url)
#     _raise_if_error(r, "Téléchargement last_export")

#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     out_path = Path(out) if out else Path(f"last_export_{ts}.json")
#     out_path.parent.mkdir(parents=True, exist_ok=True)

#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(r.json(), f, ensure_ascii=False, indent=2)

#     console.print(f"[bold green]✅ Dernier rapport d'export téléchargé -> {out_path.resolve()}[/bold green]")

# if __name__ == "__main__":
#     app()


from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

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

from . import edsan_filter
from . import edsan_filter_to_fhir
from . import display_edsan

# ✅ AJOUT : on enregistre la commande import-url depuis le module import_url.py
from .import_url import import_url as import_url_cmd


app = typer.Typer(help="CLI CHU Rouen — Entrepôt FHIR (HAPI) + Conversion EDS")
console = Console()

app.add_typer(edsan_filter.app)
app.add_typer(edsan_filter_to_fhir.app)
app.add_typer(display_edsan.app)

# ✅ La commande existe maintenant vraiment au top-level :
# python -m client_pkg.src.main import-url --url ...
app.command("import-url")(import_url_cmd)


# Entrepôt FHIR (HAPI)
FHIR_URL = os.getenv("FHIR_URL", "http://localhost:8080/fhir")
FHIR_HEADERS = {"Accept": "application/fhir+json"}

# API Converter (FastAPI)
CONVERTER_API_URL = os.getenv("CONVERTER_API_URL", "http://localhost:8000/api/v1")


def _raise_if_error(resp: requests.Response, context: str):
    if 200 <= resp.status_code < 300:
        return
    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    raise typer.BadParameter(f"{context} — HTTP {resp.status_code} — {detail}")


# =============================================================================
# COMMANDES FHIR (EXISTANTES)
# =============================================================================

@app.command()
def info():
    """Vérifie si le serveur FHIR est en ligne (metadata)."""
    try:
        r = requests.get(f"{FHIR_URL}/metadata", headers=FHIR_HEADERS, timeout=10)
        if r.status_code == 200:
            console.print("[bold green]✅ Serveur FHIR en ligne[/bold green]")
            console.print(f"URL: [cyan]{FHIR_URL}[/cyan]")
            console.print(f"FHIR Version: {r.json().get('fhirVersion', '?')}")
        else:
            console.print("[bold red]❌ Erreur serveur FHIR[/bold red]")
            console.print(r.text)
    except Exception as e:
        console.print(f"[bold red]❌ Impossible de contacter le serveur FHIR: {e}[/bold red]")


def _patient_row(res: dict):
    pid = res.get("id", "?")
    family = "N/A"
    given = "N/A"
    if "name" in res and res["name"]:
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
        table = Table(title=f"Patient {patient_id}", box=box.SIMPLE_HEAVY)
        table.add_column("ID", style="cyan")
        table.add_column("Nom", style="magenta")
        table.add_column("Prénom", style="green")
        table.add_column("Naissance")
        table.add_column("Genre")
        table.add_row(*_patient_row(p))
        console.print(table)
    else:
        console.print(f"[red]Patient {patient_id} introuvable (HTTP {r.status_code})[/red]")


@app.command()
def get_patients(ids: List[str]):
    """Récupère plusieurs patients par IDs."""
    ids_param = ",".join(ids)
    url = f"{FHIR_URL}/Patient"
    r = requests.get(url, params={"_id": ids_param}, headers=FHIR_HEADERS)
    if r.status_code != 200:
        console.print(f"[red]Erreur (HTTP {r.status_code})[/red]")
        return

    bundle = r.json()
    entries = bundle.get("entry", []) or []

    table = Table(title=f"Patients demandés: {len(ids)}", box=box.SIMPLE_HEAVY)
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
        console.print_json(json.dumps(r.json(), ensure_ascii=False))
    else:
        console.print(f"[red]Ressource introuvable (HTTP {r.status_code})[/red]")


# =============================================================================
# COMMANDES "INTERFACE -> CLI" (Conversion depuis ENTREPÔT)
# =============================================================================

@app.command()
def warehouse_convert(
    patient_limit: int = typer.Option(0, "--patient-limit", "-n", help="Nb patients à convertir (0 = tout l'entrepôt)"),
    page_size: int = typer.Option(100, "--page-size", help="Taille de page _count côté FHIR"),
    eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
    reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
):
    """
    POST /convert/fhir-warehouse-to-edsan
    """
    console.print("🔄 [bold cyan]Conversion en cours...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/convert/fhir-warehouse-to-edsan"
    payload = {"patient_limit": patient_limit, "page_size": page_size}
    if eds_dir.strip():
        payload["eds_dir"] = eds_dir.strip()
    if reports_dir.strip():
        payload["reports_dir"] = reports_dir.strip()

    r = requests.post(url, json=payload, timeout=(10, 900))
    _raise_if_error(r, "Conversion entrepôt -> EDS")

    console.print("[bold green]✅ Conversion entrepôt terminée[/bold green]")
    console.print_json(json.dumps(r.json(), ensure_ascii=False))


@app.command()
def warehouse_convert_patient(
    patient_id: str = typer.Option(..., "--id", help="Patient ID dans l'entrepôt"),
    eds_dir: str = typer.Option("", "--eds-dir", help="Dossier EDS destination (optionnel)"),
    reports_dir: str = typer.Option("", "--reports-dir", help="Dossier reports import (optionnel)"),
):
    """
    POST /convert/fhir-warehouse-patient-to-edsan
    """
    console.print("🔄 [bold cyan]Conversion en cours...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/convert/fhir-warehouse-patient-to-edsan"
    payload = {"patient_id": patient_id}
    if eds_dir.strip():
        payload["eds_dir"] = eds_dir.strip()
    if reports_dir.strip():
        payload["reports_dir"] = reports_dir.strip()

    r = requests.post(url, json=payload, timeout=(10, 900))
    _raise_if_error(r, "Conversion patient entrepôt -> EDS")

    console.print("[bold green]✅ Conversion patient terminée[/bold green]")
    console.print_json(json.dumps(r.json(), ensure_ascii=False))


# =============================================================================
# COMMANDES EDS (comme interface)
# =============================================================================

@app.command()
def eds_tables():
    """Liste les tables EDS disponibles."""
    console.print("🔄 [bold cyan]Chargement des tables de l'EDS...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/eds/tables"
    r = requests.get(url, timeout=15)
    _raise_if_error(r, "Liste tables EDS")

    tables = r.json()
    t = Table(title="Tables EDS (.parquet)", box=box.SIMPLE_HEAVY)
    t.add_column("#", style="cyan", justify="right")
    t.add_column("Nom", style="magenta")
    for i, name in enumerate(tables, 1):
        t.add_row(str(i), name)
    console.print(t)


@app.command()
def stats(
    eds_dir: str = typer.Option("", "--eds-dir", help="Override EDS dir (optionnel)"),
    reports_dir: str = typer.Option("", "--reports-dir", help="Override reports dir (optionnel)"),
):
    """Affiche les stats EDS."""
    console.print("🔄 [bold cyan]Chargement des stats...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/stats"
    params = {}
    if eds_dir.strip():
        params["eds_dir"] = eds_dir.strip()
    if reports_dir.strip():
        params["reports_dir"] = reports_dir.strip()

    r = requests.get(url, params=params, timeout=15)
    _raise_if_error(r, "Lecture stats")

    data = r.json()
    tables = data.get("tables", {})

    t = Table(title=f"Stats EDS — dir={data.get('eds_dir', '')}", box=box.SIMPLE_HEAVY)
    t.add_column("Table", style="magenta")
    t.add_column("Rows", justify="right")
    t.add_column("Cols", justify="right")

    for name, st in tables.items():
        t.add_row(name, str(st.get("rows", "?")), str(st.get("cols", "?")))

    console.print(t)


# =============================================================================
# REPORTS (last_run + historique)
# =============================================================================

@app.command()
def last_run():
    """Affiche le last_run.json."""
    url = f"{CONVERTER_API_URL}/report/last-run"
    r = requests.get(url)
    _raise_if_error(r, "Lecture last_run")
    console.print_json(json.dumps(r.json(), ensure_ascii=False))


@app.command()
def runs():
    """Liste l’historique des runs (archives)."""
    console.print("🔄 [bold cyan]Chargement de l'historique...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/report/runs"
    r = requests.get(url)
    _raise_if_error(r, "Liste runs")

    items = r.json()
    t = Table(title="Historique des runs", box=box.SIMPLE_HEAVY)
    t.add_column("Nom", style="magenta")
    t.add_column("Taille", justify="right")
    for it in items:
        t.add_row(it.get("name", "?"), str(it.get("size", "?")))
    console.print(t)


@app.command()
def download_last_run(out: Optional[str] = typer.Option(None, "--out", help="Chemin de sortie")):
    """Télécharge le last_run.json le plus récent."""
    console.print("🔄 [bold cyan]Téléchargement du last run...[/bold cyan]")

    url = f"{CONVERTER_API_URL}/report/last-run"
    r = requests.get(url)
    _raise_if_error(r, "Téléchargement last_run")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(out) if out else Path(f"last_run_{ts}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(r.json(), f, ensure_ascii=False, indent=2)

    console.print(f"[bold green]✅ last_run téléchargé -> {out_path.resolve()}[/bold green]")


if __name__ == "__main__":
    app()



