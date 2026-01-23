import typer
import requests
import json
from rich.console import Console
from rich.table import Table
import os

# Initialisation
app = typer.Typer()
console = Console()

# URL par défaut (localhost) ou URL définie par l'environnement (Docker/Podman)
DEFAULT_URL = "http://localhost:8080/fhir"
FHIR_URL = os.getenv("FHIR_URL", DEFAULT_URL)

def create_patient_table(title: str) -> Table:
    """Utilitaire pour créer un beau tableau Rich."""
    table = Table(title=title)
    table.add_column("ID", style="cyan")
    table.add_column("Nom de famille", style="magenta")
    table.add_column("Prénom", style="green")
    table.add_column("Date de naissance")
    return table

def extract_patient_data(res: dict):
    """Extrait proprement les données d'une ressource Patient FHIR."""
    pat_id = res.get("id", "?")
    family = "N/A"
    given = "N/A"
    if "name" in res and len(res["name"]) > 0:
        family = res["name"][0].get("family", "N/A")
        given = " ".join(res["name"][0].get("given", ["N/A"]))
    birth = res.get("birthDate", "N/A")
    return pat_id, family, given, birth

@app.command()
def info():
    """Vérifie si le serveur est en ligne."""
    try:
        r = requests.get(f"{FHIR_URL}/metadata")
        if r.status_code == 200:
            console.print("[bold green] Serveur FHIR en ligne ![/bold green]")
            console.print(f"Version FHIR: {r.json()['fhirVersion']}")
        else:
            console.print("[bold red] Serveur répond avec erreur.[/bold red]")
    except Exception as e:
        console.print(f"[bold red] Impossible de contacter le serveur : {e}[/bold red]")

@app.command()
def get_patient(patient_id: str):
    """Recherche un patient unique par son ID."""
    url = f"{FHIR_URL}/Patient/{patient_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        res = response.json()
        table = create_patient_table(f"Patient ID: {patient_id}")
        table.add_row(*extract_patient_data(res))
        console.print(table)
    else:
        console.print(f"[red]Patient {patient_id} introuvable (Erreur {response.status_code})[/red]")

@app.command()
def get_patients(ids: list[str]):
    """
    Recherche plusieurs patients. 
    Usage: chu-fhir get-patients 1 2 3
    """
    # En FHIR, on peut filtrer par plusieurs IDs avec l'opérateur virgule dans l'URL
    ids_param = ",".join(ids)
    url = f"{FHIR_URL}/Patient"
    params = {"_id": ids_param}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        bundle = response.json()
        table = create_patient_table(f"Liste des Patients ({len(ids)} demandés)")
        
        found_count = 0
        if "entry" in bundle:
            for entry in bundle["entry"]:
                table.add_row(*extract_patient_data(entry["resource"]))
                found_count += 1
            console.print(table)
            console.print(f"[dim]{found_count} patient(s) trouvé(s) sur {len(ids)} demandés.[/dim]")
        else:
            console.print("[yellow]Aucun de ces IDs n'a été trouvé.[/yellow]")
    else:
        console.print(f"[red]Erreur lors de la requête groupée ({response.status_code})[/red]")

@app.command()
def get_resource(resource_type: str, resource_id: str):
    """Récupère le JSON brut d'une ressource (ex: Observation/123)."""
    url = f"{FHIR_URL}/{resource_type}/{resource_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        console.print_json(json.dumps(response.json()))
    else:
        console.print(f"[red]Erreur {response.status_code}: Ressource introuvable[/red]")

if __name__ == "__main__":
    app()
