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
def search_patient(name: str = ""):
    """Cherche un patient par son nom."""
    url = f"{FHIR_URL}/Patient"
    params = {}
    if name:
        params["name"] = name
    
    response = requests.get(url, params=params)
    bundle = response.json()
    
    # Affichage joli avec un tableau
    table = Table(title=f"Résultats pour '{name}'")
    table.add_column("ID", style="cyan")
    table.add_column("Nom", style="magenta")
    table.add_column("Date Naissance")

    if "entry" in bundle:
        for entry in bundle["entry"]:
            res = entry["resource"]
            pat_id = res.get("id", "?")
            
            # Extraction du nom (un peu complexe en FHIR)
            family = "Inconnu"
            if "name" in res and len(res["name"]) > 0:
                family = res["name"][0].get("family", "")
            
            birth = res.get("birthDate", "N/A")
            table.add_row(pat_id, family, birth)
        
        console.print(table)
    else:
        console.print("[yellow]Aucun patient trouvé.[/yellow]")

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
