# client_pkg/edsan_to_fhir_cli.py
import click
import requests

API_BASE_URL = "http://localhost:8000"  # Ajuste selon ton port

@click.group()
def cli():
    """CLI pour la conversion EDSan → FHIR"""
    pass

@cli.command(name="export-zip")
@click.option('--output', default="edsan_to_fhir.zip", help="Chemin où sauvegarder le ZIP")
def export_zip(output):
    """Convertir EDSan → FHIR et télécharger un ZIP"""
    click.echo("🔄 Conversion EDSan → FHIR en cours...")
    
    response = requests.post(
        f"{API_BASE_URL}/api/v1/export/edsan-to-fhir-zip",
        stream=True  # ← IMPORTANT pour les gros fichiers
    )
    
    if response.ok:
        with open(output, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):  # ← Télécharge par morceaux
                if chunk:
                    f.write(chunk)
        click.echo(f"✅ Export ZIP réussi : {output}")
        
        # Vérification
        import os
        size = os.path.getsize(output)
        click.echo(f"   Taille du fichier : {size} octets")
    else:
        click.echo(f"❌ Erreur {response.status_code}: {response.text}", err=True)

@cli.command(name="push-warehouse")
def push_warehouse():
    """Convertir EDSan → FHIR et pousser vers l'entrepôt FHIR"""
    click.echo("🔄 Conversion et push vers FHIR en cours...")
    
    response = requests.post(f"{API_BASE_URL}/api/v1/export/edsan-to-fhir-warehouse")
    
    if response.ok:
        result = response.json()
        click.echo("✅ Push vers entrepôt FHIR réussi !")
        click.echo(f"  • Bundles générés : {result['summary']['bundles_generated']}")
        click.echo(f"  • Ressources : {result['summary']['resources_per_type']}")
    else:
        click.echo(f"❌ Erreur {response.status_code}: {response.text}", err=True)

if __name__ == '__main__':
    cli()