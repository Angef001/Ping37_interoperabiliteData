import polars as pl
import os
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
# Calcul du chemin racine du projet pour localiser le dossier de données
CURRENT_DIR = Path(__file__).resolve().parent      # .../app/utils
PROJECT_ROOT = CURRENT_DIR.parents[1]              # .../Ping37_interoperabiliteData
EDS_DIR = PROJECT_ROOT / "eds"

def display_tables():
    """
    Parcourt les fichiers Parquet attendus dans le dossier EDS 
    et affiche un aperçu de leur contenu (dimensions et premières lignes).
    """
    print(f"Inspection du dossier EDS : {EDS_DIR}\n")
    
    # Vérification de l'existence du dossier de données
    if not os.path.exists(EDS_DIR):
        print(f"[ERREUR] Le dossier 'eds/' est introuvable à l'emplacement : {EDS_DIR}")
        print("Veuillez lancer le script de construction (build_eds_with_fhir.py) avant l'inspection.")
        return

    # Liste des tables critiques à inspecter
    tables = ["patient.parquet", "mvt.parquet", "biol.parquet", "pharma.parquet", "pmsi.parquet", "doceds.parquet"]

    for table in tables:
        file_path = os.path.join(EDS_DIR, table)
        
        print(f"TABLE : {table}")
        
        if os.path.exists(file_path):
            try:
                # Chargement rapide du fichier Parquet avec Polars
                df = pl.read_parquet(file_path)
                
                # Affichage des métriques techniques (Volumétrie)
                print(f"   Volumétrie : {df.height} lignes x {df.width} colonnes")
                
                # Aperçu des données (5 premières lignes)
                print(df.head(5))
                print("-" * 60)
                
            except Exception as e:
                print(f"   [ERREUR] Impossible de lire le fichier : {e}")
        else:
            print("   [ABSENT] Fichier introuvable. Vérifiez la configuration du mapping.")
        
        print("\n")

if __name__ == "__main__":
    display_tables()
