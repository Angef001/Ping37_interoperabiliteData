import polars as pl
import os
from pathlib import Path
import argparse

# =============================================================================
# CONFIGURATION
# =============================================================================
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parents[1]

def display_tables(eds_dir: Path):
    """
    Parcourt les fichiers Parquet attendus dans le dossier EDS 
    et affiche un aperçu de leur contenu (dimensions et premières lignes).
    """
    print(f"Inspection du dossier EDS : {eds_dir}\n")
    
    if not eds_dir.exists():
        print(f"[ERREUR] Le dossier EDS est introuvable : {eds_dir}")
        return

    tables = [
        "patient.parquet",
        "mvt.parquet",
        "biol.parquet",
        "pharma.parquet",
        "pmsi.parquet",
        "doceds.parquet",
    ]

    for table in tables:
        file_path = eds_dir / table
        print(f"TABLE : {table}")

        if file_path.exists():
            try:
                df = pl.read_parquet(file_path)
                print(f"   Volumétrie : {df.height} lignes x {df.width} colonnes")
                print(df.head(5))
                print("-" * 60)
            except Exception as e:
                print(f"   [ERREUR] Lecture impossible : {e}")
        else:
            print("   [ABSENT] Fichier introuvable.")

        print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--eds-dir",
        type=Path,
        default=PROJECT_ROOT / "eds",
        help="Chemin vers le dossier EDS"
    )
    args = parser.parse_args()

    display_tables(args.eds_dir)
