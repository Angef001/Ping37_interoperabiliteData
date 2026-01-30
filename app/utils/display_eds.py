import polars as pl
import os
from pathlib import Path
import argparse


pl.Config.set_tbl_cols(-1)


CURRENT_DIR = Path(__file__).resolve().parent      # .../app/utils
PROJECT_ROOT = CURRENT_DIR.parents[1]              # .../Ping37_interoperabiliteData
DEFAULT_EDS_DIR= PROJECT_ROOT / "eds"



def display_tables(eds_dir: Path):
    """
    Parcourt les fichiers Parquet attendus dans le dossier EDS 
    et affiche un aperçu de leur contenu.
    """
    eds_dir = eds_dir.resolve()
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


def main():
    parser = argparse.ArgumentParser(
        description="Inspection des tables EDS (Parquet)"
    )

    parser.add_argument(
        "--eds-dir",
        type=Path,
        default=DEFAULT_EDS_DIR,
        help="Chemin du dossier EDS (par défaut: <project_root>/eds)",
    )

    args = parser.parse_args()
    display_tables(args.eds_dir)


if __name__ == "__main__":
    main()
