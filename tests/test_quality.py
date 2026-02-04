import polars as pl
import os
import glob

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EDS_DIR = os.path.join(BASE_DIR, "eds") 
# EDS_DIR = r"C:\Projets\Ping\eds"

# =============================================================================
# UTILITAIRES D'AFFICHAGE
# =============================================================================
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_separator(char="=", length=100):
    print(f"{Colors.BLUE}{char * length}{Colors.ENDC}")

def format_fill_rate(rate):
    """Retourne une barre de progression ASCII et la couleur associée."""
    bar_length = 10
    filled_length = int(round(bar_length * rate / 100))
    # Utilisation de caracteres ASCII standard
    bar = "#" * filled_length + "." * (bar_length - filled_length)
    
    val_str = f"{rate:6.2f}%"
    
    if rate == 100:
        return f"{Colors.GREEN}[{bar}] {val_str}{Colors.ENDC}"
    elif rate == 0:
        return f"{Colors.FAIL}[{bar}] {val_str}{Colors.ENDC}"
    elif rate < 20:
        return f"{Colors.WARNING}[{bar}] {val_str}{Colors.ENDC}"
    else:
        return f"{Colors.CYAN}[{bar}] {val_str}{Colors.ENDC}"

def print_table(headers, rows):
    """Affiche un tableau aligné."""
    # Calcul des largeurs de colonnes
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Ajout de padding
    col_widths = [w + 2 for w in col_widths]
    
    # Ligne de formatage
    row_format = "".join([f"{{:<{w}}}" for w in col_widths])
    
    # Affichage
    print(f"{Colors.BOLD}{row_format.format(*headers)}{Colors.ENDC}")
    print("-" * sum(col_widths))
    
    for row in rows:
        print(row_format.format(*[str(c) for c in row]))
    print("\n")

# =============================================================================
# MOTEUR D'ANALYSE
# =============================================================================

def analyze_dataframe(df, table_name):
    print_separator("=")
    print(f"{Colors.HEADER}TABLE : {table_name.upper()}{Colors.ENDC}")
    print(f"   Dimensions : {Colors.BOLD}{df.height}{Colors.ENDC} lignes x {Colors.BOLD}{len(df.columns)}{Colors.ENDC} colonnes")
    print_separator("-")

    if df.height == 0:
        print(f"{Colors.FAIL}[VIDE] LA TABLE EST VIDE{Colors.ENDC}")
        return

    headers = ["Colonne", "Type", "Rempli (Not Null)", "Taux %", "Uniques", "Exemple (Non-Null)"]
    rows = []

    for col in df.columns:
        # Calculs statistiques
        null_count = df[col].null_count()
        count = df.height
        filled_count = count - null_count
        fill_rate = (filled_count / count) * 100
        n_unique = df[col].n_unique()
        dtype = str(df[col].dtype)
        
        # Recuperation d'un exemple pertinent
        sample_val = "-"
        if filled_count > 0:
            sample_df = df.select(pl.col(col)).drop_nulls().head(1)
            if not sample_df.is_empty():
                val = sample_df[0,0]
                sample_val = str(val)
                if len(sample_val) > 30:
                    sample_val = sample_val[:27] + "..."
        
        # Formatage
        rows.append([
            col,
            dtype,
            f"{filled_count}/{count}",
            format_fill_rate(fill_rate), 
            n_unique,
            sample_val
        ])

    # Affichage avec alignement manuel pour gerer les codes couleurs
    col_widths = [len(h) for h in headers]
    clean_rows = []
    
    for r in rows:
        clean_r = []
        for cell in r:
            # Nettoyage des codes couleurs pour le calcul de largeur
            clean_cell = str(cell).replace(Colors.GREEN, "").replace(Colors.FAIL, "").replace(Colors.WARNING, "").replace(Colors.CYAN, "").replace(Colors.ENDC, "")
            clean_r.append(clean_cell)
        
        for i, val in enumerate(clean_r):
            col_widths[i] = max(col_widths[i], len(val))
        clean_rows.append(r) 

    col_widths = [w + 3 for w in col_widths]
    
    # Affichage Header
    header_str = ""
    for i, h in enumerate(headers):
        header_str += f"{h:<{col_widths[i]}}"
    print(f"{Colors.BOLD}{header_str}{Colors.ENDC}")
    print("-" * len(header_str))

    # Affichage Rows
    for i, row in enumerate(clean_rows):
        row_str = ""
        for j, cell in enumerate(row):
            clean_len = len(str(cell).replace(Colors.GREEN, "").replace(Colors.FAIL, "").replace(Colors.WARNING, "").replace(Colors.CYAN, "").replace(Colors.ENDC, ""))
            padding = " " * (col_widths[j] - clean_len)
            row_str += f"{cell}{padding}"
        print(row_str)

    print("\n")


def check_global_integrity(tables):
    print_separator("=")
    print(f"{Colors.HEADER}CONTROLE D'INTEGRITE GLOBAL{Colors.ENDC}")
    print_separator("-")

    # Liens a verifier (Enfant -> Parent)
    relationships = [
        ("mvt.parquet", "PATID", "patient.parquet", "PATID"),
        ("biol.parquet", "EVTID", "mvt.parquet", "EVTID"),
        ("pharma.parquet", "EVTID", "mvt.parquet", "EVTID"),
        ("pmsi.parquet", "EVTID", "mvt.parquet", "EVTID"),
        ("doceds.parquet", "EVTID", "mvt.parquet", "EVTID"),
    ]

    for child_name, child_col, parent_name, parent_col in relationships:
        if child_name in tables and parent_name in tables:
            df_child = tables[child_name]
            df_parent = tables[parent_name]
            
            if df_child.height == 0: continue

            if child_col in df_child.columns and parent_col in df_parent.columns:
                child_ids = df_child.select(pl.col(child_col)).unique()
                orphans = child_ids.join(df_parent, left_on=child_col, right_on=parent_col, how="anti")
                count_orphans = orphans.height
                
                status = f"{Colors.GREEN}[OK]{Colors.ENDC}" if count_orphans == 0 else f"{Colors.FAIL}[ERREUR] {count_orphans} Orphelins{Colors.ENDC}"
                
                print(f"{child_name:<15} ({child_col}) -> {parent_name:<15} : {status}")
            else:
                 print(f"{Colors.WARNING}[WARN] Colonnes manquantes pour verifier {child_name} -> {parent_name}{Colors.ENDC}")


def main():
    if not os.path.exists(EDS_DIR):
        print(f"Erreur : Dossier {EDS_DIR} introuvable.")
        return

    parquet_files = glob.glob(os.path.join(EDS_DIR, "*.parquet"))
    parquet_files.sort()

    tables = {}

    # 1. Analyse par table
    for file_path in parquet_files:
        file_name = os.path.basename(file_path)
        try:
            df = pl.read_parquet(file_path)
            tables[file_name] = df
            analyze_dataframe(df, file_name)
        except Exception as e:
            print(f"{Colors.FAIL}Erreur lecture {file_name}: {e}{Colors.ENDC}")

    # 2. Analyse d'integrite
    check_global_integrity(tables)

    print_separator("=")
    print("FIN DE L'ANALYSE")

if __name__ == "__main__":
    main()
