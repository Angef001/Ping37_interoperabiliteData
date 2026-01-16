import json
import glob
import os
import polars as pl
from datetime import datetime

# =============================================================================
# CONFIGURATION DES CHEMINS
# =============================================================================
# Définition dynamique des chemins pour garantir la portabilité du script
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))

MAPPING_FILE = os.path.join(PROJECT_ROOT, "app", "core", "config", "mapping.json")
FHIR_DIR = os.path.join(PROJECT_ROOT, "synthea", "output", "fhir")
EDS_DIR = os.path.join(PROJECT_ROOT, "eds")

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def compute_age(birthdate_str):
    """
    Calcule l'âge en années à partir d'une chaîne de date (format YYYY-MM-DD).
    Retourne None si la date est invalide ou manquante.
    """
    if not birthdate_str:
        return None
    try:
        # On ne garde que les 10 premiers caractères pour ignorer l'heure si présente
        bd = datetime.strptime(str(birthdate_str)[:10], "%Y-%m-%d")
        today = datetime.now()
        # Calcul précis prenant en compte le mois et le jour actuel
        return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
    except:
        return None

def get_value_from_path(data: dict, path: str):
    """
    Navigue dans un dictionnaire imbriqué (JSON) via un chemin sous forme de chaîne.
    Supporte la notation par points (.) et les index de listes (ex: [0]).
    Nettoie automatiquement les préfixes techniques FHIR (urn:uuid:, Patient/, etc).
    """
    if not path or data is None: 
        return None
    
    # Cas particulier : le type de ressource est souvent à la racine du JSON
    if path == "resourceType":
        return data.get("resourceType")

    # Transformation du chemin "address[0].city" en liste ["address", "0", "city"]
    elements = path.replace("[", ".").replace("]", "").split(".")
    current = data
    
    for key in elements:
        if current is None: return None
        
        # Gestion des index de listes
        if key.isdigit(): 
            idx = int(key)
            if isinstance(current, list) and len(current) > idx:
                current = current[idx]
            else:
                return None
        # Gestion des clés de dictionnaires
        elif isinstance(current, dict) and key in current: 
            current = current[key]
        else:
            return None
            
    # Nettoyage des identifiants (suppression des préfixes standards FHIR)
    if isinstance(current, str):
        for prefix in ["urn:uuid:", "Patient/", "Encounter/", "Practitioner/"]:
            current = current.replace(prefix, "")
    return current

# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================

def build_eds():
    print("Démarrage de la construction de l'EDS...")
    
    # Vérification de la présence du fichier de configuration
    if not os.path.exists(MAPPING_FILE):
        print(f"[ERREUR] Fichier de mapping introuvable : {MAPPING_FILE}")
        return

    # Chargement des règles de correspondance
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
        mapping_rules = json.load(f)

    # Initialisation des tampons (buffers)
    # On utilise un dictionnaire de listes pour stocker les données en mémoire
    # avant de les convertir en DataFrame Polars.
    buffers = {rule["table_name"]: [] for rule in mapping_rules.values()}
    
    # Récupération de la liste des fichiers JSON générés
    fhir_files = glob.glob(os.path.join(FHIR_DIR, "*.json"))
    print(f"Traitement de {len(fhir_files)} fichiers source...")
    
    # Création du dossier de sortie s'il n'existe pas
    os.makedirs(EDS_DIR, exist_ok=True)
    count = 0

    # Boucle de lecture et d'extraction
    for file_path in fhir_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)
        except Exception as e:
            print(f"[ATTENTION] Erreur de lecture sur le fichier {file_path}: {e}")
            continue
        
        if "entry" not in bundle: continue

        for entry in bundle["entry"]:
            resource = entry.get("resource", {})
            rtype = resource.get("resourceType")

            # Si le type de ressource est défini dans le mapping, on l'extrait
            if rtype in mapping_rules:
                rule = mapping_rules[rtype]
                target_table = rule["table_name"]
                columns_map = rule["columns"]
                
                new_row = {}
                # Extraction dynamique des champs selon le mapping
                for col_name, json_path in columns_map.items():
                    new_row[col_name] = get_value_from_path(resource, json_path)
                
                buffers[target_table].append(new_row)
        
        count += 1
        if count % 10 == 0: 
            print(f"   ... {count} fichiers traités")

    # Post-traitement et sauvegarde
    print("Sauvegarde des fichiers Parquet et application des règles métiers...")
    
    for table_name, data_rows in buffers.items():
        if not data_rows:
            print(f"[INFO] La table {table_name} est vide, aucun fichier généré.")
            continue

        df = pl.DataFrame(data_rows)

        # Règle métier 1 : Calcul de l'âge
        # Si la table contient une date de naissance (PATBD), on génère la colonne PATAGE
        if table_name == "patient.parquet" and "PATBD" in df.columns:
            df = df.with_columns(
                pl.col("PATBD").map_elements(compute_age, return_dtype=pl.Int64).alias("PATAGE")
            )
            print(f"   - Colonne PATAGE calculée pour {table_name}")

        # Règle métier 2 : Valeurs par défaut
        # Si le service hospitalier (SEJUM) est manquant, on applique une valeur par défaut
        if table_name == "mvt.parquet" and "SEJUM" in df.columns:
             df = df.with_columns(pl.col("SEJUM").fill_null("Service Général"))

        # Écriture sur le disque au format Parquet
        output_path = os.path.join(EDS_DIR, table_name)
        df.write_parquet(output_path)
        print(f"[SUCCES] {table_name} généré ({len(df)} lignes)")

    print("Construction terminée.")

if __name__ == "__main__":
    build_eds()