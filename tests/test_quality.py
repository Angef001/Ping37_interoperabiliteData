import polars as pl
import os
import sys

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EDS_DIR = os.path.join(BASE_DIR, "eds")

def run_tests():
    """
    Exécute une batterie de tests de qualité sur les données de l'EDS.
    Vérifie l'existence des fichiers, l'unicité des clés primaires
    et l'intégrité référentielle entre les tables.
    """
    print("Démarrage des tests de qualité des données...\n")
    errors = 0
    
    # 1. Validation de l'environnement
    if not os.path.exists(EDS_DIR):
        print(f"[CRITIQUE] Le dossier 'eds' est introuvable : {EDS_DIR}")
        return

    # 2. Contrôle de la table PATIENT (Table Maîtresse)
    pat_path = os.path.join(EDS_DIR, "patient.parquet")
    
    if os.path.exists(pat_path):
        try:
            df_pat = pl.read_parquet(pat_path)
            
            # Test A : Volumétrie
            if df_pat.height > 0:
                print(f"[OK] PATIENT : {df_pat.height} enregistrements trouvés.")
            else:
                print("[ERREUR] PATIENT : La table est vide.")
                errors += 1
                
            # Test B : Unicité de la Clé Primaire (PATID)
            if df_pat["PATID"].n_unique() == df_pat.height:
                print("[OK] PATIENT : Unicité des identifiants respectée.")
            else:
                print("[ERREUR] PATIENT : Doublons détectés dans les identifiants.")
                errors += 1
        except Exception as e:
            print(f"[ERREUR] PATIENT : Échec de lecture du fichier ({e})")
            errors += 1
    else:
        print("[ERREUR] PATIENT : Fichier patient.parquet manquant.")
        errors += 1

    # 3. Contrôle de la table MVT (Séjours)
    mvt_path = os.path.join(EDS_DIR, "mvt.parquet")
    
    if os.path.exists(mvt_path):
        try:
            df_mvt = pl.read_parquet(mvt_path)
            
            # Test C : Intégrité Référentielle (MVT -> PATIENT)
            # Chaque séjour doit être rattaché à un patient existant dans la table PATIENT
            if os.path.exists(pat_path) and 'df_pat' in locals():
                valid_ids = df_pat["PATID"]
                
                # Filtrage des séjours dont le PATID n'est pas dans la liste des patients valides
                invalid_links = df_mvt.filter(~pl.col("PATID").is_in(valid_ids))
                
                if invalid_links.height == 0:
                    print("[OK] MVT : Intégrité référentielle validée.")
                else:
                    print(f"[ERREUR] MVT : {invalid_links.height} séjours orphelins (Patient inconnu).")
                    errors += 1
        except Exception as e:
             print(f"[ERREUR] MVT : Échec de lecture du fichier ({e})")
    else:
        print("[INFO] MVT : Fichier absent (Ce n'est pas une erreur critique si le mapping est partiel).")

    # Bilan final
    print("-" * 50)
    if errors == 0:
        print("RÉSULTAT : SUCCES (Tous les tests sont valides)")
    else:
        print(f"RÉSULTAT : ECHEC ({errors} erreur(s) détectée(s))")
    print("-" * 50)

if __name__ == "__main__":
    run_tests()