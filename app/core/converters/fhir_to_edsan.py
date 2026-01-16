import json
import glob
import os
import polars as pl
from datetime import datetime

# =====================================================
# 1. CONFIGURATION DES CHEMINS
# =====================================================

# Chemin vers les fichiers bruts generes par Synthea
FHIR_PATH = r"C:\Projets\Ping\synthea\output\fhir\*.json"

# Dossier de destination pour les tables EDS
EDS_PATH = "eds/"

os.makedirs(EDS_PATH, exist_ok=True)

# Listes pour stocker les donnees avant conversion en DataFrame
PATIENT_rows = []
MVT_rows = []
BIOL_rows = []
PHARMA_rows = []
PMSI_rows = []
DOCEDS_rows = []

# Dictionnaires pour les jointures rapides (lookup)
MEDICATION_DICT = {}
ENCOUNTER_DICT = {}

# =====================================================
# 2. FONCTIONS UTILITAIRES CRITIQUES
# =====================================================

def safe_get(obj, *keys):
    """
    Permet de recuperer une valeur profonde dans un JSON
    sans faire planter le script si une cle manque.
    """
    for k in keys:
        if obj is None or not isinstance(obj, dict) or k not in obj:
            return None
        obj = obj[k]
    return obj

def clean_id(raw_id):
    """
    FONCTION VITALE: Nettoie les IDs pour permettre la fusion.
    Transforme 'urn:uuid:123-abc' en '123-abc'.
    """
    if not raw_id: return None
    s = str(raw_id)
    for p in ["urn:uuid:", "Patient/", "Encounter/", "Medication/"]:
        s = s.replace(p, "")
    return s

def compute_age(birthdate, event_date):
    """
    Calcule l'age exact du patient au moment de l'evenement.
    Gere les chaines de caracteres ISO-8601.
    """
    if not birthdate or not event_date: return None
    try:
        # On ne garde que les 10 premiers caracteres (YYYY-MM-DD)
        bd = datetime.fromisoformat(str(birthdate)[:10])
        ev = datetime.fromisoformat(str(event_date)[:10])
        return ev.year - bd.year - ((ev.month, ev.day) < (bd.month, bd.day))
    except:
        return None

def extract_date(resource, *keys):
    """
    Cherche une date valide parmi plusieurs champs possibles
    (ex: date effective ou date de debut de periode).
    """
    for k in keys:
        if resource.get(k): return resource.get(k)
    return resource.get("performedPeriod", {}).get("start")

# =====================================================
# 3. LECTURE ET EXTRACTION DES DONNEES
# =====================================================

files = glob.glob(FHIR_PATH)
print(f"Traitement de {len(files)} fichiers...")

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if "entry" not in data: continue

    # PASSE 1 : Construction du dictionnaire des medicaments
    for entry in data["entry"]:
        r = entry.get("resource", {})
        if r.get("resourceType") == "Medication":
            code = safe_get(r, "code", "coding", 0, "code")
            name = safe_get(r, "code", "coding", 0, "display")
            MEDICATION_DICT[clean_id(r.get("id"))] = {"code": code, "name": name}

    # PASSE 2 : Extraction des ressources cliniques
    for entry in data["entry"]:
        r = entry.get("resource", {})
        rtype = r.get("resourceType")
        rid = clean_id(r.get("id"))
        
        # Recuperation de l'ID patient nettoye
        pat_ref = clean_id(safe_get(r, "subject", "reference") or safe_get(r, "patient", "reference"))

        # --- Table PATIENT ---
        if rtype == "Patient":
            PATIENT_rows.append({
                "PATID": rid, 
                "PATSEX": r.get("gender"),
                "PATBD": r.get("birthDate")
            })

        # --- Table MVT (Rencontres/Sejours) ---
        elif rtype == "Encounter":
            sejum = safe_get(r, "location", 0, "physicalType", "text")
            sejuf = clean_id(safe_get(r, "location", 0, "location", "reference"))
            
            MVT_rows.append({
                "PATID": pat_ref, 
                "EVTID": rid, 
                "ELTID": rid,
                "DATENT": safe_get(r, "period", "start"), 
                "SEJUM": sejum,
                "SEJUF": sejuf
            })
            ENCOUNTER_DICT[rid] = {"SEJUM": sejum, "SEJUF": sejuf}

        # --- Table BIOL (Resultats Labo) ---
        elif rtype == "Observation" and "valueQuantity" in r:
            evt_ref = clean_id(safe_get(r, "encounter", "reference"))
            BIOL_rows.append({
                "PATID": pat_ref, 
                "EVTID": evt_ref, 
                "ELTID": rid,
                "PRLVTDATE": extract_date(r, "effectiveDateTime", "issued"),
                "PNAME": safe_get(r, "code", "text"),
                "RESULT": r["valueQuantity"].get("value"),
                "UNIT": r["valueQuantity"].get("unit")
            })

        # --- Table PHARMA (Prescriptions) ---
        elif rtype == "MedicationRequest":
            med_ref = clean_id(safe_get(r, "medicationReference", "reference"))
            med_info = MEDICATION_DICT.get(med_ref, {})
            name = med_info.get("name") or safe_get(r, "medicationCodeableConcept", "text")
            
            PHARMA_rows.append({
                "PATID": pat_ref, 
                "EVTID": clean_id(safe_get(r, "encounter", "reference")),
                "ELTID": rid, 
                "ALLSPELABEL": name, 
                "DATPRES": r.get("authoredOn"),
                "PRES": safe_get(r, "dosageInstruction", 0, "text")
            })

        # --- Table PMSI (Diagnostics et Actes) ---
        elif rtype in ["Condition", "Procedure"]:
            PMSI_rows.append({
                "PATID": pat_ref, 
                "EVTID": clean_id(safe_get(r, "encounter", "reference")),
                "ELTID": rid, 
                "TYPE": rtype,
                "CODE": safe_get(r, "code", "coding", 0, "code"),
                "LIBELLE": safe_get(r, "code", "text") or safe_get(r, "code", "coding", 0, "display"),
                "DATENT": extract_date(r, "onsetDateTime", "performedDateTime", "recordedDate")
            })

        # --- Table DOCEDS (Documents textuels) ---
        elif rtype in ["DiagnosticReport", "DocumentReference"]:
            evt_ref = clean_id(safe_get(r, "encounter", "reference"))
            txt = "Non extrait"
            
            if rtype == "DiagnosticReport": 
                txt = safe_get(r, "presentedForm", 0, "data")
            elif rtype == "DocumentReference": 
                txt = safe_get(r, "content", 0, "attachment", "data")

            DOCEDS_rows.append({
                "PATID": pat_ref, 
                "EVTID": evt_ref, 
                "ELTID": rid,
                "RECTXT": txt, 
                "RECDATE": extract_date(r, "effectiveDateTime", "date", "created"),
                "SEJUM": ENCOUNTER_DICT.get(evt_ref, {}).get("SEJUM")
            })

# =====================================================
# 4. EXPORT ET SAUVEGARDE
# =====================================================

if not PATIENT_rows: exit("Erreur: Pas de patients.")

df_pat = pl.DataFrame(PATIENT_rows)
df_pat.write_parquet(EDS_PATH + "patient.parquet")

def save(rows, name, date_col):
    """Sauvegarde une table en calculant l'age du patient"""
    if not rows: return
    
    # Jointure avec le patient pour avoir la date de naissance (PATBD)
    df = pl.DataFrame(rows).join(df_pat, on="PATID", how="left")
    
    if date_col:
        # Calcul vectorise de l'age via list comprehension
        ages = [compute_age(b, e) for b, e in zip(df["PATBD"], df[date_col])]
        df = df.with_columns(pl.Series("PATAGE", ages))
    
    df.write_parquet(EDS_PATH + name)
    print(f"Fichier genere: {name} ({len(df)} lignes)")

# Generation des fichiers finaux
save(MVT_rows, "mvt.parquet", "DATENT")
save(BIOL_rows, "biol.parquet", "PRLVTDATE")
save(PHARMA_rows, "pharma.parquet", "DATPRES")
save(PMSI_rows, "pmsi.parquet", "DATENT")
save(DOCEDS_rows, "doceds.parquet", "RECDATE")