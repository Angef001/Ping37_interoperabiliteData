import re
from datetime import datetime, date
from typing import Optional, Union

def clean_id(raw_id: Optional[str]) -> str:
    """
    Nettoie les identifiants FHIR pour ne garder que la partie unique.
    Ex: 'Patient/123' -> '123' ou 'urn:uuid:abc-def' -> 'abc-def'
    """
    if not raw_id:
        return ""
    
    # Supprime les préfixes courants via une expression régulière
    # On enlève "urn:uuid:", "Patient/", "Encounter/", etc.
    clean = re.sub(r"^(urn:uuid:|Patient/|Encounter/|Observation/|Procedure/|Condition/|MedicationRequest/|Location/)", "", raw_id)
    return clean

def compute_age(birth_date: Union[date, str, datetime], reference_date: Union[date, str, datetime]) -> Optional[int]:
    """
    Calcule l'âge d'un patient à un instant T (moment de l'examen ou du séjour).
    Indispensable pour le champ PATAGE demandé par le CHU.
    """
    if not birth_date or not reference_date:
        return None

    try:
        # Conversion en objet date si on reçoit des chaînes de caractères
        if isinstance(birth_date, str):
            birth_date = datetime.fromisoformat(birth_date.split('T')[0]).date()
        if isinstance(reference_date, str):
            reference_date = datetime.fromisoformat(reference_date.split('T')[0]).date()
        
        # Si on a des datetime, on convertit en date
        if isinstance(birth_date, datetime): birth_date = birth_date.date()
        if isinstance(reference_date, datetime): reference_date = reference_date.date()

        return reference_date.year - birth_date.year - ((reference_date.month, reference_date.day) < (birth_date.month, birth_date.day))
    except Exception:
        return None

def format_fhir_date(date_val: Optional[Union[str, datetime]]) -> Optional[str]:
    """
    Normalise les dates pour l'affichage ou le stockage.
    """
    if not date_val:
        return None
    if isinstance(date_val, datetime):
        return date_val.isoformat()
    return date_val

def get_coding_value(codeable_concept: Optional[dict], system_url: str) -> Optional[str]:
    """
    Extrait un code spécifique d'un CodeableConcept FHIR selon le système (ex: CIM-10).
    Utile pour le Binôme 1.
    """
    if not codeable_concept or 'coding' not in codeable_concept:
        return None
    
    for coding in codeable_concept['coding']:
        if coding.get('system') == system_url:
            return coding.get('code')
    return None