from pydantic import BaseModel, Field
from datetime import date, datetime
from typing import Optional

"""
DOCUMENTATION DES MODÈLES DE DONNÉES (EDS-AN)
---------------------------------------------
Ce fichier définit le "Contrat de Données" (Schema) pour l'Entrepôt de Données de Santé.
Il utilise la librairie Pydantic pour garantir que les données reçues ou lues 
respectent strictement les types attendus avant d'être traitées.

Structure :
1. PatientModel : Socle commun (Clés de jointure).
2. Modules Spécifiques : Héritent de PatientModel et ajoutent les données métiers.
"""

# =============================================================================
# 1. SOCLE COMMUN (Héritage)
# =============================================================================
class PatientModel(BaseModel):
    """
    Classe parente représentant les identifiants obligatoires pour toute donnée clinique.
    Toutes les autres classes (Bio, Pharma, etc.) vont hériter de celle-ci.
    Cela évite de réécrire PATID et EVTID 5 fois .
    """
    # Le '...' dans Field(...) signifie que ce champ est REQUIS (Obligatoire).
    PATID: str = Field(..., description="Identifiant unique du patient (Anonymisé ou IPP)")
    
    # EVTID permet de lier la donnée à un séjour précis (Encounter).
    EVTID: str = Field(..., description="Identifiant unique de la venue/séjour")
    
    # ELTID est l'ID technique de la ligne (ex: ID unique de la prise de sang).
    ELTID: str = Field(..., description="Clé primaire de la ligne de donnée")
    
    # Format attendu : "M" ou "F". Pydantic validera que c'est bien une string.
    PATSEX: str = Field(..., description="Sexe du patient (M/F)")
    
    # Optional[int] = None signifie que si la donnée manque, elle vaudra 'None' (null) 
    # au lieu de faire planter le programme.
    PATAGE: Optional[int] = Field(None, description="Âge calculé au moment de l'événement")


# =============================================================================
# 2. MODULE DOCUMENTS (Texte non structuré)
# =============================================================================
class DocEdsModel(PatientModel):
    """
    Modèle pour les comptes-rendus (CR) et documents textuels.
    Table cible : doceds.parquet
    """
    # Contient le texte brut ou encodé en Base64. C'est le cœur de cette table.
    RECTXT: str = Field(..., description="Contenu textuel intégral du document")
    
    # Code de la famille de document (ex: CR-OPERATOIRE, PRESCRIPTION-SORTIE).
    RECFAMTXT: Optional[str] = None
    
    # DateTime assure qu'on a bien une date ET une heure valides.
    RECDATE: datetime = Field(..., description="Date et heure de création du document")
    
    RECTYPE: str = Field(..., description="Type technique du document")
    
    # Unité médicale (Service) : Optionnel car un document peut être administratif.
    SEJUM: Optional[str] = None
    SEJUF: Optional[str] = None


# =============================================================================
# 3. MODULE PMSI (Administratif & Codage)
# =============================================================================
class PmsiModel(PatientModel):
    """
    Modèle pour les données médico-administratives (T2A).
    Contient les diagnostics (CIM-10) et les actes (CCAM).
    Table cible : pmsi.parquet
    """
    # Diagnostic Principal (DALL). Optionnel car la ligne peut ne contenir qu'un acte.
    DALL: Optional[str] = Field(None, description="Code Diagnostic CIM-10 (ex: I50.1)")
    
    DATENT: datetime = Field(..., description="Date d'entrée administrative")
    DATSORT: Optional[datetime] = None
    
    # Durée de séjour calculée (en jours).
    SEJDUR: Optional[int] = None
    
    # Ici, les Unités Médicales sont OBLIGATOIRES (pas de Optional) 
    # car le PMSI est toujours rattaché à un service.
    SEJUM: str
    SEJUF: str
    
    # Classification Commune des Actes Médicaux.
    CODEACTES: Optional[str] = None 
    
    # Mode d'entrée (ex: '8' pour Urgences) et de sortie.
    MODEENT: Optional[str] = None
    MODESORT: Optional[str] = None
    
    # Groupe Homogène de Malades (pour la tarification).
    GHM: Optional[str] = None
    SEVERITE: Optional[str] = None


# =============================================================================
# 4. MODULE PHARMA (Prescriptions Médicamenteuses)
# =============================================================================
class PharmaModel(PatientModel):
    """
    Modèle pour les lignes de prescription de médicaments.
    Table cible : pharma.parquet
    """
    DATPRES: datetime = Field(..., description="Date et heure de la prescription")
    
    # Nom du médicament en clair (Doliprane 1g).
    ALLSPELABEL: str = Field(..., description="Libellé du médicament (DC ou spécialité)")
    
    # Codes normalisés (ATC, UCD ou CIS) pour l'interopérabilité.
    ALLSPECODE: Optional[str] = None 
    
    # Texte libre pour la posologie (ex: "1 matin et soir pendant 5 jours").
    PRES: Optional[str] = None 
    
    SEJUM: Optional[str] = None


# =============================================================================
# 5. MODULE BIOLOGIE (Résultats de Labo)
# =============================================================================
class BiolModel(PatientModel):
    """
    Modèle pour les résultats d'examens biologiques.
    Attention aux types numériques !
    Table cible : biol.parquet
    """
    PRLVTDATE: datetime = Field(..., description="Date et heure exacte du prélèvement")
    
    # Nom de l'examen (ex: Hémoglobine, Sodium).
    PNAME: str = Field(..., description="Libellé de l'examen")
    
    # Code LOINC (Standard international) si disponible.
    LOINC: Optional[str] = None
    
    # CRITIQUE : Le résultat DOIT être un nombre (float) pour permettre 
    # des calculs (moyennes, courbes) et des graphiques.
    # Si la source envoie "Positif", la validation échouera (c'est voulu).
    RESULT: float = Field(..., description="Valeur numérique brute")
    
    # L'unité est inséparable du résultat (mg/L vs g/L).
    UNIT: str = Field(..., description="Unité de mesure")
    
    # Bornes de référence (Valeurs normales min/max).
    MINREF: Optional[float] = None
    MAXREF: Optional[float] = None
    
    SEJUM: str
    SEJUF: str


# =============================================================================
# 6. MODULE MOUVEMENTS (Structure du séjour)
# =============================================================================
class MvtModel(PatientModel):
    """
    Modèle simplifié pour suivre les mouvements du patient (Hospitalisation).
    Table cible : mvt.parquet
    """
    DATENT: datetime
    DATSORT: Optional[datetime] = None
    
    # Localisation géographique ou fonctionnelle du patient.
    SEJUM: str
    SEJUF: str