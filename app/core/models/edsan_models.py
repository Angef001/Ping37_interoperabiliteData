from pydantic import BaseModel, Field
from datetime import date, datetime
from typing import Optional

# --- BASE COMMUNE (Champs présents dans presque tous les modules) ---
class PatientModel(BaseModel):
    PATID: str = Field(..., description="Identifiant unique du patient")
    EVTID: str = Field(..., description="Identifiant unique du séjour (Encounter)")
    ELTID: str = Field(..., description="Identifiant unique de l'élément (ligne)")
    PATSEX: str = Field(..., description="Sexe du patient (M/F)")
    PATAGE: Optional[int] = Field(None, description="Âge au moment de l'événement")

# --- MODULE DOCEDS (Documents) ---
class DocEdsModel(PatientModel):
    RECTXT: str = Field(..., description="Texte intégral du document")
    RECFAMTXT: Optional[str] = None
    RECDATE: datetime = Field(..., description="Date du document")
    RECTYPE: str = Field(..., description="Type de document (ex: CR-HOSP)")
    SEJUM: Optional[str] = None
    SEJUF: Optional[str] = None

# --- MODULE PMSI (Diagnostics et Actes) ---
class PmsiModel(PatientModel):
    DALL: Optional[str] = Field(None, description="Diagnostics (CIM-10)")
    DATENT: datetime = Field(..., description="Date d'entrée / début")
    DATSORT: Optional[datetime] = None
    SEJDUR: Optional[int] = None # Durée du séjour
    SEJUM: str
    SEJUF: str
    CODEACTES: Optional[str] = None # Codes CCAM
    MODEENT: Optional[str] = None
    MODESORT: Optional[str] = None
    GHM: Optional[str] = None
    SEVERITE: Optional[str] = None

# --- MODULE PHARMA (Médicaments) ---
class PharmaModel(PatientModel):
    DATPRES: datetime = Field(..., description="Date de prescription")
    ALLSPELABEL: str = Field(..., description="Libellé du médicament (DC ou spécialité)")
    ALLSPECODE: Optional[str] = None # Code ATC ou CIS
    PRES: Optional[str] = None # Posologie / Instructions
    SEJUM: Optional[str] = None

# --- MODULE BIOL (Biologie) ---
class BiolModel(PatientModel):
    PRLVTDATE: datetime = Field(..., description="Date et heure du prélèvement")
    PNAME: str = Field(..., description="Nom de l'examen (ex: Glycémie)")
    ANAME: Optional[str] = None # Analyse détaillée
    LOINC: Optional[str] = None # Code LOINC
    RESULT: float = Field(..., description="Valeur numérique du résultat")
    UNIT: str = Field(..., description="Unité (ex: mmol/L)")
    MINREF: Optional[float] = None
    MAXREF: Optional[float] = None
    SEJUM: str
    SEJUF: str

# --- MODULE MVT (Mouvements / Séjours) ---
# Souvent utilisé pour centraliser les périodes d'hospitalisation
class MvtModel(PatientModel):
    DATENT: datetime
    DATSORT: Optional[datetime] = None
    SEJUM: str
    SEJUF: str