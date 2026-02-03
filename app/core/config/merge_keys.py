# app/core/config/merge_keys.py

# # Clés minimales (à ajuster si besoin)
# MERGE_KEYS = {
#     # 1 séjour / encounter
#     "mvt.parquet": ["EVTID"],

#     # 1 mesure bio = evt + analyte + date prélèvement (UNIT optionnelle, mais pas obligatoire)
#     "biol.parquet": ["EVTID", "PNAME", "PRLVTDATE"],

#     # 1 prescription = evt + médicament + date prescription
#     "pharma.parquet": ["EVTID", "PRES", "DATPRES"],

#     # 1 doc = evt + type doc + date doc (on ajoute famille pour éviter collisions)
#     "doceds.parquet": ["EVTID", "RECTYPE", "RECDATE", "RECFAMTXT"],

#     # PMSI = evt + diag (DALL) + actes + date entrée (sinon collisions)
#     "pmsi.parquet": ["EVTID", "DALL", "CODEACTES", "DATENT"],
# }

MERGE_KEYS = {
    "mvt.parquet": ["EVTID"],
    "biol.parquet": ["EVTID", "PNAME", "PRLVTDATE"],
    "pharma.parquet": ["EVTID", "ELTID"],
    "doceds.parquet": ["EVTID", "RECTYPE", "RECDATE", "RECTXT"],
    "pmsi.parquet": ["EVTID", "ELTID"],
}



