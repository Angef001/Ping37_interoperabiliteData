# Ping37_interoperabiliteData

Ce projet met en œuvre un pipeline bidirectionnel FHIR ↔ EDS, permettant :

l’ingestion de données FHIR hétérogènes (ex. Synthea),

leur structuration en EDS analytique (Parquet),

puis la reconstruction d’un FHIR strict (R5) destiné à l’interopérabilité.

L’architecture est modulaire, configurable et orientée qualité des données.
