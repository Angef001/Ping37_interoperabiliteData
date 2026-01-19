# Ping37_interoperabiliteData

Ce projet met en œuvre un pipeline bidirectionnel FHIR ↔ EDS, permettant :

- l’ingestion de données FHIR hétérogènes (ex. Synthea),

- leur structuration en EDS analytique (Parquet),

- puis la reconstruction d’un FHIR strict (R5) destiné à l’interopérabilité.

L’architecture est modulaire, configurable et orientée qualité des données.


Ce projet met en œuvre un pipeline bidirectionnel FHIR ↔ EDS, dont l’objectif est de faciliter l’exploitation et l’interopérabilité des données de santé.

Dans un premier temps, le pipeline permet l’ingestion de données FHIR , provenant de Synthea (générateur de données). Elles sont ensuite transformées et structurées sous format Parquet dans un EDS fictif (Entrepôt de Données de Santé) généré à partir du schémas des métadonées fournies par le CHU. 

Dans un second temps, le pipeline permet la reconstruction d’un FHIR strict (HL7 FHIR R5) à partir des données EDS. Cette phase vise explicitement l’interopérabilité, et met en œuvre le respect strict des contraintes FHIR (types, formats, encodage des documents).

Le FHIR généré en sortie est ainsi prêt à être échangé, validé ou consommé par des systèmes tiers (API FHIR, applications cliniques, plateformes d’échange), contrairement au FHIR d’entrée qui peut être plus permissif.
