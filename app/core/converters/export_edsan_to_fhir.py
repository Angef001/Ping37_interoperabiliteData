from edsan_to_fhir import export_eds_to_fhir

export_eds_to_fhir(
    eds_dir="C:/Users/DELL/OneDrive/Documents/projet_ping/Ping37_interoperabiliteData/eds",
    output_dir="C:/Users/DELL/OneDrive/Documents/projet_ping/Ping37_interoperabiliteData/test_output_fhir",
    bundle_strategy="patient"  # or "encounter"
)
