import json
import os
import tempfile
from app.core.converters.eds_merge import merge_run_into_eds
from app.core.config.merge_keys import MERGE_KEYS
import tempfile
from pathlib import Path
from app.core.converters.build_eds_with_fhir import EDS_DIR as DEFAULT_EDS_DIR
from pathlib import Path



from app.core.converters.build_eds_with_fhir import build_eds, PROJECT_ROOT


def process_dir(
    fhir_dir: str | None = None,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
) -> dict:
    """
    Phase 3 (FHIR -> EDS) : traite un dossier de bundles FHIR,
    génère dans un run_dir temporaire puis MERGE dans eds/ (sans écraser).
    """
    target_eds_dir = eds_dir or DEFAULT_EDS_DIR

    # Si fhir_dir est None, build_eds utilisera son défaut (souvent synthea/output/fhir)
    source_fhir_dir = fhir_dir

    with tempfile.TemporaryDirectory() as tmp_run:
        run_dir = str(Path(tmp_run))

        # 1) Build dans run_dir (PAS dans eds/)
        result = build_eds(
            fhir_dir=source_fhir_dir,
            eds_dir=run_dir,
            mapping_file=mapping_file,
            verbose=True
        )

        # 2) Merge run_dir -> target_eds_dir
        merge_reports = merge_run_into_eds(
            eds_dir=target_eds_dir,
            run_dir=run_dir,
            table_names=list(result["tables"].keys()),
            keys_by_table=MERGE_KEYS,
        )

        result["merge"] = [r.__dict__ for r in merge_reports]
        result["merged_into"] = target_eds_dir

        # 3) sauvegarde report
        _write_last_run(result, target_eds_dir)

        return result





def process_bundle(
    bundle: dict,
    eds_dir: str | None = None,
    mapping_file: str | None = None,
) -> dict:
    """
    Phase 3 (FHIR -> EDS) : traite un bundle FHIR (dict),
    génère les parquets dans un run_dir temporaire,
    puis MERGE dans le dossier EDS cible (sans écraser).
    """
    target_eds_dir = eds_dir or DEFAULT_EDS_DIR

    with tempfile.TemporaryDirectory() as tmp_fhir:
        # 1) Sauvegarde du bundle temporaire
        bundle_path = os.path.join(tmp_fhir, "bundle.json")
        with open(bundle_path, "w", encoding="utf-8") as f:
            json.dump(bundle, f)

        # 2) Run dir parquet temporaire (évite d’écraser eds/)
        with tempfile.TemporaryDirectory() as tmp_run:
            run_dir = str(Path(tmp_run))

            # Génération parquet dans run_dir (PAS dans eds/)
            result = build_eds(
                fhir_dir=tmp_fhir,
                eds_dir=run_dir,
                mapping_file=mapping_file,
                verbose=True
            )

            # 3) Merge run_dir -> target_eds_dir
            merge_reports = merge_run_into_eds(
                eds_dir=target_eds_dir,
                run_dir=run_dir,
                table_names=list(result["tables"].keys()),
                keys_by_table=MERGE_KEYS,
            )

            result["merge"] = [r.__dict__ for r in merge_reports]
            result["merged_into"] = target_eds_dir

            # 4) sauvegarde report (comme process_dir)
            _write_last_run(result, target_eds_dir)

            return result
        
def _write_last_run(result: dict, target_eds_dir: str) -> None:
    """
    Sauvegarde un rapport JSON du dernier run dans eds/last_run.json
    (utile pour la restitution et debug).
    """
    try:
        import json
        from pathlib import Path

        p = Path(target_eds_dir) / "last_run.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    except Exception:
        # pas bloquant
        pass




if __name__ == "__main__":
    # Lancement manuel : par défaut synthea/output/fhir
    print("Lancement manuel fhir_to_edsan.process_dir()")
    summary = process_dir()
    print(summary)


