#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

from app.utils.filter_dataset import filter_folder, _split_csv
from app.core.converters.edsan_to_fhir import export_eds_to_fhir


def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline CLI: filter_dataset (temp) -> export_eds_to_fhir")
    ap.add_argument("--input-dir", required=True, help="EDS source (dossier avec *.parquet)")
    ap.add_argument(
        "--output-dir",
        default=None,
        help="(Optionnel) Dossier de vérif: copie des parquets filtrés",
    )
    ap.add_argument("--fhir-output-dir", required=True, help="Dossier sortie FHIR (JSON)")
    ap.add_argument("--bundle-strategy", default="patient", choices=["patient", "encounter"])

    ap.add_argument("--only", default="", help="Tables à inclure (csv)")
    ap.add_argument("--exclude", default="", help="Tables à exclure (csv)")
    ap.add_argument("--where", action="append", default=[], help="Clause WHERE (répétable)")
    ap.add_argument("--propagate", action="append", default=[], help="Propagation key:source (répétable)")
    ap.add_argument("--no-propagate-nulls", action="store_true")

    args = ap.parse_args()
    only = _split_csv(args.only) if args.only else None
    exclude = _split_csv(args.exclude) if args.exclude else None

    # Toujours un dossier temporaire pour l'EDS filtré (celui utilisé pour l'export)
    with tempfile.TemporaryDirectory(prefix="eds_filtered_") as tmp:
        tmp_dir = Path(tmp)

        # 1) Filtrage -> TEMP
        filter_folder(
            input_dir=args.input_dir,
            output_dir=str(tmp_dir),
            only=only,
            exclude=exclude,
            where=args.where,
            propagate=args.propagate,
            propagate_drop_nulls=not args.no_propagate_nulls,
        )

        # 2) Copie optionnelle -> output-dir (vérif)
        if args.output_dir:
            vdir = Path(args.output_dir)
            vdir.mkdir(parents=True, exist_ok=True)
            for p in tmp_dir.glob("*.parquet"):
                shutil.copy2(p, vdir / p.name)

        # 3) Export FHIR depuis TEMP
        summary = export_eds_to_fhir(
            eds_dir=str(tmp_dir),
            output_dir=args.fhir_output_dir,
            bundle_strategy=args.bundle_strategy,
        )

        print("✅ OK")
        print(summary)


if __name__ == "__main__":
    main()
