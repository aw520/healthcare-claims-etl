#!/usr/bin/env python3
"""Prepare two controlled healthcare claims batches for pipeline integration tests.

This utility reads the original local Synthea CSV directory without modifying it.
It writes only the changed claims.csv and ready.json manifest for each new
batch; the remaining unchanged CSV files should be copied between S3 prefixes.

Batch 002 (expected pipeline PASS):
    Update two valid claims rows with valid decimal amounts and an ISO timestamp.
    Keep all Claim IDs unchanged, avoiding accidental orphan references.

Batch 003 (expected Glue Data Quality FAIL):
    Make the second claim use the first claim's Id, producing a duplicate key.
    It should pass file-manifest validation but fail Spark's IsUnique rule.

No AWS credentials or third-party Python dependencies are required.
"""

import argparse
import csv
import json
from pathlib import Path

EXPECTED_CSV_COUNT = 18
REQUIRED_COLUMNS = {"Id", "SERVICEDATE", "OUTSTANDING1"}


def get_source_files(source_dir: Path) -> list[str]:
    """Return the filenames needed by the ingestion Lambda's ready.json.

    Args:
        source_dir: Directory containing all 18 original Synthea CSV files.

    Returns:
        Sorted list of expected CSV filenames (names only, no directory paths).

    Raises:
        FileNotFoundError: The source directory or claims.csv is missing.
        ValueError: The source file count does not match this 18-file sample.
    """
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Source CSV directory does not exist: {source_dir}")

    names = sorted(path.name for path in source_dir.glob("*.csv") if path.is_file())
    if "claims.csv" not in names:
        raise FileNotFoundError(f"Missing claims.csv in: {source_dir}")
    if len(names) != EXPECTED_CSV_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_CSV_COUNT} source CSVs, found {len(names)}: {names}"
        )
    return names


def make_claims_csv(source_file: Path, output_file: Path, batch_id: str) -> dict:
    """Stream-copy claims.csv and inject one controlled test scenario.

    Args:
        source_file: Original claims.csv; never modified.
        output_file: New claims.csv inside the destination batch folder.
        batch_id: Either batch_002 (valid changes) or batch_003 (duplicate Id).

    Returns:
        A small summary describing which rows and values were changed.

    Raises:
        ValueError: Missing columns, too few records, or unexpected IDs.

    Notes:
        Only the output file changes; the source and batch_001 remain intact.
        The file is rewritten with Python's csv module, preserving column names
        and values except for the explicit test edits.
    """
    if batch_id not in {"batch_002", "batch_003"}:
        raise ValueError(f"Unsupported test scenario: {batch_id}")

    row_count = 0
    first_id = None
    changes = []
    with source_file.open("r", encoding="utf-8-sig", newline="") as src, \
            output_file.open("x", encoding="utf-8", newline="") as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames or not REQUIRED_COLUMNS.issubset(reader.fieldnames):
            raise ValueError(
                f"Expected CSV columns {sorted(REQUIRED_COLUMNS)}, got {reader.fieldnames}"
            )
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            row_count += 1
            if row_count == 1:
                first_id = (row["Id"] or "").strip()
                if not first_id:
                    raise ValueError("First source claim Id is empty")

                if batch_id == "batch_002":
                    before = {"SERVICEDATE": row["SERVICEDATE"],
                              "OUTSTANDING1": row["OUTSTANDING1"]}
                    row["SERVICEDATE"] = "2020-01-15T05:07:38Z"
                    row["OUTSTANDING1"] = "42.50"
                    changes.append({"row": 1, "claim_id": first_id,
                                    "before": before,
                                    "after": {"SERVICEDATE": row["SERVICEDATE"],
                                              "OUTSTANDING1": row["OUTSTANDING1"]}})

            elif row_count == 2:
                original_id = (row["Id"] or "").strip()
                if not original_id or original_id == first_id:
                    raise ValueError("Expected two distinct source Claim IDs")

                if batch_id == "batch_002":
                    before = row["OUTSTANDING1"]
                    row["OUTSTANDING1"] = "84.25"
                    changes.append({"row": 2, "claim_id": original_id,
                                    "before": {"OUTSTANDING1": before},
                                    "after": {"OUTSTANDING1": row["OUTSTANDING1"]}})
                else:
                    row["Id"] = first_id
                    changes.append({"row": 2, "before": {"Id": original_id},
                                    "after": {"Id": first_id},
                                    "purpose": "Create a duplicate Claim ID"})

            writer.writerow(row)

    if row_count < 2:
        raise ValueError("Expected claims.csv to contain at least 2 records")
    return {"batch_id": batch_id, "rows": row_count, "changes": changes}


def prepare_batch(source_dir: Path, output_dir: Path, batch_id: str,
                  filenames: list[str]) -> dict:
    """Write one modified claims file and one ready.json for an S3 test batch.

    Args:
        source_dir: Folder with the 18 original source CSVs.
        output_dir: Staging root outside the Git repository, preferably.
        batch_id: Name of this batch's test scenario.
        filenames: All 18 original CSV filenames for manifest validation.

    Returns:
        The summary of the modified claims rows.

    Note:
        Existing destination files are never overwritten. This prevents
        inadvertently changing a batch that has already been uploaded.
    """
    batch_dir = output_dir / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    if (batch_dir / "claims.csv").exists() or (batch_dir / "ready.json").exists():
        raise FileExistsError(
            f"Existing output in {batch_dir}; refusing to overwrite test files"
        )

    report = make_claims_csv(source_dir / "claims.csv",
                             batch_dir / "claims.csv", batch_id)
    manifest = {"batch_id": batch_id, "files": filenames}
    with (batch_dir / "ready.json").open("x", encoding="utf-8") as out:
        json.dump(manifest, out, indent=2)
        out.write("\n")
    return report


def main() -> None:
    """Parse local directories and prepare both integration-test batches."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("../csv"),
                        help="Directory with 18 original CSV files (default: ../csv)")
    parser.add_argument("--output-dir", type=Path, default=Path("../test_batches"),
                        help="Directory for generated test files (default: ../test_batches)")
    args = parser.parse_args()

    source_dir = args.source_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if source_dir == output_dir or source_dir in output_dir.parents:
        raise ValueError("Output directory must not be the source CSV directory or inside it")

    filenames = get_source_files(source_dir)
    for batch_id in ("batch_002", "batch_003"):
        report = prepare_batch(source_dir, output_dir, batch_id, filenames)
        print(json.dumps(report, indent=2))
    print("\nGenerated only claims.csv and ready.json per batch.")
    print("Copy the OTHER 17 original CSVs in S3, then upload the modified claims.csv.")
    print("Upload ready.json LAST for each batch, one batch at a time.")
    print("Original source data was not changed.")


if __name__ == "__main__":
    main()
