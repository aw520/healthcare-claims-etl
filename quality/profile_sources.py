
import csv
from pathlib import Path
from collections import Counter

"""
Source Data Profiling

Perform exploratory quality checks on Synthea healthcare CSV datasets,
including primary key uniqueness, referential integrity, and transaction
type distributions.

This script is used for initial data exploration. Production data quality
checks will be implemented separately in the automated PySpark pipeline.
"""

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "csv"

# File name -> candidate primary key
TABLES = {
    "claims.csv": "Id",
    "claims_transactions.csv": "ID",
    "patients.csv": "Id",
    "providers.csv": "Id",
    "encounters.csv": "Id",
    "payers.csv": "Id",
}


def profile_table(filename, primary_key):
    """
    Profile the candidate primary key of a source CSV.

    Checks:
    - Total number of records
    - Missing or blank primary key values
    - Duplicate primary key occurrences

    Used to assess source data quality before ETL processing.
    """

    path = DATA_DIR / filename

    seen = set()
    total_rows = 0
    missing_ids = 0
    duplicate_ids = 0

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1

            record_id = (row.get(primary_key) or "").strip()

            if not record_id:
                missing_ids += 1
            elif record_id in seen:
                duplicate_ids += 1
            else:
                seen.add(record_id)

    print(f"\nTable: {filename}")
    print(f"Total rows: {total_rows}")
    print(f"Missing IDs: {missing_ids}")
    print(f"Duplicate IDs: {duplicate_ids}")


for filename, primary_key in TABLES.items():
    profile_table(filename, primary_key)

def load_claim_ids():
    """
    Load all valid claim IDs into a set for referential integrity checks.
    """
    with open(DATA_DIR / "claims.csv", newline="", encoding="utf-8-sig") as f:
        return {
            row["Id"].strip()
            for row in csv.DictReader(f)
            if row["Id"].strip()
        }


claim_ids = load_claim_ids()


# Referential Integrity Check
# Verify that every transaction references an existing claim.
# Missing CLAIMIDs and orphan transactions are counted separately.
orphan_count = 0
missing_fk_count = 0

with open(
    DATA_DIR / "claims_transactions.csv",
    newline="",
    encoding="utf-8-sig"
) as f:
    for row in csv.DictReader(f):
        claim_id = (row["CLAIMID"] or "").strip()

        if not claim_id:
            missing_fk_count += 1
        elif claim_id not in claim_ids:
            orphan_count += 1

print("\nForeign Key Validation")
print("Missing CLAIMID:", missing_fk_count)
print("Orphan Transactions:", orphan_count)



# Transaction Type Distribution
# Count occurrences of each transaction type to understand
# the source data and guide downstream financial aggregations.
transaction_types = Counter()

with open(
    DATA_DIR / "claims_transactions.csv",
    newline="",
    encoding="utf-8-sig"
) as f:
    for row in csv.DictReader(f):
        transaction_types[row["TYPE"].strip()] += 1

print("\nTransaction Type Distribution")

for transaction_type, count in transaction_types.most_common():
    print(f"{transaction_type}: {count}")


