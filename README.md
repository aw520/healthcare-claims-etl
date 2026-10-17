# Healthcare Claims ETL

An AWS-based batch ingestion project for synthetic healthcare claims data, with planned PySpark transformations and SQL analytics. The currently implemented portion copies validated source batches from an upstream S3 bucket into the Bronze layer of a data lake. Later stages of the ETL architecture are not implemented yet.

## Project Status

| Component | Status |
| --- | --- |
| Synthea source exploration | **Completed locally**: six CSVs profiled for candidate-key completeness and uniqueness, plus transaction references and types |
| AWS S3 upstream and Bronze buckets | **Configured**: 18 original CSVs delivered under `incoming/batch_001/` |
| AWS Lambda batch ingestion | **Implemented and tested**: manifest validation, copying to Bronze, completion marker, sequential duplicate skip |
| AWS Glue / PySpark data quality and transformations | **Planned** |
| Silver tables (Parquet / Apache Iceberg), Gold aggregates, Athena queries | **Planned** |
| EventBridge trigger and Step Functions orchestration / retries | **Planned** |

## Current Architecture

```text
Manual batch delivery to upstream Amazon S3
  incoming/batch_001/
    18 CSV files
    ready.json                <- uploaded last; declares expected files
           |
           | Manual AWS Lambda Test invocation (currently)
           v
  AWS Lambda: healthcare-batch-ingestion
    1. Read and parse ready.json
    2. List actual CSV filenames
    3. Fail on missing, extra, or duplicate-declared filenames
    4. Check Bronze for an _INGESTED.json completion marker
    5. If not already complete, copy 18 CSVs and ready.json
    6. Write _INGESTED.json only after copies finish
           |
           v
Data lake Amazon S3
  bronze/batch_001/
    18 CSV files
    ready.json
    _INGESTED.json             <- written by Lambda after success

Planned: EventBridge -> Step Functions -> ingestion Lambda -> AWS Glue -> Silver / Gold -> Athena
```

`ready.json` is an application-defined delivery manifest, not an AWS-generated file. It signals that the upstream batch is ready and lists the expected CSVs. `_INGESTED.json` is a separate completion marker written by the Lambda after Bronze copying succeeds.

### Implemented Ingestion Behavior

The code lives in [`ingestion/lambda_function.py`](ingestion/lambda_function.py). The Lambda handler reads the manifest via `s3.get_object`, lists CSV objects via paginated `list_objects_v2`, and checks that the batch ID and exact filename sets match. Missing files, unexpected CSVs, invalid manifest filename lists, and duplicate entries cause validation errors before copying begins.

Validated batches are copied using S3 server-side `copy_object` calls. The Lambda copies the manifest last and then creates `_INGESTED.json` with `put_object`. On a later **sequential** invocation with the same valid batch, the Lambda sees the completion marker and returns `already_ingested` without copying the files again.

**Verified on AWS:** manual invocation for `batch_001` returned `status: ingested` with `file_count: 18`; the destination Bronze prefix was checked in S3. A subsequent invocation returned `status: already_ingested` with no file-copy log entries.

**Current limits:** The Lambda validates manifest filenames, not CSV contents, checksums, schema types, or row-level quality. Batch copies are not transactional; a failed run can leave a partially copied prefix and would need another invocation to retry. Sequential duplicate skipping has been tested, but concurrent invocations, source immutability, and failure recovery have not been tested or guarded with a distributed lock. Event-triggered automation and workflow retries are not in place yet.

## Run the Implemented Ingestion

The function is deployed manually to AWS Lambda with the **Python 3.12** runtime and handler `lambda_function.lambda_handler` (the file is placed at the Lambda deployment root when using the inline editor). This project currently has no infrastructure-as-code or automated deployment pipeline.

Configure these Lambda environment variables:

| Variable | Purpose |
| --- | --- |
| `UPSTREAM_BUCKET` | S3 bucket containing `incoming/<batch_id>/` |
| `LAKE_BUCKET` | Data lake bucket receiving `bronze/<batch_id>/` |

The Lambda execution role needs permission to list and read the upstream batch, list the Bronze prefix for completion-marker checks, and write objects to Bronze. Bucket names and account-specific settings are not hard-coded in the Python source.

To invoke manually in the Lambda console, use a test event like:

```json
{
  "batch_id": "batch_001"
}
```

On first successful ingestion, the response contains `status: ingested`, `file_count: 18`, and `destination: bronze/batch_001/`. On a later invocation, `status: already_ingested` indicates the completion marker was found.

## Source Dataset

The data comes from **Synthea's 1K Sample Synthetic Patient Records (CSV)** ([Synthea project](https://synthetichealth.github.io/synthea/)). All **18 source CSVs** for the sample batch have been uploaded to the upstream S3 bucket and copied into Bronze. The raw CSV files are deliberately kept outside this Git repository.

Initial deeper exploration focuses on six related files:

| File | Observed rows | Candidate record grain / role |
| --- | ---: | --- |
| `claims.csv` | 117,889 | Claim identifiers, service details, statuses, patient/provider references |
| `claims_transactions.csv` | 711,238 | Individual transactions linked to claims; charges, payments, transfers |
| `patients.csv` | 1,163 | Patient-level demographics |
| `providers.csv` | 5,056 | Provider-level attributes and organizations |
| `encounters.csv` | 61,459 | Encounter-level visits and associated costs |
| `payers.csv` | 10 | Insurance payer records |

## Exploratory Source Profiling

The local script [`quality/profile_sources.py`](quality/profile_sources.py) uses Python's standard `csv` module to inspect six source files. It checks candidate identifier blanks/duplicates, validates claim references in transactions, and counts transaction types. These are **exploratory, local checks**, not automated production data quality gates in the Lambda or Spark.

Observed results on the downloaded Synthea sample:

- All six selected files had **0 missing/blank candidate IDs** and **0 duplicate candidate IDs**.
- `claims_transactions.CLAIMID` had **0 missing values** and **0 orphan references** against `claims.Id`.
- Transaction `TYPE` counts: `PAYMENT` 252,335; `CHARGE` 218,721; `TRANSFEROUT` 120,091; `TRANSFERIN` 120,091.

The presence of a transaction `AMOUNT` does **not** imply that value represents a payment; future financial metrics must respect transaction types and the meanings of the source amount columns.

### Running Local Profiling

Place the downloaded CSVs in a sibling `csv/` directory next to the repository (not inside it):

```text
HealthcareClaimsETL/
  csv/
    claims.csv
    claims_transactions.csv
    ...
  healthcare-claims-etl/
    quality/profile_sources.py
```

Run from the repository root:

```bash
python3 quality/profile_sources.py
```

## Source Schema Reference

The columns below come from the **actual CSV headers** of the selected Synthea sample. This is a focused reference, not a complete list of every source column. Exploratory profiling has checked candidate-key uniqueness in six files and the `claims_transactions.CLAIMID` relationship. Full schema typing, nullability validation, and remaining foreign-key checks are still planned.

### 1. `claims.csv`

| Column | Description |
| --- | --- |
| `Id` | Claim identifier; candidate primary key |
| `PATIENTID` | Patient reference |
| `PROVIDERID` | Provider reference |
| `PRIMARYPATIENTINSURANCEID` | Primary patient insurance reference |
| `SECONDARYPATIENTINSURANCEID` | Secondary patient insurance reference |
| `APPOINTMENTID` | Appointment/encounter-related reference; relationship to be verified |
| `SERVICEDATE` | Date of service |
| `DIAGNOSIS1` – `DIAGNOSIS8` | Diagnosis fields |
| `STATUS1`, `STATUS2`, `STATUSP` | Claim status fields |
| `OUTSTANDING1`, `OUTSTANDING2`, `OUTSTANDINGP` | Outstanding balance fields |

### 2. `claims_transactions.csv`

| Column | Description |
| --- | --- |
| `ID` | Transaction identifier; candidate primary key |
| `CLAIMID` | Reference to a claim |
| `PATIENTID` | Patient reference |
| `TYPE` | Transaction type |
| `AMOUNT` | Transaction amount field; meaning depends on transaction type |
| `PAYMENTS` | Payment-related amount |
| `ADJUSTMENTS` | Adjustment-related amount |
| `TRANSFERS` | Transfer-related amount |
| `OUTSTANDING` | Outstanding balance field |
| `FROMDATE`, `TODATE` | Transaction-related date range |
| `PROCEDURECODE`, `UNITS`, `UNITAMOUNT` | Procedure and unit-level details |
| `PROVIDERID`, `PATIENTINSURANCEID` | Provider and insurance references |

**Important:** Do not assume that `AMOUNT` means "paid amount." Transaction types have been counted, but amount-field semantics must be established before defining financial metrics.

### 3. `patients.csv`

| Column | Description |
| --- | --- |
| `Id` | Patient identifier; candidate primary key |
| `BIRTHDATE`, `DEATHDATE` | Patient dates |
| `GENDER`, `RACE`, `ETHNICITY` | Demographic attributes |
| `CITY`, `STATE`, `ZIP` | Location attributes |
| `HEALTHCARE_EXPENSES` | Source-reported healthcare expenses |
| `HEALTHCARE_COVERAGE` | Source-reported healthcare coverage |

Other columns include names, addresses, and synthetic identity fields (for example, `SSN`). These are not needed for the initial analytical output.

### 4. `providers.csv`

| Column | Description |
| --- | --- |
| `Id` | Provider identifier; candidate primary key |
| `ORGANIZATION` | Associated organization |
| `NAME` | Provider name |
| `SPECIALITY` | Provider specialty (spelling as provided in source) |
| `CITY`, `STATE` | Provider location |
| `UTILIZATION` | Source-reported utilization |

### 5. `encounters.csv`

| Column | Description |
| --- | --- |
| `Id` | Encounter identifier; candidate primary key |
| `START`, `STOP` | Encounter timestamps |
| `PATIENT` | Patient reference |
| `PROVIDER` | Provider reference |
| `PAYER` | Payer reference |
| `ENCOUNTERCLASS`, `CODE`, `DESCRIPTION` | Encounter classification |
| `BASE_ENCOUNTER_COST` | Source-reported base encounter cost |
| `TOTAL_CLAIM_COST` | Source-reported total claim cost |
| `PAYER_COVERAGE` | Source-reported payer coverage |

### 6. `payers.csv`

| Column | Description |
| --- | --- |
| `Id` | Payer identifier; candidate primary key |
| `NAME` | Insurance payer name |
| `AMOUNT_COVERED`, `AMOUNT_UNCOVERED` | Source-reported coverage totals |
| `REVENUE` | Source-reported revenue |
| `COVERED_ENCOUNTERS`, `UNCOVERED_ENCOUNTERS` | Encounter coverage counts |
| `UNIQUE_CUSTOMERS` | Source-reported unique customer count |

## Expected Data Relationships

These are join candidates inferred from source headers. Only `claims.Id` → `claims_transactions.CLAIMID` has been checked for missing and orphan references in local profiling; the other paths remain unverified.

| Parent key | Child key | Expected relationship |
| --- | --- | --- |
| `claims.Id` | `claims_transactions.CLAIMID` | One claim to many transactions |
| `patients.Id` | `claims.PATIENTID` | One patient to many claims |
| `patients.Id` | `encounters.PATIENT` | One patient to many encounters |
| `providers.Id` | `claims.PROVIDERID` | One provider to many claims |
| `providers.Id` | `encounters.PROVIDER` | One provider to many encounters |
| `payers.Id` | `encounters.PAYER` | One payer to many encounters |

**Not yet confirmed:** Whether `claims.APPOINTMENTID` maps to `encounters.Id`, and how the claim-level patient insurance identifiers relate to `payers.Id`. We will not build joins on those assumptions without checking the actual values.

For claim-level calculations, transaction records should generally be **aggregated by `CLAIMID` before joining** to the claims table; otherwise a one-to-many join could multiply claim-level values.

## Next Steps (Not Yet Implemented)

- [ ] Build an AWS Glue PySpark job to read the Bronze CSVs, apply typed schemas and row-level quality checks, and define treatment of invalid records.
- [ ] Produce curated Silver datasets, with Parquet / Apache Iceberg tables where appropriate.
- [ ] Define claim-transaction aggregation semantics and create Gold-level metrics / Athena SQL queries.
- [ ] Configure EventBridge and Step Functions for automatic batch triggering, orchestration, and retries.
- [ ] Improve ingestion reliability with source-file integrity checks, batch immutability guarantees, and explicit handling of concurrency and partial failures.

The documentation is updated as each stage is implemented and verified; planned capabilities are not described as completed.
