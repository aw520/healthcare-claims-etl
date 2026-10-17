# Healthcare Claims ETL

An AWS-based batch data pipeline for **synthetic healthcare claims**. It ingests source files into an S3 data lake, transforms claims with AWS Glue / Apache Spark, and uses Amazon Athena and a Lambda audit to validate the resulting Silver data.

**Scope:** The sample delivery contains 18 Synthea CSV files, but the implemented Silver transformation and automated audit currently focus on **`claims.csv`**. Other datasets have only been explored locally.

## Architecture

```text
Synthea CSV batch
     |
     v
S3 upstream: incoming/<batch_id>/
  CSV files + ready.json (uploaded last)
     |
     | S3 Object Created event
     v
Amazon EventBridge (ready.json rule)
     |
     v
AWS Step Functions (batch workflow)
     |
     +--> Ingestion Lambda
     |      Validate manifest -> copy original files
     |      -> S3 bronze/<batch_id>/ + _INGESTED.json
     |
     +--> AWS Glue 5.1 / Apache Spark
     |      Read Bronze claims.csv -> rename columns
     |      -> cast timestamp / decimal fields
     |      -> claim_id completeness & uniqueness checks
     |      -> S3 silver/claims_typed_param/batch_id=<batch_id>/ (Snappy Parquet)
     |
     +--> Audit Lambda
            Register/check partition in Glue Data Catalog
            -> query Silver with Athena
            -> reconcile Bronze and Silver claim IDs
            -> S3 audit/batch_id=<batch_id>/*.json

Amazon Athena queries Parquet in S3 using Glue Data Catalog metadata.
```

**Integration status:** The ingestion Lambda, parameterized Glue job, and audit Lambda have each been run successfully. The Step Functions state machine and EventBridge rule have been created, but the full event-triggered workflow has **not yet been validated end to end with a new batch**.

## AWS Components

| Component | Responsibility |
| --- | --- |
| **Amazon S3** | Upstream delivery; immutable-in-intent Bronze originals; Parquet Silver outputs; Athena results and audit JSON reports |
| **AWS Lambda — ingestion** | Validate `ready.json` against delivered CSV filenames, copy a complete batch into Bronze, write `_INGESTED.json`, skip previously ingested batches on sequential retries |
| **AWS Glue / Spark** | Parameterized `--batch_id` ETL for claims; schema mapping, Spark SQL type conversion, and Glue Data Quality |
| **Amazon Athena + Glue Data Catalog** | Query Silver Parquet through an external, `batch_id`-partitioned table; store schema and partition metadata separately from S3 data |
| **AWS Lambda — audit** | Compare source and target counts and claim IDs, check duplicates and null typed values, register/check the catalog partition, and write a PASS/FAIL JSON report |
| **Step Functions + EventBridge** | Created for triggering on delivery of `ready.json` and coordinating ingestion → Glue → audit; integration test pending |

## Verified Results — `batch_001`

| Check | Result |
| --- | --- |
| Delivered and ingested source files | 18 CSVs; manifest validation passed |
| Claims in Bronze / Silver | 117,889 / 117,889 |
| Glue Data Quality | `claim_id` completeness and uniqueness: 2/2 rules passed |
| Timestamp / decimal conversion | 0 NULL results in both added typed columns |
| Automated audit | **PASS**; 0 missing, unexpected, or duplicate claim IDs |
| Audit Lambda execution | ~15.3 s; 256 MB configured, 135 MB peak memory |
| Audit output | JSON report written automatically under `audit/batch_id=batch_001/` |

The original claims data was transformed to Parquet with additional `service_timestamp` (TIMESTAMP) and `outstanding1_decimal` (DECIMAL(18,2)) columns. The original string columns are retained for comparison. The audit validates row/ID integrity and missing typed values; it does **not** yet compare every transformed value against its source.

The parameterized Glue job has been tested with `--batch_id=batch_001`. Its output-exists guard prevents a straightforward second write into a nonempty batch prefix, but it is **not** a full concurrent-execution or partial-failure recovery solution.

## Data Layout

```text
Upstream S3
  incoming/batch_001/
    18 original CSVs
    ready.json

Data lake S3
  bronze/batch_001/
    18 original CSVs
    ready.json
    _INGESTED.json
  silver/claims_typed_param/batch_id=batch_001/
    *.parquet
  audit/batch_id=batch_001/
    <lambda_request_id>.json
  athena-results/
    ...
```

`ready.json` is an application-level delivery manifest listing the exact CSV filenames expected for the batch. `_INGESTED.json` is written only after the ingestion Lambda has copied the batch to Bronze. Neither marker by itself proves that downstream ETL and audit succeeded.

## Code & Development

```text
ingestion/lambda_function.py    # S3 manifest validation and Bronze ingestion
audit/lambda_function.py        # Automated claims reconciliation and audit
quality/profile_sources.py      # Local exploratory profiling of source CSVs
```

Source: [Synthea synthetic patient data](https://synthetichealth.github.io/synthea/), using the 1K CSV sample. Raw CSV files are intentionally excluded from this repository. Exploratory profiling examined six CSVs, including transaction-to-claim references, but the deployed Spark transformation currently processes **claims only**.

AWS resources have been configured and deployed manually; infrastructure-as-code and automated deployments are not yet included in this repository.

### Runtime Inputs

The ingestion Lambda and audit Lambda accept an event such as:

```json
{"batch_id": "batch_001"}
```

The Glue job `healthcare-claims-bronze-to-silver-param` accepts a `--batch_id` run argument. Ingestion uses `UPSTREAM_BUCKET` and `LAKE_BUCKET`; audit uses `LAKE_BUCKET`, `ATHENA_DATABASE`, `ATHENA_TABLE`, and `ATHENA_OUTPUT` environment variables.

## Next Steps

- **Validate `batch_002` end to end:** upload a new delivery manifest last; confirm EventBridge → Step Functions → Lambda → Glue → audit without manual invocations.
- Improve idempotency, retries, concurrent-batch handling, and recovery from partial writes. Separate passing audit status from release of data to downstream consumers.
- Extend transformations to related datasets, then consider Apache Iceberg upserts/snapshots and a Gold analytics layer.

This README distinguishes **implemented and tested components** from integration and production-hardening work still in progress.
