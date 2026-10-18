# Healthcare Claims ETL

An event-driven **AWS healthcare claims batch data pipeline** using synthetic [Synthea](https://synthetichealth.github.io/synthea/) CSV data. The project ingests complete file deliveries into an S3 Bronze layer, transforms claims into typed Parquet in Silver using AWS Glue / Apache Spark, and audits each successful batch with Athena.

**Scope:** Each delivery contains 18 source CSVs. Ingestion preserves all 18, while the implemented Spark transformation and reconciliation currently focus on **`claims.csv`**. A curated **Gold layer has not been implemented yet**.

## Architecture

```text
Synthetic Synthea CSV delivery (18 files)
                    |
                    v
S3 upstream: incoming/batch_XXX/  (ready.json uploaded last)
                    |
             S3 Object Created
                    v
EventBridge rule: healthcare-claims-on-batch-ready
                    |
                    v
Step Functions: healthcare-claims-pipeline
                    |
      ExtractBatchId (from S3 object key)
                    |
                    v
      IngestBronze — Lambda
      - Compare delivered CSV filenames with ready.json
      - Validate batch ID and manifest
      - Copy all CSVs + manifest to Bronze
      - Write _INGESTED.json after copying
                    |
                    v
      TransformSilver — Glue 5.1 / Apache Spark
      - Parameterized --batch_id
      - Read Bronze claims.csv
      - Rename selected fields; parse timestamp and decimal
      - Apply claim_id completeness / uniqueness DQ rules
      - Write Snappy Parquet to partition-style S3 prefix
                    |
                    v
      AuditBatch — Lambda
      - Inspect Bronze IDs and Silver existence
      - Register batch partition in Glue Data Catalog
      - Read Silver through Athena
      - Reconcile counts / IDs / duplicates / typed NULLs
      - Save S3 JSON audit report (PASS / FAIL / ERROR)

S3 lake:  bronze/  ->  silver/  ->  audit/
                          |
                          +-- Glue Data Catalog (schema and partitions)
                                      |
                                      +-- Athena SQL queries
```

**Two different workflows:** EventBridge + Step Functions run the **data pipeline when a delivery arrives**. GitHub Actions, if added later, would instead run **CI/CD on code changes**. They solve different problems.

## AWS Services and Responsibilities

| Service | Deployed responsibility |
| --- | --- |
| **Amazon S3** | Upstream deliveries, Bronze originals, Silver Parquet, Athena query results, and JSON audit reports |
| **Amazon EventBridge** | Match an S3 Object Created event for `incoming/batch_*/ready.json` and trigger the state machine |
| **AWS Step Functions (Standard)** | Extract `batch_id`, invoke the ingestion Lambda, synchronously run Glue, then invoke the audit Lambda |
| **Ingestion Lambda (Python)** | Validate manifest/file set, copy originals into Bronze, and mark completed ingestion |
| **AWS Glue 5.1 / Apache Spark** | Transform one claims batch, enforce Glue Data Quality checks, output typed Snappy Parquet |
| **Glue Data Catalog** | Hold Athena external-table schema and `batch_id` partition metadata |
| **Amazon Athena** | Query Silver Parquet; supply claim rows for reconciliation and ad hoc comparisons |
| **Audit Lambda (Python)** | Check counts, claim IDs, missing/duplicate records, NULL typed fields, and persist a JSON report |
| **AWS IAM / CloudWatch** | Resource-specific permissions and runtime logs; an ingestion `CopyObject` failure exposed additional object-tagging permission requirements |

AWS resources and deployment settings were configured **manually in the AWS Console**. This repository is **not yet a reproducible IaC deployment**.

## Data and Tables

```text
Upstream bucket
  incoming/batch_001/                 18 CSVs + ready.json
  incoming/batch_002/                 edited valid claims.csv + 17 other CSVs + ready.json
  incoming/batch_003/                 deliberately duplicated Claim ID + other CSVs + ready.json

Lake bucket
  bronze/batch_XXX/                   original CSVs + ready.json + _INGESTED.json
  silver/claims_typed_param/
    batch_id=batch_001/               Snappy Parquet
    batch_id=batch_002/               Snappy Parquet
  audit/batch_id=batch_XXX/           JSON reports for audited batches
  athena-results/                     Athena query output
```

The active Athena external table is `healthcare_claims.silver_claims_typed_param`, partitioned by `batch_id`. The audit Lambda registers each new successful Glue batch with the Glue Data Catalog API, instead of running `MSCK REPAIR TABLE` manually. S3 holds the actual Parquet files; the catalog holds table and partition metadata.

The Silver claims transformation retains source string columns and adds `service_timestamp` (TIMESTAMP) and `outstanding1_decimal` (DECIMAL(18,2)). The earlier non-parameterized Glue visual job and `silver_claims` / `silver_claims_typed_check` Athena tables were development prototypes, not the active event-driven workflow.

## Integration Test Results

| Scenario | Input / purpose | Observed result |
| --- | --- | --- |
| **`batch_001` — baseline** | 18 source CSVs; 117,889 claims | Manually verified parameterized Glue run and **audit PASS**. Counts match, zero missing/unexpected/duplicate IDs, zero NULL typed values; original Glue DQ rules passed |
| **`batch_002` — event-driven E2E PASS** | Edited **2 amounts and 1 service date** without changing claim IDs | **Pipeline succeeded:** EventBridge → Step Functions → Ingestion (18 CSVs) → Glue (101 s, 2 G.1X workers) → Audit **PASS**. Bronze / Silver counts: **117,889 / 117,889**, zero missing, unexpected or duplicate IDs and zero NULL typed values; new catalog partition registered. **Athena comparison against `batch_001`:** 117,889 matching IDs, **2 changed amounts and 1 changed date**, as intended. |
| **`batch_003` — negative DQ test** | Make the second claim use the first claim's ID, preserving file count | Ingestion reached Glue; Glue job **FAILED** with `AssertionError: The job failed due to failing DQ rules`. Step Functions did not proceed to a successful audit. **Glue Data Quality results confirmed** `IsComplete "claim_id"` **passed** and `IsUnique "claim_id"` **failed**; the DQ assertion stopped the job |

**Important distinction:** A Step Functions Lambda task turning green means the invocation succeeded; a successful audit must also return business-level `status: PASS`. That was confirmed for `batch_002`.

The manual Athena comparison confirms that intended changes reached Silver. It is **not** a full row-by-row Bronze-to-Silver value comparison; the automated audit currently compares identity, counts, duplicates, and typed-column NULLs.

### Data Quality

- **File-level (ingestion):** validate `ready.json` batch ID, declared filenames, and actual delivered CSV filenames. This does not parse or validate every CSV row.
- **Row-level / dataset-level (Glue):** evaluate `IsComplete "claim_id"` and `IsUnique "claim_id"` before the Silver target.
- **Post-transform (audit):** reconcile Bronze versus Silver counts and claim ID sets; reject missing/duplicate IDs or NULL typed values.

The `batch_003` rule-level evidence was confirmed in **AWS Glue Studio → job → Data quality → Rules**: `IsComplete "claim_id"` **Rule passed**, `IsUnique "claim_id"` **Rule failed**. The initial job exception was generic; the structured rule evaluation provided the specific cause.

## Repository Layout

```text
healthcare-claims-etl/
├── ingestion/
│   └── lambda_function.py          # Manifest validation and Bronze copying
├── audit/
│   └── lambda_function.py          # Catalog registration, Athena reconciliation, report
├── quality/
│   └── profile_sources.py          # Local exploratory source profiling
├── tests/
│   └── prepare_test_batches.py     # Reproducible batch_002 and batch_003 scenarios
├── .gitignore
└── README.md
```

**Still in AWS Console, not yet committed to this repo:** the **parameterized Glue Spark script**, **Step Functions ASL definition**, EventBridge configuration, and IAM / job / catalog infrastructure settings. Version-controlling these is a priority below.

### Recreate test inputs locally

From the repository root, with the original 18 Synthea CSVs in a sibling `../csv` folder:

```bash
python3 tests/prepare_test_batches.py \
  --source-dir ../csv \
  --output-dir ../test_batches
```

The script writes only the changed `claims.csv` and `ready.json` for each test batch. Copy the unchanged 17 CSVs from the first delivery into each new S3 prefix, upload the generated `claims.csv`, then **upload `ready.json` last** to trigger the workflow. The raw input and generated CSV data are not committed.

### Runtime interfaces

- Ingestion and audit Lambda event: `{"batch_id": "batch_002"}`
- Glue job: `healthcare-claims-bronze-to-silver-param` with `--batch_id=batch_002`
- Ingestion environment: `UPSTREAM_BUCKET`, `LAKE_BUCKET`
- Audit environment: `LAKE_BUCKET`, `ATHENA_DATABASE`, `ATHENA_TABLE`, `ATHENA_OUTPUT`

## Limitations

This is a learning / portfolio MVP, **not** a production-ready healthcare data platform.

- **Batch semantics:** `batch_002` contains modified versions of some `batch_001` claims. Both partitions coexist; the project does **not** implement claim-level upserts, CDC, latest-version views, or deduplication *across* batches.
- **Idempotency and recovery:** an ingestion completion marker avoids ordinary sequential duplicate ingestion; the Glue output-exists guard avoids straightforward overwrites. Neither guarantees concurrent exactly-once execution, rollback, or safe recovery after partially written output.
- **Quality visibility:** Glue Studio exposes per-rule DQ results (verified for `batch_003`), but a durable custom failure report, quarantine, and structured alerting need improvement.
- **Audit scaling:** the current Lambda holds claim ID sets in memory and enforces a 250,000-row limit. It does not compare every transformed amount/date to its source.
- **Publication:** the catalog partition is registered before final audit PASS. No atomic publish/promote step or downstream visibility gate exists.
- **Security and deployment:** only synthetic data is used; no real patient records or PHI. Manual Console configuration is not yet managed through IaC or CI/CD.

## Roadmap / TODO

### P1 — Reproducibility and developer workflow

- [ ] **Commit deployed logic:** export the parameterized Glue Spark script and Step Functions Amazon States Language (ASL) JSON to the repository.
- [ ] **Capture AWS configuration:** EventBridge rule/pattern, Glue job properties, Lambda runtime/environment variable names, IAM permission scopes, S3 layout, Athena / catalog table definitions.
- [ ] **Infrastructure as Code:** define/manage existing AWS resources with **Terraform or AWS CDK**; carefully import/adopt manually created resources before applying changes.
- [ ] **GitHub Actions CI/CD:** lint/test/package Python and deploy versioned Glue/Lambda/workflow assets; use GitHub **OIDC** to assume an AWS IAM deployment role instead of storing long-lived access keys.
- [ ] Add automated unit/integration tests and an explicit deployment / rollback procedure.

### P2 — Expand the data model and analytics

- [ ] Add transformations for related Synthea entities (e.g. encounters, patients, providers, payers, claims transactions) and validate join keys / referential integrity.
- [ ] **Build the Gold layer:** curated claims analytics tables or aggregates, such as payer-level claim amounts, provider utilization, outstanding balances, and time-series summaries.
- [ ] Support updated claims across deliveries using **Apache Iceberg** tables, `MERGE INTO` / upserts, snapshots, and explicitly defined latest-record semantics.
- [ ] Extend typed schema validation to more columns, invalid-date/amount handling, and source-to-target field-level reconciliation.
- [ ] Consider late-arriving data, schema evolution, and clear batch versus business-event timestamps.

### P3 — Reliability, observability, and operational hardening

- [ ] Export/persist **per-rule Glue Data Quality outcomes** and failing-row evidence outside the Glue UI; add quarantine/rejected-record paths and meaningful error reports.
- [ ] Add Step Functions retries/backoff where safe, failure notifications, EventBridge DLQ, monitoring dashboards, and resource-level metrics.
- [ ] Implement concurrency controls, safer idempotency, partial-write cleanup, and an audit-gated publish/promotion flow.
- [ ] Replace in-memory Lambda ID comparison with scalable Athena/Spark SQL reconciliation for larger datasets.
- [ ] Improve IAM least privilege, object-tag handling, encryption/cost controls, and reproducible teardown.

The primary near-term objective was to validate a working **event-triggered Bronze-to-Silver pipeline with a positive audit and a controlled negative DQ test**. Those tests have now been exercised; broader data modeling, Gold analytics, IaC, and CI/CD remain future work.
