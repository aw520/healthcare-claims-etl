# healthcare-claims-etl

Automated healthcare data pipeline using AWS, PySpark, and SQL.

> **Project status:** Initial dataset exploration and AWS storage setup. The ETL pipeline, data quality checks, and analytics tables are planned but have not yet been implemented.

## Source Dataset

This project uses the **1K Sample Synthetic Patient Records (CSV)** dataset from [Synthea](https://synthetichealth.github.io/synthea/), an open-source synthetic healthcare data generator.

The original CSV files are kept outside this repository. An upstream Amazon S3 bucket will simulate data deliveries from an external healthcare data provider.

### Selected Source Files

The initial scope focuses on six related CSV files:

| File | Record grain | Main purpose |
| --- | --- | --- |
| `claims.csv` | One row per claim (to verify) | Claim identifiers, patient/provider references, service dates, diagnoses, and claim status |
| `claims_transactions.csv` | One row per claim transaction (to verify) | Charges, payments, adjustments, transfers, and outstanding balances |
| `patients.csv` | One row per patient | Patient attributes and demographic information |
| `providers.csv` | One row per provider | Provider names, specialties, organizations, and locations |
| `encounters.csv` | One row per encounter | Visit dates, provider/payer references, and encounter costs |
| `payers.csv` | One row per payer | Insurance payer information and source-level coverage statistics |

The record grains and expected identifiers will be confirmed by profiling the downloaded data.

## Source Schema Reference

The columns below come from the **actual CSV headers** of the selected Synthea sample. This is a focused reference, not a complete list of every source column. Data types, nullability, uniqueness, and referential integrity have **not yet been validated**.

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

**Important:** Do not assume that `AMOUNT` means "paid amount." Transaction types and amount columns must be profiled before defining financial metrics.

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

These are **candidate join paths**, inferred from column names. We will check actual key matches before treating them as established relationships.

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

## Initial Data Profiling Checklist

Before implementing transformations, inspect and record:

- [ ] Row counts and candidate primary-key uniqueness for all six files
- [ ] Null and blank values in required identifiers
- [ ] Candidate foreign-key match rates and orphan transaction records
- [ ] Date and numeric field formats, including invalid values
- [ ] Actual `claims_transactions.TYPE` values and the meaning of the different amount columns
- [ ] Join cardinality and potential duplication when combining tables

This README will be expanded incrementally as the pipeline is implemented and validated.
