"""Audit one healthcare claims batch after the Glue Bronze-to-Silver ETL.

This AWS Lambda function:
1. Reads claim IDs from the original Bronze claims.csv.
2. Checks that typed Silver Parquet files exist.
3. Registers the batch's S3 location as an Athena table partition.
4. Queries typed Silver data through Athena and compares claim IDs.
5. Saves a JSON audit report in S3 and fails the invocation if audit fails.

Designed as a small-batch educational MVP, not a production-scale auditor.
"""

import copy
import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3

s3 = boto3.client("s3")
glue = boto3.client("glue")  # Glue Data Catalog client, NOT the Spark ETL job.
athena = boto3.client("athena")

BUCKET = os.environ["LAKE_BUCKET"]
DATABASE = os.environ["ATHENA_DATABASE"]
TABLE = os.environ["ATHENA_TABLE"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]

MAX_ROWS = 250_000  # In-memory ID comparisons are suitable only for small batches.


def read_csv(bucket, key):
    """Stream an S3 CSV file as a header followed by row dictionaries.

    Args:
        bucket: S3 bucket containing the CSV file.
        key: Object key inside the bucket (without ``s3://``).

    Yields:
        First, a list of CSV column names; then one ``dict`` for each row.

    Why:
        A single helper can read both the original Bronze CSV and the
        CSV result file produced by Athena, without loading either entire
        file into memory all at once.
    """
    response = s3.get_object(Bucket=bucket, Key=key)

    with io.TextIOWrapper(
        response["Body"], encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {key}")

        yield reader.fieldnames
        for row in reader:
            yield row


def collect_source_ids(batch_id):
    """Profile claim IDs in this batch's original Bronze claims.csv.

    Args:
        batch_id: E.g. ``batch_001``.

    Returns:
        A tuple of (unique_nonempty_ids, row_count,
        missing_id_count, duplicate_id_count).

    Why:
        We need an independent view of the original input to detect rows
        lost, duplicated, or unexpectedly added by the ETL process.
    """
    key = f"bronze/{batch_id}/claims.csv"
    rows = read_csv(BUCKET, key)
    columns = next(rows)

    id_column = next(
        (name for name in ("Id", "ID", "id") if name in columns),
        None,
    )
    if not id_column:
        raise ValueError("No Claim ID column in Bronze CSV")

    ids = set()
    total = 0
    missing = 0
    duplicates = 0

    for row in rows:
        total += 1
        if total > MAX_ROWS:
            raise ValueError("Batch exceeds Lambda audit limit")

        claim_id = (row.get(id_column) or "").strip()
        if not claim_id:
            missing += 1
        elif claim_id in ids:
            duplicates += 1
        else:
            ids.add(claim_id)

    return ids, total, missing, duplicates


def check_silver_exists(batch_id):
    """Check for at least one output Parquet file and return its S3 folder.

    Args:
        batch_id: Batch to audit, e.g. ``batch_001``.

    Returns:
        The partition's full S3 URI, ending in ``/``.

    Raises:
        ValueError: No Parquet file is found in the expected prefix.

    Note:
        This is an existence check, NOT proof that the ETL wrote every
        expected file or completed successfully. Reconciliation follows.
    """
    prefix = f"silver/claims_typed_param/batch_id={batch_id}/"
    result = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=prefix,
        MaxKeys=1000,
    )
    files = result.get("Contents", [])
    if not any(item["Key"].endswith(".parquet") for item in files):
        raise ValueError(f"No Silver Parquet found for {batch_id}")

    return f"s3://{BUCKET}/{prefix}"


def register_partition(batch_id, location):
    """Register one S3 batch folder as a Glue Data Catalog partition.

    Why:
        Adding files under ``batch_id=batch_002/`` does not automatically
        add ``batch_002`` to Athena's list of queryable table partitions.
        This registers the metadata for *only this batch*, avoiding a
        manual ``MSCK REPAIR TABLE`` scan of all S3 folders.

    Args:
        batch_id: Value of the table's ``batch_id`` partition column.
        location: S3 folder containing that batch's Silver Parquet files.

    Returns:
        ``"registered"`` if newly created, or
        ``"already_registered"`` if it already exists.

    Important:
        This does NOT execute Spark, copy data, or validate row contents.
        It only updates metadata in the Glue Data Catalog.
    """
    try:
        # Step 1: Avoid registering an existing partition twice.
        glue.get_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionValues=[batch_id],
        )
        return "already_registered"
    except glue.exceptions.EntityNotFoundException:
        # Expected for a brand-new batch; any other error must propagate.
        pass

    # Step 2: Reuse the table's column definitions and Parquet settings.
    table = glue.get_table(DatabaseName=DATABASE, Name=TABLE)["Table"]
    partition_keys = table.get("PartitionKeys", [])
    if len(partition_keys) != 1 or partition_keys[0]["Name"] != "batch_id":
        raise ValueError("Unexpected Athena partition schema")

    descriptor = copy.deepcopy(table["StorageDescriptor"])
    descriptor["Location"] = location  # Point this partition to its folder.

    # Step 3: Tell the catalog that batch_id maps to this location.
    try:
        glue.create_partition(
            DatabaseName=DATABASE,
            TableName=TABLE,
            PartitionInput={
                "Values": [batch_id],
                "StorageDescriptor": descriptor,
            },
        )
        return "registered"
    except glue.exceptions.AlreadyExistsException:
        # Handles a concurrent attempt to register the same partition.
        return "already_registered"


def run_athena_query(batch_id):
    """Query claim IDs and typed fields for exactly one Silver batch.

    Args:
        batch_id: Batch to read from the partitioned Athena table.

    Returns:
        ``(query_execution_id, s3_csv_result_uri)``. Athena writes query
        results into the configured S3 output folder.

    Why:
        S3 stores Parquet, but Athena lets us inspect it with SQL. Here
        we select only the columns needed by the audit.

    Raises:
        RuntimeError: Athena reports query FAILED/CANCELLED.
        TimeoutError: Athena does not finish within the polling window.
    """
    # The handler validates batch_id against a strict regex before this SQL.
    sql = f"""
        SELECT claim_id, service_timestamp, outstanding1_decimal
        FROM {DATABASE}.{TABLE}
        WHERE batch_id = '{batch_id}'
    """

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        ResultReuseConfiguration={
            "ResultReuseByAgeConfiguration": {"Enabled": False}
        },
        WorkGroup="primary",
    )
    query_id = response["QueryExecutionId"]

    # Athena queries run asynchronously, so poll until they finish.
    for _ in range(55):
        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]
        status = execution["Status"]["State"]

        if status == "SUCCEEDED":
            output = execution["ResultConfiguration"]["OutputLocation"]
            return query_id, output
        if status in ("FAILED", "CANCELLED"):
            reason = execution["Status"].get("StateChangeReason", status)
            raise RuntimeError(f"Athena failed: {reason}")
        time.sleep(2)

    athena.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError("Athena query timed out")


def collect_target_ids(output_url):
    """Read Athena's Silver query result and compute audit metrics.

    Args:
        output_url: S3 URI of the CSV written by Athena for the query.

    Returns:
        A dict containing unique claim IDs, row count, missing/duplicate
        ID counts, and NULL counts for the two newly typed columns.

    Why:
        We use the same kind of ID statistics as Bronze, so the two data
        sets can be compared after the ETL transformation.
    """
    parsed = urlparse(output_url)
    rows = read_csv(parsed.netloc, parsed.path.lstrip("/"))
    columns = next(rows)
    required = {"claim_id", "service_timestamp", "outstanding1_decimal"}
    if not required.issubset(set(columns)):
        raise ValueError("Unexpected Athena result schema")

    ids = set()
    total = 0
    missing = 0
    duplicates = 0
    null_timestamps = 0
    null_decimals = 0

    for row in rows:
        total += 1
        if total > MAX_ROWS:
            raise ValueError("Batch exceeds Lambda audit limit")

        claim_id = (row.get("claim_id") or "").strip()
        if not claim_id:
            missing += 1
        elif claim_id in ids:
            duplicates += 1
        else:
            ids.add(claim_id)

        if not row.get("service_timestamp"):
            null_timestamps += 1
        if not row.get("outstanding1_decimal"):
            null_decimals += 1

    return {
        "ids": ids,
        "count": total,
        "missing": missing,
        "duplicates": duplicates,
        "null_timestamps": null_timestamps,
        "null_decimals": null_decimals,
    }


def save_report(batch_id, request_id, report):
    """Save one JSON audit report to S3 and return its location.

    Args:
        batch_id: Used to group reports by batch in S3.
        request_id: Unique Lambda invocation ID; avoids overwriting reports.
        report: JSON-serializable audit metrics and status.

    Returns:
        S3 URI of the saved report.
    """
    key = f"audit/batch_id={batch_id}/{request_id}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{BUCKET}/{key}"


def lambda_handler(event, context):
    """Coordinate the audit for one batch and return PASS or fail the run.

    Args:
        event: Lambda event with ``{"batch_id": "batch_001"}``.
        context: AWS Lambda context; provides a unique request ID.

    Returns:
        A report dictionary containing metrics and report location if PASS.

    Raises:
        ValueError/RuntimeError/etc. if validation or AWS operations fail.
        An ERROR or FAIL report is saved before raising when possible.

    Sequence:
        Verify ingestion -> read Bronze -> locate Silver -> register
        partition -> query via Athena -> compare IDs -> save JSON.
    """
    batch_id = event.get("batch_id")
    if not isinstance(batch_id, str) or not re.fullmatch(
        r"batch_[0-9]{3,}", batch_id
    ):
        raise ValueError("Invalid or missing batch_id")

    report = {
        "batch_id": batch_id,
        "audit_time": datetime.now(timezone.utc).isoformat(),
        "status": "ERROR",
    }

    try:
        # An ingestion completion marker means upstream files were copied.
        s3.head_object(
            Bucket=BUCKET,
            Key=f"bronze/{batch_id}/_INGESTED.json",
        )
        source_ids, source_count, source_missing, source_duplicates = (
            collect_source_ids(batch_id)
        )
        silver_path = check_silver_exists(batch_id)
        partition_status = register_partition(batch_id, silver_path)
        query_id, output_url = run_athena_query(batch_id)
        target = collect_target_ids(output_url)

        # Set difference finds IDs present on one side but not the other.
        missing_ids = len(source_ids - target["ids"])
        unexpected_ids = len(target["ids"] - source_ids)

        passed = all([
            source_count == target["count"],
            source_missing == 0,
            source_duplicates == 0,
            target["missing"] == 0,
            target["duplicates"] == 0,
            missing_ids == 0,
            unexpected_ids == 0,
            target["null_timestamps"] == 0,
            target["null_decimals"] == 0,
        ])
        report.update({
            "source_count": source_count,
            "target_count": target["count"],
            "source_missing_ids": source_missing,
            "target_missing_ids": target["missing"],
            "source_duplicate_ids": source_duplicates,
            "target_duplicate_ids": target["duplicates"],
            "missing_ids": missing_ids,
            "unexpected_ids": unexpected_ids,
            "null_timestamps": target["null_timestamps"],
            "null_decimals": target["null_decimals"],
            "partition_status": partition_status,
            "athena_query_id": query_id,
            "status": "PASS" if passed else "FAIL",
        })

    except Exception as exc:
        report["status"] = "ERROR"
        report["error"] = str(exc)
        save_report(batch_id, context.aws_request_id, report)
        raise

    report_url = save_report(batch_id, context.aws_request_id, report)
    report["report_location"] = report_url

    if report["status"] != "PASS":
        raise RuntimeError(f"Audit FAILED for {batch_id}: {report_url}")

    return report
