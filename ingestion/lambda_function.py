
import os
import json
import boto3

s3 = boto3.client("s3")


def read_manifest(bucket, prefix):
    """
    Read the batch manifest from S3 and parse its JSON content.
    """
    response = s3.get_object(
        Bucket=bucket,
        Key=f"{prefix}ready.json"
    )

    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def list_csv_files(bucket, prefix):
    """
    List CSV filenames delivered under the batch prefix.
    """
    files = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".csv"):
                files.add(key[len(prefix):])

    return files


def validate_manifest(manifest, batch_id, actual_files):
    """
    Validate batch identity and compare expected vs actual files.
    """
    if manifest.get("batch_id") != batch_id:
        raise ValueError("Batch ID mismatch")

    expected_files = manifest.get("files")

    if (
        not isinstance(expected_files, list)
        or not expected_files
        or any(
            not isinstance(name, str)
            or not name.endswith(".csv")
            or "/" in name
            for name in expected_files
        )
    ):
        raise ValueError("Invalid manifest file list")

    if len(expected_files) != len(set(expected_files)):
        raise ValueError("Duplicate filenames in manifest")

    expected = set(expected_files)

    missing = expected - actual_files
    unexpected = actual_files - expected

    if missing or unexpected:
        raise ValueError(
            f"Missing files: {sorted(missing)}; "
            f"Unexpected files: {sorted(unexpected)}"
        )

def copy_to_bronze(source_bucket, lake_bucket, batch_id, files):
    """
    Copy validated raw files from upstream S3 to Bronze.
    """
    source_prefix = f"incoming/{batch_id}/"
    target_prefix = f"bronze/{batch_id}/"

    for filename in sorted(files):
        source_key = f"{source_prefix}{filename}"
        target_key = f"{target_prefix}{filename}"

        s3.copy_object(
            Bucket=lake_bucket,
            Key=target_key,
            CopySource={
                "Bucket": source_bucket,
                "Key": source_key
            }
        )

        print(f"Copied {filename} to Bronze")

    # Copy the manifest only after all CSV copies succeed.
    s3.copy_object(
        Bucket=lake_bucket,
        Key=f"{target_prefix}ready.json",
        CopySource={
            "Bucket": source_bucket,
            "Key": f"{source_prefix}ready.json"
        }
    )

    return len(files)


def batch_already_ingested(lake_bucket, batch_id):
    """
    Check whether the completion marker exists in Bronze.
    """
    marker_key = f"bronze/{batch_id}/_INGESTED.json"

    response = s3.list_objects_v2(
        Bucket=lake_bucket,
        Prefix=marker_key,
        MaxKeys=1
    )

    return any(
        obj["Key"] == marker_key
        for obj in response.get("Contents", [])
    )


def mark_batch_ingested(lake_bucket, batch_id, file_count):
    """
    Write the completion marker after all copies succeed.
    """
    marker_key = f"bronze/{batch_id}/_INGESTED.json"

    marker = {
        "batch_id": batch_id,
        "status": "ingested",
        "file_count": file_count
    }

    s3.put_object(
        Bucket=lake_bucket,
        Key=marker_key,
        Body=json.dumps(marker),
        ContentType="application/json"
    )



def lambda_handler(event, context):
    source_bucket = os.environ["UPSTREAM_BUCKET"]
    lake_bucket = os.environ["LAKE_BUCKET"]

    batch_id = event["batch_id"]
    prefix = f"incoming/{batch_id}/"

    # 1. Validate the source batch.
    manifest = read_manifest(source_bucket, prefix)
    actual_files = list_csv_files(source_bucket, prefix)
    validate_manifest(manifest, batch_id, actual_files)

    # 2. Skip a previously completed batch.
    if batch_already_ingested(lake_bucket, batch_id):
        print(f"Batch {batch_id} already ingested. Skipping.")

        return {
            "batch_id": batch_id,
            "status": "already_ingested",
            "file_count": len(actual_files)
        }

    # 3. Copy files to Bronze.
    copied_count = copy_to_bronze(
        source_bucket,
        lake_bucket,
        batch_id,
        actual_files
    )

    # 4. Mark completion only after all copies succeed.
    mark_batch_ingested(
        lake_bucket,
        batch_id,
        copied_count
    )

    print(f"Batch {batch_id} ingested successfully")

    return {
        "batch_id": batch_id,
        "status": "ingested",
        "file_count": copied_count,
        "destination": f"bronze/{batch_id}/"
    }

