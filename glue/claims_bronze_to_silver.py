import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import re
import boto3

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'batch_id']
)

batch_id = args["batch_id"]

# Validate batch ID
if not re.fullmatch(r"batch_[0-9]{3,}", batch_id):
    raise ValueError(f"Invalid batch_id: {batch_id}")

bucket = "healthcare-lake-146699354444-us-east-1-an"

source_path = (
    f"s3://{bucket}/bronze/{batch_id}/claims.csv"
)

target_prefix = (
    f"silver/claims_typed_param/batch_id={batch_id}/"
)

target_path = f"s3://{bucket}/{target_prefix}"

s3 = boto3.client("s3")

# Confirm ingestion completed
s3.head_object(
    Bucket=bucket,
    Key=f"bronze/{batch_id}/_INGESTED.json"
)

# Prevent accidental duplicate output on rerun
existing = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=target_prefix,
    MaxKeys=1
)

if existing.get("KeyCount", 0) > 0:
    raise RuntimeError(
        f"Output already exists for {batch_id}: "
        f"{target_path}"
    )

print(f"Processing batch: {batch_id}")
print(f"Source: {source_path}")
print(f"Target: {target_path}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1792257752971 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [source_path], "recurse": True}, transformation_ctx="AmazonS3_node1792257752971")

# Script generated for node Change Schema
ChangeSchema_node1792264803538 = ApplyMapping.apply(frame=AmazonS3_node1792257752971, mappings=[("id", "string", "claim_id", "string"), ("patientid", "string", "patient_id", "string"), ("providerid", "string", "provider_id", "string"), ("primarypatientinsuranceid", "string", "primarypatientinsuranceid", "string"), ("secondarypatientinsuranceid", "string", "secondarypatientinsuranceid", "string"), ("departmentid", "string", "departmentid", "string"), ("patientdepartmentid", "string", "patientdepartmentid", "string"), ("diagnosis1", "string", "diagnosis1", "string"), ("diagnosis2", "string", "diagnosis2", "string"), ("diagnosis3", "string", "diagnosis3", "string"), ("diagnosis4", "string", "diagnosis4", "string"), ("diagnosis5", "string", "diagnosis5", "string"), ("diagnosis6", "string", "diagnosis6", "string"), ("diagnosis7", "string", "diagnosis7", "string"), ("diagnosis8", "string", "diagnosis8", "string"), ("referringproviderid", "string", "referringproviderid", "string"), ("appointmentid", "string", "appointmentid", "string"), ("currentillnessdate", "string", "currentillnessdate", "string"), ("servicedate", "string", "service_date", "string"), ("supervisingproviderid", "string", "supervisingproviderid", "string"), ("status1", "string", "status1", "string"), ("status2", "string", "status2", "string"), ("statusp", "string", "statusp", "string"), ("outstanding1", "string", "outstanding1", "string"), ("outstanding2", "string", "outstanding2", "string"), ("outstandingp", "string", "outstandingp", "string"), ("lastbilleddate1", "string", "lastbilleddate1", "string"), ("lastbilleddate2", "string", "lastbilleddate2", "string"), ("lastbilleddatep", "string", "lastbilleddatep", "string"), ("healthcareclaimtypeid1", "string", "healthcareclaimtypeid1", "string"), ("healthcareclaimtypeid2", "string", "healthcareclaimtypeid2", "string")], transformation_ctx="ChangeSchema_node1792264803538")

# Script generated for node SQL Query
SqlQuery1354 = '''

SELECT
    *,
    TO_TIMESTAMP(
        service_date,
        "yyyy-MM-dd'T'HH:mm:ssX"
    ) AS service_timestamp,
    CAST(outstanding1 AS DECIMAL(18,2))
        AS outstanding1_decimal
FROM myDataSource

'''
SQLQuery_node1792270704778 = sparkSqlQuery(glueContext, query = SqlQuery1354, mapping = {"myDataSource":ChangeSchema_node1792264803538}, transformation_ctx = "SQLQuery_node1792270704778")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1792265190392_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "claim_id",
        IsUnique "claim_id"
    ]
"""

EvaluateDataQuality_node1792265190392 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1792270704778, ruleset=EvaluateDataQuality_node1792265190392_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792265190392", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

assert EvaluateDataQuality_node1792265190392[EvaluateDataQuality.DATA_QUALITY_RULE_OUTCOMES_KEY].filter(lambda x: x["Outcome"] == "Failed").count() == 0, "The job failed due to failing DQ rules for node: SQLQuery_node1792270704778"

# Script generated for node originalData
originalData_node1792265546341 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1792265190392, key="originalData", transformation_ctx="originalData_node1792265546341")

# Script generated for node Amazon S3
AmazonS3_node1792266159424 = glueContext.write_dynamic_frame.from_options(frame=originalData_node1792265546341, connection_type="s3", format="glueparquet", connection_options={"path": target_path, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1792266159424")

job.commit()