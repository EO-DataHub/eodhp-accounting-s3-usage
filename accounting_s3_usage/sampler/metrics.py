from .athena_utils import run_athena_query
import boto3

ATHENA_DB = "s3_access_logs_db"
ATHENA_OUTPUT_BUCKET = "eodhp-jh-test-bucket-logs"
ATHENA_TABLE = "workspaces_logs_eodhp_dev"


def get_prefix_storage_size(bucket_name, prefix):
    print(f"Calculating storage size for bucket: {bucket_name}, prefix: {prefix}")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_size_bytes = 0
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        if "Contents" in page:
            total_size_bytes += sum(obj["Size"] for obj in page["Contents"])

    size_gb = total_size_bytes / (1024**3)
    return size_gb


def get_access_point_data_transfer(workspace_prefix):
    """
    We mainly care about the object-level operations like GET, PUT, DELETE.

    The query will sum the bytes transferred for GET operations in the last 24 hours.
    """
    query = f"""
    SELECT COALESCE(SUM(bytessent), 0)/1073741824.0 AS total_gb_transferred
    FROM {ATHENA_DB}.{ATHENA_TABLE}
    WHERE operation LIKE 'REST.GET.%'
      AND key LIKE '{workspace_prefix}/%'
      AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
          BETWEEN current_timestamp - interval '1' day AND current_timestamp
    """
    return run_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)


def get_access_point_api_calls(workspace_prefix):
    """
    This query will count the number of API calls made with the specified prefix in the last 24 hours.
    """
    query = f"""
    SELECT COUNT(*) AS total_api_calls
    FROM {ATHENA_DB}.{ATHENA_TABLE}
    WHERE key LIKE '{workspace_prefix}/%'
      AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
          BETWEEN current_timestamp - interval '1' day AND current_timestamp
    """
    return run_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)
