import os
from datetime import datetime
from typing import Generator

import boto3

from .athena_utils import run_long_result_athena_query, run_single_result_athena_query

ATHENA_DB = os.getenv("ATHENA_DB", "s3_access_logs_db")
ATHENA_OUTPUT_BUCKET = os.getenv("ATHENA_OUTPUT_BUCKET", "workspaces-logs-eodhp-dev")
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "workspaces_logs_eodhp_dev")


def format_datetime(dt: datetime) -> str:
    """Format datetime to string in the format 'YYYY-MM-DD HH:MM:SS'."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_prefix_storage_size(bucket_name, prefix):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_size_bytes = 0
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        if "Contents" in page:
            total_size_bytes += sum(obj["Size"] for obj in page["Contents"])

    size_gb = total_size_bytes / (1024**3)
    return size_gb


def get_access_point_data_transfer(
    workspace_prefix, start_time: datetime, end_time: datetime
) -> Generator[tuple[str, str]]:
    query = f"""
    SELECT remoteip, COALESCE(SUM(bytessent), 0)/1073741824.0 AS total_gb_transferred
    FROM {ATHENA_DB}.{ATHENA_TABLE}
    WHERE key LIKE '{workspace_prefix}/%'
      AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
          BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'
    GROUP BY remoteip
    """
    return run_long_result_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)


def get_access_point_api_calls(workspace_prefix, start_time: datetime, end_time: datetime) -> float:
    query = f"""
    SELECT COUNT(*) AS total_api_calls FROM (
        SELECT requestid FROM {ATHENA_DB}.{ATHENA_TABLE}
        WHERE key LIKE '{workspace_prefix}/%'
          AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'

        UNION ALL

        SELECT requestid FROM {ATHENA_DB}.{ATHENA_TABLE}
        WHERE request_uri LIKE '%prefix={workspace_prefix}%/%'
          AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'
    )
    """
    return run_single_result_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)
