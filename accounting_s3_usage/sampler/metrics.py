import os
from datetime import datetime
from typing import Generator, Tuple

import boto3

from .athena_utils import (
    run_athena_query,
    run_long_result_athena_query,
    run_single_result_athena_query,
)

ATHENA_DB = os.getenv("ATHENA_DB", "accounting_eodhp_dev")
ATHENA_OUTPUT_BUCKET = os.getenv("ATHENA_OUTPUT_BUCKET", "accounting-athena-eodhp-dev")
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "workspaces_s3_access_logs_eodhp_dev")
LOGS_PREFIX = os.getenv(
    "WORKSPACE_S3_ACCESS_LOGS_S3_PREFIX",
    "s3://workspaces-access-logs-eodhp-dev/012345678901/us-east-1/workspaces-eodhp-dev",
)


def format_datetime(dt: datetime) -> str:
    """Format datetime to string in the format 'YYYY-MM-DD HH:MM:SS'."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_partition_start_end_days(start_time: datetime, end_time: datetime) -> Tuple[str, str]:
    return (start_time.strftime("%Y/%m/%d"), end_time.strftime("%Y/%m/%d"))


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
    start_partition, end_partition = get_partition_start_end_days(start_time, end_time)
    query = f"""
    SELECT remoteip, COALESCE(SUM(bytessent), 0)/1073741824.0 AS total_gb_transferred
    FROM {ATHENA_DB}.{ATHENA_TABLE}
    WHERE key LIKE '{workspace_prefix}/%'
      AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
          BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'
      AND timestamp BETWEEN '{start_partition}' AND '{end_partition}'
    GROUP BY remoteip
    """
    return run_long_result_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)


def get_access_point_api_calls(workspace_prefix, start_time: datetime, end_time: datetime) -> float:
    start_partition, end_partition = get_partition_start_end_days(start_time, end_time)
    query = f"""
    SELECT COUNT(*) AS total_api_calls FROM (
        SELECT requestid FROM {ATHENA_DB}.{ATHENA_TABLE}
        WHERE key LIKE '{workspace_prefix}/%'
          AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'
          AND timestamp BETWEEN '{start_partition}' AND '{end_partition}'

        UNION ALL

        SELECT requestid FROM {ATHENA_DB}.{ATHENA_TABLE}
        WHERE request_uri LIKE '%prefix={workspace_prefix}%/%'
          AND parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z')
            BETWEEN TIMESTAMP '{format_datetime(start_time)}' AND TIMESTAMP '{format_datetime(end_time)}'
          AND timestamp BETWEEN '{start_partition}' AND '{end_partition}'
    )
    """
    return run_single_result_athena_query(query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)


def create_athena_table():
    query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE} (
    bucket_owner STRING,
    bucket STRING,
    requestdatetime STRING,
    remoteip STRING,
    requester STRING,
    requestid STRING,
    operation STRING,
    key STRING,
    request_uri STRING,
    httpstatus STRING,
    errorcode STRING,
    bytessent BIGINT,
    objectsize BIGINT,
    totaltime STRING,
    turnaroundtime STRING,
    referrer STRING,
    useragent STRING,
    versionid STRING,
    hostid STRING,
    sigv STRING,
    ciphersuite STRING,
    authtype STRING,
    endpoint STRING,
    tlsversion STRING,
    accesspointarn STRING
)
PARTITIONED BY (
    timestamp STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
 'input.regex'='([^ ]*) ([^ ]*) \\\\[([^]]*)\\\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ("[^"]*"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ("[^"]*"|-) ("[^"]*"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*)(?: ([^ ]*))?.*$'
)
LOCATION '{LOGS_PREFIX}'
TBLPROPERTIES (
 'projection.enabled'='true',
 'projection.timestamp.type'='date',
 'projection.timestamp.format'='yyyy/MM/dd',
 'projection.timestamp.interval'='1',
 'projection.timestamp.interval.unit'='DAYS',
 'projection.timestamp.range'='2025/01/01,NOW',
 'storage.location.template'='{LOGS_PREFIX}${{timestamp}}'
);
"""  # noqa

    athena = boto3.client("athena")
    run_athena_query(athena, query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)
