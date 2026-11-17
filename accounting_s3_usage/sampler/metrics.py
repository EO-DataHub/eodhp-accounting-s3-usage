import os
import re
from collections.abc import Generator
from datetime import datetime, timedelta

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


def get_partition_start_end_days(start_time: datetime, end_time: datetime) -> tuple[str, str]:
    # S3 access log delivery is best-effort and can lag behind the event, so a request made
    # just before midnight can be delivered into the *next* day's partition. Extend the upper
    # bound by a day to still find it; the precise `requestdatetime` filter in the query still
    # constrains results to the actual requested interval, so this can't return extra rows.
    return (start_time.strftime("%Y/%m/%d"), (end_time + timedelta(days=1)).strftime("%Y/%m/%d"))


def get_prefix_storage_size(bucket_name: str, prefix: str) -> float:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_size_bytes = 0
    # A trailing slash is required so that, e.g., prefix "team1" doesn't also match objects
    # under "team10/" - matching the convention used for the Athena `key LIKE` queries below.
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=f"{prefix}/")

    for page in page_iterator:
        if "Contents" in page:
            total_size_bytes += sum(obj["Size"] for obj in page["Contents"])

    size_gb = total_size_bytes / (1024**3)
    return size_gb


def get_access_point_data_transfer(
    workspace_prefix: str, start_time: datetime, end_time: datetime
) -> Generator[tuple[str | None, ...]]:
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


def get_access_point_api_calls(workspace_prefix: str, start_time: datetime, end_time: datetime) -> float:
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


def create_athena_table() -> None:
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
"""

    athena = boto3.client("athena")
    run_athena_query(athena, query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)


# Requester-pays reads (ADR-003 in eodhp-guide). The data-objects service signs each read of a
# requester-pays bucket, such as usgs-landsat, as an STS session of its signer role named after
# the reading workspace. The account's data-access CloudTrail trail records each read with that
# session in userIdentity.arn and the bytes sent in additionalEventData. The Hub pays AWS for
# those bytes, and this charges them to the workspace.
#
# The trail is per AWS account, and staging and prod share an account, so only the signer role
# of this cluster counts. That also leaves out the thumbnail role: catalogue previews are
# anonymous and the project pays for them.
#
# Unset DATA_ACCESS_TRAIL_TABLE to turn this off, for example where there is no trail.
DATA_ACCESS_TRAIL_TABLE = os.getenv("DATA_ACCESS_TRAIL_TABLE", "")
# s3://<trail bucket>/AWSLogs/<account>/CloudTrail/ - region and date follow.
DATA_ACCESS_TRAIL_S3_PREFIX = os.getenv("DATA_ACCESS_TRAIL_S3_PREFIX", "")
# Regions of the requester-pays buckets. The trail files each event under its bucket's region.
DATA_ACCESS_TRAIL_REGIONS = os.getenv("DATA_ACCESS_TRAIL_REGIONS", "us-west-2")
# data-objects-signer-<cluster name>
DATA_OBJECTS_SIGNER_ROLE_NAME = os.getenv("DATA_OBJECTS_SIGNER_ROLE_NAME", "")

# data-objects' readiness probe reads as this session. It is not a workspace.
READINESS_CHECK_SESSION = "_readiness-check"

# The trail began in November 2026. Partition projection needs a start date.
DATA_ACCESS_TRAIL_START = "2026/11/01"

# IAM role names and AWS region names. These go into SQL, so they must not carry quotes.
_IAM_ROLE_NAME = re.compile(r"^[\w+=,.@-]{1,64}$")
_AWS_REGIONS = re.compile(r"^[a-z0-9-]+(,[a-z0-9-]+)*$")
_TRAIL_PREFIX = re.compile(r"^s3://[a-z0-9.-]+/AWSLogs/\d{12}/CloudTrail/$")


def requester_pays_billing_enabled() -> bool:
    return bool(DATA_ACCESS_TRAIL_TABLE)


def check_requester_pays_settings() -> None:
    """Raise ValueError if requester-pays billing is on and its settings are not usable."""
    if not _IAM_ROLE_NAME.match(DATA_OBJECTS_SIGNER_ROLE_NAME):
        raise ValueError(f"DATA_OBJECTS_SIGNER_ROLE_NAME is not an IAM role name: {DATA_OBJECTS_SIGNER_ROLE_NAME!r}")
    if not _AWS_REGIONS.match(DATA_ACCESS_TRAIL_REGIONS):
        raise ValueError(
            f"DATA_ACCESS_TRAIL_REGIONS is not a comma-separated region list: {DATA_ACCESS_TRAIL_REGIONS!r}"
        )
    if not _TRAIL_PREFIX.match(DATA_ACCESS_TRAIL_S3_PREFIX):
        raise ValueError(
            "DATA_ACCESS_TRAIL_S3_PREFIX must look like s3://<bucket>/AWSLogs/<account>/CloudTrail/, "
            f"not {DATA_ACCESS_TRAIL_S3_PREFIX!r}"
        )
    if not re.match(r"^\w+$", DATA_ACCESS_TRAIL_TABLE):
        raise ValueError(f"DATA_ACCESS_TRAIL_TABLE is not a table name: {DATA_ACCESS_TRAIL_TABLE!r}")


def requester_pays_transfer_query(start_time: datetime, end_time: datetime) -> str:
    """
    Gigabytes each workspace read through data-objects in [start_time, end_time).

    Failed reads (errorcode set) sent no data and are not charged; most are GDAL probing for
    sidecar files that don't exist.
    """
    start_partition, end_partition = get_partition_start_end_days(start_time, end_time)
    return f"""
    SELECT workspace, SUM(bytes_out)/1073741824.0 AS total_gb_transferred
    FROM (
        SELECT
            element_at(split(useridentity.arn, '/'), 3) AS workspace,
            COALESCE(CAST(json_extract_scalar(additionaleventdata, '$.bytesTransferredOut') AS BIGINT), 0) AS bytes_out
        FROM {ATHENA_DB}.{DATA_ACCESS_TRAIL_TABLE}
        WHERE eventsource = 's3.amazonaws.com'
          AND eventname = 'GetObject'
          AND errorcode IS NULL
          AND element_at(split(useridentity.arn, '/'), 2) = '{DATA_OBJECTS_SIGNER_ROLE_NAME}'
          AND from_iso8601_timestamp(eventtime) >= from_iso8601_timestamp('{start_time.isoformat()}')
          AND from_iso8601_timestamp(eventtime) < from_iso8601_timestamp('{end_time.isoformat()}')
          AND timestamp BETWEEN '{start_partition}' AND '{end_partition}'
    )
    WHERE workspace IS NOT NULL AND workspace <> '{READINESS_CHECK_SESSION}'
    GROUP BY workspace
    """


def get_requester_pays_transfer(start_time: datetime, end_time: datetime) -> Generator[tuple[str | None, ...]]:
    """Yields (workspace, gigabytes) for each workspace that read anything in the period."""
    return run_long_result_athena_query(
        requester_pays_transfer_query(start_time, end_time), ATHENA_DB, ATHENA_OUTPUT_BUCKET
    )


def create_data_access_trail_table() -> None:
    """
    Creates the Athena table over the data-access trail's log files, if it doesn't exist.

    The columns are the subset of the CloudTrail record that the query reads. The JSON SerDe
    ignores the rest. Partitions are projected: one per bucket region and delivery day.
    """
    query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{DATA_ACCESS_TRAIL_TABLE} (
    eventversion STRING,
    useridentity STRUCT<type: STRING, principalid: STRING, arn: STRING, accountid: STRING>,
    eventtime STRING,
    eventsource STRING,
    eventname STRING,
    awsregion STRING,
    errorcode STRING,
    errormessage STRING,
    requestparameters STRING,
    additionaleventdata STRING,
    requestid STRING,
    eventid STRING,
    eventtype STRING,
    recipientaccountid STRING
)
PARTITIONED BY (
    region STRING,
    timestamp STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{DATA_ACCESS_TRAIL_S3_PREFIX}'
TBLPROPERTIES (
 'projection.enabled'='true',
 'projection.region.type'='enum',
 'projection.region.values'='{DATA_ACCESS_TRAIL_REGIONS}',
 'projection.timestamp.type'='date',
 'projection.timestamp.format'='yyyy/MM/dd',
 'projection.timestamp.interval'='1',
 'projection.timestamp.interval.unit'='DAYS',
 'projection.timestamp.range'='{DATA_ACCESS_TRAIL_START},NOW',
 'storage.location.template'='{DATA_ACCESS_TRAIL_S3_PREFIX}${{region}}/${{timestamp}}'
);
"""

    athena = boto3.client("athena")
    run_athena_query(athena, query, ATHENA_DB, ATHENA_OUTPUT_BUCKET)
