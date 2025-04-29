import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import boto3

AWS_PREFIX = os.getenv("AWS_PREFIX", "eodhp-dev-go3awhw0-")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "workspaces-eodhp-dev")

# we need a slight delay for the server log delivery
# read more here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#LogDeliveryBestEffort
LOG_DELAY_BUFFER = timedelta(hours=3)
# due to the delay, it is safest to sample once a day
COLLECTION_INTERVAL = timedelta(days=1)


@dataclass
class SampleRequestMsg:
    workspace: str
    bucket_name: str
    access_point_name: str
    interval_start: datetime
    interval_end: datetime


def parse_workspace_prefix(workspace_prefix: str) -> str:
    if workspace_prefix.startswith(AWS_PREFIX):
        removed_prefix = workspace_prefix[len(AWS_PREFIX) :]
        removed_s3 = removed_prefix.replace("-s3", "")
        return removed_s3
    else:
        raise ValueError(f"Invalid workspace prefix: {workspace_prefix}")


def generate_sample_requests(interval_start: datetime, interval_end: datetime):
    s3control = boto3.client("s3control")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    response = s3control.list_access_points(AccountId=account_id)

    for ap in response["AccessPointList"]:
        bucket_name = ap["Bucket"]
        access_point_name = ap["Name"]

        if bucket_name == AWS_BUCKET_NAME and access_point_name.startswith(AWS_PREFIX):
            logging.info(f"Found access point: {access_point_name} for bucket: {bucket_name}")
            yield SampleRequestMsg(
                workspace=parse_workspace_prefix(access_point_name),
                bucket_name=bucket_name,
                access_point_name=access_point_name,
                interval_start=interval_start,
                interval_end=interval_end,
            )


def generate_sample_times(last_end: datetime) -> Iterable[tuple[datetime, datetime]]:
    """
    Generates intervals to sample based on either the end of the last sampled period or a
    timestamp within the period which we should start backfilling from.
    """
    begin_at = last_end.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    end_at = begin_at + COLLECTION_INTERVAL
    limit = datetime.now(timezone.utc) - LOG_DELAY_BUFFER

    while end_at < limit:
        yield ((begin_at, end_at))

        begin_at = end_at
        end_at += COLLECTION_INTERVAL
