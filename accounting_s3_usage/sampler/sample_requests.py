import itertools
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import boto3

from accounting_s3_usage.sampler.time_utils import align_to_interval

AWS_PREFIX = os.getenv("AWS_WORKSPACE_S3_ACCESS_POINT_PREFIX", "eodhp-dev-go3awhw0-")
AWS_BUCKET_NAME = os.getenv("AWS_WORKSPACE_BUCKET_NAME", "workspaces-eodhp-dev")

# we need a slight delay for the server log delivery
# read more here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#LogDeliveryBestEffort
LOG_DELAY_BUFFER = timedelta(hours=3)


@dataclass(eq=True, frozen=True)
class SampleStorageUseRequestMsg:
    workspace: str
    bucket_name: str
    access_point_name: str


@dataclass(eq=True, frozen=True)
class GenerateAccessBillingEventRequestMsg:
    workspace: str
    bucket_name: str
    interval_start: datetime
    interval_end: datetime


def parse_workspace_prefix(workspace_prefix: str) -> str:
    if workspace_prefix.startswith(AWS_PREFIX):
        removed_prefix = workspace_prefix[len(AWS_PREFIX) :]
        removed_s3 = removed_prefix.replace("-s3", "")
        return removed_s3
    else:
        raise ValueError(f"Invalid workspace prefix: {workspace_prefix}")


def generate_workspace_s3_access_point_list():
    """
    Generates access point information for the access points used for S3 workspace stores.
    """

    def is_workspace_store_access_point(ap):
        return bucket_name == AWS_BUCKET_NAME and access_point_name.startswith(AWS_PREFIX)

    s3control = boto3.client("s3control")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    response = s3control.list_access_points(AccountId=account_id)

    while True:
        for ap in response["AccessPointList"]:
            bucket_name = ap["Bucket"]
            access_point_name = ap["Name"]

            if is_workspace_store_access_point(ap):
                yield ap

        if response.get("NextToken"):
            response = s3control.list_access_points(
                AccountId=account_id, NextToken=response["NextToken"]
            )
        else:
            return


def generate_access_billing_requests(
    access_points: Iterable[dict], intervals: Iterable[tuple[datetime, datetime]]
) -> Iterable[GenerateAccessBillingEventRequestMsg]:
    """
    Generates access billing requests, which are requests to the Messagers to generate S3
    access (bandwidth and API calls) billing data for a particular workspace object store for
    a particular time period.

    Requests will be generated for every workspace object store detected by inspecting AWS's API.
    There will be one such request for every time period given by the `intervals` argument.
    """
    for ap, interval in itertools.product(access_points, intervals):
        logging.info(f"Found access point: {ap=} for interval: {interval=}")
        yield GenerateAccessBillingEventRequestMsg(
            workspace=parse_workspace_prefix(ap["Name"]),
            bucket_name=ap["Bucket"],
            interval_start=interval[0],
            interval_end=interval[1],
        )


def generate_sample_times(
    last_end: datetime, interval: timedelta
) -> Iterable[tuple[datetime, datetime]]:
    """
    Generates intervals to sample based on either the end of the last sampled period or a
    timestamp within the period which we should start backfilling from.
    """
    begin_at = align_to_interval(last_end, interval)
    end_at = begin_at + interval
    limit = datetime.now(timezone.utc) - LOG_DELAY_BUFFER

    while end_at < limit:
        yield ((begin_at, end_at))

        begin_at = end_at
        end_at += interval


def generate_storage_sample_requests(
    access_points: Iterable[dict],
) -> Iterable[SampleStorageUseRequestMsg]:
    """
    Generates access storage sample requests, which are requests to the Messagers to generate S3
    storage consumption samples.

    Requests will be generated for every workspace object store detected by inspecting AWS's API.
    """
    for ap in access_points:
        logging.info(f"Found access point: {ap=}")
        yield SampleStorageUseRequestMsg(
            workspace=parse_workspace_prefix(ap["Name"]),
            bucket_name=ap["Bucket"],
            access_point_name=ap["Name"],
        )


def next_collection_after(after: datetime, interval: timedelta) -> datetime:
    """
    When, after `after`, should we next attempt to collect billing data?
    """
    return align_to_interval(after, interval) + interval + LOG_DELAY_BUFFER + timedelta(seconds=1)
