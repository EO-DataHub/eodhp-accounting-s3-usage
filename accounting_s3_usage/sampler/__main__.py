import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable

import boto3
import click
from eodhp_utils.pulsar.messages import (
    generate_billingevent_schema,
    generate_billingresourceconsumptionratesample_schema,
)
from eodhp_utils.runner import get_pulsar_client, log_component_version, setup_logging

from accounting_s3_usage.sampler.messager import S3StorageSamplerMessager, S3UsageSamplerMessager

AWS_PREFIX = "eodhp-dev-go3awhw0-"
AWS_BUCKET_NAME = "workspaces-eodhp-dev"


@dataclass
class SampleRequestMsg:
    workspace: str
    bucket_name: str
    access_point_name: str


def parse_workspace_prefix(workspace_prefix: str) -> str:
    if workspace_prefix.startswith(AWS_PREFIX):
        removed_prefix = workspace_prefix[len(AWS_PREFIX) :]
        removed_s3 = removed_prefix.replace("-s3", "")
        return removed_s3
    else:
        raise ValueError(f"Invalid workspace prefix: {workspace_prefix}")


def generate_sample_requests():
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
            )


@click.command
@click.option("-v", "--verbose", count=True)
@click.option("--pulsar-url", required=True)
@click.option("--interval", type=int, default=3600)
@click.option("--once", is_flag=True)
def cli(verbose: int, interval: int, pulsar_url: str, once: bool):
    setup_logging(verbosity=verbose)
    log_component_version("eodhp-accounting-s3-usage")

    logging.info("Monitoring AWS S3 usage with interval %i seconds", interval)

    main(verbose, interval, pulsar_url, once)


def main(verbose: int, interval: int, pulsar_url: str, once: bool):
    pulsar_client = get_pulsar_client(pulsar_url=pulsar_url)

    pod_name = os.getenv("K8S_POD_NAME", "unknown")

    # Producer for storage metrics (snapshot)
    storage_producer = pulsar_client.create_producer(
        topic="billing-events-consumption-rate-samples",
        producer_name=f"s3-storage-monitor-{pod_name}",
        schema=generate_billingresourceconsumptionratesample_schema(),
    )

    # Producer for data transfer and API calls (interval events)
    usage_producer = pulsar_client.create_producer(
        topic="billing-events",
        producer_name=f"s3-usage-monitor-{pod_name}",
        schema=generate_billingevent_schema(),
    )

    # Create messagers
    storage_messager = S3StorageSamplerMessager(producer=storage_producer)
    usage_messager = S3UsageSamplerMessager(producer=usage_producer)

    while True:
        scan_start = time.time()

        logging.info("Scanning all S3 buckets")

        sample_requests = list(generate_sample_requests())

        # Process storage
        storage_failures = storage_messager.consume(sample_requests)

        # Process usage
        usage_failures = usage_messager.consume(sample_requests)

        scan_end = time.time()
        scan_time = scan_end - scan_start
        logging.info("Scan completed in %.2f seconds", scan_time)

        # Check failures for both messagers
        if storage_failures.any_permanent():
            logging.fatal("Permanent error from StorageSamplerMessager detected - exiting")
            exit(1)

        if usage_failures.any_permanent():
            logging.fatal("Permanent error from UsageSamplerMessager detected - exiting")
            exit(1)

        if once:
            break

        wait_time = interval - scan_time
        if wait_time > 0:
            time.sleep(wait_time)


if __name__ == "__main__":
    cli()
