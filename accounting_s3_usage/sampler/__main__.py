import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import click
import pulsar
from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import (
    generate_billingevent_schema,
    generate_billingresourceconsumptionratesample_schema,
)
from eodhp_utils.runner import log_component_version, setup_logging

from accounting_s3_usage.sampler.messager import (
    S3StorageSamplerMessager,
    S3UsageSamplerMessager,
)
from accounting_s3_usage.sampler.sample_requests import (
    generate_sample_requests,
    generate_sample_times,
    generate_workspace_s3_access_point_list,
    next_collection_after,
)
from accounting_s3_usage.sampler.time_utils import wait_until

PULSAR_SERVICE_URL = os.getenv("PULSAR_URL", "pulsar://localhost:6650")

# TODO: Currently an issue that we need to have two seperate topics due to the fact we have two different
# schemas.
TOPIC_EVENTS = os.getenv("PULSAR_TOPIC", "billing-events")
TOPIC_STORAGE = os.getenv("PULSAR_TOPIC_STORAGE", "billing-storage-events")

client = None
storage_messager = None
usage_messager = None


def generate_billing_events(last_generation: datetime, interval: timedelta) -> Messager.Failures:
    """This generates and sends all billing events which are new since last_generation."""
    global storage_messager
    global usage_messager
    global client

    if not storage_messager or not usage_messager:
        storage_producer = client.create_producer(
            topic=TOPIC_STORAGE, schema=generate_billingresourceconsumptionratesample_schema()
        )

        usage_producer = client.create_producer(
            topic=TOPIC_EVENTS, schema=generate_billingevent_schema()
        )

        storage_messager = S3StorageSamplerMessager(producer=storage_producer)
        usage_messager = S3UsageSamplerMessager(producer=usage_producer)

    sample_requests = list(
        generate_sample_requests(
            generate_workspace_s3_access_point_list(),
            generate_sample_times(last_generation, interval),
        )
    )

    storage_failures = storage_messager.consume(sample_requests)
    usage_failures = usage_messager.consume(sample_requests)
    return storage_failures.add(usage_failures)


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity level.")
@click.option("--pulsar-url", default=PULSAR_SERVICE_URL, help="URL for Pulsar service.")
@click.option("--backfill", type=int, default=3, help="Intervals to backfill on startup.")
@click.option(
    "--interval",
    type=str,
    default="1d",
    help="Interval for periodic sampling in the form '1d', '2h', '30m' or '30s'.",
)
@click.option("--once", is_flag=True, help="Run sampling once immediately, then exit.")
def cli(verbose: int, pulsar_url: str, backfill: int, interval: str, once: bool):
    setup_logging(verbosity=verbose, enable_otel_logging=True)
    log_component_version("eodhp-accounting-s3-usage")

    interval_num = int(interval[:-1])
    match interval[-1]:
        case "s":
            interval = timedelta(seconds=interval_num)

        case "m":
            interval = timedelta(minutes=interval_num)

        case "h":
            interval = timedelta(hours=interval_num)

        case "d":
            interval = timedelta(days=interval_num)

        case _:
            logging.fatal("Failed to parse --interval")
            sys.exit(2)

    global client
    client = pulsar.Client(pulsar_url)

    try:
        exit_code = main_loop(interval * backfill, interval, once)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logging.info("Stopping S3 Usage Sampler.")
    finally:
        client.close()


def main_loop(backfill: timedelta, interval: timedelta, once: bool):
    generation_start = datetime.now(timezone.utc)
    last_generation = generation_start - backfill

    failures = generate_billing_events(last_generation, interval)

    while True:
        if failures.any_permanent():
            return 2

        if once:
            return 1 if failures.any_temporary() else 0

        if failures.any_temporary():
            next_collection = datetime.now(timezone.utc) + timedelta(minutes=10)
        else:
            next_collection = next_collection_after(generation_start, interval)
            last_generation = generation_start

        wait_until(next_collection)
        generation_start = datetime.now(timezone.utc)
        failures = generate_billing_events(last_generation, interval)


if __name__ == "__main__":
    cli()
