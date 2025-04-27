import logging
import os
import time

import click
import pulsar
from eodhp_utils.pulsar.messages import (
    generate_billingevent_schema,
    generate_billingresourceconsumptionratesample_schema,
)
from eodhp_utils.runner import log_component_version, setup_logging

from accounting_s3_usage.sampler.messager import (
    S3StorageSamplerMessager,
    S3UsageSamplerMessager,
)

PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")

# TODO: Currently an issue that we need to have two seperate topics due to the fact we have two different
# schemas.
TOPIC_EVENTS = os.getenv("PULSAR_TOPIC", "billing-events")
TOPIC_STORAGE = os.getenv("PULSAR_TOPIC_STORAGE", "billing-storage-events")


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity level.")
@click.option("--pulsar-url", default=PULSAR_SERVICE_URL, help="URL for Pulsar service.")
@click.option("--backfill-days", type=int, default=3, help="Days to backfill on startup.")
@click.option(
    "--interval", type=int, default=3600, help="Interval in seconds for periodic sampling."
)
@click.option("--once", is_flag=True, help="Run sampling once immediately, then exit.")
def cli(verbose: int, pulsar_url: str, backfill_days: int, interval: int, once: bool):
    setup_logging(verbosity=verbose, enable_otel_logging=True)
    log_component_version("eodhp-accounting-s3-usage")

    client = pulsar.Client(pulsar_url)

    storage_producer = client.create_producer(
        topic=TOPIC_STORAGE, schema=generate_billingresourceconsumptionratesample_schema()
    )

    usage_producer = client.create_producer(
        topic=TOPIC_EVENTS, schema=generate_billingevent_schema()
    )

    storage_messager = S3StorageSamplerMessager(producer=storage_producer)
    usage_messager = S3UsageSamplerMessager(producer=usage_producer)

    try:
        if once:
            # Run the samplers once immediately and exit
            print("Running once mode: exiting after first run.")
            logging.info("Running samplers once.")

            storage_messager.run_periodic(run_once=True)
            usage_messager.run_periodic(backfill_days=backfill_days, run_once=True)
        else:
            # Run the samplers periodically
            while True:
                storage_messager.run_periodic(run_once=False)
                usage_messager.run_periodic(backfill_days=backfill_days, run_once=False)
                logging.info(f"Sleeping {interval} seconds until next sample run.")
                time.sleep(interval)

    except KeyboardInterrupt:
        logging.info("Stopping S3 Usage Sampler.")
    finally:
        client.close()


if __name__ == "__main__":
    cli()
