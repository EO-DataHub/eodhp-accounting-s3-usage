import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar.messages import (
    BillingEvent,
    BillingResourceConsumptionRateSample,
)
from opentelemetry import baggage, trace
from opentelemetry.context import attach, detach

from .metrics import (
    get_access_point_api_calls,
    get_access_point_data_transfer,
    get_prefix_storage_size,
)
from .sample_requests import generate_sample_requests
from .time_utils import align_to_midnight

tracer = trace.get_tracer("s3-usage-sampler")

LOG_DELAY_BUFFER = timedelta(hours=3)
COLLECTION_INTERVAL = timedelta(days=1)


class S3StorageSamplerMessager(
    PulsarJSONMessager[BillingResourceConsumptionRateSample, BillingResourceConsumptionRateSample]
):
    def send_storage_sample(self, workspace, storage_gb, sample_time):
        sample_uuid = uuid.uuid5(
            uuid.NAMESPACE_DNS, f"{workspace}-AWS-S3-STORAGE-{sample_time.isoformat()}"
        )
        sample = BillingResourceConsumptionRateSample(
            uuid=str(sample_uuid),
            sample_time=sample_time.isoformat() + "Z",
            sku="AWS-S3-STORAGE",
            user=None,
            workspace=workspace,
            rate=round(storage_gb, 6),
        )
        return Messager.PulsarMessageAction(payload=sample)

    def run_periodic(self, run_once=False):
        def sample_storage(sample_time):
            requests = list(generate_sample_requests())

            for request_msg in requests:
                workspace = request_msg.workspace
                bucket_name = request_msg.bucket_name
                storage_gb = get_prefix_storage_size(bucket_name, workspace)

                print(f"======= {workspace} =======")
                print(f"Sampled at: {sample_time.isoformat()}")
                print(f"Storage Size: {storage_gb:.6f} GB")
                print("============================\n")

                if storage_gb is not None:
                    action = self.send_storage_sample(workspace, storage_gb, sample_time)
                    with tracer.start_as_current_span(
                        "send_storage_event",
                        attributes={"workspace": workspace, "sku": "AWS-S3-STORAGE"},
                    ):
                        token = attach(baggage.set_baggage("workspace", workspace))
                        try:
                            self._runaction(
                                action, Messager.CatalogueChanges(), Messager.Failures()
                            )
                        finally:
                            detach(token)

        if run_once:
            interval_end = datetime.now(timezone.utc) - LOG_DELAY_BUFFER
            logging.info(f"Running immediate storage sampling at {interval_end.isoformat()}")
            sample_storage(interval_end)
        else:
            next_run_time = align_to_midnight(datetime.now(timezone.utc))
            logging.info(f"Next run time: {next_run_time}")

            while True:
                current_time = datetime.now(timezone.utc) - LOG_DELAY_BUFFER
                interval_end = next_run_time + COLLECTION_INTERVAL

                if interval_end > current_time:
                    sleep_duration = (interval_end - current_time).total_seconds()
                    time.sleep(max(sleep_duration, 60))
                    continue

                logging.info(f"Collecting S3 storage usage at {interval_end}")
                sample_storage(interval_end)
                next_run_time = interval_end

    def process_payload(self, _: BillingEvent):
        return []


class S3UsageSamplerMessager(PulsarJSONMessager[BillingEvent, BillingEvent]):
    def send_billing_event(self, workspace, sku, quantity, start, end):
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{workspace}-{sku}-{start.isoformat()}")
        event = BillingEvent(
            uuid=str(event_uuid),
            event_start=start.isoformat() + "Z",
            event_end=end.isoformat() + "Z",
            sku=sku,
            user=None,
            workspace=workspace,
            quantity=round(quantity, 6),
        )
        return Messager.PulsarMessageAction(payload=event)

    def run_periodic(self, backfill_days=3, run_once=False):
        def sample_usage(interval_start, interval_end):
            logging.info(f"Collecting S3 usage from {interval_start} to {interval_end}")
            requests = list(generate_sample_requests())

            for request_msg in requests:
                workspace = request_msg.workspace

                data_transfer_gb = get_access_point_data_transfer(
                    workspace, interval_start, interval_end
                )
                api_calls = get_access_point_api_calls(workspace, interval_start, interval_end)

                print(f"======= {workspace} =======")
                print(f"Time Interval: {interval_start} to {interval_end}")
                print(f"Data Transfer: {data_transfer_gb:.6f} GB")
                print(f"API Calls: {api_calls}")
                print("============================\n")

                for sku, quantity in [
                    ("AWS-S3-DATA-TRANSFER-OUT", data_transfer_gb),
                    ("AWS-S3-API-CALLS", api_calls),
                ]:
                    action = self.send_billing_event(
                        workspace, sku, quantity, interval_start, interval_end
                    )

                    with tracer.start_as_current_span(
                        "send_billing_event",
                        attributes={"workspace": workspace, "sku": sku},
                    ):
                        token = attach(baggage.set_baggage("workspace", workspace))
                        try:
                            self._runaction(
                                action, Messager.CatalogueChanges(), Messager.Failures()
                            )
                        finally:
                            detach(token)

        if run_once:
            interval_end = datetime.now(timezone.utc) - LOG_DELAY_BUFFER
            interval_start = interval_end - COLLECTION_INTERVAL
            sample_usage(interval_start, interval_end)
            return  # Exit immediately after running once

        # Regular periodic sampling
        next_run_time = (
            align_to_midnight(datetime.now(timezone.utc)) - COLLECTION_INTERVAL * backfill_days
        )

        while True:
            current_time = datetime.now(timezone.utc) - LOG_DELAY_BUFFER
            interval_end = next_run_time + COLLECTION_INTERVAL

            if interval_end > current_time:
                sleep_duration = (interval_end - current_time).total_seconds()
                logging.info(f"Sleeping for {max(sleep_duration, 60)} seconds")
                time.sleep(max(sleep_duration, 60))
                continue

            sample_usage(next_run_time, interval_end)
            next_run_time = interval_end

    def process_payload(self, payload: BillingEvent):
        return []
