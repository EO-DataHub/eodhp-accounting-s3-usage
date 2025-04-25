import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable

from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingEvent, BillingResourceConsumptionRateSample

from .metrics import (
    get_access_point_api_calls,
    get_access_point_data_transfer,
    get_prefix_storage_size,
)

# S3 logs can be delayed... set a safe buffer (e.g. 3 hours)
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#LogDeliveryBestEffort
LOG_DELAY_BUFFER = timedelta(hours=3)


class S3StorageSamplerMessager(Messager[Iterable[str], BillingResourceConsumptionRateSample]):
    def process_msg(self, msg: Iterable[str]) -> Iterable[Messager.Action]:
        sample_time = datetime.now(timezone.utc) - timedelta(hours=3)

        for request_msg in msg:
            bucket_name = request_msg.bucket_name
            workspace = request_msg.workspace

            storage_gb = get_prefix_storage_size(bucket_name, workspace)

            if storage_gb is None:
                logging.warning(f"No storage metrics found for bucket {bucket_name}")
                yield Messager.FailureAction(permanent=False)
                continue

            print(f"Storage size for bucket {bucket_name}: {storage_gb} GB")

            storage_msg = BillingResourceConsumptionRateSample(
                uuid=str(uuid.uuid4()),
                sample_time=sample_time.isoformat(),
                sku="AWS-S3-STORAGE",
                user=None,
                workspace=workspace,
                rate=storage_gb,
            )

            yield Messager.PulsarMessageAction(payload=storage_msg)

    def gen_empty_catalogue_message(self, msg: Iterable[str]) -> dict:
        return {}


class S3UsageSamplerMessager(Messager[Iterable[str], BillingEvent]):
    def process_msg(self, msg: Iterable[str]) -> Iterable[Messager.Action]:
        event_end = datetime.now(timezone.utc) - timedelta(hours=3)
        event_start = event_end - timedelta(days=1)

        for request_msg in msg:
            workspace = request_msg.workspace

            data_transfer_gb = get_access_point_data_transfer(workspace, event_start, event_end)
            api_calls = get_access_point_api_calls(workspace, event_start, event_end)

            # Data Transfer event
            data_transfer_event = BillingEvent(
                uuid=str(uuid.uuid4()),
                event_start=event_start.isoformat(),
                event_end=event_end.isoformat(),
                sku="AWS-S3-DATA-TRANSFER-OUT",
                user=None,
                workspace=workspace,
                quantity=data_transfer_gb,
            )
            yield Messager.PulsarMessageAction(payload=data_transfer_event)

            # API Calls event
            api_calls_event = BillingEvent(
                uuid=str(uuid.uuid4()),
                event_start=event_start.isoformat(),
                event_end=event_end.isoformat(),
                sku="AWS-S3-API-CALLS",
                user=None,
                workspace=workspace,
                quantity=api_calls,
            )

            print(f"API calls for workspace {workspace}: {api_calls}")
            print(f"Data transfer for workspace {workspace}: {data_transfer_gb} GB")

            yield Messager.PulsarMessageAction(payload=api_calls_event)

    def gen_empty_catalogue_message(self, msg: Iterable[str]) -> dict:
        return {}
