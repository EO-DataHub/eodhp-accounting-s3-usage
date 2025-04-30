import uuid
from datetime import datetime, timezone
from typing import Iterable, Iterator

from eodhp_utils.messagers import Messager
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
from .sample_requests import SampleRequestMsg

tracer = trace.get_tracer("s3-usage-sampler")


class S3StorageSamplerMessager(
    Messager[Iterator[SampleRequestMsg], BillingResourceConsumptionRateSample]
):
    def generate_storage_sample(self, workspace, storage_gb, sample_time):
        sample_uuid = uuid.uuid5(
            uuid.NAMESPACE_DNS, f"{workspace}-AWS-S3-STORAGE-{sample_time.isoformat()}"
        )
        sample = BillingResourceConsumptionRateSample(
            uuid=str(sample_uuid),
            sample_time=sample_time.isoformat(),
            sku="AWS-S3-STORAGE",
            user=None,
            workspace=workspace,
            rate=round(storage_gb, 6),
        )
        return Messager.PulsarMessageAction(payload=sample)

    def process_msg(self, msgs: Iterator[SampleRequestMsg]) -> Iterable[Messager.Action]:
        for msg in msgs:
            token = attach(baggage.set_baggage("workspace", msg.workspace))

            try:
                workspace = msg.workspace
                bucket_name = msg.bucket_name
                sample_time = datetime.now(timezone.utc)
                storage_gb = get_prefix_storage_size(bucket_name, workspace)

                print(f"======= {workspace} =======")
                print(f"Sampled at: {sample_time.isoformat()}")
                print(f"Storage Size: {storage_gb:.6f} GB")
                print("============================\n")

                yield self.generate_storage_sample(workspace, storage_gb, sample_time)
            finally:
                detach(token)

    def gen_empty_catalogue_message(self, msg):
        raise NotImplementedError()


class S3UsageSamplerMessager(Messager[Iterator[SampleRequestMsg], BillingEvent]):
    def generate_billing_event(self, request: SampleRequestMsg, sku: str, quantity: float):
        event_uuid = uuid.uuid5(
            uuid.NAMESPACE_DNS, f"{request.workspace}-{sku}-{request.interval_start.isoformat()}"
        )
        event = BillingEvent(
            uuid=str(event_uuid),
            event_start=request.interval_start.isoformat() + "Z",
            event_end=request.interval_end.isoformat() + "Z",
            sku=sku,
            user=None,
            workspace=request.workspace,
            quantity=round(quantity, 6),
        )
        return Messager.PulsarMessageAction(payload=event)

    def process_msg(self, msgs: Iterator[SampleRequestMsg]) -> Iterable[Messager.Action]:
        for msg in msgs:
            token = attach(baggage.set_baggage("workspace", msg.workspace))
            try:
                data_transfer_gb = get_access_point_data_transfer(
                    msg.workspace, msg.interval_start, msg.interval_end
                )
                api_calls = get_access_point_api_calls(
                    msg.workspace, msg.interval_start, msg.interval_end
                )

                print(f"======= {msg.workspace} =======")
                print(f"Time Interval: {msg.interval_start} to {msg.interval_end}")
                print(f"Data Transfer: {data_transfer_gb:.6f} GB")
                print(f"API Calls: {api_calls}")
                print("============================\n")

                for sku, quantity in [
                    ("AWS-S3-DATA-TRANSFER-OUT", data_transfer_gb),
                    ("AWS-S3-API-CALLS", api_calls),
                ]:
                    yield self.generate_billing_event(msg, sku, quantity)
            finally:
                detach(token)

    def gen_empty_catalogue_message(self, msg):
        raise NotImplementedError()
