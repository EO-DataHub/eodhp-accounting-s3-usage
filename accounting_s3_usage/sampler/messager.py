import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Never

import pulsar
from eodhp_utils.aws.egress_classifier import AWSIPClassifier, EgressClass
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
from .sample_requests import (
    GenerateAccessBillingEventRequestMsg,
    SampleStorageUseRequestMsg,
)

tracer = trace.get_tracer("s3-usage-sampler")


class S3StorageSamplerMessager(Messager[Iterator[SampleStorageUseRequestMsg], BillingResourceConsumptionRateSample]):
    """
    This generates resource consumption rate samples (storage space consumption samples) for
    workspace object stores.

    Historical samples cannot be generated - we can only sample usage right now.
    """

    def generate_storage_sample(
        self, workspace: str, storage_gb: float, sample_time: datetime
    ) -> Messager.PulsarMessageAction:
        sample_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{workspace}-AWS-S3-STORAGE-{sample_time.isoformat()}")
        sample = BillingResourceConsumptionRateSample(
            uuid=str(sample_uuid),
            sample_time=sample_time.isoformat(),
            sku="AWS-S3-STORAGE",
            user=None,
            workspace=workspace,
            rate=round(storage_gb, 6),
        )
        return Messager.PulsarMessageAction(payload=sample)

    def process_msg(self, msg: Iterator[SampleStorageUseRequestMsg]) -> Iterable[Messager.Action]:
        for request in msg:
            token = attach(baggage.set_baggage("workspace", request.workspace))

            try:
                workspace = request.workspace
                bucket_name = request.bucket_name
                sample_time = datetime.now(UTC)
                storage_gb = get_prefix_storage_size(bucket_name, workspace)

                print(f"======= {workspace} =======")
                print(f"Sampled at: {sample_time.isoformat()}")
                print(f"Storage Size: {storage_gb:.6f} GB")
                print("============================\n")

                yield self.generate_storage_sample(workspace, storage_gb, sample_time)
            finally:
                detach(token)

    def gen_empty_catalogue_message(self, msg: Iterator[SampleStorageUseRequestMsg]) -> Never:
        raise NotImplementedError()


class S3AccessBillingEventMessager(Messager[Iterator[GenerateAccessBillingEventRequestMsg], BillingEvent]):
    """
    This generates BillingEvents for the cost of API calls and data transfer from workspace
    object stores.

    This can generate events for any specified period in the past.
    """

    def __init__(self, producer: pulsar.Producer | None = None) -> None:
        super().__init__(producer=producer)

        self._aws_ip_classifier = AWSIPClassifier()

    def generate_billing_event(
        self, request: GenerateAccessBillingEventRequestMsg, sku: str, quantity: float
    ) -> Messager.PulsarMessageAction:
        event_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{request.workspace}-{sku}-{request.interval_start.isoformat()}")
        event = BillingEvent(
            uuid=str(event_uuid),
            event_start=request.interval_start.isoformat(),
            event_end=request.interval_end.isoformat(),
            sku=sku,
            user=None,
            workspace=request.workspace,
            quantity=round(quantity, 6),
        )
        return Messager.PulsarMessageAction(payload=event)

    def process_msg(self, msg: Iterator[GenerateAccessBillingEventRequestMsg]) -> Iterable[Messager.Action]:
        for request in msg:
            token = attach(baggage.set_baggage("workspace", request.workspace))
            try:
                sku_quantities: defaultdict[str, float] = defaultdict(lambda: 0)

                data_transfer_by_destination = get_access_point_data_transfer(
                    request.workspace, request.interval_start, request.interval_end
                )

                for destination, transferred in data_transfer_by_destination:
                    if destination is None or destination == "-":
                        # "-" is used as the remote IP when CloudFront accesses S3. We charge
                        # for data transfer from CloudFront separately so it's important we
                        # ignore these. It's not obvious in what other circumstances it might be
                        # "-"
                        #
                        # None has not been observed and is here to be defensive.
                        continue

                    print(f"{destination=}, {transferred=}")
                    egress_type = self._aws_ip_classifier.classify(destination)
                    sku = {
                        EgressClass.REGION: "AWS-S3-DATA-TRANSFER-OUT-REGION",
                        EgressClass.INTERREGION: "AWS-S3-DATA-TRANSFER-OUT-INTERREGION",
                        EgressClass.INTERNET: "AWS-S3-DATA-TRANSFER-OUT-INTERNET",
                    }[egress_type]

                    assert transferred is not None
                    sku_quantities[sku] += float(transferred)

                sku_quantities["AWS-S3-API-CALLS"] = get_access_point_api_calls(
                    request.workspace, request.interval_start, request.interval_end
                )

                print(f"======= {request.workspace} =======")
                print(f"Time Interval: {request.interval_start} to {request.interval_end}")
                print(f"{sku_quantities}")
                print("============================\n")

                for sku, quantity in sku_quantities.items():
                    yield self.generate_billing_event(request, sku, quantity)
            finally:
                detach(token)

    def gen_empty_catalogue_message(self, msg: Iterator[GenerateAccessBillingEventRequestMsg]) -> Never:
        raise NotImplementedError()
