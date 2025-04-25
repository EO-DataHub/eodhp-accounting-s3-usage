import logging
import uuid
from datetime import datetime
from typing import Iterable

from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample

from .metrics import (
    get_access_point_api_calls,
    get_access_point_data_transfer,
    get_prefix_storage_size,
)


class S3SamplerMessager(Messager[Iterable[str], BillingResourceConsumptionRateSample]):
    def process_msg(self, msg: Iterable[str]) -> Iterable[Messager.Action]:
        for request_msg in msg:
            bucket_name = request_msg.bucket_name
            workspace = request_msg.workspace

            print(f"\n============= Processing workspace: {workspace} =============")

            storage_gb = get_prefix_storage_size(bucket_name, workspace)
            data_transfer_gb = get_access_point_data_transfer(workspace)
            api_calls = get_access_point_api_calls(workspace)

            print(f"Storage (GB): {storage_gb}")
            print(f"Data Transfer (GB): {data_transfer_gb}")
            print(f"API Calls: {api_calls}\n")

            if storage_gb is None:
                logging.warning(f"No storage metrics found for bucket {bucket_name}")
                yield Messager.FailureAction(permanent=False)
                continue

            current_time_iso = datetime.utcnow().isoformat() + "Z"

            # Storage usage message
            storage_msg = BillingResourceConsumptionRateSample(
                uuid=str(uuid.uuid4()),
                sample_time=current_time_iso,
                sku="AWS-S3-STORAGE",
                user=None,
                workspace=workspace,
                rate=storage_gb,
            )
            yield Messager.PulsarMessageAction(payload=storage_msg)

            # Data transfer usage message
            data_transfer_msg = BillingResourceConsumptionRateSample(
                uuid=str(uuid.uuid4()),
                sample_time=current_time_iso,
                sku="AWS-S3-DATA-TRANSFER-OUT",
                user=None,
                workspace=workspace,
                rate=data_transfer_gb,
            )
            yield Messager.PulsarMessageAction(payload=data_transfer_msg)

            # API calls usage message
            api_calls_msg = BillingResourceConsumptionRateSample(
                uuid=str(uuid.uuid4()),
                sample_time=current_time_iso,
                sku="AWS-S3-API-CALLS",
                user=None,
                workspace=workspace,
                rate=api_calls,
            )
            yield Messager.PulsarMessageAction(payload=api_calls_msg)

    def gen_empty_catalogue_message(self, msg: Iterable[str]) -> dict:
        return {}
