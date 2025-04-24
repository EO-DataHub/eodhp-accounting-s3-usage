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
            access_point = request_msg.access_point_name

            print(f"\n============= Processing workspace: {workspace} =============")

            storage_gb = get_prefix_storage_size(bucket_name, workspace)
            data_transfer_gb = get_access_point_data_transfer(bucket_name, access_point)
            api_calls = get_access_point_api_calls(bucket_name, access_point)

            print(f"Storage (GB): {storage_gb}")
            print(f"Data Transfer (GB): {data_transfer_gb}")
            print(f"API Calls: {api_calls}")
            print("\n")

            if storage_gb is None:
                logging.warning(f"No metrics found for bucket {bucket_name}")
                yield Messager.FailureAction(permanent=False)
                continue

            sample_msg = BillingResourceConsumptionRateSample(
                uuid=str(uuid.uuid4()),
                sample_time=datetime.utcnow().isoformat() + "Z",
                sku="AWS-S3-USAGE",
                user=None,
                workspace=workspace,
                rate=storage_gb,
                metadata={
                    "bucket_name": bucket_name,
                    "access_point": access_point,
                    "data_transfer_gb": data_transfer_gb,
                    "api_calls_count": api_calls,
                    "note": "Bandwidth and API call metrics currently bucket-level aggregated.",
                },
            )

            yield Messager.PulsarMessageAction(payload=sample_msg)

    def gen_empty_catalogue_message(self, msg: Iterable[str]) -> dict:
        return {}
