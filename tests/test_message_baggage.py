"""
Messager sends each message while process_msg is paused at its `yield`, with the baggage that is
current then. The accounting ingester adds that baggage to its spans' attributes. These tests
record the baggage at each yield, which is the baggage each message would carry.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
from unittest import mock

from eodhp_utils.messagers import Messager
from opentelemetry import baggage

from accounting_s3_usage.sampler.messager import (
    REQUESTER_PAYS_SKU,
    STORAGE_SKU,
    RequesterPaysBillingEventMessager,
    S3AccessBillingEventMessager,
    S3StorageSamplerMessager,
    message_baggage,
)
from accounting_s3_usage.sampler.sample_requests import (
    GenerateAccessBillingEventRequestMsg,
    GenerateRequesterPaysBillingEventRequestMsg,
    SampleStorageUseRequestMsg,
)

START = datetime(2026, 11, 17, 12, 0, 0, tzinfo=UTC)
END = datetime(2026, 11, 17, 16, 0, 0, tzinfo=UTC)


def baggage_at_each_message(actions: Iterable[Messager.Action]) -> list[dict[str, object]]:
    sent = [dict(baggage.get_all()) for _ in actions]
    # Nothing is left behind for whatever runs next.
    assert dict(baggage.get_all()) == {}
    return sent


def test_message_baggage_sets_values_inside_the_block_only() -> None:
    with message_baggage(workspace="sg2"):
        with message_baggage(sku="X"):
            assert dict(baggage.get_all()) == {"workspace": "sg2", "sku": "X"}
        assert dict(baggage.get_all()) == {"workspace": "sg2"}
    assert dict(baggage.get_all()) == {}


def test_requester_pays_events_carry_workspace_and_sku() -> None:
    with mock.patch("accounting_s3_usage.sampler.messager.get_requester_pays_transfer") as transfer:
        transfer.return_value = [("ormsbydaniel", "0.1687"), ("sg2", "0.0845")]
        actions = RequesterPaysBillingEventMessager().process_msg(
            iter([GenerateRequesterPaysBillingEventRequestMsg(interval_start=START, interval_end=END)])
        )
        sent = baggage_at_each_message(actions)

    assert sent == [
        {"workspace": "ormsbydaniel", "sku": REQUESTER_PAYS_SKU},
        {"workspace": "sg2", "sku": REQUESTER_PAYS_SKU},
    ]


def test_access_events_carry_workspace_and_their_own_sku() -> None:
    with (
        mock.patch("accounting_s3_usage.sampler.messager.get_access_point_data_transfer") as transfer,
        mock.patch("accounting_s3_usage.sampler.messager.get_access_point_api_calls") as api_calls,
    ):
        transfer.return_value = [("89.241.216.125", "1.5")]  # An internet address.
        api_calls.return_value = 7.0
        actions = S3AccessBillingEventMessager().process_msg(
            iter(
                [
                    GenerateAccessBillingEventRequestMsg(
                        workspace="sg2", bucket_name="bucket", interval_start=START, interval_end=END
                    )
                ]
            )
        )
        sent = baggage_at_each_message(actions)

    assert sent == [
        {"workspace": "sg2", "sku": "AWS-S3-DATA-TRANSFER-OUT-INTERNET"},
        {"workspace": "sg2", "sku": "AWS-S3-API-CALLS"},
    ]


def test_storage_samples_carry_workspace_and_sku() -> None:
    with mock.patch("accounting_s3_usage.sampler.messager.get_prefix_storage_size", return_value=2.5):
        actions = S3StorageSamplerMessager().process_msg(
            iter([SampleStorageUseRequestMsg(workspace="sg2", bucket_name="bucket", access_point_name="ap")])
        )
        sent = baggage_at_each_message(actions)

    assert sent == [{"workspace": "sg2", "sku": STORAGE_SKU}]
