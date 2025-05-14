from datetime import datetime, timezone
from unittest import mock

import pytest

from accounting_s3_usage.sampler.messager import S3AccessBillingEventMessager
from accounting_s3_usage.sampler.sample_requests import (
    GenerateAccessBillingEventRequestMsg,
)


@pytest.fixture
def sampler_messager():
    return S3AccessBillingEventMessager()


def test_no_logged_requests_means_messager_produces_no_messages(sampler_messager):
    actions = sampler_messager.process_msg(iter([]))
    assert list(actions) == []


def test_data_transfer_and_api_calls_correctly_calculated_from_athena_results(sampler_messager):
    with (
        mock.patch(
            "accounting_s3_usage.sampler.messager.get_access_point_data_transfer"
        ) as dt_mock,
        mock.patch("accounting_s3_usage.sampler.messager.get_access_point_api_calls") as api_mock,
    ):
        dt_mock.side_effect = [
            (
                ("3.8.0.146", "42.3"),
                ("3.8.0.147", "42.42"),  # Same region
                ("89.241.216.125", "22.3"),
                ("89.241.216.126", "22.4"),  # Internet
                ("3.5.140.0", "10.0"),  # Different region
                ("-", "14"),  # Observed in logs
                (None, "10.2"),  # Never observed in logs, but just in case
            ),
            (("3.8.0.146", "12.3"),),
        ]
        api_mock.side_effect = [314.0, 0]

        actions = sampler_messager.process_msg(
            [
                GenerateAccessBillingEventRequestMsg(
                    workspace="workspace1",
                    bucket_name="bucket1",
                    interval_start=datetime(2025, 2, 2, 12, 00, 00, tzinfo=timezone.utc),
                    interval_end=datetime(2025, 2, 2, 13, 00, 00, tzinfo=timezone.utc),
                ),
                GenerateAccessBillingEventRequestMsg(
                    workspace="workspace2",
                    bucket_name="bucket1",
                    interval_start=datetime(2025, 2, 2, 13, 00, 00, tzinfo=timezone.utc),
                    interval_end=datetime(2025, 2, 2, 14, 00, 00, tzinfo=timezone.utc),
                ),
            ]
        )

        actions = list(actions)
        assert len(actions) == 6

        distinct_uuids = {action.payload.uuid for action in actions}
        assert len(distinct_uuids) == len(actions)

        payloads = {
            f"{action.payload.workspace}-{action.payload.sku}": action.payload for action in actions
        }

        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERNET"].event_start
            == "2025-02-02T12:00:00+00:00"
        )
        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-REGION"].event_start
            == "2025-02-02T12:00:00+00:00"
        )
        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERREGION"].event_start
            == "2025-02-02T12:00:00+00:00"
        )
        assert payloads["workspace1-AWS-S3-API-CALLS"].event_start == "2025-02-02T12:00:00+00:00"
        assert (
            payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT-REGION"].event_start
            == "2025-02-02T13:00:00+00:00"
        )
        assert payloads["workspace2-AWS-S3-API-CALLS"].event_start == "2025-02-02T13:00:00+00:00"

        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERNET"].event_end
            == "2025-02-02T13:00:00+00:00"
        )
        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-REGION"].event_end
            == "2025-02-02T13:00:00+00:00"
        )
        assert (
            payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERREGION"].event_end
            == "2025-02-02T13:00:00+00:00"
        )
        assert payloads["workspace1-AWS-S3-API-CALLS"].event_end == "2025-02-02T13:00:00+00:00"
        assert (
            payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT-REGION"].event_end
            == "2025-02-02T14:00:00+00:00"
        )
        assert payloads["workspace2-AWS-S3-API-CALLS"].event_end == "2025-02-02T14:00:00+00:00"

        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERNET"].quantity == 22.3 + 22.4
        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-REGION"].quantity == 42.3 + 42.42
        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT-INTERREGION"].quantity == 10.0
        assert payloads["workspace1-AWS-S3-API-CALLS"].quantity == 314
        assert payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT-REGION"].quantity == 12.3
        assert payloads["workspace2-AWS-S3-API-CALLS"].quantity == 0


def test_dup_sample_request_result_in_msgs_with_same_uuid(sampler_messager):
    with (
        mock.patch(
            "accounting_s3_usage.sampler.messager.get_access_point_data_transfer"
        ) as dt_mock,
        mock.patch("accounting_s3_usage.sampler.messager.get_access_point_api_calls") as api_mock,
    ):
        dt_mock.side_effect = [
            (("3.8.0.146", "42.3"),),
            (("3.8.0.146", "12.3"),),
        ]
        api_mock.side_effect = [314.0, 0]

        actions = sampler_messager.process_msg(
            [
                GenerateAccessBillingEventRequestMsg(
                    workspace="workspace1",
                    bucket_name="bucket1",
                    interval_start=datetime(2025, 2, 2, 12, 00, 00),
                    interval_end=datetime(2025, 2, 2, 13, 00, 00),
                ),
            ]
            * 2
        )

        actions = list(actions)
        assert len(actions) == 4

        assert actions[0].payload.uuid != actions[1].payload.uuid
        assert actions[0].payload.uuid == actions[2].payload.uuid
        assert actions[1].payload.uuid == actions[3].payload.uuid
