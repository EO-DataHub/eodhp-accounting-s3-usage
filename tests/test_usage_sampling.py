from datetime import datetime
from unittest import mock

import pytest

from accounting_s3_usage.sampler.messager import S3UsageSamplerMessager
from accounting_s3_usage.sampler.sample_requests import SampleRequestMsg


@pytest.fixture
def sampler_messager():
    return S3UsageSamplerMessager()


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
        dt_mock.side_effect = [42.3, 12.3]
        api_mock.side_effect = [314.0, 0]

        actions = sampler_messager.process_msg(
            [
                SampleRequestMsg(
                    workspace="workspace1",
                    bucket_name="bucket1",
                    access_point_name="ap1",
                    interval_start=datetime(2025, 2, 2, 12, 00, 00),
                    interval_end=datetime(2025, 2, 2, 13, 00, 00),
                ),
                SampleRequestMsg(
                    workspace="workspace2",
                    bucket_name="bucket1",
                    access_point_name="ap2",
                    interval_start=datetime(2025, 2, 2, 13, 00, 00),
                    interval_end=datetime(2025, 2, 2, 14, 00, 00),
                ),
            ]
        )

        actions = list(actions)
        assert len(actions) == 4

        assert actions[0].payload.uuid != actions[1].payload.uuid
        assert actions[0].payload.uuid != actions[2].payload.uuid
        assert actions[0].payload.uuid != actions[3].payload.uuid
        assert actions[1].payload.uuid != actions[2].payload.uuid
        assert actions[1].payload.uuid != actions[3].payload.uuid

        payloads = {
            f"{action.payload.workspace}-{action.payload.sku}": action.payload for action in actions
        }

        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT"].event_start == "2025-02-02T12:00:00Z"
        assert payloads["workspace1-AWS-S3-API-CALLS"].event_start == "2025-02-02T12:00:00Z"
        assert payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT"].event_start == "2025-02-02T13:00:00Z"
        assert payloads["workspace2-AWS-S3-API-CALLS"].event_start == "2025-02-02T13:00:00Z"

        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT"].event_end == "2025-02-02T13:00:00Z"
        assert payloads["workspace1-AWS-S3-API-CALLS"].event_end == "2025-02-02T13:00:00Z"
        assert payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT"].event_end == "2025-02-02T14:00:00Z"
        assert payloads["workspace2-AWS-S3-API-CALLS"].event_end == "2025-02-02T14:00:00Z"

        assert payloads["workspace1-AWS-S3-DATA-TRANSFER-OUT"].quantity == 42.3
        assert payloads["workspace1-AWS-S3-API-CALLS"].quantity == 314
        assert payloads["workspace2-AWS-S3-DATA-TRANSFER-OUT"].quantity == 12.3
        assert payloads["workspace2-AWS-S3-API-CALLS"].quantity == 0


def test_dup_sample_request_result_in_msgs_with_same_uuid(sampler_messager):
    with (
        mock.patch(
            "accounting_s3_usage.sampler.messager.get_access_point_data_transfer"
        ) as dt_mock,
        mock.patch("accounting_s3_usage.sampler.messager.get_access_point_api_calls") as api_mock,
    ):
        dt_mock.side_effect = [42.3, 12.3]
        api_mock.side_effect = [314.0, 0]

        actions = sampler_messager.process_msg(
            [
                SampleRequestMsg(
                    workspace="workspace1",
                    bucket_name="bucket1",
                    access_point_name="ap1",
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
