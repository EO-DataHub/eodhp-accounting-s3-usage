from datetime import datetime, timezone
from unittest import mock

import pytest

from accounting_s3_usage.sampler.messager import S3StorageSamplerMessager
from accounting_s3_usage.sampler.sample_requests import SampleRequestMsg


@pytest.fixture
def sampler_messager():
    return S3StorageSamplerMessager()


def test_no_requests_means_messager_produces_no_messages(sampler_messager):
    actions = sampler_messager.process_msg(iter([]))
    assert list(actions) == []


def test_storage_sample_correctly_calculated_from_computed_storage_use(sampler_messager):
    with (
        mock.patch(
            "accounting_s3_usage.sampler.messager.get_prefix_storage_size"
        ) as storage_size_mock,
        mock.patch("accounting_s3_usage.sampler.messager.datetime", wraps=datetime) as dt,
    ):
        storage_size_mock.side_effect = [100.5, 0]
        dt.now.side_effect = [
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        ]

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
        assert len(actions) == 2

        assert actions[0].payload.uuid != actions[1].payload.uuid

        payloads = {
            f"{action.payload.workspace}-{action.payload.sku}": action.payload for action in actions
        }

        assert payloads["workspace1-AWS-S3-STORAGE"].sample_time == "2025-01-01T12:00:00+00:00"
        assert payloads["workspace1-AWS-S3-STORAGE"].rate == 100.5

        assert payloads["workspace2-AWS-S3-STORAGE"].sample_time == "2025-01-01T12:00:01+00:00"
        assert payloads["workspace2-AWS-S3-STORAGE"].rate == 0
