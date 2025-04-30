from datetime import datetime, timedelta, timezone
from unittest import mock

import botocore.client
import moto

from accounting_s3_usage.sampler.sample_requests import (
    SampleRequestMsg,
    generate_sample_requests,
    generate_sample_times,
    generate_workspace_s3_access_point_list,
)

orig_moto = botocore.client.BaseClient._make_api_call


def mock_make_api_call(self, operation_name, kwarg):
    if operation_name == "ListAccessPoints":
        if kwarg.get("NextToken"):
            # Page 2
            return {
                "AccessPointList": [
                    {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace4-s3"},
                ],
                "NextToken": None,
            }
        else:
            # Page 1
            return {
                "AccessPointList": [
                    {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace1-s3"},
                    {"Bucket": "other-bucket", "Name": "aws-prefix-not-a-workspace-s3"},
                    {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace3-s3"},
                ],
                "NextToken": "next-token",
            }

    return orig_moto(self, operation_name, kwarg)


@mock.patch("accounting_s3_usage.sampler.sample_requests.AWS_BUCKET_NAME", "ws-bucket")
@mock.patch("accounting_s3_usage.sampler.sample_requests.AWS_PREFIX", "aws-prefix-")
@moto.mock_aws
def test_access_point_list_generation_returns_expected_aps():
    with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        aplist = list(generate_workspace_s3_access_point_list())

        assert aplist == [
            {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace1-s3"},
            {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace3-s3"},
            {"Bucket": "ws-bucket", "Name": "aws-prefix-workspace4-s3"},
        ]


def test_sample_time_generation_backfills_from_correct_point():
    times = next(generate_sample_times(datetime(2024, 1, 2, 11, 43, 00, 00), timedelta(days=1)))
    assert times[0] == datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc)
    assert times[1] == datetime(2024, 1, 3, 00, 00, 00, 00, tzinfo=timezone.utc)


def test_sample_time_generation_produces_consecutive_periods():
    times = list(generate_sample_times(datetime(2024, 1, 2, 00, 00, 00, 00), timedelta(days=1)))

    assert times[0][0] == datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc)
    assert times[0][1] == datetime(2024, 1, 3, 00, 00, 00, 00, tzinfo=timezone.utc)

    assert times[1][0] == times[0][1]
    assert times[2][0] == times[1][1]
    assert times[3][0] == times[2][1]


def test_sample_time_generation_stops_at_buffer_time():
    with (
        mock.patch(
            "accounting_s3_usage.sampler.sample_requests.datetime", wraps=datetime
        ) as dt_mock,
    ):
        dt_mock.now.return_value = datetime(2024, 1, 5, 2, 00, 00, 00, tzinfo=timezone.utc)
        times = list(
            generate_sample_times(
                datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc), timedelta(days=1)
            )
        )

        assert times == [
            (
                datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc),
                datetime(2024, 1, 3, 00, 00, 00, 00, tzinfo=timezone.utc),
            ),
            (
                datetime(2024, 1, 3, 00, 00, 00, 00, tzinfo=timezone.utc),
                datetime(2024, 1, 4, 00, 00, 00, 00, tzinfo=timezone.utc),
            ),
        ]


def test_sample_request_generation_produces_no_requests_for_empty_time_interval():
    requests = list(
        generate_sample_requests([{"Bucket": "ws-bucket", "Name": "aws-prefix-workspace1-s3"}], [])
    )
    assert not requests


def test_sample_request_generation_produces_no_requests_for_empty_ap_list():
    requests = list(
        generate_sample_requests(
            [],
            [
                datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            ],
        )
    )
    assert not requests


@mock.patch("accounting_s3_usage.sampler.sample_requests.AWS_BUCKET_NAME", "ws-bucket")
@mock.patch("accounting_s3_usage.sampler.sample_requests.AWS_PREFIX", "aws-prefix-")
@moto.mock_aws
def test_sample_request_generation_produces_correct_requests_for_time_intervals():
    with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        intervals = [
            [
                datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            ],
            [
                datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
            ],
        ]

        requests = set(
            generate_sample_requests(generate_workspace_s3_access_point_list(), intervals)
        )

        assert requests == {
            SampleRequestMsg(
                workspace=workspace,
                bucket_name="ws-bucket",
                access_point_name=f"aws-prefix-{workspace}-s3",
                interval_start=interval[0],
                interval_end=interval[1],
            )
            for workspace in ["workspace1", "workspace3", "workspace4"]
            for interval in intervals
        }
