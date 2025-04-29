from datetime import datetime, timezone
from unittest import mock

from accounting_s3_usage.sampler.sample_requests import generate_sample_times


def test_sample_time_generation_backfills_from_correct_point():
    times = next(generate_sample_times(datetime(2024, 1, 2, 11, 43, 00, 00)))
    assert times[0] == datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc)
    assert times[1] == datetime(2024, 1, 3, 00, 00, 00, 00, tzinfo=timezone.utc)


def test_sample_time_generation_produces_consecutive_periods():
    times = list(generate_sample_times(datetime(2024, 1, 2, 00, 00, 00, 00)))

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
            generate_sample_times(datetime(2024, 1, 2, 00, 00, 00, 00, tzinfo=timezone.utc))
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
