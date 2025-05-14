from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from eodhp_utils.messagers import Messager

from accounting_s3_usage.sampler.__main__ import main_loop


@pytest.mark.parametrize(
    "temporary_err, permanent_err, exit_code",
    [
        pytest.param(False, False, 0),
        pytest.param(True, False, 1),
        pytest.param(False, True, 2),
        pytest.param(True, True, 2),
    ],
)
def test_main_loop_exits_if_once_set(temporary_err, permanent_err, exit_code):
    with mock.patch("accounting_s3_usage.sampler.__main__.generate_billing_events") as gen_mock:
        gen_mock.return_value = Messager.Failures(temporary=temporary_err, permanent=permanent_err)

        exit_code = main_loop(timedelta(seconds=30), interval=timedelta(days=1), once=True)

        assert exit_code == exit_code


def test_permanent_error_after_two_generation_persios_results_in_correct_generations_then_exit():
    with (
        mock.patch("accounting_s3_usage.sampler.__main__.datetime", wraps=datetime) as dt,
        mock.patch("accounting_s3_usage.sampler.__main__.wait_until") as wait_until,
        mock.patch("accounting_s3_usage.sampler.__main__.generate_billing_events") as gen_events,
    ):
        gen_events.side_effect = [
            Messager.Failures(),
            Messager.Failures(),
            Messager.Failures(temporary=True),
            Messager.Failures(permanent=True),
        ]
        dt.now.side_effect = [
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),  # Simulated startup time
            datetime(
                2025, 1, 2, 3, 0, 1, tzinfo=timezone.utc
            ),  # Simulated time at start of second event generation
            datetime(
                2025, 1, 3, 3, 0, 1, tzinfo=timezone.utc
            ),  # Simulated time at start of third event generation
            datetime(
                2025, 1, 3, 3, 0, 1, tzinfo=timezone.utc
            ),  # Simulated time after third event generation
            datetime(2025, 1, 3, 3, 10, 0, tzinfo=timezone.utc),
        ]  # Simulated time at start of fourth event generation

        # Sequence of events simulated:
        #   - Startup at 2025-1-1 12:00:00. Generation for backfill from 1 day before.
        #   - Wait until 2025-1-2 03:00:01. Generation happens up to this day.
        #   - Wait until 2025-1-3 03:00:01. Generation happens up to this day, fails with temp err
        #   - Wait until 2025-1-3 03:10:00. Retry happens, fails with perm error.

        exit_code = main_loop(timedelta(days=1), timedelta(days=1), False)

        assert exit_code == 2
        gen_events.assert_has_calls(
            [
                # Backfilling from the day after the start time
                mock.call(datetime(2024, 12, 31, 12, 0, 0, tzinfo=timezone.utc), timedelta(days=1)),
                # Generating from time of previous successful run.
                mock.call(datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc), timedelta(days=1)),
                # Generating from time of previous successful run.
                mock.call(datetime(2025, 1, 2, 3, 0, 1, tzinfo=timezone.utc), timedelta(days=1)),
                # Retry of same generation after temporary failure.
                mock.call(datetime(2025, 1, 2, 3, 0, 1, tzinfo=timezone.utc), timedelta(days=1)),
            ]
        )
        wait_until.assert_has_calls(
            [
                # Wait till next day after first successful run.
                mock.call(datetime(2025, 1, 2, 3, 0, 1, tzinfo=timezone.utc)),
                # Wait till next day after second successful run.
                mock.call(datetime(2025, 1, 3, 3, 0, 1, tzinfo=timezone.utc)),
                # Wait 10 mins to retry after temporary failure.
                mock.call(datetime(2025, 1, 3, 3, 10, 1, tzinfo=timezone.utc)),
            ]
        )
