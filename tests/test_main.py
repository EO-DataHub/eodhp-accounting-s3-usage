from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from eodhp_utils.messagers import Messager

from accounting_s3_usage.sampler.__main__ import main_loop
from accounting_s3_usage.sampler.sample_requests import generate_sample_times


@pytest.mark.parametrize(
    ("temporary_err", "permanent_err", "expected_exit_code"),
    [
        pytest.param(False, False, 0),
        pytest.param(True, False, 1),
        pytest.param(False, True, 2),
        pytest.param(True, True, 2),
    ],
)
def test_main_loop_exits_if_once_set(temporary_err: bool, permanent_err: bool, expected_exit_code: int) -> None:
    with mock.patch("accounting_s3_usage.sampler.__main__.generate_billing_events") as gen_mock:
        gen_mock.return_value = Messager.Failures(temporary=temporary_err, permanent=permanent_err)

        exit_code = main_loop(timedelta(seconds=30), interval=timedelta(days=1), once=True)

        assert exit_code == expected_exit_code


def test_permanent_error_after_two_generation_persios_results_in_correct_generations_then_exit() -> None:
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
            datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),  # Simulated startup time
            datetime(2025, 1, 2, 3, 0, 1, tzinfo=UTC),  # Simulated time at start of second event generation
            datetime(2025, 1, 3, 3, 0, 1, tzinfo=UTC),  # Simulated time at start of third event generation
            datetime(2025, 1, 3, 3, 0, 1, tzinfo=UTC),  # Simulated time after third event generation
            datetime(2025, 1, 3, 3, 10, 0, tzinfo=UTC),
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
                mock.call(datetime(2024, 12, 31, 12, 0, 0, tzinfo=UTC), timedelta(days=1)),
                # Generating from the end of the log delay at the previous successful run.
                mock.call(datetime(2025, 1, 1, 9, 0, 0, tzinfo=UTC), timedelta(days=1)),
                # Generating from the end of the log delay at the previous successful run.
                mock.call(datetime(2025, 1, 2, 0, 0, 1, tzinfo=UTC), timedelta(days=1)),
                # Retry of same generation after temporary failure.
                mock.call(datetime(2025, 1, 2, 0, 0, 1, tzinfo=UTC), timedelta(days=1)),
            ]
        )
        wait_until.assert_has_calls(
            [
                # Wait till next day after first successful run.
                mock.call(datetime(2025, 1, 2, 3, 0, 1, tzinfo=UTC)),
                # Wait till next day after second successful run.
                mock.call(datetime(2025, 1, 3, 3, 0, 1, tzinfo=UTC)),
                # Wait 10 mins to retry after temporary failure.
                mock.call(datetime(2025, 1, 3, 3, 10, 1, tzinfo=UTC)),
            ]
        )


def test_restart_inside_the_log_delay_does_not_skip_a_period() -> None:
    """
    A restart at 13:31 with 4h periods and a 3h log delay can only finish periods that ended
    by 10:31, so 08:00-12:00 waits for the next run, at 19:00:01. That run must cover it.
    """
    run_times = [
        datetime(2026, 11, 17, 13, 31, 0, tzinfo=UTC),  # Startup
        datetime(2026, 11, 17, 19, 0, 1, tzinfo=UTC),  # Second run
    ]
    covered: list[tuple[datetime, datetime]] = []

    def generate(last_generation: datetime, interval: timedelta) -> Messager.Failures:
        with mock.patch("accounting_s3_usage.sampler.sample_requests.datetime", wraps=datetime) as sample_dt:
            sample_dt.now.return_value = run_times[0]
            covered.extend(generate_sample_times(last_generation, interval))
        run_times.pop(0)
        # Stop the loop after the second run.
        return Messager.Failures(permanent=not run_times)

    with (
        mock.patch("accounting_s3_usage.sampler.__main__.datetime", wraps=datetime) as dt,
        mock.patch("accounting_s3_usage.sampler.__main__.wait_until"),
        mock.patch("accounting_s3_usage.sampler.__main__.generate_billing_events", side_effect=generate),
    ):
        dt.now.side_effect = lambda tz=None: run_times[0]
        main_loop(timedelta(hours=12), timedelta(hours=4), once=False)

    day = datetime(2026, 11, 17, tzinfo=UTC)
    four_hours = timedelta(hours=4)
    assert covered == [(day + n * four_hours, day + (n + 1) * four_hours) for n in range(4)]
