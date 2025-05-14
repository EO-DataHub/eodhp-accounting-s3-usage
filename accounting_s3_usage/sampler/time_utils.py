import time
from datetime import datetime, timedelta, timezone


def align_to_interval(dt: datetime, interval: timedelta) -> datetime:
    REFERENCE_TIME = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    since_reference_time = dt.astimezone(timezone.utc) - REFERENCE_TIME
    intervals = since_reference_time / interval
    return REFERENCE_TIME + int(intervals) * interval


def wait_until(dt: datetime):
    wait_time = dt.timestamp() - time.time()
    time.sleep(wait_time)
