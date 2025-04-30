import time
from datetime import datetime, timezone


def align_to_midnight(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def wait_until(dt: datetime):
    wait_time = dt.timestamp() - time.time()
    time.sleep(wait_time)
