from dataclasses import dataclass
from typing import Optional


@dataclass
class BucketUsageSample:
    workspace: str
    bucket_name: str
    storage_gb: float
    data_transfer_gb: Optional[float] = None
    api_calls_count: Optional[int] = None
