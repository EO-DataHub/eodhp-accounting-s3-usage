from dataclasses import dataclass


@dataclass
class BucketUsageSample:
    workspace: str
    bucket_name: str
    storage_gb: float
    data_transfer_gb: float | None = None
    api_calls_count: int | None = None
