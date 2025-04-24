from datetime import datetime, timedelta

import boto3


def get_prefix_storage_size(bucket_name, prefix):
    print(
        f"Calculating storage size for bucket: {bucket_name}, prefix: {prefix}"
    )  # Calculating storage size for bucket: workspaces-eodhp-dev, prefix: billing-test
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_size_bytes = 0
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        if "Contents" in page:
            total_size_bytes += sum(obj["Size"] for obj in page["Contents"])

    print(f"Total size in bytes: {total_size_bytes}")
    size_gb = total_size_bytes / (1024**3)
    return size_gb


def get_access_point_data_transfer(bucket_name, access_point_name):
    return 0


def get_access_point_api_calls(bucket_name, access_point_name):
    return 0
