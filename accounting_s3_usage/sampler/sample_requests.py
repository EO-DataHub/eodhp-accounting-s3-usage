import logging
from dataclasses import dataclass

import boto3

AWS_PREFIX = "eodhp-dev-go3awhw0-"
AWS_BUCKET_NAME = "workspaces-eodhp-dev"


@dataclass
class SampleRequestMsg:
    workspace: str
    bucket_name: str
    access_point_name: str


def parse_workspace_prefix(workspace_prefix: str) -> str:
    if workspace_prefix.startswith(AWS_PREFIX):
        removed_prefix = workspace_prefix[len(AWS_PREFIX) :]
        removed_s3 = removed_prefix.replace("-s3", "")
        return removed_s3
    else:
        raise ValueError(f"Invalid workspace prefix: {workspace_prefix}")


def generate_sample_requests():
    s3control = boto3.client("s3control")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    response = s3control.list_access_points(AccountId=account_id)

    for ap in response["AccessPointList"]:
        bucket_name = ap["Bucket"]
        access_point_name = ap["Name"]

        if bucket_name == AWS_BUCKET_NAME and access_point_name.startswith(AWS_PREFIX):
            logging.info(f"Found access point: {access_point_name} for bucket: {bucket_name}")
            yield SampleRequestMsg(
                workspace=parse_workspace_prefix(access_point_name),
                bucket_name=bucket_name,
                access_point_name=access_point_name,
            )
