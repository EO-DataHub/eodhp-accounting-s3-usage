from typing import cast

import boto3
import moto
import pytest
from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample

from accounting_s3_usage.sampler.messager import S3StorageSamplerMessager
from accounting_s3_usage.sampler.sample_requests import SampleStorageUseRequestMsg

MIB = 1024 * 1024


@moto.mock_aws
def test_storage_sample_counts_only_the_workspace_directory() -> None:
    """
    S3 prefixes are plain string matches: measuring workspace `test` with Prefix="test" would also
    count `test-other/` and `testing/`. The sampler must measure `test/`.
    """
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.create_bucket(Bucket="workspaces-test", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
    s3.put_object(Bucket="workspaces-test", Key="test/a.bin", Body=b"x" * MIB)
    s3.put_object(Bucket="workspaces-test", Key="test-other/b.bin", Body=b"x" * (100 * MIB))
    s3.put_object(Bucket="workspaces-test", Key="testing/c.bin", Body=b"x" * (10 * MIB))

    actions = list(
        S3StorageSamplerMessager().process_msg(
            iter([SampleStorageUseRequestMsg(workspace="test", bucket_name="workspaces-test", access_point_name="ap")])
        )
    )

    assert len(actions) == 1
    assert isinstance(actions[0], Messager.PulsarMessageAction)
    sample = cast(BillingResourceConsumptionRateSample, actions[0].payload)
    assert sample.workspace == "test"
    # The sampler rounds to 6 decimal GiB, so compare approximately: 1 MiB, not the 111 MiB of test*.
    assert float(sample.rate) * 1024 == pytest.approx(1, abs=0.01)
