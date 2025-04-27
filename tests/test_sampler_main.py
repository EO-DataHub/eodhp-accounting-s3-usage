# TODO: These tests are for the EFS accounting messager, we need to create tests for this instead.

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from accounting_efs.sampler.__main__ import main
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample


def test_main_scans_dirs(block_size):
    with (
        mock.patch("accounting_efs.sampler.__main__.get_pulsar_client") as mock_getclient,
        TemporaryDirectory() as tmpdir_str,
    ):
        ############## Setup
        tmpdir = Path(tmpdir_str)

        (tmpdir / "workspace0").mkdir()
        w1 = tmpdir / "workspace1"
        w1.mkdir()

        with open(w1 / "500-byte-file", "wb") as fh:
            fh.write(b"1234" * 125)

        ############## Invoke sample
        main(tmpdir_str, verbose=2, once=True)

        ############## Check results
        calls = (
            mock_getclient()
            .create_producer(
                topic="billing-events-consumption-rate-samples", producer_name=any, schema=any
            )
            .send.call_args_list
        )

        be0: BillingResourceConsumptionRateSample = calls[0].args[0]
        be1: BillingResourceConsumptionRateSample = calls[1].args[0]

        if be0.workspace == "workspace1":
            tmp = be0
            be0 = be1
            be1 = tmp

        assert be0.sku == "AWS-S3-USAGE"
        assert be0.workspace == "workspace0"
        assert be0.rate == block_size / (1024.0**3)

        assert be1.sku == "AWS-S3-USAGE"
        assert be1.workspace == "workspace1"
        assert be1.rate == block_size * 2 / (1024.0**3)
