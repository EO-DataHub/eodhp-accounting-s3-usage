import uuid
from datetime import datetime

import pytest
from accounting_efs.sampler.messager import EFSSamplerMessager, SampleRequestMsg
from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingResourceConsumptionRateSample


@pytest.fixture
def sampler_messager():
    return EFSSamplerMessager()


def test_no_workspaces_produces_no_messages(sampler_messager):
    actions = sampler_messager.process_msg([])
    assert list(actions) == []


def test_consume_no_workspaces_produces_no_failures(sampler_messager):
    failures = sampler_messager.consume([])

    assert not failures.any_temporary()
    assert not failures.any_permanent()


def test_workspace_sample_produces_correct_message(test_dir, sampler_messager):
    # This is a parameterized fixture so this test will run with a variety of different sizes
    # and cases.
    test_dir_path, test_dir_size = test_dir

    actions = list(sampler_messager.process_msg([SampleRequestMsg("workspace0", test_dir_path)]))

    assert len(actions) == 1
    assert isinstance(actions[0], Messager.PulsarMessageAction)
    assert isinstance(actions[0].payload, BillingResourceConsumptionRateSample)

    sample = actions[0].payload
    uuid.UUID(sample.uuid)  # Check valid UUID
    datetime.fromisoformat(sample.sample_time)  # Check valid ISO datetime
    assert sample.sku == "AWS-S3-USAGE"
    assert sample.user is None
    assert sample.workspace == "workspace0"
    assert sample.rate == test_dir_size
