from collections.abc import Iterator
from datetime import UTC, datetime
from typing import cast
from unittest import mock

import pytest
from eodhp_utils.messagers import Messager
from eodhp_utils.pulsar.messages import BillingEvent

from accounting_s3_usage.sampler import metrics
from accounting_s3_usage.sampler.messager import (
    REQUESTER_PAYS_SKU,
    RequesterPaysBillingEventMessager,
)
from accounting_s3_usage.sampler.sample_requests import (
    GenerateRequesterPaysBillingEventRequestMsg,
    generate_requester_pays_billing_requests,
)

SIGNER_ROLE = "data-objects-signer-eodhp-test-GSTjKHpo"
TRAIL_PREFIX = "s3://eodhp-312280911266-data-access-trail/AWSLogs/312280911266/CloudTrail/"

START = datetime(2026, 11, 16, 20, 0, 0, tzinfo=UTC)
END = datetime(2026, 11, 17, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def trail_settings() -> Iterator[None]:
    with (
        mock.patch.object(metrics, "DATA_ACCESS_TRAIL_TABLE", "data_access_trail_test"),
        mock.patch.object(metrics, "DATA_ACCESS_TRAIL_S3_PREFIX", TRAIL_PREFIX),
        mock.patch.object(metrics, "DATA_ACCESS_TRAIL_REGIONS", "us-west-2"),
        mock.patch.object(metrics, "DATA_OBJECTS_SIGNER_ROLE_NAME", SIGNER_ROLE),
    ):
        yield


def test_one_request_per_interval_not_per_workspace() -> None:
    intervals = [(START, END), (END, END.replace(hour=4))]

    requests = list(generate_requester_pays_billing_requests(intervals))

    assert requests == [
        GenerateRequesterPaysBillingEventRequestMsg(interval_start=START, interval_end=END),
        GenerateRequesterPaysBillingEventRequestMsg(interval_start=END, interval_end=END.replace(hour=4)),
    ]


@pytest.mark.usefixtures("trail_settings")
def test_query_counts_only_successful_reads_signed_by_this_clusters_signer_role() -> None:
    query = metrics.requester_pays_transfer_query(START, END)

    # Exact role match, not LIKE: staging and prod share an account, and the thumbnail role
    # (project-paid previews) must not be charged to anyone.
    assert f"element_at(split(useridentity.arn, '/'), 2) = '{SIGNER_ROLE}'" in query
    assert "errorcode IS NULL" in query
    assert "eventname = 'GetObject'" in query
    assert "'$.bytesTransferredOut'" in query
    assert "workspace <> '_readiness-check'" in query
    assert "GROUP BY workspace" in query


@pytest.mark.usefixtures("trail_settings")
def test_query_period_is_half_open_and_partitions_allow_late_delivery() -> None:
    query = metrics.requester_pays_transfer_query(START, END)

    # Half open, so a read on a boundary is charged in exactly one period.
    assert "from_iso8601_timestamp(eventtime) >= from_iso8601_timestamp('2026-11-16T20:00:00+00:00')" in query
    assert "from_iso8601_timestamp(eventtime) < from_iso8601_timestamp('2026-11-17T00:00:00+00:00')" in query
    # The trail files events by delivery day, which can be the day after the read.
    assert "timestamp BETWEEN '2026/11/16' AND '2026/11/18'" in query


@pytest.mark.usefixtures("trail_settings")
def test_valid_settings_pass() -> None:
    metrics.check_requester_pays_settings()


@pytest.mark.usefixtures("trail_settings")
@pytest.mark.parametrize(
    ("setting", "value"),
    [
        pytest.param("DATA_OBJECTS_SIGNER_ROLE_NAME", "", id="no-role"),
        pytest.param("DATA_OBJECTS_SIGNER_ROLE_NAME", "x' OR '1'='1", id="quote-in-role"),
        pytest.param("DATA_ACCESS_TRAIL_REGIONS", "us-west-2'", id="quote-in-region"),
        pytest.param("DATA_ACCESS_TRAIL_S3_PREFIX", "s3://bucket/AWSLogs/", id="prefix-without-account"),
        pytest.param("DATA_ACCESS_TRAIL_TABLE", "trail; DROP", id="bad-table"),
    ],
)
def test_unusable_settings_are_refused(setting: str, value: str) -> None:
    with mock.patch.object(metrics, setting, value), pytest.raises(ValueError, match=setting):
        metrics.check_requester_pays_settings()


def test_enabled_only_when_trail_table_set() -> None:
    with mock.patch.object(metrics, "DATA_ACCESS_TRAIL_TABLE", ""):
        assert not metrics.requester_pays_billing_enabled()
    with mock.patch.object(metrics, "DATA_ACCESS_TRAIL_TABLE", "data_access_trail_test"):
        assert metrics.requester_pays_billing_enabled()


def _events(
    rows_per_request: list[list[tuple[str, str]]], requests: list[tuple[datetime, datetime]]
) -> list[BillingEvent]:
    with mock.patch("accounting_s3_usage.sampler.messager.get_requester_pays_transfer") as transfer:
        transfer.side_effect = rows_per_request
        actions = RequesterPaysBillingEventMessager().process_msg(
            iter(GenerateRequesterPaysBillingEventRequestMsg(interval_start=s, interval_end=e) for s, e in requests)
        )
        results = list(actions)
    assert all(isinstance(a, Messager.PulsarMessageAction) for a in results)
    return [cast(BillingEvent, cast(Messager.PulsarMessageAction, a).payload) for a in results]


def test_one_event_per_workspace_with_gigabytes_from_athena() -> None:
    events = _events([[("sg2", "1.5"), ("jdonovan", "0.000123")]], [(START, END)])

    by_workspace = {str(e.workspace): e for e in events}
    assert set(by_workspace) == {"sg2", "jdonovan"}
    for e in events:
        assert e.sku == REQUESTER_PAYS_SKU
        assert e.user is None
        assert e.event_start == "2026-11-16T20:00:00+00:00"
        assert e.event_end == "2026-11-17T00:00:00+00:00"
    assert by_workspace["sg2"].quantity == 1.5
    assert by_workspace["jdonovan"].quantity == 0.000123
    assert len({e.uuid for e in events}) == 2


def test_no_reads_means_no_events() -> None:
    assert _events([[]], [(START, END)]) == []


def test_same_period_again_gives_same_uuids_so_accounting_ignores_the_repeat() -> None:
    first = _events([[("sg2", "1.5")]], [(START, END)])
    again = _events([[("sg2", "1.5")]], [(START, END)])
    later = _events([[("sg2", "1.5")]], [(END, END.replace(hour=4))])

    assert first[0].uuid == again[0].uuid
    assert first[0].uuid != later[0].uuid
