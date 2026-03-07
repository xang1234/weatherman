"""Tests for ingestion trigger adapters (polling + SQS/SNS)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from weatherman.ingest.triggers import (
    PollingTrigger,
    SQSConfig,
    SQSTrigger,
    parse_gfs_notification,
)
from weatherman.storage.paths import RunID


# ---------------------------------------------------------------------------
# parse_gfs_notification
# ---------------------------------------------------------------------------


class TestParseGfsNotification:
    """Test SNS message parsing for GFS RunID extraction."""

    def _sns_envelope(self, inner: str) -> str:
        """Wrap an inner message in an SNS envelope."""
        return json.dumps({"Message": inner, "Type": "Notification"})

    def test_plain_key_in_sns_envelope(self):
        key = "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000"
        body = self._sns_envelope(key)
        result = parse_gfs_notification(body)
        assert result == RunID("20260306T00Z")

    def test_s3_event_notification_format(self):
        s3_event = json.dumps({
            "Records": [{
                "s3": {
                    "bucket": {"name": "noaa-gfs-bdp-pds"},
                    "object": {
                        "key": "gfs.20260306/12/atmos/gfs.t12z.pgrb2.0p25.f003"
                    },
                }
            }]
        })
        body = self._sns_envelope(s3_event)
        result = parse_gfs_notification(body)
        assert result == RunID("20260306T12Z")

    def test_different_cycle_hours(self):
        for hour in ("00", "06", "12", "18"):
            key = f"gfs.20260307/{hour}/atmos/gfs.t{hour}z.pgrb2.0p25.f000"
            body = self._sns_envelope(key)
            result = parse_gfs_notification(body)
            assert result == RunID(f"20260307T{hour}Z")

    def test_non_gfs_key_returns_none(self):
        key = "hrrr.20260306/conus/hrrr.t00z.wrfsfcf00.grib2"
        body = self._sns_envelope(key)
        assert parse_gfs_notification(body) is None

    def test_invalid_json_returns_none(self):
        assert parse_gfs_notification("not json {{{") is None

    def test_empty_message_returns_none(self):
        assert parse_gfs_notification("") is None

    def test_missing_message_field(self):
        body = json.dumps({"Type": "Notification"})
        # Falls back to using body itself as key — won't match GFS pattern
        assert parse_gfs_notification(body) is None

    def test_mismatched_dir_and_file_hour_rejected(self):
        """Directory says 00Z but filename says t12z — should be rejected."""
        key = "gfs.20260306/00/atmos/gfs.t12z.pgrb2.0p25.f000"
        body = self._sns_envelope(key)
        assert parse_gfs_notification(body) is None

    def test_multiple_forecast_hours_same_cycle(self):
        """Different forecast hours from the same cycle yield the same RunID."""
        results = set()
        for fxx in (0, 3, 6, 24, 120):
            key = f"gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f{fxx:03d}"
            body = self._sns_envelope(key)
            r = parse_gfs_notification(body)
            assert r is not None
            results.add(r.value)
        assert results == {"20260306T00Z"}


# ---------------------------------------------------------------------------
# PollingTrigger
# ---------------------------------------------------------------------------


class TestPollingTrigger:
    @patch("weatherman.ingest.gfs.latest_available_cycle")
    def test_returns_new_cycle(self, mock_latest):
        mock_latest.return_value = RunID("20260306T00Z")
        trigger = PollingTrigger()
        runs = trigger.pending_runs()
        assert runs == [RunID("20260306T00Z")]

    @patch("weatherman.ingest.gfs.latest_available_cycle")
    def test_deduplicates_same_cycle(self, mock_latest):
        mock_latest.return_value = RunID("20260306T00Z")
        trigger = PollingTrigger()
        trigger.pending_runs()
        # Second call with same cycle should return empty
        assert trigger.pending_runs() == []

    @patch("weatherman.ingest.gfs.latest_available_cycle")
    def test_no_cycle_available(self, mock_latest):
        mock_latest.return_value = None
        trigger = PollingTrigger()
        assert trigger.pending_runs() == []

    @patch("weatherman.ingest.gfs.latest_available_cycle")
    def test_skips_pre_ingested(self, mock_latest):
        mock_latest.return_value = RunID("20260306T00Z")
        trigger = PollingTrigger(already_ingested={"20260306T00Z"})
        assert trigger.pending_runs() == []

    @patch("weatherman.ingest.gfs.latest_available_cycle")
    def test_does_not_mutate_caller_set(self, mock_latest):
        mock_latest.return_value = RunID("20260306T06Z")
        caller_set = {"20260306T00Z"}
        trigger = PollingTrigger(already_ingested=caller_set)
        trigger.pending_runs()
        # Trigger should have its own copy — caller's set unchanged
        assert caller_set == {"20260306T00Z"}


# ---------------------------------------------------------------------------
# SQSTrigger
# ---------------------------------------------------------------------------


class TestSQSTrigger:
    def _make_trigger(self) -> tuple[SQSTrigger, MagicMock]:
        config = SQSConfig(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/gfs-notify",
        )
        trigger = SQSTrigger(config=config)
        mock_client = MagicMock()
        trigger._sqs_client = mock_client
        return trigger, mock_client

    def _sqs_message(self, key: str, receipt: str = "r1") -> dict:
        body = json.dumps({
            "Message": key,
            "Type": "Notification",
        })
        return {
            "MessageId": "msg-1",
            "ReceiptHandle": receipt,
            "Body": body,
        }

    def test_receives_and_parses_gfs_notification(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000"
                )
            ]
        }
        client.delete_message_batch.return_value = {"Failed": []}

        runs = trigger.pending_runs()
        assert runs == [RunID("20260306T00Z")]
        client.delete_message_batch.assert_called_once()

    def test_deduplicates_multiple_notifications_same_cycle(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000",
                    receipt="r1",
                ),
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f003",
                    receipt="r2",
                ),
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f006",
                    receipt="r3",
                ),
            ]
        }
        client.delete_message_batch.return_value = {"Failed": []}

        runs = trigger.pending_runs()
        assert len(runs) == 1
        assert runs[0] == RunID("20260306T00Z")

    def test_returns_multiple_distinct_cycles(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000",
                    receipt="r1",
                ),
                self._sqs_message(
                    "gfs.20260306/06/atmos/gfs.t06z.pgrb2.0p25.f000",
                    receipt="r2",
                ),
            ]
        }
        client.delete_message_batch.return_value = {"Failed": []}

        runs = trigger.pending_runs()
        assert len(runs) == 2
        values = {r.value for r in runs}
        assert values == {"20260306T00Z", "20260306T06Z"}

    def test_empty_queue_returns_empty(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {"Messages": []}

        assert trigger.pending_runs() == []
        client.delete_message_batch.assert_not_called()

    def test_no_messages_key(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {}

        assert trigger.pending_runs() == []

    def test_non_gfs_messages_deleted(self):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "hrrr.20260306/conus/something.grib2",
                    receipt="r1",
                ),
            ]
        }
        client.delete_message_batch.return_value = {"Failed": []}

        runs = trigger.pending_runs()
        assert runs == []
        # Non-GFS message should still be deleted to prevent redelivery
        client.delete_message_batch.assert_called_once()

    def test_seen_runs_not_returned_again(self):
        trigger, client = self._make_trigger()

        # First poll
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000"
                )
            ]
        }
        client.delete_message_batch.return_value = {"Failed": []}
        trigger.pending_runs()

        # Second poll with same cycle
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f012",
                    receipt="r2",
                )
            ]
        }
        runs = trigger.pending_runs()
        assert runs == []

    def test_all_config_params_forwarded_to_sqs(self):
        config = SQSConfig(
            queue_url="https://sqs.example.com/queue",
            max_messages=5,
            wait_time_seconds=10,
            visibility_timeout=1800,
        )
        trigger = SQSTrigger(config=config)
        client = MagicMock()
        trigger._sqs_client = client
        client.receive_message.return_value = {"Messages": []}

        trigger.pending_runs()

        call_kwargs = client.receive_message.call_args[1]
        assert call_kwargs["QueueUrl"] == "https://sqs.example.com/queue"
        assert call_kwargs["MaxNumberOfMessages"] == 5
        assert call_kwargs["WaitTimeSeconds"] == 10
        assert call_kwargs["VisibilityTimeout"] == 1800

    def test_delete_batch_failure_logged(self, caplog):
        trigger, client = self._make_trigger()
        client.receive_message.return_value = {
            "Messages": [
                self._sqs_message(
                    "gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000"
                )
            ]
        }
        client.delete_message_batch.return_value = {
            "Failed": [{"Id": "0", "Code": "InternalError"}]
        }

        with caplog.at_level("WARNING"):
            trigger.pending_runs()
        assert "Failed to delete" in caplog.text
