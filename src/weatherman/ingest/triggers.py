"""Trigger adapters for GFS ingestion pipeline.

Two trigger sources that share the same interface:

  PollingTrigger  — periodically checks if new GFS data is available
  SQSTrigger      — reacts to NOAA Big Data Program SNS notifications
                    delivered via an SQS queue (event-driven, lower latency)

Both yield RunID values that the downstream pipeline consumes identically.

Usage:
    trigger = SQSTrigger(queue_url="https://sqs.us-east-1.amazonaws.com/...")
    for run_id in trigger.pending_runs():
        download_gfs_cycle(run_id, staging_dir)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)

# NOAA GFS S3 key pattern:
# noaa-gfs-bdp-pds/gfs.20260306/00/atmos/gfs.t00z.pgrb2.0p25.f000
_GFS_KEY_RE = re.compile(
    r"(?:^|/)gfs\.(\d{8})/(\d{2})/atmos/gfs\.t\2z\.pgrb2"
)


class TriggerSource(Protocol):
    """Protocol for ingestion trigger sources.

    Implementations yield RunIDs that are ready for ingestion.
    """

    def pending_runs(self) -> list[RunID]:
        """Return run IDs that should be ingested now."""
        ...


# ---------------------------------------------------------------------------
# Polling trigger (wraps existing check)
# ---------------------------------------------------------------------------


class PollingTrigger:
    """Polls for the latest available GFS cycle using Herbie.

    This is the baseline trigger — checks data availability by probing
    the GRIB2 index file. Typical latency: up to 15 minutes after data
    appears on NOAA servers.
    """

    def __init__(self, *, already_ingested: set[str] | None = None) -> None:
        self._ingested: set[str] = set(already_ingested) if already_ingested else set()

    def pending_runs(self) -> list[RunID]:
        from weatherman.ingest.gfs import latest_available_cycle

        run_id = latest_available_cycle()
        if run_id is None:
            return []
        if run_id.value in self._ingested:
            logger.debug("Cycle %s already ingested, skipping", run_id)
            return []
        self._ingested.add(run_id.value)
        return [run_id]


# ---------------------------------------------------------------------------
# SQS trigger (event-driven via NOAA SNS)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SQSConfig:
    """Configuration for the SQS-based trigger."""

    queue_url: str
    region: str = "us-east-1"
    max_messages: int = 10
    wait_time_seconds: int = 20  # long-poll
    visibility_timeout: int = 900  # 15 min — must exceed max ingestion time


def parse_gfs_notification(message_body: str) -> RunID | None:
    """Extract a GFS RunID from an SNS notification delivered via SQS.

    SNS wraps the original notification in an envelope. The inner message
    contains the S3 key of the newly-available file. We extract the date
    and cycle hour from the key path.

    Returns None if the message doesn't match the expected GFS pattern.
    """
    try:
        outer = json.loads(message_body)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Could not parse SQS message body as JSON")
        return None

    # SNS wraps the actual notification — the "Message" field contains
    # either a JSON string or a plain-text S3 key notification.
    inner_raw = outer.get("Message", message_body)

    # Try parsing as JSON (some SNS topics use structured messages)
    key = _extract_key(inner_raw)
    if key is None:
        return None

    match = _GFS_KEY_RE.search(key)
    if not match:
        logger.debug("S3 key does not match GFS pattern: %s", key)
        return None

    date_str, cycle_hour_str = match.group(1), match.group(2)
    run_id_str = f"{date_str}T{cycle_hour_str}Z"

    try:
        return RunID(run_id_str)
    except ValueError:
        logger.warning("Invalid run ID from notification: %s", run_id_str)
        return None


def _extract_key(raw: str) -> str | None:
    """Pull the S3 key out of a notification payload.

    Handles two formats:
    1. JSON with Records[].s3.object.key (S3 event notification)
    2. Plain text containing the key directly
    """
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        # Not JSON — treat the raw string as the key itself
        return raw

    # S3 event notification format
    if isinstance(parsed, dict):
        records = parsed.get("Records", [])
        if records and isinstance(records, list):
            s3_info = records[0].get("s3", {})
            key = s3_info.get("object", {}).get("key")
            if key:
                return key

    # Valid JSON but not a recognized structure — not a key
    return None


@dataclass
class SQSTrigger:
    """Event-driven trigger via AWS SQS subscribed to NOAA GFS SNS topic.

    Receives notifications when new GFS data is published to the NOAA
    Big Data Program S3 bucket. Eliminates polling delay — reacts to
    data the moment it's available.

    Deduplicates by RunID so that multiple file-level notifications for
    the same cycle result in a single ingestion trigger.
    """

    config: SQSConfig
    _seen: set[str] = field(default_factory=set, init=False)
    _sqs_client: Any = field(default=None, init=False, repr=False)

    def _get_client(self) -> Any:
        if self._sqs_client is None:
            import boto3

            self._sqs_client = boto3.client(
                "sqs", region_name=self.config.region
            )
        return self._sqs_client

    def pending_runs(self) -> list[RunID]:
        """Poll SQS for new GFS data notifications.

        Receives up to max_messages, parses each for a GFS RunID,
        deduplicates, and deletes processed messages. Returns unique
        RunIDs not previously seen.
        """
        client = self._get_client()

        resp = client.receive_message(
            QueueUrl=self.config.queue_url,
            MaxNumberOfMessages=self.config.max_messages,
            WaitTimeSeconds=self.config.wait_time_seconds,
            VisibilityTimeout=self.config.visibility_timeout,
        )

        messages = resp.get("Messages", [])
        if not messages:
            return []

        new_runs: dict[str, RunID] = {}
        receipts_to_delete: list[str] = []

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            run_id = parse_gfs_notification(msg.get("Body", ""))

            if run_id is None:
                # Unrecognized message — delete to prevent redelivery
                logger.info(
                    "Deleting non-GFS message %s",
                    msg.get("MessageId", "?"),
                )
                receipts_to_delete.append(receipt)
                continue

            receipts_to_delete.append(receipt)

            if run_id.value in self._seen:
                logger.debug("Duplicate notification for %s, skipping", run_id)
                continue

            self._seen.add(run_id.value)
            new_runs[run_id.value] = run_id

        # Batch delete processed messages
        if receipts_to_delete:
            self._delete_messages(client, receipts_to_delete)

        if new_runs:
            logger.info(
                "SQS trigger: %d new run(s): %s",
                len(new_runs),
                ", ".join(new_runs.keys()),
            )

        return list(new_runs.values())

    def _delete_messages(
        self, client: Any, receipts: list[str]
    ) -> None:
        """Delete processed messages from SQS in batches of 10."""
        # SQS delete_message_batch accepts max 10 entries
        for i in range(0, len(receipts), 10):
            batch = receipts[i : i + 10]
            entries = [
                {"Id": str(idx), "ReceiptHandle": r}
                for idx, r in enumerate(batch)
            ]
            resp = client.delete_message_batch(
                QueueUrl=self.config.queue_url,
                Entries=entries,
            )
            failed = resp.get("Failed", [])
            if failed:
                logger.warning(
                    "Failed to delete %d SQS messages: %s",
                    len(failed),
                    failed,
                )
