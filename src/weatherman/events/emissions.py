"""Event emission helpers — build and publish domain events to the SSE bus.

Each helper constructs a ``ServerEvent`` with the correct event type and
JSON payload, then publishes it synchronously (safe from sync call sites
like ``publish_run``).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

from weatherman.events.bus import ServerEvent
from weatherman.events.router import (
    get_event_bus_optional,
    get_event_journal_optional,
    init_event_journal,
)
from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)

__all__ = ["emit_run_published", "emit_ais_refreshed"]


def _default_journal_path() -> Path | None:
    explicit = os.environ.get("WEATHERMAN_EVENT_JOURNAL_PATH")
    if explicit:
        return Path(explicit)
    data_dir = os.environ.get("WEATHERMAN_DATA_DIR")
    if data_dir:
        return Path(data_dir) / "events" / "sse-events.jsonl"
    ais_db_path = os.environ.get("AIS_DB_PATH")
    if ais_db_path:
        return Path(ais_db_path).resolve().parent / "events" / "sse-events.jsonl"
    return None


def _ensure_event_journal():
    journal = get_event_journal_optional()
    if journal is not None:
        return journal
    path = _default_journal_path()
    if path is None:
        return None
    return init_event_journal(path)


def _next_event_id() -> str:
    journal = _ensure_event_journal()
    if journal is not None:
        return journal.next_event_id()
    bus = get_event_bus_optional()
    if bus is not None:
        return bus.next_event_id()
    raise RuntimeError("No event sink initialised — start the app or configure an event journal")


def _publish_event(event: ServerEvent) -> int:
    journal = _ensure_event_journal()
    if journal is not None:
        journal.append(event)
    bus = get_event_bus_optional()
    if bus is None:
        if journal is None:
            raise RuntimeError("No event sink initialised — start the app or configure an event journal")
        return 0
    return bus.publish_sync(event)


def emit_run_published(
    model: str,
    run_id: RunID,
    published_at: datetime,
) -> None:
    """Emit a ``run.published`` SSE event.

    Called after atomic publish completes. The event is broadcast to all
    tenants (``tenant_id="*"``) because weather data is shared.

    Payload:
        model: Model name (e.g. "gfs").
        run_id: Run identifier (e.g. "20260308T00Z").
        published_at: ISO 8601 timestamp of publish.
        manifest_url: Relative URL to the UI manifest.
    """
    data = json.dumps({
        "model": model,
        "run_id": str(run_id),
        "published_at": published_at.isoformat(),
        "manifest_url": f"/api/manifest/{model}/{run_id}",
    })
    event = ServerEvent(
        id=_next_event_id(),
        event="run.published",
        data=data,
        tenant_id="*",  # weather data is shared across tenants
    )
    delivered = _publish_event(event)
    logger.info(
        "Emitted run.published event",
        extra={
            "model": model,
            "run_id": str(run_id),
            "event_id": event.id,
            "delivered_to": delivered,
        },
    )


def emit_ais_refreshed(
    ais_date: date,
    tile_url_template: str,
) -> None:
    """Emit an ``ais.refreshed`` SSE event.

    Called after a new AIS day is loaded into DuckDB and MVT tiles are
    generated. The event is broadcast to all tenants (``tenant_id="*"``)
    because AIS data is shared.

    Payload:
        ais_date: ISO 8601 date of the refreshed AIS data (e.g. "2026-03-08").
        tile_url_template: URL template for accessing the generated MVT tiles.
    """
    data = json.dumps({
        "ais_date": ais_date.isoformat(),
        "tile_url_template": tile_url_template,
    })
    event = ServerEvent(
        id=_next_event_id(),
        event="ais.refreshed",
        data=data,
        tenant_id="*",  # AIS data is shared across tenants
    )
    delivered = _publish_event(event)
    logger.info(
        "Emitted ais.refreshed event",
        extra={
            "ais_date": ais_date.isoformat(),
            "event_id": event.id,
            "delivered_to": delivered,
        },
    )
