"""Event emission helpers — build and publish domain events to the SSE bus.

Each helper constructs a ``ServerEvent`` with the correct event type and
JSON payload, then publishes it synchronously (safe from sync call sites
like ``publish_run``).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from weatherman.events.bus import ServerEvent
from weatherman.events.router import get_event_bus
from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)


def emit_run_published(
    *,
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
    bus = get_event_bus()
    data = json.dumps({
        "model": model,
        "run_id": str(run_id),
        "published_at": published_at.isoformat(),
        "manifest_url": f"/api/manifest/{model}/{run_id}",
    })
    event = ServerEvent(
        id=bus.next_event_id(),
        event="run.published",
        data=data,
        tenant_id="*",  # weather data is shared across tenants
    )
    delivered = bus.publish_sync(event)
    logger.info(
        "Emitted run.published event",
        extra={
            "model": model,
            "run_id": str(run_id),
            "event_id": event.id,
            "delivered_to": delivered,
        },
    )
