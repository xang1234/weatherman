"""FastAPI SSE endpoint — GET /events/stream.

Streams Server-Sent Events to connected clients with:
- Per-tenant filtering via tenant_id from auth context
- Reconnection support via Last-Event-ID header
- Periodic keepalive comments to detect dead connections
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from fastapi import APIRouter, Depends, Header, Request
from starlette.responses import StreamingResponse

from weatherman.events.bus import EventBus, ServerEvent
from weatherman.tenancy import get_tenant_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])

# Keepalive interval in seconds — SSE comment to prevent proxy/LB timeouts.
_KEEPALIVE_INTERVAL = 15

# Module-level singleton, initialised from app lifespan.
_bus: EventBus | None = None


def init_event_bus(replay_limit: int = 1000) -> EventBus:
    """Create and store the global EventBus singleton. Call from app lifespan startup."""
    global _bus
    _bus = EventBus(replay_limit=replay_limit)
    logger.info("EventBus initialised", extra={"replay_limit": replay_limit})
    return _bus


def get_event_bus() -> EventBus:
    """Return the global EventBus. Raises if not initialised."""
    if _bus is None:
        raise RuntimeError("EventBus not initialised — call init_event_bus() first")
    return _bus


def shutdown_event_bus() -> None:
    """Tear down the EventBus. Call from app lifespan shutdown."""
    global _bus
    if _bus is not None:
        count = _bus.subscriber_count
        if count:
            logger.info(
                "Shutting down EventBus with active subscribers",
                extra={"subscriber_count": count},
            )
        _bus = None


def _format_sse(event: ServerEvent) -> str:
    """Format a ServerEvent as an SSE text block per the spec."""
    lines = [
        f"id: {event.id}",
        f"event: {event.event}",
    ]
    for data_line in event.data.split("\n"):
        lines.append(f"data: {data_line}")
    lines.append("")  # trailing blank line terminates the event
    lines.append("")
    return "\n".join(lines)


async def _sse_generator(
    request: Request,
    tenant_id: str,
    last_event_id: str | None,
) -> AsyncIterator[str]:
    """Async generator that yields SSE-formatted text.

    Handles keepalive, client disconnect detection, and reconnection replay.
    """
    bus = get_event_bus()
    async with bus.subscribe(tenant_id, last_event_id) as queue:
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(
                    queue.get(), timeout=_KEEPALIVE_INTERVAL
                )
                yield _format_sse(event)
            except TimeoutError:
                # Send SSE comment as keepalive
                yield ": keepalive\n\n"


@router.get(
    "/stream",
    summary="SSE event stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Server-Sent Events stream",
            "content": {"text/event-stream": {}},
        },
    },
)
async def stream_events(
    request: Request,
    tenant_id: str = Depends(get_tenant_id),
    last_event_id: str | None = Header(None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """Stream real-time events to the client via SSE.

    Events are filtered by the caller's tenant_id (extracted from auth context).
    Supports reconnection: if the client sends a ``Last-Event-ID`` header,
    any buffered events after that ID are replayed before live streaming resumes.

    Event types (added by downstream beads):
    - ``run.published`` — a new weather model run is available
    - ``ais.refreshed`` — AIS data has been updated
    """
    logger.info(
        "SSE client connected",
        extra={"tenant_id": tenant_id, "last_event_id": last_event_id},
    )
    return StreamingResponse(
        _sse_generator(request, tenant_id, last_event_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )
