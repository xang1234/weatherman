"""In-memory event bus with per-tenant pub/sub and bounded replay buffer.

The EventBus is the core of the SSE push channel. Publishers (e.g. the
ingest pipeline) call ``publish()`` to broadcast events. Each SSE client
subscribes via ``subscribe()`` which returns an async iterator of events
filtered by tenant_id.

A bounded replay buffer allows reconnecting clients (via Last-Event-ID)
to catch up on missed events without requiring external storage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import AsyncIterator

logger = logging.getLogger(__name__)

# Maximum events kept in replay buffer per bus instance.
_DEFAULT_REPLAY_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class ServerEvent:
    """A single SSE event ready for broadcast.

    Attributes:
        id: Monotonically increasing event ID (string for SSE spec).
        event: Event type name (e.g. "run.published", "ais.refreshed").
        data: JSON-encoded payload string.
        tenant_id: Tenant scope. Use "*" for broadcast to all tenants.
    """

    id: str
    event: str
    data: str
    tenant_id: str = "*"


class EventBus:
    """In-memory pub/sub event bus with tenant filtering and replay.

    Thread-safety: all mutations happen on the asyncio event loop, so no
    locks are needed. The bus is intended for single-process use; for
    multi-process deployments, plug in Redis pub/sub upstream.
    """

    def __init__(self, replay_limit: int = _DEFAULT_REPLAY_LIMIT) -> None:
        self._subscribers: dict[int, _Subscription] = {}
        self._next_sub_id = 0
        self._counter = 0
        self._replay_buffer: deque[ServerEvent] = deque(maxlen=replay_limit)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def next_event_id(self) -> str:
        """Generate the next monotonic event ID."""
        self._counter += 1
        return str(self._counter)

    async def publish(self, event: ServerEvent) -> int:
        """Broadcast an event to all matching subscribers.

        Returns the number of subscribers that received the event.
        """
        self._replay_buffer.append(event)
        delivered = 0
        for sub in list(self._subscribers.values()):
            if sub.accepts(event):
                try:
                    sub.queue.put_nowait(event)
                    delivered += 1
                except asyncio.QueueFull:
                    logger.warning(
                        "SSE subscriber queue full, dropping event",
                        extra={"sub_id": sub.id, "event_id": event.id},
                    )
        return delivered

    def subscribe(
        self,
        tenant_id: str,
        last_event_id: str | None = None,
        queue_size: int = 256,
    ) -> _SubscriptionContext:
        """Create a subscription filtered by tenant_id.

        If last_event_id is provided, replays buffered events after that ID.
        Returns an async context manager that yields an async iterator of events.
        """
        sub_id = self._next_sub_id
        self._next_sub_id += 1
        sub = _Subscription(
            id=sub_id,
            tenant_id=tenant_id,
            queue=asyncio.Queue(maxsize=queue_size),
        )
        return _SubscriptionContext(self, sub, last_event_id)

    def _register(self, sub: _Subscription, last_event_id: str | None) -> None:
        """Register a subscription and replay missed events."""
        self._subscribers[sub.id] = sub
        if last_event_id is not None:
            self._replay(sub, last_event_id)
        logger.debug(
            "SSE subscriber registered",
            extra={"sub_id": sub.id, "tenant_id": sub.tenant_id},
        )

    def _unregister(self, sub_id: int) -> None:
        self._subscribers.pop(sub_id, None)
        logger.debug("SSE subscriber unregistered", extra={"sub_id": sub_id})

    def _replay(self, sub: _Subscription, after_id: str) -> None:
        """Replay buffered events with ID > after_id into the subscription queue."""
        try:
            cutoff = int(after_id)
        except (ValueError, TypeError):
            return
        replayed = 0
        for event in self._replay_buffer:
            try:
                if int(event.id) > cutoff and sub.accepts(event):
                    sub.queue.put_nowait(event)
                    replayed += 1
            except asyncio.QueueFull:
                logger.warning(
                    "Replay queue full, truncating replay",
                    extra={"sub_id": sub.id, "replayed": replayed},
                )
                break
        if replayed:
            logger.debug(
                "Replayed events for reconnecting client",
                extra={"sub_id": sub.id, "after_id": after_id, "replayed": replayed},
            )


@dataclass
class _Subscription:
    id: int
    tenant_id: str
    queue: asyncio.Queue[ServerEvent]

    def accepts(self, event: ServerEvent) -> bool:
        """Check if this subscription should receive the event."""
        return event.tenant_id == "*" or event.tenant_id == self.tenant_id


class _SubscriptionContext:
    """Async context manager for a subscription lifetime."""

    def __init__(
        self,
        bus: EventBus,
        sub: _Subscription,
        last_event_id: str | None,
    ) -> None:
        self._bus = bus
        self._sub = sub
        self._last_event_id = last_event_id

    async def __aenter__(self) -> AsyncIterator[ServerEvent]:
        self._bus._register(self._sub, self._last_event_id)
        return self._iter_events()

    async def __aexit__(self, *exc: object) -> None:
        self._bus._unregister(self._sub.id)

    async def _iter_events(self) -> AsyncIterator[ServerEvent]:
        while True:
            event = await self._sub.queue.get()
            yield event
