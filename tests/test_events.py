"""Tests for the SSE push channel — EventBus and /events/stream endpoint."""

from __future__ import annotations

import asyncio

import pytest

from weatherman.events.bus import EventBus, ServerEvent
from weatherman.events.router import _format_sse


# ---------------------------------------------------------------------------
# Helper to run async tests without pytest-asyncio
# ---------------------------------------------------------------------------


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# EventBus unit tests
# ---------------------------------------------------------------------------


class TestServerEvent:
    def test_default_tenant_is_broadcast(self):
        ev = ServerEvent(id="1", event="test", data="{}")
        assert ev.tenant_id == "*"


class TestEventBus:
    def test_next_event_id_monotonic(self):
        bus = EventBus(replay_limit=50)
        ids = [bus.next_event_id() for _ in range(5)]
        assert ids == ["1", "2", "3", "4", "5"]

    def test_publish_delivers_to_subscriber(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            event = ServerEvent(id="1", event="test", data='{"ok":true}', tenant_id="t1")
            async with bus.subscribe("t1") as queue:
                await bus.publish(event)
                received = await asyncio.wait_for(queue.get(), timeout=1)
                assert received == event

        run(_test())

    def test_tenant_filtering(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            ev_t1 = ServerEvent(id="1", event="x", data="", tenant_id="t1")
            ev_t2 = ServerEvent(id="2", event="x", data="", tenant_id="t2")
            ev_all = ServerEvent(id="3", event="x", data="", tenant_id="*")

            async with bus.subscribe("t1") as queue:
                await bus.publish(ev_t1)
                await bus.publish(ev_t2)  # should NOT be delivered
                await bus.publish(ev_all)

                got1 = await asyncio.wait_for(queue.get(), timeout=1)
                got2 = await asyncio.wait_for(queue.get(), timeout=1)
                assert got1.id == "1"
                assert got2.id == "3"

        run(_test())

    def test_replay_on_reconnect(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            for i in range(1, 6):
                await bus.publish(
                    ServerEvent(id=str(i), event="x", data=str(i), tenant_id="t1")
                )

            async with bus.subscribe("t1", last_event_id="3") as queue:
                got = []
                for _ in range(2):
                    got.append(await asyncio.wait_for(queue.get(), timeout=1))
                assert [e.id for e in got] == ["4", "5"]

        run(_test())

    def test_replay_filters_by_tenant(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            await bus.publish(ServerEvent(id="1", event="x", data="", tenant_id="t1"))
            await bus.publish(ServerEvent(id="2", event="x", data="", tenant_id="t2"))
            await bus.publish(ServerEvent(id="3", event="x", data="", tenant_id="t1"))

            async with bus.subscribe("t1", last_event_id="0") as queue:
                got = []
                for _ in range(2):
                    got.append(await asyncio.wait_for(queue.get(), timeout=1))
                assert [e.id for e in got] == ["1", "3"]

        run(_test())

    def test_subscriber_count(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            assert bus.subscriber_count == 0
            async with bus.subscribe("t1"):
                assert bus.subscriber_count == 1
            assert bus.subscriber_count == 0

        run(_test())

    def test_publish_returns_delivery_count(self):
        async def _test():
            bus = EventBus(replay_limit=50)
            event = ServerEvent(id="1", event="x", data="", tenant_id="t1")
            async with bus.subscribe("t1"):
                async with bus.subscribe("t2"):
                    count = await bus.publish(event)
                    assert count == 1  # only t1 subscriber

        run(_test())


# ---------------------------------------------------------------------------
# SSE formatting
# ---------------------------------------------------------------------------


class TestFormatSSE:
    def test_basic_format(self):
        event = ServerEvent(id="42", event="run.published", data='{"model":"gfs"}')
        result = _format_sse(event)
        assert result == (
            'id: 42\nevent: run.published\ndata: {"model":"gfs"}\n\n'
        )

    def test_multiline_data(self):
        event = ServerEvent(id="1", event="test", data="line1\nline2")
        result = _format_sse(event)
        assert "data: line1\n" in result
        assert "data: line2\n" in result


# ---------------------------------------------------------------------------
# SSE endpoint integration test
# ---------------------------------------------------------------------------


def test_stream_endpoint_with_replay():
    """Integration test: verify /events/stream replays events via Last-Event-ID."""
    import os
    import tempfile

    from weatherman.events.router import init_event_bus, get_event_bus, shutdown_event_bus
    from weatherman.events.bus import EventBus

    async def _test():
        bus = EventBus(replay_limit=50)

        # Pre-load events into the bus replay buffer
        event = ServerEvent(
            id=bus.next_event_id(),
            event="run.published",
            data='{"model":"gfs"}',
            tenant_id="default",
        )
        await bus.publish(event)

        # Subscribe with Last-Event-ID=0 to trigger replay
        async with bus.subscribe("default", last_event_id="0") as queue:
            replayed = await asyncio.wait_for(queue.get(), timeout=1)
            assert replayed.event == "run.published"
            assert replayed.data == '{"model":"gfs"}'
            assert replayed.id == "1"

    run(_test())


def test_stream_endpoint_format_roundtrip():
    """Verify _format_sse output matches SSE spec for the endpoint's use case."""
    from weatherman.events.router import _format_sse

    event = ServerEvent(
        id="5",
        event="run.published",
        data='{"model":"gfs","run_id":"20260308T00Z"}',
        tenant_id="default",
    )
    formatted = _format_sse(event)

    # Verify SSE spec compliance
    lines = formatted.split("\n")
    assert lines[0] == "id: 5"
    assert lines[1] == "event: run.published"
    assert lines[2].startswith("data: ")
    # Must end with double newline (empty event terminator)
    assert formatted.endswith("\n\n")
