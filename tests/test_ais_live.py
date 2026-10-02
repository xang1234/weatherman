"""Live AIS ingest in the backend process: same-day rebuilds reach an open map (#72)."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import date
from pathlib import Path

import mapbox_vector_tile as mvt
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.conftest_ais import ROW_BULK_CARRIER, ROW_GRAIN_STAR, _write_test_parquet
from weatherman.ais.db import AISDatabase
from weatherman.ais.live import start_live_ingest
from weatherman.ais.refresh import refresh_day
from weatherman.ais.router import init_ais_tile_service, router, shutdown_ais_tile_service
from weatherman.events.bus import EventBus, ServerEvent

DAY = date(2025, 12, 25)
TENANT = "default"


def test_a_same_day_rebuild_reaches_the_running_server(tmp_path: Path, monkeypatch):
    """The ingest writes on the server's own connection while tiles keep being served."""
    import weatherman.ais.router as mod

    monkeypatch.setattr("weatherman.ais.refresh.emit_ais_refreshed", lambda **_: None)
    _write_test_parquet(tmp_path / "first", f"movement_date={DAY}", ROW_BULK_CARRIER)
    _write_test_parquet(tmp_path / "later", f"movement_date={DAY}", f"{ROW_BULK_CARRIER} UNION ALL {ROW_GRAIN_STAR}")

    db_path = str(tmp_path / "ais.duckdb")
    db = AISDatabase(db_path)
    refresh_day(tmp_path / "first" / f"movement_date={DAY}" / "*", load_date=DAY, tenant_id=TENANT, con=db.connect(), emit_event=False)
    db.close()

    mod._service = None
    svc = init_ais_tile_service(db_path, writable=True)
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    try:
        before = client.get("/ais/tiles/latest").json()["revision"]
        tile = lambda rev: mvt.decode(client.get(f"/ais/tiles/{DAY}/0/0/0.pbf?rev={rev}").content)
        assert len(tile(before)["vessels"]["features"]) == 1

        # A later batch of the same day, ingested in the background.
        done = threading.Event()

        def fake_ingest(*, con, tenant_id, **_):
            refresh_day(tmp_path / "later" / f"movement_date={DAY}" / "*", load_date=DAY, tenant_id=tenant_id, con=con, emit_event=False)
            done.set()
            threading.Event().wait()  # keep "streaming" until the process ends

        _, stop = start_live_ingest(svc.connection, db_path=db_path, tenant_id=TENANT, run=fake_ingest)
        assert done.wait(10)
        stop.set()

        after = client.get("/ais/tiles/latest").json()["revision"]
        assert after > before
        assert len(tile(after)["vessels"]["features"]) == 2
        pinned = client.get(f"/ais/tiles/{DAY}/0/0/0.pbf?rev={after}")
        assert pinned.headers["cache-control"].endswith("immutable")
    finally:
        shutdown_ais_tile_service()


def test_an_event_published_from_another_thread_reaches_subscribers():
    """The ingest thread emits ais.refreshed; asyncio queues may only be touched on the bus's loop."""

    async def scenario() -> ServerEvent:
        bus = EventBus()
        async with bus.subscribe("default") as queue:
            event = ServerEvent(id="1", event="ais.refreshed", data=json.dumps({"ais_date": str(DAY)}), tenant_id="*")
            thread = threading.Thread(target=bus.publish_sync, args=(event,))
            thread.start()
            thread.join()
            return await asyncio.wait_for(queue.get(), timeout=2)

    assert asyncio.run(scenario()).event == "ais.refreshed"
