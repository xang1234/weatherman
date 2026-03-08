"""Tests for AIS refresh orchestration."""

from __future__ import annotations

import asyncio
import json
from datetime import date

from weatherman.ais.db import AISDatabase
from weatherman.ais.refresh import refresh_day
from weatherman.ais.router import AISTileService
from weatherman.events.router import get_event_bus, init_event_bus, shutdown_event_bus
from tests.conftest_ais import ROW_BULK_CARRIER, ROW_GRAIN_STAR, _write_test_parquet

SNAPSHOT_DATE = date(2025, 12, 25)
TENANT = "default"


def run(coro):
    return asyncio.run(coro)


def test_refresh_day_loads_builds_snapshot_and_emits_event(tmp_path):
    db_path = tmp_path / "ais-refresh.duckdb"
    db = AISDatabase(str(db_path))
    con = db.connect()
    db_closed = False
    _write_test_parquet(
        tmp_path,
        "movement_date=2025-12-25",
        f"{ROW_BULK_CARRIER} UNION ALL {ROW_GRAIN_STAR}",
    )

    init_event_bus()
    try:
        bus = get_event_bus()

        async def _test() -> None:
            async with bus.subscribe(TENANT) as queue:
                result = refresh_day(
                    f"{tmp_path}/movement_date=2025-12-25/*",
                    load_date=SNAPSHOT_DATE,
                    tenant_id=TENANT,
                    con=con,
                    emit_event=True,
                )

                assert result.snapshot_date == SNAPSHOT_DATE
                assert result.tenant_id == TENANT
                assert result.rows_loaded > 0
                assert result.vessels_visible == 2
                assert result.event_emitted is True

                position_count = con.execute(
                    'SELECT COUNT(*) FROM ais_positions WHERE "date" = ? AND tenant_id = ?',
                    [SNAPSHOT_DATE, TENANT],
                ).fetchone()[0]
                snapshot_count = con.execute(
                    'SELECT COUNT(*) FROM ais_snapshot WHERE "date" = ? AND tenant_id = ?',
                    [SNAPSHOT_DATE, TENANT],
                ).fetchone()[0]
                assert position_count == result.rows_loaded
                assert snapshot_count == result.vessels_visible

                db.close()
                nonlocal db_closed
                db_closed = True
                svc = AISTileService(str(db_path))
                svc.connect()
                try:
                    assert svc.latest_snapshot_date() == SNAPSHOT_DATE
                finally:
                    svc.close()

                event = await asyncio.wait_for(queue.get(), timeout=1)
                assert event.event == "ais.refreshed"
                payload = json.loads(event.data)
                assert payload["ais_date"] == "2025-12-25"
                assert payload["tile_url_template"] == "/ais/tiles/2025-12-25/{z}/{x}/{y}.pbf"

        run(_test())
    finally:
        shutdown_event_bus()
        if not db_closed:
            db.close()
