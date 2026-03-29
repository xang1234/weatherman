"""Tests for Neptune-backed AIS ingest and live refresh helpers."""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb

from weatherman.ais.db import AISDatabase
from weatherman.ais.ingest import load_day_from_select
from weatherman.ais.neptune import (
    NeptuneConfig,
    NeptuneLiveConfig,
    _neptune_select_sql,
    run_neptune_live_ingest,
)
from weatherman.ais.refresh import AISRefreshResult, refresh_neptune_day
from weatherman.events.router import get_event_bus, init_event_bus, shutdown_event_bus


SNAPSHOT_DATE = date(2025, 12, 25)


def _relation_column_types(
    con: duckdb.DuckDBPyConnection,
    name: str,
) -> dict[str, str]:
    rows = con.execute(f"DESCRIBE {name}").fetchall()
    return {str(column_name): str(data_type) for column_name, data_type, *_ in rows}


def test_neptune_mapping_loads_rows_into_ais_positions() -> None:
    db = AISDatabase(":memory:")
    con = db.connect()
    try:
        con.execute(
            """
            CREATE TEMP TABLE neptune_positions AS
            SELECT
                CAST(211234567 AS BIGINT) AS mmsi,
                TIMESTAMP '2025-12-25 12:00:00' AS "timestamp",
                1.35 AS lat,
                103.8 AS lon,
                12.5 AS sog,
                245.0 AS heading,
                14.2 AS draught,
                'ABCD1' AS callsign,
                'MV BULK CARRIER' AS vessel_name,
                292.0 AS length,
                45.0 AS beam,
                'SINGAPORE' AS destination,
                TIMESTAMP '2025-12-30 06:00:00' AS eta,
                'IMO1234567' AS imo,
                'Under way using engine' AS nav_status,
                '70' AS ship_type,
                'noaa' AS source,
                NULL AS source_record_id
            UNION ALL
            SELECT
                CAST(311234567 AS BIGINT),
                TIMESTAMP '2025-12-25 14:30:00',
                51.5,
                -0.1,
                8.3,
                180.0,
                11.0,
                'EFGH2',
                'MV TANKER STAR',
                190.0,
                32.0,
                'ROTTERDAM',
                TIMESTAMP '2025-12-28 10:00:00',
                NULL,
                'At anchor',
                'Oil Tanker',
                'aishub',
                'src-002'
            UNION ALL
            SELECT
                CAST(999999999 AS BIGINT),
                TIMESTAMP '2025-12-26 00:15:00',
                0.0,
                0.0,
                0.0,
                NULL,
                NULL,
                NULL,
                'OUT OF DAY',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                'noaa',
                NULL
            """
        )

        count = load_day_from_select(
            _neptune_select_sql(
                "neptune_positions",
                _relation_column_types(con, "neptune_positions"),
            ),
            load_date=SNAPSHOT_DATE,
            tenant_id="default",
            con=con,
            params={"load_date": SNAPSHOT_DATE, "tenant_id": "default"},
        )

        assert count == 2
        rows = con.execute(
            """
            SELECT
                mmsi, imommsi, movementid, shiptype, movestatus,
                destinationtidied, vessel_class, dwt, max_draught, tenant_id
            FROM ais_positions
            ORDER BY mmsi
            """
        ).fetchall()

        assert rows[0][0] == 211234567
        assert rows[0][1] == "IMO1234567-211234567"
        assert rows[0][2]  # deterministic fallback hash
        assert rows[0][3] == "Cargo"
        assert rows[0][4] == "Under way using engine"
        assert rows[0][5] == "SINGAPORE"
        assert rows[0][6] is None
        assert rows[0][7] is None
        assert rows[0][8] is None
        assert rows[0][9] == "default"

        assert rows[1][0] == 311234567
        assert rows[1][1] == "311234567"
        assert rows[1][2] == "src-002"
        assert rows[1][3] == "Tanker"
    finally:
        db.close()


def test_neptune_mapping_filters_timestamptz_rows_by_utc_day() -> None:
    db = AISDatabase(":memory:")
    con = db.connect()
    try:
        con.execute("SET TimeZone = 'America/Los_Angeles'")
        con.execute(
            """
            CREATE TEMP TABLE neptune_positions_tz AS
            SELECT
                CAST(211234567 AS BIGINT) AS mmsi,
                TIMESTAMPTZ '2025-12-25 00:30:00+00' AS "timestamp",
                1.35 AS lat,
                103.8 AS lon,
                'noaa' AS source
            UNION ALL
            SELECT
                CAST(311234567 AS BIGINT),
                TIMESTAMPTZ '2025-12-24 23:30:00+00',
                51.5,
                -0.1,
                'noaa'
            """
        )

        count = load_day_from_select(
            _neptune_select_sql(
                "neptune_positions_tz",
                _relation_column_types(con, "neptune_positions_tz"),
            ),
            load_date=SNAPSHOT_DATE,
            tenant_id="default",
            con=con,
            params={"load_date": SNAPSHOT_DATE, "tenant_id": "default"},
        )

        assert count == 1
        row = con.execute(
            'SELECT mmsi, "date" FROM ais_positions WHERE tenant_id = ?',
            ["default"],
        ).fetchone()
        assert row == (211234567, SNAPSHOT_DATE)
    finally:
        db.close()


def test_neptune_mapping_filters_offset_timestamps_by_utc_day() -> None:
    db = AISDatabase(":memory:")
    con = db.connect()
    try:
        con.execute("SET TimeZone = 'America/Los_Angeles'")
        con.execute(
            """
            CREATE TEMP TABLE neptune_positions_text AS
            SELECT
                CAST(211234567 AS BIGINT) AS mmsi,
                '2025-12-25T00:30:00+00:00' AS "timestamp",
                1.35 AS lat,
                103.8 AS lon,
                'noaa' AS source
            UNION ALL
            SELECT
                CAST(311234567 AS BIGINT),
                '2025-12-24T23:30:00+00:00',
                51.5,
                -0.1,
                'noaa'
            """
        )

        count = load_day_from_select(
            _neptune_select_sql(
                "neptune_positions_text",
                _relation_column_types(con, "neptune_positions_text"),
            ),
            load_date=SNAPSHOT_DATE,
            tenant_id="default",
            con=con,
            params={"load_date": SNAPSHOT_DATE, "tenant_id": "default"},
        )

        assert count == 1
        row = con.execute(
            'SELECT mmsi, "date" FROM ais_positions WHERE tenant_id = ?',
            ["default"],
        ).fetchone()
        assert row == (211234567, SNAPSHOT_DATE)
    finally:
        db.close()


def test_neptune_mapping_normalizes_eta_to_utc_timestamp() -> None:
    db = AISDatabase(":memory:")
    con = db.connect()
    try:
        con.execute("SET TimeZone = 'America/Los_Angeles'")
        con.execute(
            """
            CREATE TEMP TABLE neptune_positions_eta AS
            SELECT
                CAST(211234567 AS BIGINT) AS mmsi,
                TIMESTAMPTZ '2025-12-25 00:30:00+00' AS "timestamp",
                TIMESTAMPTZ '2025-12-30 06:00:00+00' AS eta,
                1.35 AS lat,
                103.8 AS lon,
                'noaa' AS source
            """
        )

        count = load_day_from_select(
            _neptune_select_sql(
                "neptune_positions_eta",
                _relation_column_types(con, "neptune_positions_eta"),
            ),
            load_date=SNAPSHOT_DATE,
            tenant_id="default",
            con=con,
            params={"load_date": SNAPSHOT_DATE, "tenant_id": "default"},
        )

        assert count == 1
        eta = con.execute(
            'SELECT eta FROM ais_positions WHERE tenant_id = ?',
            ["default"],
        ).fetchone()[0]
        assert eta == datetime(2025, 12, 30, 6, 0)
    finally:
        db.close()


def test_refresh_neptune_day_builds_snapshot_and_emits_event(monkeypatch) -> None:
    db = AISDatabase(":memory:")
    con = db.connect()

    def fake_loader(*, load_date, tenant_id, con, config, download):
        assert load_date == SNAPSHOT_DATE
        assert tenant_id == "default"
        assert download is True
        con.execute(
            """
            INSERT INTO ais_positions (
                imo, mmsi, imommsi, lrimoshipno, movementid,
                vessel_name, shiptype, vessel_class, dwt, callsign,
                beam, length,
                "timestamp", "date", lat, lon,
                sog, heading, draught, max_draught, movestatus,
                destination, destinationtidied, eta,
                additionalinfo, tenant_id
            ) VALUES (
                'IMO1234567', 211234567, 'IMO1234567-211234567', NULL, 'mov-001',
                'MV BULK CARRIER', 'Cargo', NULL, NULL, 'ABCD1',
                45.0, 292.0,
                '2025-12-25 12:00:00', '2025-12-25', 1.35, 103.8,
                12.5, 245.0, 14.2, NULL, 'Under way using engine',
                'SINGAPORE', 'SINGAPORE', '2025-12-30 06:00:00',
                NULL, 'default'
            )
            """
        )
        return 1

    monkeypatch.setattr("weatherman.ais.refresh.load_day_from_neptune", fake_loader)

    init_event_bus()
    try:
        bus = get_event_bus()

        async def _exercise() -> None:
            async with bus.subscribe("default") as queue:
                result = refresh_neptune_day(
                    load_date=SNAPSHOT_DATE,
                    tenant_id="default",
                    con=con,
                    config=NeptuneConfig(store_root=Path("/tmp/neptune-test")),
                    emit_event=True,
                )
                assert result == AISRefreshResult(
                    snapshot_date=SNAPSHOT_DATE,
                    tenant_id="default",
                    rows_loaded=1,
                    vessels_visible=1,
                    event_emitted=True,
                )

                snapshot_count = con.execute(
                    'SELECT COUNT(*) FROM ais_snapshot WHERE "date" = ?',
                    [SNAPSHOT_DATE],
                ).fetchone()[0]
                assert snapshot_count == 1

                event = await asyncio.wait_for(queue.get(), timeout=1)
                assert event.event == "ais.refreshed"

        asyncio.run(_exercise())
    finally:
        shutdown_event_bus()
        db.close()


def test_run_neptune_live_ingest_promotes_and_refreshes(monkeypatch, tmp_path: Path) -> None:
    sentinel = object()

    class FakeParquetSink:
        def __init__(self, landing_dir, source):
            self.landing_dir = landing_dir
            self.source = source
            self.writes: list[dict[str, object]] = []
            self.flushes = 0

        async def write(self, messages):
            self.writes.extend(messages)

        async def flush(self):
            self.flushes += 1

        async def close(self):
            return None

    class FakeStreamConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeNeptuneStream:
        def __init__(self, *, config):
            self.config = config
            self.stats = SimpleNamespace(messages_delivered=0)
            self._message_queue = asyncio.Queue()

        async def __aenter__(self):
            self.is_running = True
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.is_running = False
            await self._message_queue.put(sentinel)
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            message = await self._message_queue.get()
            if message is sentinel:
                raise StopAsyncIteration
            return message

        async def ingest(self, message):
            self.stats.messages_delivered += 1
            await self._message_queue.put(message)
            return True

    promotions = [
        SimpleNamespace(date="2025-12-25", record_count=3, shard_files=["part-0000.parquet"]),
        SimpleNamespace(date="2025-12-26", record_count=2, shard_files=["part-0000.parquet", "part-0001.parquet"]),
    ]
    refresh_calls: list[tuple[date, tuple[str, ...], bool]] = []

    def fake_import():
        async def fake_run_with_reconnect(stream, connect_fn, *, max_retries=None):
            assert max_retries is None
            await connect_fn()

        return FakeNeptuneStream, FakeStreamConfig, FakeParquetSink, (
            lambda landing_dir, store_root, source, cleanup=False: promotions
        ), fake_run_with_reconnect

    def fake_connect_and_stream(stream, *, api_key, bbox):
        assert api_key == ""
        assert bbox is None

        async def _inject():
            for mmsi in range(25):
                await stream.ingest({"mmsi": mmsi, "timestamp": f"2025-12-25T00:00:{mmsi:02d}"})

        return _inject()

    def fake_refresh_neptune_day(*, load_date, tenant_id, con, config, emit_event, download):
        refresh_calls.append((load_date, config.sources, download))
        return AISRefreshResult(
            snapshot_date=load_date,
            tenant_id=tenant_id,
            rows_loaded=1,
            vessels_visible=1,
            event_emitted=emit_event,
        )

    monkeypatch.setattr("weatherman.ais.neptune._import_neptune_streaming", fake_import)
    monkeypatch.setattr(
        "weatherman.ais.neptune._import_neptune_stream_source",
        lambda source: fake_connect_and_stream,
    )
    monkeypatch.setattr("weatherman.ais.refresh.refresh_neptune_day", fake_refresh_neptune_day)

    result = run_neptune_live_ingest(
        live_config=NeptuneLiveConfig(
            source="aisstream",
            landing_dir=tmp_path / "landing",
            max_messages=25,
            cleanup=True,
        ),
        archival_config=NeptuneConfig(
            store_root=tmp_path / "store",
            sources=("noaa", "dma"),
        ),
        db_path=tmp_path / "live.duckdb",
        tenant_id="default",
        emit_event=True,
    )

    assert result.source == "aisstream"
    assert result.dates_refreshed == (date(2025, 12, 25), date(2025, 12, 26))
    assert result.records_promoted == 5
    assert result.shard_files == 3
    assert refresh_calls == [
        (date(2025, 12, 25), ("aisstream",), False),
        (date(2025, 12, 26), ("aisstream",), False),
    ]


def test_run_neptune_live_ingest_refreshes_before_stream_exit(monkeypatch, tmp_path: Path) -> None:
    sentinel = object()
    events: list[str] = []

    class FakeParquetSink:
        def __init__(self, landing_dir, source):
            self.landing_dir = landing_dir
            self.source = source
            self.messages: list[dict[str, object]] = []

        async def write(self, messages):
            self.messages.extend(messages)

        async def flush(self):
            return None

        async def close(self):
            return None

    class FakeStreamConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeNeptuneStream:
        def __init__(self, *, config):
            self.config = config
            self.stats = SimpleNamespace(messages_delivered=0)
            self._message_queue = asyncio.Queue()

        async def __aenter__(self):
            self.is_running = True
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.is_running = False
            await self._message_queue.put(sentinel)
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            message = await self._message_queue.get()
            if message is sentinel:
                raise StopAsyncIteration
            return message

        async def ingest(self, message):
            self.stats.messages_delivered += 1
            await self._message_queue.put(message)
            return True

    promotions = [SimpleNamespace(date="2025-12-25", record_count=1, shard_files=["part-0000.parquet"])]
    promoted = False

    def fake_import():
        async def fake_run_with_reconnect(stream, connect_fn, *, max_retries=None):
            await connect_fn()

        def fake_promote(landing_dir, store_root, source, cleanup=False):
            nonlocal promoted
            if promoted:
                return []
            promoted = True
            events.append("promote")
            return promotions

        return FakeNeptuneStream, FakeStreamConfig, FakeParquetSink, fake_promote, fake_run_with_reconnect

    async def fake_connect_and_stream(stream, *, api_key, bbox):
        events.append("connect_start")
        await stream.ingest({"mmsi": 123456789, "timestamp": "2025-12-25T00:00:00"})
        events.append("sleeping")
        await asyncio.sleep(0.02)
        events.append("connect_end")

    def fake_refresh_neptune_day(*, load_date, tenant_id, con, config, emit_event, download):
        events.append("refresh")
        return AISRefreshResult(
            snapshot_date=load_date,
            tenant_id=tenant_id,
            rows_loaded=1,
            vessels_visible=1,
            event_emitted=emit_event,
        )

    monkeypatch.setattr("weatherman.ais.neptune._import_neptune_streaming", fake_import)
    monkeypatch.setattr(
        "weatherman.ais.neptune._import_neptune_stream_source",
        lambda source: fake_connect_and_stream,
    )
    monkeypatch.setattr("weatherman.ais.refresh.refresh_neptune_day", fake_refresh_neptune_day)

    result = run_neptune_live_ingest(
        live_config=NeptuneLiveConfig(
            source="aisstream",
            landing_dir=tmp_path / "landing",
            flush_interval_s=0.001,
        ),
        archival_config=NeptuneConfig(
            store_root=tmp_path / "store",
            sources=("noaa",),
        ),
        db_path=tmp_path / "live.daemon.duckdb",
        tenant_id="default",
        emit_event=True,
    )

    assert result.dates_refreshed == (date(2025, 12, 25),)
    assert events.index("refresh") < events.index("connect_end")
