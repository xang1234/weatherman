"""Tests for Neptune parity comparison helpers."""

from __future__ import annotations

from datetime import date, datetime

from weatherman.ais.db import AISDatabase
from weatherman.ais.parity import BBoxCheck, TileCheck, TrackCheck, compare_ais_databases
from weatherman.ais.snapshot import build_snapshot


SNAPSHOT_DATE = date(2025, 12, 25)


def _seed_positions(db: AISDatabase, *, second_heading: float = 185.0) -> None:
    con = db.connect()
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
        ) VALUES
        (
            'IMO1234567', 211234567, 'IMO1234567-211234567', NULL, 'mov-001',
            'MV BULK CARRIER', 'Cargo', NULL, NULL, 'ABCD1',
            45.0, 292.0,
            ?, ?, 1.35, 103.8,
            12.5, 245.0, 14.2, NULL, 'Under way using engine',
            'SINGAPORE', 'SINGAPORE', '2025-12-30 06:00:00',
            NULL, 'default'
        ),
        (
            NULL, 311234567, '311234567', NULL, 'mov-002',
            'MV TANKER STAR', 'Tanker', NULL, NULL, 'EFGH2',
            32.0, 190.0,
            ?, ?, 51.5, -0.1,
            8.3, ?, 11.0, NULL, 'At anchor',
            'ROTTERDAM', 'ROTTERDAM', '2025-12-28 10:00:00',
            NULL, 'default'
        )
        """,
        [
            datetime(2025, 12, 25, 12, 0),
            SNAPSHOT_DATE,
            datetime(2025, 12, 25, 14, 30),
            SNAPSHOT_DATE,
            second_heading,
        ],
    )
    build_snapshot(snapshot_date=SNAPSHOT_DATE, tenant_id="default", con=con)


def test_compare_ais_databases_reports_matching_summary_checks() -> None:
    legacy_db = AISDatabase(":memory:")
    neptune_db = AISDatabase(":memory:")
    try:
        _seed_positions(legacy_db)
        _seed_positions(neptune_db)

        report = compare_ais_databases(
            legacy_con=legacy_db.connect(),
            neptune_con=neptune_db.connect(),
            snapshot_date=SNAPSHOT_DATE,
            tenant_id="default",
            bbox_checks=(BBoxCheck(west=100, south=0, east=110, north=10),),
            tile_checks=(TileCheck(z=4, x=12, y=7),),
            track_checks=(
                TrackCheck(
                    mmsi=211234567,
                    start_date=SNAPSHOT_DATE,
                    end_date=SNAPSHOT_DATE,
                ),
            ),
        )

        assert report.ok is True
        assert all(check.match for check in report.checks)
    finally:
        legacy_db.close()
        neptune_db.close()


def test_compare_ais_databases_flags_mismatched_bbox_and_tile() -> None:
    legacy_db = AISDatabase(":memory:")
    neptune_db = AISDatabase(":memory:")
    try:
        _seed_positions(legacy_db)
        _seed_positions(neptune_db, second_heading=90.0)

        report = compare_ais_databases(
            legacy_con=legacy_db.connect(),
            neptune_con=neptune_db.connect(),
            snapshot_date=SNAPSHOT_DATE,
            tenant_id="default",
            bbox_checks=(BBoxCheck(west=-5, south=45, east=5, north=60),),
            tile_checks=(TileCheck(z=4, x=7, y=5),),
        )

        assert report.ok is False
        mismatch_names = {check.name for check in report.mismatches}
        assert "bbox:-5,45,5,60:limit=1000" in mismatch_names
        assert "tile:4/7/5" in mismatch_names
    finally:
        legacy_db.close()
        neptune_db.close()
