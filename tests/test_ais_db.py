"""Tests for weatherman.ais.db — DuckDB setup with spatial extension."""

from __future__ import annotations

import duckdb
import pytest

from weatherman.ais.db import AISDatabase, SCHEMA_VERSION


class TestAISDatabase:
    """Core lifecycle and schema tests."""

    def test_connect_creates_schema(self) -> None:
        db = AISDatabase(":memory:")
        con = db.connect()

        # Table should exist
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'ais_positions'"
        ).fetchall()
        assert len(tables) == 1
        db.close()

    def test_connect_idempotent(self) -> None:
        db = AISDatabase(":memory:")
        con1 = db.connect()
        con2 = db.connect()
        assert con1 is con2
        db.close()

    def test_close_then_reconnect(self) -> None:
        db = AISDatabase(":memory:")
        db.connect()
        db.close()
        # Can reconnect (new in-memory DB)
        con = db.connect()
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'ais_positions'"
        ).fetchall()
        assert len(tables) == 1
        db.close()

    def test_connection_property_raises_before_connect(self) -> None:
        db = AISDatabase(":memory:")
        with pytest.raises(RuntimeError, match="not connected"):
            _ = db.connection

    def test_connection_property_after_connect(self) -> None:
        db = AISDatabase(":memory:")
        con = db.connect()
        assert db.connection is con
        db.close()


class TestSpatialExtension:
    """Verify the spatial extension is loaded and usable."""

    def test_spatial_functions_available(self) -> None:
        db = AISDatabase(":memory:")
        con = db.connect()

        # ST_Point should work
        result = con.execute(
            "SELECT ST_AsText(ST_Point(103.8, 1.35))"
        ).fetchone()
        assert result is not None
        assert "103.8" in result[0]
        db.close()


class TestAISPositionsSchema:
    """Verify the ais_positions table schema matches source Parquet columns."""

    @pytest.fixture()
    def con(self) -> duckdb.DuckDBPyConnection:
        db = AISDatabase(":memory:")
        con = db.connect()
        yield con
        db.close()

    def test_all_columns_present(self, con: duckdb.DuckDBPyConnection) -> None:
        cols = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'ais_positions' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]

        expected = [
            "imommsi", "mmsi", "lrimoshipno", "movementid",
            "shipname", "shiptype", "vessel_class", "dwt", "callsign",
            "beam", "length",
            "movementdatetime", "movement_date", "latitude", "longitude",
            "speed", "heading", "draught", "max_draught", "movestatus",
            "destination", "destinationtidied", "eta",
            "additionalinfo", "tenant_id",
        ]
        assert col_names == expected

    def test_tenant_id_not_null(self, con: duckdb.DuckDBPyConnection) -> None:
        col_info = con.execute(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_name = 'ais_positions' AND column_name = 'tenant_id'"
        ).fetchone()
        assert col_info is not None
        assert col_info[0] == "NO"

    def test_insert_and_query(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute("""
            INSERT INTO ais_positions (
                imommsi, mmsi, lrimoshipno, movementid,
                shipname, shiptype, vessel_class, dwt, callsign,
                beam, length,
                movementdatetime, movement_date, latitude, longitude,
                speed, heading, draught, max_draught, movestatus,
                destination, destinationtidied, eta,
                additionalinfo, tenant_id
            ) VALUES (
                '9876543-211234567', 211234567, '9876543', 'mov-001',
                'MV BULK CARRIER', 'Cargo', 'Capesize', 180000, 'ABCD1',
                45.0, 292.0,
                '2025-12-25 12:00:00', '2025-12-25', 1.35, 103.8,
                12.5, 245.0, 14.2, 18.5, 'Under way using engine',
                'SINGAPORE', 'Singapore', '2025-12-30 06:00:00',
                NULL, 'default'
            )
        """)
        rows = con.execute(
            "SELECT mmsi, shipname, speed FROM ais_positions "
            "WHERE movement_date = '2025-12-25'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 211234567
        assert rows[0][1] == "MV BULK CARRIER"
        assert rows[0][2] == 12.5

    def test_cog_column_absent(self, con: duckdb.DuckDBPyConnection) -> None:
        """Source data does not contain COG — schema should not have it."""
        cols = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'ais_positions' AND column_name = 'cog'"
        ).fetchall()
        assert len(cols) == 0

    def test_indexes_created(self, con: duckdb.DuckDBPyConnection) -> None:
        indexes = con.execute(
            "SELECT index_name FROM duckdb_indexes() "
            "WHERE table_name = 'ais_positions'"
        ).fetchall()
        index_names = {i[0] for i in indexes}
        assert "idx_ais_positions_date" in index_names
        assert "idx_ais_positions_mmsi" in index_names

    def test_schema_version_defined(self) -> None:
        assert SCHEMA_VERSION >= 1


class TestSchemaIdempotent:
    """Verify CREATE IF NOT EXISTS is truly idempotent."""

    def test_double_connect_same_db(self, tmp_path) -> None:
        db_path = tmp_path / "test.duckdb"
        db1 = AISDatabase(db_path)
        db1.connect()
        db1.close()

        # Second open should not fail
        db2 = AISDatabase(db_path)
        con = db2.connect()
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'ais_positions'"
        ).fetchall()
        assert len(tables) == 1
        db2.close()
