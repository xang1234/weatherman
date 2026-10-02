"""Self-hosted basemap (#70): the low-zoom extract and the /basemap route."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from pmtiles.reader import MemorySource, MmapSource, Reader
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from starlette.testclient import TestClient

import scripts.fetch_basemap as fetch_basemap
from weatherman.app import create_app


def _planet(max_zoom: int) -> bytes:
    """A small clustered archive with a distinct tile at every z/x/y."""
    buf = io.BytesIO()
    writer = Writer(buf)
    tiles = sorted(
        (zxy_to_tileid(z, x, y), f"{z}/{x}/{y}".encode())
        for z in range(max_zoom + 1)
        for x in range(2**z)
        for y in range(2**z)
    )
    for tile_id, data in tiles:
        writer.write_tile(tile_id, data)
    writer.finalize(
        {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.NONE,
            "min_lon_e7": -1800000000, "min_lat_e7": -850000000,
            "max_lon_e7": 1800000000, "max_lat_e7": 850000000,
            "center_zoom": max_zoom, "center_lon_e7": 0, "center_lat_e7": 0,
        },
        {"name": "test planet"},
    )
    return buf.getvalue()


def test_extract_keeps_exactly_the_low_zooms(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    planet = _planet(max_zoom=4)
    source = MemorySource(planet)
    # Stand in for HTTP: range reads and the streamed download, from memory.
    monkeypatch.setattr(fetch_basemap, "range_reader", lambda url: source)
    monkeypatch.setattr(
        fetch_basemap, "download_range",
        lambda url, offset, length, dest: dest.write(planet[offset:offset + length]),
    )

    out = tmp_path / "basemap" / "basemap.pmtiles"
    fetch_basemap.extract("https://example.invalid/planet.pmtiles", out, max_zoom=2)

    with out.open("rb") as f:
        reader = Reader(MmapSource(f))
        header = reader.header()
        assert (header["min_zoom"], header["max_zoom"]) == (0, 2)
        assert header["center_zoom"] == 2
        assert reader.metadata() == {"name": "test planet"}
        for z in range(3):
            for x in range(2**z):
                for y in range(2**z):
                    assert reader.get(z, x, y) == f"{z}/{x}/{y}".encode()
        assert reader.get(3, 0, 0) is None
    assert not [p for p in out.parent.iterdir() if p != out]  # no temporary file left


def test_backend_serves_the_basemap_with_ranges(tmp_path: Path):
    (tmp_path / "basemap").mkdir()
    (tmp_path / "basemap" / "basemap.pmtiles").write_bytes(bytes(range(256)))
    app = create_app(data_dir=str(tmp_path), titiler_base_url="http://localhost:9999")
    with TestClient(app) as client:
        part = client.get("/basemap/basemap.pmtiles", headers={"Range": "bytes=16-31"})
        assert part.status_code == 206
        assert part.content == bytes(range(16, 32))
        assert client.get("/basemap/missing.pmtiles").status_code == 404


def test_backend_starts_without_a_basemap(tmp_path: Path):
    app = create_app(data_dir=str(tmp_path), titiler_base_url="http://localhost:9999")
    with TestClient(app) as client:
        assert client.get("/basemap/basemap.pmtiles").status_code == 404
