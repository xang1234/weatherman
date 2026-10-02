"""Fetch a low-zoom basemap extract for the app to serve itself (#70).

Protomaps' daily planet builds need no API key, but browsers may only read
them from a few localhost origins (CORS) and old builds are pruned, so the
app serves its own copy from <data dir>/basemap/basemap.pmtiles instead.

The archives are clustered: tiles are stored in tile-id order, which puts
zooms 0..N at the start of the tile data. The extract is therefore one range
read of that start plus the directories that index it — about 190 MB for
zooms 0-7 — not one request per tile. Vector tiles overzoom cleanly, so the
map stays sharp several levels past the extract's max zoom.

Usage:
    uv run python scripts/fetch_basemap.py              # newest build, z0-7
    uv run python scripts/fetch_basemap.py --max-zoom 6 # about 45 MB
"""

from __future__ import annotations

import argparse
import json
import logging
import mmap
import os
import tempfile
import urllib.request
from pathlib import Path

from pmtiles.reader import Reader
from pmtiles.tile import Compression, Entry, deserialize_directory, zxy_to_tileid
from pmtiles.writer import Writer

BUILDS_URL = "https://build-metadata.protomaps.dev/builds.json"
BUILD_URL = "https://build.protomaps.com/{key}"
# build.protomaps.com refuses Python's default User-Agent.
HEADERS = {"User-Agent": "weatherman-basemap/1.0 (+https://github.com/xang1234/weatherman)"}

logger = logging.getLogger("fetch_basemap")


def newest_build() -> str:
    """URL of the newest Protomaps daily planet build."""
    with urllib.request.urlopen(urllib.request.Request(BUILDS_URL, headers=HEADERS), timeout=30) as resp:
        keys = sorted(build["key"] for build in json.load(resp))
    return BUILD_URL.format(key=keys[-1])


def range_reader(url: str):
    """get_bytes(offset, length) over HTTP range requests."""
    def get_bytes(offset: int, length: int) -> bytes:
        headers = {**HEADERS, "Range": f"bytes={offset}-{offset + length - 1}"}
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as resp:
            return resp.read()
    return get_bytes


def low_zoom_entries(get_bytes, header: dict, end_tile_id: int) -> list[Entry]:
    """Directory entries for tiles below end_tile_id, leaf directories included."""
    entries: list[Entry] = []

    def walk(offset: int, length: int) -> None:
        for entry in deserialize_directory(get_bytes(offset, length)):
            if entry.tile_id >= end_tile_id:
                return
            if entry.run_length == 0:  # a leaf directory
                walk(header["leaf_directory_offset"] + entry.offset, entry.length)
            else:
                entries.append(entry)

    walk(header["root_offset"], header["root_length"])
    return entries


def download_range(url: str, offset: int, length: int, dest) -> None:
    """Stream bytes [offset, offset + length) of url into dest, logging progress."""
    headers = {**HEADERS, "Range": f"bytes={offset}-{offset + length - 1}"}
    done, next_log = 0, 0
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as resp:
        while chunk := resp.read(1 << 20):
            dest.write(chunk)
            done += len(chunk)
            if done >= next_log:
                logger.info("  %d / %d MB", done >> 20, length >> 20)
                next_log += 25 << 20
    if done != length:
        raise RuntimeError(f"short read: {done} of {length} bytes")


def extract(url: str, out: Path, max_zoom: int) -> None:
    get_bytes = range_reader(url)
    reader = Reader(get_bytes)
    header = reader.header()
    if not header.get("clustered"):
        raise RuntimeError(f"{url} is not clustered; its low zooms are not contiguous")
    # pmtiles' deserialize_directory gunzips the directories itself, and can
    # read nothing else (Protomaps builds use gzip).
    if header["internal_compression"] != Compression.GZIP:
        raise RuntimeError(f"{url} has {header['internal_compression']} directories; only gzip is supported")
    end_tile_id = zxy_to_tileid(max_zoom + 1, 0, 0)

    entries = low_zoom_entries(get_bytes, header, end_tile_id)
    span = max(entry.offset + entry.length for entry in entries)
    logger.info("Zooms 0-%d: %d tiles, %.0f MB from %s", max_zoom, len(entries), span / 1e6, url)

    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile() as data, tempfile.NamedTemporaryFile(dir=out.parent, delete=False) as tmp:
        try:
            download_range(url, header["tile_data_offset"], span, data)
            data.flush()
            with mmap.mmap(data.fileno(), 0, access=mmap.ACCESS_READ) as tiles:
                writer = Writer(tmp)
                for entry in entries:
                    tile = tiles[entry.offset:entry.offset + entry.length]
                    # A run can reach past max_zoom; stop at the boundary.
                    for tile_id in range(entry.tile_id, min(entry.tile_id + entry.run_length, end_tile_id)):
                        writer.write_tile(tile_id, tile)
                # min/max zoom, offsets and counts are recomputed by finalize().
                writer.finalize({**header, "center_zoom": min(header["center_zoom"], max_zoom)}, reader.metadata())
            tmp.flush()
            os.chmod(tmp.name, 0o644)  # temp files are owner-only; the server may run as another user
            os.replace(tmp.name, out)  # never leave a half-written basemap behind
        except BaseException:
            os.unlink(tmp.name)
            raise
    logger.info("Wrote %s (%.0f MB)", out, out.stat().st_size / 1e6)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, default=Path(os.environ.get("WEATHERMAN_DATA_DIR", ".data")) / "basemap" / "basemap.pmtiles")
    parser.add_argument("--max-zoom", type=int, default=7)
    parser.add_argument("--build", help="PMTiles URL to extract from (default: newest Protomaps daily build)")
    parser.add_argument("--force", action="store_true", help="replace an existing extract")
    args = parser.parse_args()

    if args.out.exists() and not args.force:
        logger.info("%s already exists (use --force to replace it)", args.out)
        return
    extract(args.build or newest_build(), args.out, args.max_zoom)


if __name__ == "__main__":
    main()
