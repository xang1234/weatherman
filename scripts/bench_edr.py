"""Benchmark EDR position and trajectory queries (#91).

Two modes, both against a published run in --data-dir:

``reads`` (in process): times position queries — all hours, one hour, a
point between the last and first longitude columns — and trajectories of
40 and 200 samples, regional and across the antimeridian, counting the
Zarr chunks each one reads. It calls ``EDRService.query_position`` and
``_sample_trajectory``, so the same script measures any commit.

``http``: against a running backend, keeps --workers threads firing
position queries at random points for --seconds while another pings
``/health/live`` every 20 ms, and reports both latencies. A handler that
blocks the event loop shows up in the health pings.

    uv run python -m scripts.bench_edr reads --data-dir .data --reps 30
    uv run python -m scripts.bench_edr http --url http://localhost:8000 --seconds 20
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import subprocess
import threading
import time
from pathlib import Path

import httpx
import zarr
import zarr.storage

from weatherman.storage.catalog import RunCatalog
from weatherman.storage.paths import RunID, StorageLayout

POINT = (-30.0, 40.0)
DATELINE_POINT = (179.95, 20.0)  # between the last column (179.75) and the first (-180)
ROUTES = {
    "regional": [(-10.0, 45.0), (-5.0, 50.0)],
    "transpacific": [(140.0, 35.0), (-125.0, 40.0)],  # crosses the antimeridian
}


class CountingStore(zarr.storage.WrapperStore):
    """Counts the chunk reads that go through it."""

    chunks = 0

    async def get(self, key, prototype, byte_range=None):
        if "/c/" in key or "/c." in key:
            CountingStore.chunks += 1
        return await super().get(key, prototype, byte_range)


def _stats(ms: list[float]) -> dict[str, float]:
    s = sorted(ms)
    p95 = s[max(0, math.ceil(0.95 * len(s)) - 1)]  # nearest rank
    return {"n": len(s), "median_ms": round(statistics.median(s), 2), "p95_ms": round(p95, 2)}


def reads(args: argparse.Namespace) -> None:
    from weatherman.edr.position import EDRService
    from weatherman.edr.resample import resample_linestring
    from weatherman.edr.trajectory import _sample_trajectory

    data = Path(args.data_dir)
    layout = StorageLayout(args.model)
    catalog = RunCatalog.from_json((data / layout.catalog_path).read_text())
    run_id = RunID(args.run_id or str(catalog.current_run_id))

    def opener(path: str) -> zarr.Group:
        return zarr.open_group(CountingStore(zarr.storage.LocalStore(str(data / path), read_only=True)), mode="r")

    svc = EDRService(lambda _m: catalog, opener)

    def run(name: str, fn, reps: int) -> None:
        if args.cases and args.cases not in name:
            return
        for _ in range(args.warmup):
            fn()
        times, chunks = [], []
        for _ in range(reps):
            CountingStore.chunks = 0
            start = time.perf_counter()
            fn()
            times.append((time.perf_counter() - start) * 1000)
            chunks.append(CountingStore.chunks)
        print(json.dumps({"label": args.label, "case": name, **_stats(times), "chunk_reads": statistics.median(chunks)}), flush=True)

    lon, lat = POINT
    run("position all hours", lambda: svc.query_position(args.model, run_id, lon, lat, None, None), args.reps)
    run("position one hour", lambda: svc.query_position(args.model, run_id, lon, lat, None, "3"), args.reps)
    lon, lat = DATELINE_POINT
    run("position dateline", lambda: svc.query_position(args.model, run_id, lon, lat, None, None), args.reps)
    for route, coords in ROUTES.items():
        for n in (40, 200):
            samples = resample_linestring(coords, num_samples=n)
            run(f"trajectory {route} {n}", lambda s=samples: _sample_trajectory(svc, args.model, run_id, s), args.traj_reps)


def http(args: argparse.Namespace) -> None:
    stop = time.monotonic() + args.seconds
    health: list[float] = []
    position: list[float] = []

    def ping() -> None:
        with httpx.Client(base_url=args.url, timeout=30) as c:
            while time.monotonic() < stop:
                start = time.perf_counter()
                c.get("/health/live")
                health.append((time.perf_counter() - start) * 1000)
                time.sleep(0.02)

    def query(seed: int) -> None:
        rng = random.Random(seed)
        with httpx.Client(base_url=args.url, timeout=60) as c:
            while time.monotonic() < stop:
                lon, lat = rng.uniform(-179.9, 179.9), rng.uniform(-60, 60)
                start = time.perf_counter()
                r = c.get(f"/v1/edr/collections/{args.model}/instances/latest/position", params={"coords": f"POINT({lon:.3f} {lat:.3f})"})
                r.raise_for_status()
                position.append((time.perf_counter() - start) * 1000)

    threads = [threading.Thread(target=ping)] + [threading.Thread(target=query, args=(i,)) for i in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(json.dumps({"label": args.label, "workers": args.workers, "health": _stats(health), "position": _stats(position)}), flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="mode", required=True)
    r = sub.add_parser("reads")
    r.add_argument("--data-dir", default=".data")
    r.add_argument("--model", default="gfs")
    r.add_argument("--run-id")
    r.add_argument("--reps", type=int, default=30)
    r.add_argument("--traj-reps", type=int, default=5, help="trajectories are slow before #91: a 200-sample transpacific one is about a minute")
    r.add_argument("--warmup", type=int, default=3)
    r.add_argument("--cases", help="only the cases whose name contains this, e.g. 'position'")
    r.add_argument("--label", default="")
    h = sub.add_parser("http")
    h.add_argument("--url", default="http://localhost:8000")
    h.add_argument("--model", default="gfs")
    h.add_argument("--seconds", type=float, default=20)
    h.add_argument("--workers", type=int, default=4)
    h.add_argument("--label", default="")
    args = p.parse_args()
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(json.dumps({"commit": commit, "mode": args.mode}))
    (reads if args.mode == "reads" else http)(args)


if __name__ == "__main__":
    main()
