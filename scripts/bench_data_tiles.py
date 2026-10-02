"""Benchmark the pipeline's data-tile step on a published run's COGs (#95).

Copies the run's COGs into a scratch staging area and times
``step_generate_data_tiles`` once per format profile, counting source warps
and the tiles and bytes written. It calls the step through its public
signature, so the same script measures any commit.

    uv run python -m scripts.bench_data_tiles --data-dir .data --hours 0 \\
        --profiles png f16 png,f16 --reps 3
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

import weatherman.processing.data_tiles as data_tiles
from weatherman.storage.object_store import LocalObjectStore
from weatherman.storage.paths import RunID, StorageLayout

from scripts.run_pipeline import step_generate_data_tiles


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default=".data")
    p.add_argument("--model", default="gfs")
    p.add_argument("--run-id", help="default: the newest published run")
    p.add_argument("--hours", default="0")
    p.add_argument("--layers", help="comma-separated; default: every layer with a COG")
    p.add_argument("--max-zoom", type=int, default=data_tiles.MAX_DATA_TILE_ZOOM)
    p.add_argument("--profiles", nargs="+", default=["png", "f16", "png,f16"])
    p.add_argument("--reps", type=int, default=3)
    args = p.parse_args()
    logging.basicConfig(level=logging.WARNING)

    layout = StorageLayout(args.model)
    runs = Path(args.data_dir) / layout.model_prefix / "runs"
    run_id = RunID(args.run_id or sorted(d.name for d in runs.iterdir() if d.is_dir())[-1])
    hours = [int(h) for h in args.hours.split(",")]
    cogs = Path(args.data_dir) / layout.run_prefix(run_id) / "cogs"
    layers = set(args.layers.split(",")) if args.layers else {d.name for d in cogs.iterdir() if d.is_dir()}

    # Count warps through the module attribute every generator call looks up.
    warps = 0
    real_warp = data_tiles._warp_tile

    def counting_warp(*a, **kw):
        nonlocal warps
        warps += 1
        return real_warp(*a, **kw)

    data_tiles._warp_tile = counting_warp
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(json.dumps({"commit": commit, "run": str(run_id), "hours": hours, "layers": sorted(layers),
                      "max_zoom": args.max_zoom, "python": platform.python_version(),
                      "machine": f"{platform.system()} {platform.release()} {platform.machine()}"}))

    results: dict[str, list[dict]] = {}
    for rep in range(args.reps):
        for profile in args.profiles:
            formats = tuple(profile.split(","))
            with tempfile.TemporaryDirectory() as tmp:
                for layer in layers:
                    for h in hours:
                        src = cogs / layer / f"{h:03d}.tif"
                        if src.exists():
                            dst = Path(tmp) / layout.staging_cog_path(run_id, layer, h)
                            dst.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy(src, dst)
                warps = 0
                start = time.perf_counter()
                step_generate_data_tiles(run_id, hours, Path(tmp), LocalObjectStore(Path(tmp)), layout, layers,
                                         max_zoom=args.max_zoom, tile_formats=formats)
                seconds = time.perf_counter() - start
                tiles = [f for f in (Path(tmp) / layout.staging_prefix(run_id) / "data_tiles").rglob("*") if f.is_file()]
                row = {"rep": rep, "profile": profile, "seconds": round(seconds, 2), "warps": warps,
                       "files": len(tiles), "bytes": sum(f.stat().st_size for f in tiles)}
                print(json.dumps(row), flush=True)
                results.setdefault(profile, []).append(row)

    print(f"\n{'profile':<10}{'median s':>10}{'min s':>8}{'max s':>8}{'warps':>8}{'files':>8}{'MB':>9}")
    for profile, rows in results.items():
        s = [r["seconds"] for r in rows]
        print(f"{profile:<10}{statistics.median(s):>10.1f}{min(s):>8.1f}{max(s):>8.1f}"
              f"{rows[0]['warps']:>8}{rows[0]['files']:>8}{rows[0]['bytes'] / 1e6:>9.1f}")


if __name__ == "__main__":
    main()
