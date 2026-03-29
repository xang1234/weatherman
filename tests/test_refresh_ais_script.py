"""Regression tests for scripts/refresh_ais.py CLI input handling."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from weatherman.ais.refresh import AISBackend


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "refresh_ais.py"


def _load_refresh_script_module():
    spec = importlib.util.spec_from_file_location("refresh_ais_script", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_refresh_script_accepts_legacy_positional_order() -> None:
    module = _load_refresh_script_module()

    args = module.parse_args(
        [".data/ais/movement_date=2026-03-08/*", "2026-03-08"]
    )
    snapshot_date, parquet_path = module.resolve_inputs(
        backend=AISBackend(args.backend),
        inputs=args.inputs,
    )

    assert snapshot_date.isoformat() == "2026-03-08"
    assert parquet_path == ".data/ais/movement_date=2026-03-08/*"


def test_refresh_script_accepts_neptune_date_only() -> None:
    module = _load_refresh_script_module()

    args = module.parse_args(["2026-03-08", "--backend", "neptune"])
    snapshot_date, parquet_path = module.resolve_inputs(
        backend=AISBackend(args.backend),
        inputs=args.inputs,
    )

    assert snapshot_date.isoformat() == "2026-03-08"
    assert parquet_path is None


def test_refresh_script_reads_backend_and_db_path_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AIS_BACKEND", "neptune")
    monkeypatch.setenv("AIS_DB_PATH", "/tmp/test-ais.duckdb")
    module = _load_refresh_script_module()

    args = module.parse_args(["2026-03-08"])

    assert args.backend == "neptune"
    assert args.db_path == "/tmp/test-ais.duckdb"
