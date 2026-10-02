"""Regression tests for top-level docker-compose wiring."""

from __future__ import annotations

from pathlib import Path

import yaml


COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"


def _load_compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text())


def test_live_ais_runs_inside_the_backend() -> None:
    """DuckDB can't share ais.duckdb between a writing and a reading process (#72)."""
    services = _load_compose()["services"]

    assert "ais-neptune-live" not in services
    assert "AIS_LIVE" in services["backend"]["environment"]
