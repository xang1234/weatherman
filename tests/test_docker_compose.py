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


def test_sample_env_switches_live_ais_with_ais_live() -> None:
    """The ais-live profile is gone; a copied .env must not still ask for it."""
    sample = (COMPOSE_PATH.parent / ".env.example").read_text()
    keys = {line.split("=", 1)[0] for line in sample.splitlines() if "=" in line and not line.startswith("#")}
    assert "AIS_LIVE" in keys
    assert "COMPOSE_PROFILES" not in keys
