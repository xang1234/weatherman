"""Regression tests for top-level docker-compose wiring."""

from __future__ import annotations

from pathlib import Path

import yaml


COMPOSE_PATH = Path(__file__).resolve().parents[1] / "docker-compose.yml"


def _load_compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text())


def test_neptune_live_service_uses_same_ais_db_path_as_backend() -> None:
    compose = _load_compose()
    services = compose["services"]

    backend_env = services["backend"]["environment"]
    live_env = services["ais-neptune-live"]["environment"]

    assert live_env["AIS_DB_PATH"] == backend_env["AIS_DB_PATH"]
