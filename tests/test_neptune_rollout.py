"""Smoke tests for Neptune rollout config surfaces."""

from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT / ".env.example"
DOCS_PATH = ROOT / "docs" / "ais-neptune.md"
HELM_VALUES = ROOT / "infrastructure" / "helm" / "weatherman" / "values.yaml"
COMPOSE_PATH = ROOT / "docker-compose.yml"


def _parse_env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def test_rollout_env_surfaces_include_shared_event_journal_path() -> None:
    env_values = _parse_env_example()
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    helm_values = yaml.safe_load(HELM_VALUES.read_text(encoding="utf-8"))
    services = compose["services"]

    assert env_values["WEATHERMAN_EVENT_JOURNAL_PATH"] == "/data/events/sse-events.jsonl"
    assert (
        services["backend"]["environment"]["WEATHERMAN_EVENT_JOURNAL_PATH"]
        == services["ais-neptune-live"]["environment"]["WEATHERMAN_EVENT_JOURNAL_PATH"]
    )
    assert (
        helm_values["global"]["env"]["WEATHERMAN_EVENT_JOURNAL_PATH"]
        == "/data/events/sse-events.jsonl"
    )


def test_neptune_docs_cover_parity_and_shared_journal_rollout() -> None:
    docs = DOCS_PATH.read_text(encoding="utf-8")

    assert "compare_ais_parity.py" in docs
    assert "WEATHERMAN_EVENT_JOURNAL_PATH" in docs
    assert "shared filesystem path" in docs
