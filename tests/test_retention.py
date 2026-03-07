"""Tests for the retention policy stub (wx-ir5.1.3)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from weatherman.storage.catalog import RunCatalog, RunStatus
from weatherman.storage.paths import RunID, StorageLayout
from weatherman.storage.retention import (
    RetentionPolicy,
    RetentionRule,
    dry_run_retention,
    evaluate_retention,
)


# -- Helpers --

def _make_catalog(model: str = "gfs", num_runs: int = 30) -> RunCatalog:
    """Build a catalog with ``num_runs`` superseded runs + 1 current published run."""
    layout = StorageLayout(model)
    catalog = RunCatalog.new(model)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    for i in range(num_runs):
        hour = (i % 4) * 6  # cycle through 00, 06, 12, 18
        day = 1 + i // 4
        run_id = RunID(f"202601{day:02d}T{hour:02d}Z")
        catalog.publish_run(
            run_id,
            layout=layout,
            published_at=base + timedelta(hours=i * 6),
            processing_version="1.0.0",
        )

    return catalog


# -- RetentionRule tests --


class TestRetentionRule:
    def test_defaults(self):
        rule = RetentionRule()
        assert rule.max_age_days == 7
        assert rule.max_runs == 28

    def test_custom_values(self):
        rule = RetentionRule(max_age_days=14, max_runs=56)
        assert rule.max_age_days == 14
        assert rule.max_runs == 56

    def test_invalid_max_age_days(self):
        with pytest.raises(ValueError, match="max_age_days"):
            RetentionRule(max_age_days=0)

    def test_invalid_max_runs(self):
        with pytest.raises(ValueError, match="max_runs"):
            RetentionRule(max_runs=0)


# -- RetentionPolicy tests --


class TestRetentionPolicy:
    def test_default_rule(self):
        policy = RetentionPolicy()
        assert policy.rule_for("gfs") == RetentionRule()
        assert policy.rule_for("unknown_model") == RetentionRule()

    def test_per_model_override(self):
        override = RetentionRule(max_age_days=3, max_runs=16)
        policy = RetentionPolicy(overrides={"gfs": override})
        assert policy.rule_for("gfs") == override
        assert policy.rule_for("icon_global") == RetentionRule()  # default

    def test_from_dict(self):
        data = {
            "default": {"max_age_days": 7, "max_runs": 28},
            "overrides": {
                "gfs": {"max_age_days": 3, "max_runs": 16},
            },
        }
        policy = RetentionPolicy.from_dict(data)
        assert policy.default == RetentionRule(max_age_days=7, max_runs=28)
        assert policy.rule_for("gfs") == RetentionRule(max_age_days=3, max_runs=16)

    def test_from_dict_empty(self):
        policy = RetentionPolicy.from_dict({})
        assert policy.default == RetentionRule()
        assert policy.overrides == {}

    def test_roundtrip(self):
        policy = RetentionPolicy(
            default=RetentionRule(max_age_days=10, max_runs=40),
            overrides={"gfs": RetentionRule(max_age_days=3, max_runs=12)},
        )
        restored = RetentionPolicy.from_dict(policy.to_dict())
        assert restored.default == policy.default
        assert restored.overrides == policy.overrides


# -- evaluate_retention tests --


class TestEvaluateRetention:
    def test_no_candidates_when_under_max_runs(self):
        """Even old runs are kept if catalog has fewer than max_runs entries."""
        catalog = _make_catalog(num_runs=5)  # 5 total (4 superseded + 1 published)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=28))
        now = datetime(2026, 3, 1, tzinfo=timezone.utc)  # well past age threshold
        candidates = evaluate_retention(catalog, policy, now=now)
        assert candidates == []

    def test_no_candidates_at_exact_max_runs_boundary(self):
        """At exactly max_runs non-expired entries, no expiry is triggered."""
        catalog = _make_catalog(num_runs=10)  # 10 entries (9 superseded + 1 published)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=10))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        assert candidates == []

    def test_candidates_when_over_max_runs(self):
        """At max_runs + 1 non-expired entries, old runs become candidates."""
        catalog = _make_catalog(num_runs=11)  # 11 entries, max_runs=10 -> over limit
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=10))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        assert len(candidates) > 0

    def test_no_candidates_when_runs_are_recent(self):
        """Runs within max_age_days are kept even if over max_runs."""
        catalog = _make_catalog(num_runs=30)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=365, max_runs=5))
        now = datetime(2026, 1, 10, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        assert candidates == []

    def test_candidates_when_both_thresholds_exceeded(self):
        """Runs that are old AND catalog exceeds max_runs are candidates."""
        catalog = _make_catalog(num_runs=30)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=5))
        # Far in the future so all superseded runs exceed age threshold
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        # Should have candidates — all superseded runs older than 1 day
        assert len(candidates) > 0
        # All candidates must be superseded
        for c in candidates:
            assert c.status == RunStatus.SUPERSEDED

    def test_current_run_never_a_candidate(self):
        """The current (published) run is never flagged for expiry."""
        catalog = _make_catalog(num_runs=30)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=5))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        candidate_ids = {c.run_id for c in candidates}
        assert catalog.current_run_id not in candidate_ids

    def test_already_expired_not_re_evaluated(self):
        """Runs already in EXPIRED status are not returned as candidates."""
        catalog = _make_catalog(num_runs=30)
        # Manually expire a superseded run
        superseded = catalog.superseded_runs()
        assert len(superseded) > 0
        catalog.expire_run(superseded[0].run_id)

        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=5))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        candidate_ids = {c.run_id for c in candidates}
        assert superseded[0].run_id not in candidate_ids

    def test_per_model_override_applied(self):
        """Per-model overrides change the effective thresholds."""
        catalog = _make_catalog(num_runs=30)
        # Default: very lenient. GFS override: strict.
        policy = RetentionPolicy(
            default=RetentionRule(max_age_days=365, max_runs=100),
            overrides={"gfs": RetentionRule(max_age_days=1, max_runs=5)},
        )
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        candidates = evaluate_retention(catalog, policy, now=now)
        assert len(candidates) > 0

    def test_empty_catalog(self):
        """An empty catalog produces no candidates."""
        catalog = RunCatalog.new("gfs")
        policy = RetentionPolicy()
        candidates = evaluate_retention(catalog, policy)
        assert candidates == []


# -- dry_run_retention tests --


class TestDryRunRetention:
    def test_returns_structured_records(self):
        catalog = _make_catalog(num_runs=30)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=5))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        results = dry_run_retention(catalog, policy, now=now)
        assert len(results) > 0
        for rec in results:
            assert "run_id" in rec
            assert "model" in rec
            assert "age_days" in rec
            assert "reason" in rec
            assert rec["model"] == "gfs"

    def test_empty_when_nothing_eligible(self):
        catalog = _make_catalog(num_runs=3)
        policy = RetentionPolicy()
        results = dry_run_retention(catalog, policy)
        assert results == []

    def test_logs_output(self, caplog):
        """Verify dry-run produces log messages."""
        catalog = _make_catalog(num_runs=30)
        policy = RetentionPolicy(default=RetentionRule(max_age_days=1, max_runs=5))
        now = datetime(2026, 6, 1, tzinfo=timezone.utc)
        with caplog.at_level("INFO"):
            dry_run_retention(catalog, policy, now=now)
        assert "retention dry-run" in caplog.text
