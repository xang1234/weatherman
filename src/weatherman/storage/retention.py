"""Retention policy definition and evaluation stub.

Defines configurable retention rules (max_age_days, max_runs) with per-model
overrides.  Evaluates which catalog runs *would* be expired, but does NOT
perform actual deletion — that is deferred to Phase 4 GC.

Default policy: 7 days / 28 runs (whichever is more conservative — i.e. a run
must exceed BOTH thresholds to be considered expired).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from weatherman.storage.catalog import RunCatalog, RunEntry, RunStatus

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetentionRule:
    """Retention thresholds for a single model (or the default).

    A superseded run is eligible for expiry only if it exceeds BOTH:
      - max_age_days since it was superseded
      - the catalog has more than max_runs total non-expired entries
    """

    max_age_days: int = 7
    max_runs: int = 28

    def __post_init__(self) -> None:
        if self.max_age_days < 1:
            raise ValueError(f"max_age_days must be >= 1, got {self.max_age_days}")
        if self.max_runs < 1:
            raise ValueError(f"max_runs must be >= 1, got {self.max_runs}")


@dataclass(frozen=True)
class RetentionPolicy:
    """Retention policy with a default rule and optional per-model overrides.

    Example config dict::

        {
            "default": {"max_age_days": 7, "max_runs": 28},
            "overrides": {
                "gfs": {"max_age_days": 3, "max_runs": 16},
                "icon_global": {"max_age_days": 14, "max_runs": 56}
            }
        }
    """

    default: RetentionRule = field(default_factory=RetentionRule)
    overrides: dict[str, RetentionRule] = field(default_factory=dict)

    def rule_for(self, model: str) -> RetentionRule:
        """Return the retention rule for a model (falls back to default)."""
        return self.overrides.get(model, self.default)

    @classmethod
    def from_dict(cls, data: dict) -> RetentionPolicy:
        """Build a policy from a configuration dict."""
        default_data = data.get("default", {})
        default_rule = RetentionRule(**default_data) if default_data else RetentionRule()

        overrides: dict[str, RetentionRule] = {}
        for model, rule_data in data.get("overrides", {}).items():
            overrides[model] = RetentionRule(**rule_data)

        return cls(default=default_rule, overrides=overrides)

    def to_dict(self) -> dict:
        """Serialize the policy to a configuration dict."""
        result: dict = {
            "default": {
                "max_age_days": self.default.max_age_days,
                "max_runs": self.default.max_runs,
            },
        }
        if self.overrides:
            result["overrides"] = {
                model: {
                    "max_age_days": rule.max_age_days,
                    "max_runs": rule.max_runs,
                }
                for model, rule in self.overrides.items()
            }
        return result


def evaluate_retention(
    catalog: RunCatalog,
    policy: RetentionPolicy,
    *,
    now: datetime | None = None,
) -> list[RunEntry]:
    """Determine which runs would be expired under the given policy.

    Returns a list of RunEntry objects that are candidates for expiry.
    Does NOT mutate the catalog — this is a read-only evaluation.

    A run is an expiry candidate if ALL of the following are true:
      1. It is in SUPERSEDED status (never expire PUBLISHED or already EXPIRED)
      2. It is NOT the current run
      3. It was superseded more than ``max_age_days`` ago
      4. The catalog has more than ``max_runs`` non-expired entries

    The "both thresholds" approach is conservative: it keeps runs that are
    recent OR when the catalog is still small.
    """
    now = now or datetime.now(timezone.utc)
    rule = policy.rule_for(catalog.model)

    non_expired = [e for e in catalog.runs if e.status != RunStatus.EXPIRED]
    under_max_runs = len(non_expired) <= rule.max_runs

    candidates: list[RunEntry] = []
    age_cutoff = now - timedelta(days=rule.max_age_days)

    for entry in catalog.runs:
        if entry.status != RunStatus.SUPERSEDED:
            continue
        if catalog.current_run_id == entry.run_id:
            continue
        if entry.superseded_at is None:
            continue
        if entry.superseded_at > age_cutoff:
            continue
        if under_max_runs:
            continue
        candidates.append(entry)

    return candidates


def dry_run_retention(
    catalog: RunCatalog,
    policy: RetentionPolicy,
    *,
    now: datetime | None = None,
) -> list[dict]:
    """Evaluate retention and return a log-friendly summary of what would expire.

    This is the "dry-run mode" mentioned in the spec — it logs what would be
    expired without taking any action.
    """
    now = now or datetime.now(timezone.utc)
    candidates = evaluate_retention(catalog, policy, now=now)
    rule = policy.rule_for(catalog.model)
    non_expired_count = sum(1 for e in catalog.runs if e.status != RunStatus.EXPIRED)

    results = []
    for entry in candidates:
        age_days = (
            (now - entry.superseded_at).days
            if entry.superseded_at
            else 0
        )
        record = {
            "run_id": str(entry.run_id),
            "model": catalog.model,
            "status": entry.status.value,
            "superseded_at": entry.superseded_at.isoformat() if entry.superseded_at else None,
            "age_days": age_days,
            "reason": f"age={age_days}d > {rule.max_age_days}d AND "
            f"runs={non_expired_count} > {rule.max_runs}",
        }
        results.append(record)
        logger.info(
            "retention dry-run: would expire %s (model=%s, %s)",
            entry.run_id,
            catalog.model,
            record["reason"],
        )

    if not candidates:
        logger.info(
            "retention dry-run: no runs eligible for expiry "
            "(model=%s, non_expired=%d, max_runs=%d, max_age_days=%d)",
            catalog.model,
            non_expired_count,
            rule.max_runs,
            rule.max_age_days,
        )

    return results
