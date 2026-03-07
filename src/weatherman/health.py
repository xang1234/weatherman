"""Health check endpoints for Kubernetes probes.

/health/live  — Liveness probe: process is running, event loop is responsive.
                No dependency checks. Must respond in <5ms.
/health/ready — Readiness probe: downstream dependencies are reachable.
                Returns 200 when ready, 503 when not ready.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from fastapi import APIRouter
from starlette.responses import JSONResponse

# -- Dependency check protocol --


class DependencyChecker(Protocol):
    """Protocol for a single dependency health check."""

    @property
    def name(self) -> str:
        """Short identifier for this dependency (e.g. 's3', 'titiler')."""
        ...

    @property
    def critical(self) -> bool:
        """If True, failure means not_ready. If False, failure means degraded."""
        ...

    async def check(self) -> bool:
        """Return True if the dependency is reachable/healthy."""
        ...


# -- Cached readiness state --

_CHECK_CACHE_TTL_S = 5.0
_PER_CHECK_TIMEOUT_S = 2.0
_TOTAL_TIMEOUT_S = 5.0


@dataclass
class CheckResult:
    name: str
    status: str  # "ok" or "fail"
    latency_ms: float
    critical: bool


@dataclass
class ReadinessState:
    """Cached readiness check results."""

    status: str  # "ready", "degraded", "not_ready"
    checks: dict[str, dict]
    timestamp: str
    _cached_at: float = 0.0


_cached_state: ReadinessState | None = None


async def _run_single_check(checker: DependencyChecker) -> CheckResult:
    """Run a single dependency check with a per-check timeout."""
    start = time.monotonic()
    try:
        ok = await asyncio.wait_for(
            checker.check(), timeout=_PER_CHECK_TIMEOUT_S
        )
        elapsed_ms = (time.monotonic() - start) * 1000
        return CheckResult(
            name=checker.name,
            status="ok" if ok else "fail",
            latency_ms=round(elapsed_ms, 2),
            critical=checker.critical,
        )
    except (TimeoutError, Exception):
        elapsed_ms = (time.monotonic() - start) * 1000
        return CheckResult(
            name=checker.name,
            status="fail",
            latency_ms=round(elapsed_ms, 2),
            critical=checker.critical,
        )


async def _evaluate_readiness(
    checkers: list[DependencyChecker],
) -> ReadinessState:
    """Run all dependency checks and compute aggregate status."""
    if not checkers:
        return ReadinessState(
            status="ready",
            checks={},
            timestamp=datetime.now(timezone.utc).isoformat(),
            _cached_at=time.monotonic(),
        )

    results = await asyncio.gather(
        *[_run_single_check(c) for c in checkers]
    )

    checks = {}
    any_critical_fail = False
    any_fail = False
    for r in results:
        checks[r.name] = {
            "status": r.status,
            "latency_ms": r.latency_ms,
        }
        if r.status == "fail":
            any_fail = True
            if r.critical:
                any_critical_fail = True

    if any_critical_fail:
        status = "not_ready"
    elif any_fail:
        status = "degraded"
    else:
        status = "ready"

    return ReadinessState(
        status=status,
        checks=checks,
        timestamp=datetime.now(timezone.utc).isoformat(),
        _cached_at=time.monotonic(),
    )


# -- Router and endpoints --

router = APIRouter(prefix="/health", tags=["health"])

# Registered dependency checkers — populated via register_check()
_checkers: list[DependencyChecker] = []


def register_check(checker: DependencyChecker) -> None:
    """Register a dependency checker for the readiness probe."""
    _checkers.append(checker)


def clear_checks() -> None:
    """Remove all registered checkers (for testing)."""
    global _cached_state
    _checkers.clear()
    _cached_state = None


@router.get("/live", summary="Liveness probe")
async def liveness() -> dict:
    """Return 200 OK if the process is alive and the event loop is responsive."""
    return {
        "status": "alive",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/ready", summary="Readiness probe")
async def readiness() -> JSONResponse:
    """Check downstream dependencies and return readiness status.

    Returns 200 for ready/degraded, 503 for not_ready.
    """
    global _cached_state

    now = time.monotonic()
    if _cached_state is not None and (now - _cached_state._cached_at) < _CHECK_CACHE_TTL_S:
        state = _cached_state
    else:
        state = await _evaluate_readiness(_checkers)
        _cached_state = state

    body = {
        "status": state.status,
        "checks": state.checks,
        "timestamp": state.timestamp,
    }

    status_code = 200 if state.status in ("ready", "degraded") else 503
    return JSONResponse(content=body, status_code=status_code)
