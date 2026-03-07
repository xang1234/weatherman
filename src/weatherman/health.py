"""Health check endpoints for Kubernetes probes.

/health/live — Liveness probe: process is running, event loop is responsive.
               No dependency checks. Must respond in <5ms.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", summary="Liveness probe")
async def liveness() -> dict:
    """Return 200 OK if the process is alive and the event loop is responsive."""
    return {
        "status": "alive",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
