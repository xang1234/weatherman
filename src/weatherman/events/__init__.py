"""SSE push channel — real-time event streaming to clients.

Components:
- EventBus: in-memory pub/sub with per-tenant filtering and replay buffer
- router: FastAPI router with GET /events/stream SSE endpoint
"""

from weatherman.events.bus import EventBus, ServerEvent
from weatherman.events.router import router

__all__ = ["EventBus", "ServerEvent", "router"]
