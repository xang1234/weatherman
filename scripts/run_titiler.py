#!/usr/bin/env python
"""Run a minimal TiTiler server for local development.

Serves COG tiles from the local filesystem — no Docker or S3 required.

Usage:
    uv run python scripts/run_titiler.py
    uv run python scripts/run_titiler.py --port 8080
"""

from __future__ import annotations

import argparse

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from titiler.core.factory import TilerFactory

app = FastAPI(title="Weatherman TiTiler (dev)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

cog = TilerFactory()
app.include_router(cog.router, prefix="/cog")


@app.get("/api")
async def health():
    """Health check endpoint (matches production TiTiler)."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(description="Run TiTiler dev server")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=args.port,
        log_level="info",
    )
