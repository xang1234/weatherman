"""CLI entrypoint: uv run python -m weatherman."""

from weatherman.app import create_app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "weatherman.__main__:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        # The SSE stream (/events/stream) never finishes on its own. Without a
        # limit, a reload or a Ctrl+C waits for it forever: the server stops
        # answering and the old process keeps the port.
        timeout_graceful_shutdown=3,
        log_level="info",
    )
