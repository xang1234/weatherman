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
        log_level="info",
    )
