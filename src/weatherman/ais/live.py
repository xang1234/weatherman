"""Live AIS ingest inside the backend process (#72).

DuckDB allows one process to write a database or several to read it, never
both, so a separate ingest process could not write while the backend served
tiles. With ``AIS_LIVE`` set, the backend opens the database read-write and
runs the Neptune live ingest on a background thread, on a cursor of the same
connection; tile requests keep reading through their own cursors.

Only one backend instance may run it: each would ingest the same stream.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable

import duckdb

logger = logging.getLogger(__name__)

# Backoff between attempts after the ingest stops or fails: 5 s doubling to 5 min.
_FIRST_RETRY_S = 5.0
_MAX_RETRY_S = 300.0


def live_ingest_enabled() -> bool:
    """Whether ``AIS_LIVE`` asks for live ingest in this process."""
    return os.environ.get("AIS_LIVE", "").strip().lower() in {"1", "true", "yes", "on"}


def start_live_ingest(
    con: duckdb.DuckDBPyConnection,
    *,
    db_path: str,
    tenant_id: str,
    run: Callable[..., object] | None = None,
) -> tuple[threading.Thread, threading.Event]:
    """Run live ingest on a daemon thread until the returned event is set.

    The stream reconnects by itself; if the ingest still returns or raises,
    it is restarted after a growing pause. `run` replaces the ingest (tests).
    """
    from weatherman.ais.neptune import (
        live_config_from_env,
        neptune_config_from_env,
        run_neptune_live_ingest,
    )

    run = run or run_neptune_live_ingest
    stop = threading.Event()

    def _loop() -> None:
        delay = _FIRST_RETRY_S
        while not stop.is_set():
            try:
                run(
                    live_config=live_config_from_env(),
                    archival_config=neptune_config_from_env(),
                    db_path=db_path,
                    tenant_id=tenant_id,
                    emit_event=True,
                    con=con.cursor(),  # never share a connection across threads
                )
                logger.warning("Live AIS ingest stopped; restarting in %.0fs", delay)
            except Exception:
                logger.exception("Live AIS ingest failed; restarting in %.0fs", delay)
            if stop.wait(delay):
                break
            delay = min(delay * 2, _MAX_RETRY_S)

    thread = threading.Thread(target=_loop, name="ais-live-ingest", daemon=True)
    thread.start()
    logger.info("Live AIS ingest started in-process", extra={"db_path": db_path})
    return thread, stop
