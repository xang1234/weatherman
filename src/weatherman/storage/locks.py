"""Per-model advisory locks for publish concurrency safety.

Prevents two concurrent pipeline runs for the same model from racing
to update the catalog simultaneously.  Lock scope is per-model so that
different models (e.g. GFS and ECMWF) can publish concurrently.

Implementations:
    FilePublishLock  — file-based (fcntl.flock), suitable for single-node / dev.
    NullPublishLock  — no-op, for testing or guaranteed single-writer setups.

For multi-node production, add a PostgreSQL advisory lock implementation
using pg_advisory_lock with a hash of the model name.
"""

from __future__ import annotations

import fcntl
import logging
import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60.0  # seconds
_POLL_INTERVAL = 0.1  # seconds between non-blocking lock attempts


class PublishLockTimeout(Exception):
    """Raised when a publish lock cannot be acquired within the timeout."""


class PublishLock(ABC):
    """Advisory lock acquired around the publish-to-catalog critical section."""

    @abstractmethod
    @contextmanager
    def __call__(self, model: str) -> Generator[None, None, None]:
        """Context manager that holds the lock for *model*."""
        ...


class FilePublishLock(PublishLock):
    """File-based advisory lock using ``fcntl.flock``.

    Creates one lock file per model under *lock_dir*.  Uses non-blocking
    flock with a retry loop so that a timeout can be enforced without
    depending on signals or threads.
    """

    def __init__(self, lock_dir: Path, *, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._lock_dir = lock_dir
        self._timeout = timeout

    @contextmanager
    def __call__(self, model: str) -> Generator[None, None, None]:
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._lock_dir / f"{model}.publish.lock"
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        acquired = False
        try:
            deadline = time.monotonic() + self._timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise PublishLockTimeout(
                            f"Could not acquire publish lock for model {model!r} "
                            f"within {self._timeout}s"
                        )
                    time.sleep(_POLL_INTERVAL)

            logger.info("Acquired publish lock for model %s", model)
            yield
        finally:
            if acquired:
                fcntl.flock(fd, fcntl.LOCK_UN)
                logger.info("Released publish lock for model %s", model)
            os.close(fd)


class NullPublishLock(PublishLock):
    """No-op lock for testing or single-writer environments."""

    @contextmanager
    def __call__(self, model: str) -> Generator[None, None, None]:
        yield
