"""Object storage abstraction for staging and publishing artifacts.

Provides a minimal protocol for operations needed by the publish pipeline:
list, copy, delete, read, and write. Implementations exist for local
filesystem (dev/test) and can be extended for S3.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Protocol


class ObjectStore(Protocol):
    """Minimal object storage interface for the publish pipeline."""

    def list_keys(self, prefix: str) -> list[str]:
        """List all keys under a prefix (recursive)."""
        ...

    def copy(self, src_key: str, dst_key: str) -> None:
        """Copy an object from src to dst key."""
        ...

    def delete(self, key: str) -> None:
        """Delete an object by key. No error if missing."""
        ...

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        ...

    def read_bytes(self, key: str) -> bytes:
        """Read the contents of an object."""
        ...

    def write_bytes(self, key: str, data: bytes) -> None:
        """Write data to an object (creates parent dirs/prefixes as needed)."""
        ...


class LocalObjectStore:
    """Filesystem-backed object store for development and testing.

    Keys are relative paths under the root directory.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def _resolve(self, key: str) -> Path:
        return self._root / key

    def list_keys(self, prefix: str) -> list[str]:
        base = self._resolve(prefix)
        if not base.exists():
            return []
        keys = []
        for p in sorted(base.rglob("*")):
            if p.is_file():
                keys.append(str(p.relative_to(self._root)))
        return keys

    def copy(self, src_key: str, dst_key: str) -> None:
        src = self._resolve(src_key)
        dst = self._resolve(dst_key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    def delete(self, key: str) -> None:
        p = self._resolve(key)
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p)

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def read_bytes(self, key: str) -> bytes:
        return self._resolve(key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self._resolve(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
