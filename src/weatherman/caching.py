"""Shared HTTP caching utilities — ETag computation and validation.

Used by catalog, manifest, and EDR endpoints to implement conditional
requests (If-None-Match → 304 Not Modified) per RFC 9110.
"""

from __future__ import annotations

import hashlib


def compute_content_etag(data: bytes) -> str:
    """Compute a strong ETag from raw content bytes.

    Returns a quoted SHA-256 prefix, e.g. ``'"a1b2c3d4e5f67890"'``.
    """
    digest = hashlib.sha256(data).hexdigest()[:16]
    return f'"{digest}"'


def etag_matches(if_none_match: str, etag: str) -> bool:
    """Check if any ETag in an If-None-Match header matches.

    Handles multi-value headers (``"a", "b"``) and weak ETags (``W/"a"``),
    per RFC 9110 §8.8.3 (weak comparison for GET).
    """
    # Strip weak indicator from the server ETag once (RFC 9110 §8.8.3)
    target = etag[2:] if etag.startswith("W/") else etag
    for token in if_none_match.split(","):
        token = token.strip()
        if token == "*":
            return True
        # Strip weak indicator prefix for weak comparison
        if token.startswith("W/"):
            token = token[2:]
        if token == target:
            return True
    return False


# Common Cache-Control header values.
CACHE_IMMUTABLE = "public, max-age=31536000, immutable"
CACHE_REVALIDATE = "public, max-age=60, must-revalidate"
