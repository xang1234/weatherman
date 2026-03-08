"""Atomic publish — staging to published artifact move + catalog update.

The publish operation is a 5-step sequence designed so that the catalog
update is the linearization point (commit). Object storage has no
multi-key transactions, so we rely on ordering:

    1. Copy artifacts from staging/ → runs/ (idempotent)
    2. Verify all artifacts exist in runs/
    3. Update catalog.json (THE COMMIT POINT — data becomes visible)
    4. Transition lifecycle state to PUBLISHED
    5. Delete staging copy (cleanup — failure is harmless)

Failure modes:
    - Step 1-2 fail: Nothing visible, staging preserved for debugging.
    - Step 3 fails: Orphaned artifacts in runs/ (invisible, no catalog entry).
    - Step 5 fails: Staging copy remains; can be cleaned up later.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from weatherman.storage.catalog import RunCatalog
from weatherman.storage.lifecycle import RunLifecycle, RunState
from weatherman.storage.locks import NullPublishLock, PublishLock
from weatherman.storage.object_store import ObjectStore
from weatherman.storage.paths import RunID, StorageLayout

# Type for the optional post-publish callback.
# Signature: (model, run_id, published_at) -> None
OnPublishedCallback = Callable[[str, RunID, datetime], None]

logger = logging.getLogger(__name__)


class PublishError(Exception):
    """Raised when the publish operation fails."""


class PublishVerificationError(PublishError):
    """Raised when artifact verification fails after copy."""


def publish_run(
    *,
    store: ObjectStore,
    layout: StorageLayout,
    catalog: RunCatalog,
    lifecycle: RunLifecycle,
    run_id: RunID,
    processing_version: str = "",
    now: datetime | None = None,
    lock: PublishLock | None = None,
    on_published: OnPublishedCallback | None = None,
) -> None:
    """Execute the atomic publish sequence for a validated run.

    Preconditions:
        - Artifacts exist under staging/<run_id>/
        - Lifecycle state is VALIDATED

    Args:
        store: Object storage backend.
        layout: Path layout for the model.
        catalog: Run catalog (will be mutated and saved).
        lifecycle: Run lifecycle manager.
        run_id: The run to publish.
        processing_version: Version string for idempotency key.
        now: Override for current time (testing).
        lock: Per-model advisory lock.  Prevents concurrent publishes
              for the same model from corrupting the catalog.  Defaults
              to ``NullPublishLock`` (no-op) when not provided.
        on_published: Optional callback invoked after successful publish
              (after catalog + lifecycle update, before staging cleanup).
              Signature: ``(model, run_id, published_at) -> None``.

    Raises:
        PublishError: If staging has no artifacts.
        PublishVerificationError: If copied artifacts fail verification.
        PublishLockTimeout: If the lock cannot be acquired in time.
    """
    now = now or datetime.now(timezone.utc)
    model = layout.model
    lock = lock or NullPublishLock()

    with lock(model):
        _publish_run_inner(
            store=store,
            layout=layout,
            catalog=catalog,
            lifecycle=lifecycle,
            run_id=run_id,
            processing_version=processing_version,
            now=now,
            on_published=on_published,
        )


def _publish_run_inner(
    *,
    store: ObjectStore,
    layout: StorageLayout,
    catalog: RunCatalog,
    lifecycle: RunLifecycle,
    run_id: RunID,
    processing_version: str,
    now: datetime,
    on_published: OnPublishedCallback | None = None,
) -> None:
    """Core publish logic, called while holding the per-model lock."""
    model = layout.model

    # -- Step 0: Validate preconditions --
    staging_prefix = layout.staging_prefix(run_id)
    staging_keys = store.list_keys(staging_prefix)
    if not staging_keys:
        raise PublishError(
            f"No staging artifacts found at {staging_prefix} for {model}/{run_id}"
        )

    logger.info(
        "Publishing %s/%s: %d artifacts from staging",
        model, run_id, len(staging_keys),
    )

    # -- Step 1: Copy staging → published --
    published_keys = []
    for src_key in staging_keys:
        # Replace staging/<run_id>/ with runs/<run_id>/
        relative = src_key[len(staging_prefix):]  # e.g. /zarr/..., /cogs/...
        dst_key = layout.run_prefix(run_id) + relative
        store.copy(src_key, dst_key)
        published_keys.append(dst_key)

    logger.info(
        "Copied %d artifacts to %s", len(published_keys), layout.run_prefix(run_id),
    )

    # -- Step 2: Verify all artifacts in published location --
    missing = [k for k in published_keys if not store.exists(k)]
    if missing:
        raise PublishVerificationError(
            f"Verification failed: {len(missing)} artifacts missing after copy: "
            f"{missing[:5]}"  # show first 5
        )

    # -- Step 3: Update catalog (THE COMMIT POINT) --
    catalog.publish_run(
        run_id,
        layout=layout,
        published_at=now,
        processing_version=processing_version,
    )
    _save_catalog(store, layout, catalog)

    logger.info("Catalog updated — %s/%s is now current", model, run_id)

    # -- Step 4: Transition lifecycle to PUBLISHED --
    lifecycle.transition(
        model, run_id, processing_version,
        RunState.PUBLISHED,
        context="artifacts published, catalog updated",
        now=now,
    )

    # -- Step 4b: Notify subscribers (best-effort, never blocks publish) --
    if on_published is not None:
        try:
            on_published(model, run_id, now)
        except Exception:
            logger.warning(
                "on_published callback failed for %s/%s",
                model, run_id, exc_info=True,
            )

    # -- Step 5: Clean up staging (best-effort) --
    _cleanup_staging(store, staging_keys, model, run_id)


def _save_catalog(
    store: ObjectStore, layout: StorageLayout, catalog: RunCatalog
) -> None:
    """Write catalog.json to the object store."""
    catalog_key = layout.catalog_path
    store.write_bytes(catalog_key, catalog.to_json().encode("utf-8"))


def _cleanup_staging(
    store: ObjectStore,
    staging_keys: list[str],
    model: str,
    run_id: RunID,
) -> None:
    """Delete staging artifacts. Failures are logged but not raised."""
    cleaned = 0
    for key in staging_keys:
        try:
            store.delete(key)
            cleaned += 1
        except Exception:
            logger.warning("Failed to delete staging artifact: %s", key, exc_info=True)
    logger.info(
        "Staging cleanup for %s/%s: %d/%d deleted",
        model, run_id, cleaned, len(staging_keys),
    )
