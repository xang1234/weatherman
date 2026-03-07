"""Run lifecycle state machine — tracks runs through the processing pipeline.

States: discovered → ingesting → staged → validated → published → superseded → expired

The only backward transition is rollback: published → staged.

Each run is uniquely identified by its idempotency key: (model, run_id, processing_version).
State transitions are atomic and logged for audit.

Storage: SQLite for dev, PostgreSQL for prod (via SQLAlchemy).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, String, DateTime, Integer, Text, Index

from weatherman.storage.paths import RunID

logger = logging.getLogger(__name__)

metadata = MetaData()

# -- Schema --

run_lifecycle = Table(
    "run_lifecycle",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("model", String(64), nullable=False),
    Column("run_id", String(32), nullable=False),
    Column("processing_version", String(32), nullable=False, server_default="1.0.0"),
    Column("state", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Index("uq_idempotency", "model", "run_id", "processing_version", unique=True),
)

transition_log = Table(
    "transition_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("lifecycle_id", Integer, sa.ForeignKey("run_lifecycle.id"), nullable=False),
    Column("from_state", String(32), nullable=False),
    Column("to_state", String(32), nullable=False),
    Column("context", Text, nullable=True),
    Column("transitioned_at", DateTime(timezone=True), nullable=False),
    Index("ix_transition_lifecycle", "lifecycle_id"),
)


class RunState(Enum):
    """Lifecycle states for a weather model run."""

    DISCOVERED = "discovered"
    INGESTING = "ingesting"
    STAGED = "staged"
    VALIDATED = "validated"
    PUBLISHED = "published"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"


# Valid forward transitions + rollback (published → staged)
VALID_TRANSITIONS: dict[RunState, set[RunState]] = {
    RunState.DISCOVERED: {RunState.INGESTING},
    RunState.INGESTING: {RunState.STAGED},
    RunState.STAGED: {RunState.VALIDATED},
    RunState.VALIDATED: {RunState.PUBLISHED},
    RunState.PUBLISHED: {RunState.SUPERSEDED, RunState.STAGED},  # staged = rollback
    RunState.SUPERSEDED: {RunState.EXPIRED},
    RunState.EXPIRED: set(),
}


class InvalidTransition(Exception):
    """Raised when a state transition is not allowed."""


class RunNotFound(Exception):
    """Raised when a run is not found by its idempotency key."""


class DuplicateRun(Exception):
    """Raised when a run with the same idempotency key already exists."""


class RunLifecycle:
    """Manages run lifecycle state transitions backed by a SQL database.

    Usage::

        engine = sa.create_engine("sqlite:///runs.db")
        lifecycle = RunLifecycle(engine)
        lifecycle.create_tables()

        # Register a new run
        lifecycle.register("gfs", RunID("20260306T00Z"), "1.0.0")

        # Advance through states
        lifecycle.transition("gfs", RunID("20260306T00Z"), "1.0.0",
                             RunState.INGESTING, context="download started")
    """

    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def create_tables(self) -> None:
        """Create lifecycle tables if they don't exist."""
        metadata.create_all(self._engine)

    def register(
        self,
        model: str,
        run_id: RunID,
        processing_version: str = "1.0.0",
        *,
        now: datetime | None = None,
    ) -> int:
        """Register a new run in DISCOVERED state.

        Returns the lifecycle row ID.

        Raises:
            DuplicateRun: If a run with this idempotency key already exists.
        """
        now = now or datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            try:
                result = conn.execute(
                    run_lifecycle.insert().values(
                        model=model,
                        run_id=str(run_id),
                        processing_version=processing_version,
                        state=RunState.DISCOVERED.value,
                        created_at=now,
                        updated_at=now,
                    )
                )
            except sa.exc.IntegrityError:
                raise DuplicateRun(
                    f"Run already exists: {model}/{run_id}/{processing_version}"
                )
            row_id = result.inserted_primary_key[0]

            # Log the initial state
            conn.execute(
                transition_log.insert().values(
                    lifecycle_id=row_id,
                    from_state="",
                    to_state=RunState.DISCOVERED.value,
                    context="run registered",
                    transitioned_at=now,
                )
            )

        logger.info(
            "Registered run %s/%s (v%s) → DISCOVERED",
            model, run_id, processing_version,
        )
        return row_id

    def transition(
        self,
        model: str,
        run_id: RunID,
        processing_version: str,
        to_state: RunState,
        *,
        context: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """Transition a run to a new state.

        Raises:
            RunNotFound: If no run matches the idempotency key.
            InvalidTransition: If the transition is not valid.
        """
        now = now or datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            row = conn.execute(
                sa.select(run_lifecycle.c.id, run_lifecycle.c.state)
                .where(
                    run_lifecycle.c.model == model,
                    run_lifecycle.c.run_id == str(run_id),
                    run_lifecycle.c.processing_version == processing_version,
                )
                .with_for_update()
            ).first()

            if row is None:
                raise RunNotFound(
                    f"Run not found: {model}/{run_id}/{processing_version}"
                )

            current_state = RunState(row.state)
            if to_state not in VALID_TRANSITIONS[current_state]:
                raise InvalidTransition(
                    f"Cannot transition {model}/{run_id} from "
                    f"{current_state.value} → {to_state.value}"
                )

            conn.execute(
                run_lifecycle.update()
                .where(run_lifecycle.c.id == row.id)
                .values(state=to_state.value, updated_at=now)
            )

            conn.execute(
                transition_log.insert().values(
                    lifecycle_id=row.id,
                    from_state=current_state.value,
                    to_state=to_state.value,
                    context=context,
                    transitioned_at=now,
                )
            )

        logger.info(
            "Transition %s/%s (v%s): %s → %s%s",
            model, run_id, processing_version,
            current_state.value, to_state.value,
            f" ({context})" if context else "",
        )

    def get_state(
        self, model: str, run_id: RunID, processing_version: str
    ) -> RunState:
        """Get the current state of a run.

        Raises:
            RunNotFound: If no run matches the idempotency key.
        """
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(run_lifecycle.c.state).where(
                    run_lifecycle.c.model == model,
                    run_lifecycle.c.run_id == str(run_id),
                    run_lifecycle.c.processing_version == processing_version,
                )
            ).first()
            if row is None:
                raise RunNotFound(
                    f"Run not found: {model}/{run_id}/{processing_version}"
                )
            return RunState(row.state)

    def get_run(
        self, model: str, run_id: RunID, processing_version: str
    ) -> dict[str, Any]:
        """Get full run record as a dict.

        Raises:
            RunNotFound: If no run matches the idempotency key.
        """
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(run_lifecycle).where(
                    run_lifecycle.c.model == model,
                    run_lifecycle.c.run_id == str(run_id),
                    run_lifecycle.c.processing_version == processing_version,
                )
            ).first()
            if row is None:
                raise RunNotFound(
                    f"Run not found: {model}/{run_id}/{processing_version}"
                )
            return row._asdict()

    def get_history(
        self, model: str, run_id: RunID, processing_version: str
    ) -> list[dict[str, Any]]:
        """Get the full transition history for a run.

        Raises:
            RunNotFound: If no run matches the idempotency key.
        """
        with self._engine.connect() as conn:
            # First get the lifecycle ID
            row = conn.execute(
                sa.select(run_lifecycle.c.id).where(
                    run_lifecycle.c.model == model,
                    run_lifecycle.c.run_id == str(run_id),
                    run_lifecycle.c.processing_version == processing_version,
                )
            ).first()
            if row is None:
                raise RunNotFound(
                    f"Run not found: {model}/{run_id}/{processing_version}"
                )

            rows = conn.execute(
                sa.select(transition_log)
                .where(transition_log.c.lifecycle_id == row.id)
                .order_by(transition_log.c.id)
            ).fetchall()
            return [r._asdict() for r in rows]

    def list_runs(
        self,
        model: str,
        *,
        state: RunState | None = None,
    ) -> list[dict[str, Any]]:
        """List runs for a model, optionally filtered by state."""
        with self._engine.connect() as conn:
            query = sa.select(run_lifecycle).where(
                run_lifecycle.c.model == model
            )
            if state is not None:
                query = query.where(run_lifecycle.c.state == state.value)
            query = query.order_by(run_lifecycle.c.run_id.desc())
            return [r._asdict() for r in conn.execute(query).fetchall()]
