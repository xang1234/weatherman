"""Tests for the run lifecycle state machine."""

import pytest
import sqlalchemy as sa

from weatherman.storage.lifecycle import (
    RunLifecycle,
    RunState,
    VALID_TRANSITIONS,
    InvalidTransition,
    RunNotFound,
    DuplicateRun,
)
from weatherman.storage.paths import RunID


@pytest.fixture
def engine():
    """In-memory SQLite engine for testing."""
    return sa.create_engine("sqlite:///:memory:")


@pytest.fixture
def lifecycle(engine):
    lc = RunLifecycle(engine)
    lc.create_tables()
    return lc


RUN = RunID("20260306T00Z")
MODEL = "gfs"
VERSION = "1.0.0"


class TestRegister:
    def test_register_creates_run_in_discovered(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        assert lifecycle.get_state(MODEL, RUN, VERSION) == RunState.DISCOVERED

    def test_register_duplicate_raises(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        with pytest.raises(DuplicateRun):
            lifecycle.register(MODEL, RUN, VERSION)

    def test_different_processing_versions_allowed(self, lifecycle):
        lifecycle.register(MODEL, RUN, "1.0.0")
        lifecycle.register(MODEL, RUN, "2.0.0")
        assert lifecycle.get_state(MODEL, RUN, "1.0.0") == RunState.DISCOVERED
        assert lifecycle.get_state(MODEL, RUN, "2.0.0") == RunState.DISCOVERED

    def test_register_returns_id(self, lifecycle):
        row_id = lifecycle.register(MODEL, RUN, VERSION)
        assert isinstance(row_id, int)
        assert row_id > 0


class TestTransitions:
    def test_happy_path_full_lifecycle(self, lifecycle):
        """Advance through the entire forward lifecycle."""
        lifecycle.register(MODEL, RUN, VERSION)

        for state in [
            RunState.INGESTING,
            RunState.STAGED,
            RunState.VALIDATED,
            RunState.PUBLISHED,
            RunState.SUPERSEDED,
            RunState.EXPIRED,
        ]:
            lifecycle.transition(MODEL, RUN, VERSION, state)
            assert lifecycle.get_state(MODEL, RUN, VERSION) == state

    def test_skip_state_raises(self, lifecycle):
        """Cannot skip from DISCOVERED directly to STAGED."""
        lifecycle.register(MODEL, RUN, VERSION)
        with pytest.raises(InvalidTransition):
            lifecycle.transition(MODEL, RUN, VERSION, RunState.STAGED)

    def test_backward_transition_raises(self, lifecycle):
        """Cannot go backward (ingesting → discovered)."""
        lifecycle.register(MODEL, RUN, VERSION)
        lifecycle.transition(MODEL, RUN, VERSION, RunState.INGESTING)
        with pytest.raises(InvalidTransition):
            lifecycle.transition(MODEL, RUN, VERSION, RunState.DISCOVERED)

    def test_rollback_published_to_staged(self, lifecycle):
        """The only allowed backward transition: published → staged."""
        lifecycle.register(MODEL, RUN, VERSION)
        for s in [RunState.INGESTING, RunState.STAGED, RunState.VALIDATED, RunState.PUBLISHED]:
            lifecycle.transition(MODEL, RUN, VERSION, s)

        lifecycle.transition(
            MODEL, RUN, VERSION, RunState.STAGED, context="bad data, rolling back"
        )
        assert lifecycle.get_state(MODEL, RUN, VERSION) == RunState.STAGED

    def test_transition_nonexistent_run_raises(self, lifecycle):
        with pytest.raises(RunNotFound):
            lifecycle.transition(MODEL, RUN, VERSION, RunState.INGESTING)

    def test_expired_is_terminal(self, lifecycle):
        """No transitions out of EXPIRED."""
        lifecycle.register(MODEL, RUN, VERSION)
        for s in [
            RunState.INGESTING, RunState.STAGED, RunState.VALIDATED,
            RunState.PUBLISHED, RunState.SUPERSEDED, RunState.EXPIRED,
        ]:
            lifecycle.transition(MODEL, RUN, VERSION, s)

        with pytest.raises(InvalidTransition):
            lifecycle.transition(MODEL, RUN, VERSION, RunState.DISCOVERED)

    def test_transition_with_context(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        lifecycle.transition(
            MODEL, RUN, VERSION, RunState.INGESTING, context="download started"
        )
        history = lifecycle.get_history(MODEL, RUN, VERSION)
        assert history[-1]["context"] == "download started"


class TestValidTransitions:
    def test_all_states_have_transition_entry(self):
        for state in RunState:
            assert state in VALID_TRANSITIONS

    def test_rollback_is_only_backward(self):
        """Published → staged is the only backward transition."""
        state_order = list(RunState)
        for from_state, targets in VALID_TRANSITIONS.items():
            for target in targets:
                if state_order.index(target) < state_order.index(from_state):
                    assert from_state == RunState.PUBLISHED
                    assert target == RunState.STAGED


class TestGetRun:
    def test_get_run_returns_dict(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        run = lifecycle.get_run(MODEL, RUN, VERSION)
        assert run["model"] == MODEL
        assert run["run_id"] == str(RUN)
        assert run["state"] == RunState.DISCOVERED.value

    def test_get_run_not_found(self, lifecycle):
        with pytest.raises(RunNotFound):
            lifecycle.get_run(MODEL, RUN, VERSION)


class TestHistory:
    def test_history_records_all_transitions(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        lifecycle.transition(MODEL, RUN, VERSION, RunState.INGESTING)
        lifecycle.transition(MODEL, RUN, VERSION, RunState.STAGED)

        history = lifecycle.get_history(MODEL, RUN, VERSION)
        assert len(history) == 3  # register + 2 transitions
        assert history[0]["from_state"] == ""
        assert history[0]["to_state"] == RunState.DISCOVERED.value
        assert history[1]["from_state"] == RunState.DISCOVERED.value
        assert history[1]["to_state"] == RunState.INGESTING.value
        assert history[2]["to_state"] == RunState.STAGED.value


class TestListRuns:
    def test_list_all(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        lifecycle.register(MODEL, RunID("20260306T12Z"), VERSION)
        runs = lifecycle.list_runs(MODEL)
        assert len(runs) == 2

    def test_list_filtered_by_state(self, lifecycle):
        lifecycle.register(MODEL, RUN, VERSION)
        run2 = RunID("20260306T12Z")
        lifecycle.register(MODEL, run2, VERSION)
        lifecycle.transition(MODEL, run2, VERSION, RunState.INGESTING)

        discovered = lifecycle.list_runs(MODEL, state=RunState.DISCOVERED)
        assert len(discovered) == 1
        assert discovered[0]["run_id"] == str(RUN)
