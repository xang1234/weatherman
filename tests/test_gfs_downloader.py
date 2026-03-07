"""Tests for the Herbie-based GFS downloader.

All tests use mocked Herbie to avoid network calls.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from weatherman.ingest.gfs import (
    DEFAULT_FORECAST_HOURS,
    DEFAULT_SEARCH_PATTERNS,
    GFS_CYCLE_HOURS,
    CycleDownloadResult,
    DownloadResult,
    check_cycle_available,
    download_gfs_cycle,
    download_variable,
    latest_available_cycle,
)
from weatherman.storage.paths import RunID


@pytest.fixture()
def staging_dir(tmp_path: Path) -> Path:
    return tmp_path / "staging"


@pytest.fixture()
def run_id() -> RunID:
    return RunID("20260306T00Z")


class TestDownloadVariable:
    def test_downloads_and_stages_file(
        self, run_id: RunID, staging_dir: Path, tmp_path: Path
    ) -> None:
        # Create a fake GRIB2 file that Herbie would produce
        fake_grib = tmp_path / "fake.grib2"
        fake_grib.write_bytes(b"\x00" * 1024)

        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.download.return_value = fake_grib
            MockHerbie.return_value = mock_h

            result = download_variable(
                run_id=run_id,
                forecast_hour=6,
                variable_name="tmp_2m",
                search_pattern=":TMP:2 m above ground:",
                staging_dir=staging_dir,
            )

        assert result.variable == "tmp_2m"
        assert result.forecast_hour == 6
        assert result.size_bytes == 1024
        assert result.local_path.exists()
        assert result.local_path.name == "f006.grib2"

        # Verify Herbie was called with correct params
        MockHerbie.assert_called_once_with(
            "2026-03-06 00:00",
            model="gfs",
            product="pgrb2.0p25",
            fxx=6,
        )
        mock_h.download.assert_called_once_with(":TMP:2 m above ground:")

    def test_raises_on_nonexistent_download(
        self, run_id: RunID, staging_dir: Path, tmp_path: Path
    ) -> None:
        # Simulate Herbie returning a path to a file that doesn't exist
        nonexistent = tmp_path / "does_not_exist.grib2"

        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.download.return_value = nonexistent
            MockHerbie.return_value = mock_h

            with pytest.raises(FileNotFoundError, match="no file"):
                download_variable(
                    run_id=run_id,
                    forecast_hour=0,
                    variable_name="tmp_2m",
                    search_pattern=":TMP:2 m above ground:",
                    staging_dir=staging_dir,
                )


class TestDownloadGfsCycle:
    def test_downloads_all_variables_and_hours(
        self, run_id: RunID, staging_dir: Path, tmp_path: Path
    ) -> None:
        fake_grib = tmp_path / "fake.grib2"
        fake_grib.write_bytes(b"\x00" * 512)

        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.download.return_value = fake_grib
            MockHerbie.return_value = mock_h

            result = download_gfs_cycle(
                run_id=run_id,
                staging_dir=staging_dir,
                forecast_hours=[0, 3],
                variables={"tmp_2m": ":TMP:2 m above ground:"},
            )

        assert result.run_id == run_id
        assert result.success_count == 2
        assert result.error_count == 0
        assert result.total_bytes == 512 * 2

    def test_captures_partial_failures(
        self, run_id: RunID, staging_dir: Path, tmp_path: Path
    ) -> None:
        fake_grib = tmp_path / "fake.grib2"
        fake_grib.write_bytes(b"\x00" * 256)

        call_count = 0

        def download_side_effect(search: str) -> Path:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("Simulated network error")
            return fake_grib

        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.download.side_effect = download_side_effect
            MockHerbie.return_value = mock_h

            result = download_gfs_cycle(
                run_id=run_id,
                staging_dir=staging_dir,
                forecast_hours=[0, 3],
                variables={"tmp_2m": ":TMP:2 m above ground:"},
            )

        assert result.success_count == 1
        assert result.error_count == 1
        assert "Simulated network error" in result.errors[0]

    def test_rejects_invalid_cycle_hour(self, staging_dir: Path) -> None:
        bad_run = RunID("20260306T03Z")  # 03Z is not a GFS cycle
        with pytest.raises(ValueError, match="cycle hour 3"):
            download_gfs_cycle(
                run_id=bad_run,
                staging_dir=staging_dir,
            )

    def test_uses_defaults_when_no_overrides(
        self, run_id: RunID, staging_dir: Path, tmp_path: Path
    ) -> None:
        fake_grib = tmp_path / "fake.grib2"
        fake_grib.write_bytes(b"\x00" * 100)

        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.download.return_value = fake_grib
            MockHerbie.return_value = mock_h

            result = download_gfs_cycle(
                run_id=run_id,
                staging_dir=staging_dir,
            )

        expected_count = len(DEFAULT_FORECAST_HOURS) * len(DEFAULT_SEARCH_PATTERNS)
        assert result.success_count == expected_count


class TestCheckCycleAvailable:
    def test_returns_true_when_inventory_exists(self, run_id: RunID) -> None:
        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.inventory.return_value = pd.DataFrame({"grib_message": [1, 2]})
            MockHerbie.return_value = mock_h

            assert check_cycle_available(run_id) is True

    def test_returns_false_when_no_inventory(self, run_id: RunID) -> None:
        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.inventory.side_effect = Exception("404")
            MockHerbie.return_value = mock_h

            assert check_cycle_available(run_id) is False

    def test_returns_false_on_empty_inventory(self, run_id: RunID) -> None:
        with patch("weatherman.ingest.gfs.Herbie") as MockHerbie:
            mock_h = MagicMock()
            mock_h.inventory.return_value = pd.DataFrame()
            MockHerbie.return_value = mock_h

            assert check_cycle_available(run_id) is False


class TestLatestAvailableCycle:
    def test_finds_most_recent_cycle(self) -> None:
        with patch("weatherman.ingest.gfs.check_cycle_available") as mock_check:
            # First cycle not available, second one is
            mock_check.side_effect = [False, True]

            result = latest_available_cycle()

            assert result is not None
            assert mock_check.call_count == 2

    def test_returns_none_when_no_cycles_available(self) -> None:
        with patch("weatherman.ingest.gfs.check_cycle_available") as mock_check:
            mock_check.return_value = False

            result = latest_available_cycle()

            assert result is None
