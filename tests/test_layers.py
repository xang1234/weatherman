"""Tests for the config-driven layer definition system."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from weatherman.layers import (
    DerivedSpec,
    LayerRegistry,
    load_registry,
    get_layer_registry,
)


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    """Write a minimal valid layers.yaml and return its path."""
    content = dedent("""\
        variables:
          tmp_2m:
            long_name: "Temperature at 2m above ground"
            units: "K"
            grib2_key: ":TMP:2 m above ground:"
            level: "2 m above ground"
          ugrd_10m:
            long_name: "U-wind at 10m"
            units: "m/s"
            grib2_key: ":UGRD:10 m above ground:"
            level: "10 m above ground"
          vgrd_10m:
            long_name: "V-wind at 10m"
            units: "m/s"
            grib2_key: ":VGRD:10 m above ground:"
            level: "10 m above ground"

        layers:
          temperature:
            display_name: "Temperature at 2m"
            variable: tmp_2m
            units: "K"
            palette: temperature
            value_range: [220.0, 330.0]
          wind_speed:
            display_name: "Wind Speed"
            units: "m/s"
            palette: wind_speed
            value_range: [0.0, 50.0]
            derived:
              method: magnitude
              sources: [ugrd_10m, vgrd_10m]
    """)
    p = tmp_path / "layers.yaml"
    p.write_text(content)
    return p


class TestLoadRegistry:
    def test_loads_variables(self, config_file: Path):
        reg = load_registry(config_file)
        assert "tmp_2m" in reg.variable_names
        assert "ugrd_10m" in reg.variable_names
        assert "vgrd_10m" in reg.variable_names

    def test_loads_layers(self, config_file: Path):
        reg = load_registry(config_file)
        assert "temperature" in reg.layer_ids
        assert "wind_speed" in reg.layer_ids

    def test_simple_layer_has_variable(self, config_file: Path):
        reg = load_registry(config_file)
        entry = reg.get_layer("temperature")
        assert entry is not None
        assert entry.variable == "tmp_2m"
        assert entry.derived is None

    def test_derived_layer_has_spec(self, config_file: Path):
        reg = load_registry(config_file)
        entry = reg.get_layer("wind_speed")
        assert entry is not None
        assert entry.variable is None
        assert entry.derived == DerivedSpec(
            method="magnitude", sources=("ugrd_10m", "vgrd_10m")
        )

    def test_value_range_parsed(self, config_file: Path):
        reg = load_registry(config_file)
        entry = reg.get_layer("temperature")
        assert entry is not None
        assert entry.value_min == 220.0
        assert entry.value_max == 330.0


class TestValidation:
    def test_unknown_variable_reference(self, tmp_path: Path):
        content = dedent("""\
            variables: {}
            layers:
              bad_layer:
                display_name: "Bad"
                variable: nonexistent
                units: "K"
                palette: x
                value_range: [0, 1]
        """)
        p = tmp_path / "bad.yaml"
        p.write_text(content)
        with pytest.raises(ValueError, match="unknown variable 'nonexistent'"):
            load_registry(p)

    def test_unknown_derived_source(self, tmp_path: Path):
        content = dedent("""\
            variables:
              a:
                long_name: A
                units: K
                grib2_key: ":A:"
            layers:
              bad:
                display_name: "Bad"
                units: K
                palette: x
                value_range: [0, 1]
                derived:
                  method: magnitude
                  sources: [a, missing]
        """)
        p = tmp_path / "bad.yaml"
        p.write_text(content)
        with pytest.raises(ValueError, match="derived source 'missing'"):
            load_registry(p)

    def test_both_variable_and_derived(self, tmp_path: Path):
        content = dedent("""\
            variables:
              a:
                long_name: A
                units: K
                grib2_key: ":A:"
            layers:
              bad:
                display_name: "Bad"
                variable: a
                units: K
                palette: x
                value_range: [0, 1]
                derived:
                  method: magnitude
                  sources: [a]
        """)
        p = tmp_path / "bad.yaml"
        p.write_text(content)
        with pytest.raises(ValueError, match="both 'variable' and 'derived'"):
            load_registry(p)

    def test_neither_variable_nor_derived(self, tmp_path: Path):
        content = dedent("""\
            variables:
              a:
                long_name: A
                units: K
                grib2_key: ":A:"
            layers:
              bad:
                display_name: "Bad"
                units: K
                palette: x
                value_range: [0, 1]
        """)
        p = tmp_path / "bad.yaml"
        p.write_text(content)
        with pytest.raises(ValueError, match="must specify either"):
            load_registry(p)

    def test_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_registry(tmp_path / "nope.yaml")


class TestRegistryAccessors:
    def test_variable_defs(self, config_file: Path):
        reg = load_registry(config_file)
        defs = reg.variable_defs()
        assert "tmp_2m" in defs
        vd = defs["tmp_2m"]
        assert vd.grib_key == ":TMP:2 m above ground:"
        assert vd.units == "K"
        assert vd.level == "2 m above ground"

    def test_grib2_search_patterns(self, config_file: Path):
        reg = load_registry(config_file)
        patterns = reg.grib2_search_patterns()
        assert patterns["tmp_2m"] == ":TMP:2 m above ground:"
        assert patterns["ugrd_10m"] == ":UGRD:10 m above ground:"

    def test_layer_configs(self, config_file: Path):
        reg = load_registry(config_file)
        configs = reg.layer_configs()
        assert len(configs) == 2
        temp = next(c for c in configs if c.id == "temperature")
        assert temp.display_name == "Temperature at 2m"
        assert temp.unit == "K"
        assert temp.palette_name == "temperature"
        assert temp.value_range.min == 220.0
        assert temp.value_range.max == 330.0

    def test_derived_layers(self, config_file: Path):
        reg = load_registry(config_file)
        derived = reg.derived_layers()
        assert "wind_speed" in derived
        assert derived["wind_speed"].method == "magnitude"
        assert "temperature" not in derived

    def test_source_variable(self, config_file: Path):
        reg = load_registry(config_file)
        assert reg.source_variable("temperature") == "tmp_2m"
        assert reg.source_variable("wind_speed") is None
        assert reg.source_variable("nonexistent") is None


class TestDefaultRegistry:
    def test_loads_bundled_config(self):
        """The bundled layers.yaml should load without errors."""
        reg = get_layer_registry()
        assert len(reg.variable_names) >= 3
        assert len(reg.layer_ids) >= 2

    def test_bundled_variables_match_phase1(self):
        """Bundled config should define the same Phase 1 variables."""
        from weatherman.storage.zarr_schema import PHASE1_VARIABLES

        reg = get_layer_registry()
        defs = reg.variable_defs()
        for var_name, expected in PHASE1_VARIABLES.items():
            assert var_name in defs, f"Missing variable: {var_name}"
            actual = defs[var_name]
            assert actual.grib_key == expected.grib_key
            assert actual.units == expected.units
            assert actual.long_name == expected.long_name

    def test_bundled_grib_keys_match_downloader(self):
        """Bundled config should match the GFS downloader patterns."""
        from weatherman.ingest.gfs import DEFAULT_SEARCH_PATTERNS

        reg = get_layer_registry()
        patterns = reg.grib2_search_patterns()
        for var_name, expected_key in DEFAULT_SEARCH_PATTERNS.items():
            assert var_name in patterns, f"Missing variable: {var_name}"
            assert patterns[var_name] == expected_key
