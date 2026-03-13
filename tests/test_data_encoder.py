"""Tests for the float-to-RGBA data tile encoder and Float16 binary encoder."""

import numpy as np
import pytest

from weatherman.tiling.data_encoder import (
    FLOAT16_NODATA,
    decode_f16_to_float,
    decode_rgba_to_float,
    encode_float_to_f16,
    encode_float_to_rgba,
    rgba_to_png_bytes,
)


class TestEncodeFloatToRgba:
    def test_basic_encoding_shape(self):
        data = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 50.0)
        assert rgba.shape == (2, 2, 4)
        assert rgba.dtype == np.uint8

    def test_alpha_always_0xff(self):
        data = np.array([[0.0, 25.0], [50.0, np.nan]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 50.0)
        assert np.all(rgba[:, :, 3] == 0xFF)

    def test_nodata_nan_flagged(self):
        data = np.array([[1.0, np.nan], [2.0, 3.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 10.0)
        # NaN pixel: B=0xFF, R=0, G=0
        assert rgba[0, 1, 2] == 0xFF
        assert rgba[0, 1, 0] == 0
        assert rgba[0, 1, 1] == 0
        # Valid pixels: B=0x00
        assert rgba[0, 0, 2] == 0x00
        assert rgba[1, 0, 2] == 0x00

    def test_nodata_sentinel_flagged(self):
        data = np.array([[1.0, -9999.0], [2.0, 3.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 10.0, nodata=-9999.0)
        assert rgba[0, 1, 2] == 0xFF
        assert rgba[0, 1, 0] == 0
        assert rgba[0, 1, 1] == 0

    def test_min_encodes_to_zero(self):
        data = np.array([[0.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 100.0)
        # 0 normalized -> 0 encoded -> R=0, G=0
        assert rgba[0, 0, 0] == 0
        assert rgba[0, 0, 1] == 0

    def test_max_encodes_to_ffff(self):
        data = np.array([[100.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 100.0)
        # 1.0 normalized -> 65535 -> R=0xFF, G=0xFF
        assert rgba[0, 0, 0] == 0xFF
        assert rgba[0, 0, 1] == 0xFF

    def test_values_clamped_to_range(self):
        data = np.array([[-10.0, 200.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 100.0)
        # Below min -> clamped to 0
        assert rgba[0, 0, 0] == 0
        assert rgba[0, 0, 1] == 0
        # Above max -> clamped to 65535
        assert rgba[0, 1, 0] == 0xFF
        assert rgba[0, 1, 1] == 0xFF

    def test_zero_range_all_zero(self):
        data = np.array([[5.0, 5.0]], dtype=np.float32)
        rgba = encode_float_to_rgba(data, 5.0, 5.0)
        assert rgba[0, 0, 0] == 0
        assert rgba[0, 0, 1] == 0


class TestRoundTrip:
    """Acceptance criteria: round-trip error within 0.1%."""

    def _round_trip(self, data, vmin, vmax):
        rgba = encode_float_to_rgba(data, vmin, vmax)
        decoded, nodata_mask = decode_rgba_to_float(rgba, vmin, vmax)
        return decoded, nodata_mask

    def test_temperature_round_trip(self):
        """Temperature range: -55 to +55°C."""
        rng = np.random.default_rng(42)
        data = rng.uniform(-55.0, 55.0, size=(64, 64)).astype(np.float32)
        decoded, mask = self._round_trip(data, -55.0, 55.0)
        assert not np.any(mask)
        value_range = 55.0 - (-55.0)
        max_error = np.max(np.abs(decoded - data))
        # 16-bit quantization: max error = range / 65535 ≈ 0.0017
        # 0.1% of range = 0.11
        assert max_error < value_range * 0.001, f"Max error {max_error} exceeds 0.1%"

    def test_wind_speed_round_trip(self):
        """Wind speed range: 0 to 50 m/s."""
        rng = np.random.default_rng(43)
        data = rng.uniform(0.0, 50.0, size=(64, 64)).astype(np.float32)
        decoded, mask = self._round_trip(data, 0.0, 50.0)
        assert not np.any(mask)
        max_error = np.max(np.abs(decoded - data))
        assert max_error < 50.0 * 0.001

    def test_precipitation_round_trip(self):
        """Precipitation range: 0 to 250 kg/m²."""
        rng = np.random.default_rng(44)
        data = rng.uniform(0.0, 250.0, size=(64, 64)).astype(np.float32)
        decoded, mask = self._round_trip(data, 0.0, 250.0)
        assert not np.any(mask)
        max_error = np.max(np.abs(decoded - data))
        assert max_error < 250.0 * 0.001

    def test_nodata_survives_round_trip(self):
        data = np.array([[1.0, np.nan, 3.0], [np.nan, 5.0, 6.0]], dtype=np.float32)
        decoded, mask = self._round_trip(data, 0.0, 10.0)
        expected_mask = np.isnan(data)
        np.testing.assert_array_equal(mask, expected_mask)


class TestEncodeFloatToF16:
    def test_basic_encoding_size(self):
        data = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
        buf = encode_float_to_f16(data)
        assert len(buf) == 2 * 2 * 2  # H * W * 2 bytes per float16

    def test_nan_encoded_as_sentinel(self):
        data = np.array([[1.0, np.nan], [2.0, 3.0]], dtype=np.float32)
        buf = encode_float_to_f16(data)
        arr = np.frombuffer(buf, dtype=np.float16).reshape(2, 2)
        assert arr[0, 1] == np.float16(FLOAT16_NODATA)
        assert arr[0, 0] != np.float16(FLOAT16_NODATA)

    def test_explicit_nodata_sentinel(self):
        data = np.array([[1.0, -9999.0], [2.0, 3.0]], dtype=np.float32)
        buf = encode_float_to_f16(data, nodata=-9999.0)
        arr = np.frombuffer(buf, dtype=np.float16).reshape(2, 2)
        assert arr[0, 1] == np.float16(FLOAT16_NODATA)

    def test_physical_values_preserved(self):
        """Float16 stores physical values directly — no normalization."""
        data = np.array([[0.5, -25.3, 49.9]], dtype=np.float32)
        buf = encode_float_to_f16(data)
        arr = np.frombuffer(buf, dtype=np.float16).reshape(1, 3)
        # Float16 precision: values should be very close (within f16 rounding)
        np.testing.assert_allclose(arr.astype(np.float32), data, rtol=1e-2)


class TestF16RoundTrip:
    def test_wind_round_trip(self):
        """Wind U/V range: -50 to +50 m/s."""
        rng = np.random.default_rng(42)
        data = rng.uniform(-50.0, 50.0, size=(64, 64)).astype(np.float32)
        buf = encode_float_to_f16(data)
        decoded, mask = decode_f16_to_float(buf, 64, 64)
        assert not np.any(mask)
        # Float16 has ~0.03 m/s precision at 50 m/s, much better near 0
        max_error = np.max(np.abs(decoded - data))
        assert max_error < 0.1, f"Max error {max_error} exceeds 0.1 m/s"

    def test_light_wind_precision(self):
        """Float16 should have much better precision near zero than 8-bit PNG."""
        rng = np.random.default_rng(43)
        data = rng.uniform(-1.0, 1.0, size=(64, 64)).astype(np.float32)
        buf = encode_float_to_f16(data)
        decoded, mask = decode_f16_to_float(buf, 64, 64)
        assert not np.any(mask)
        max_error = np.max(np.abs(decoded - data))
        # Float16 near zero: precision ~0.001 m/s (much better than PNG's ~0.0015)
        assert max_error < 0.002, f"Max error {max_error} for light winds"

    def test_nodata_survives_round_trip(self):
        data = np.array([[1.0, np.nan, 3.0], [np.nan, 5.0, 6.0]], dtype=np.float32)
        buf = encode_float_to_f16(data)
        decoded, mask = decode_f16_to_float(buf, 2, 3)
        expected_mask = np.isnan(data)
        np.testing.assert_array_equal(mask, expected_mask)

    def test_temperature_round_trip(self):
        """Temperature range: -55 to +55 C."""
        rng = np.random.default_rng(44)
        data = rng.uniform(-55.0, 55.0, size=(64, 64)).astype(np.float32)
        buf = encode_float_to_f16(data)
        decoded, mask = decode_f16_to_float(buf, 64, 64)
        assert not np.any(mask)
        max_error = np.max(np.abs(decoded - data))
        # Float16 at 55: precision ~0.03
        assert max_error < 0.1, f"Max error {max_error}"


class TestRgbaToPngBytes:
    def test_produces_valid_png(self):
        rgba = np.full((4, 4, 4), 128, dtype=np.uint8)
        png = rgba_to_png_bytes(rgba)
        assert png[:8] == b"\x89PNG\r\n\x1a\n"

    def test_png_round_trip_preserves_values(self):
        """Encode to PNG, decode back, verify no lossy compression."""
        from PIL import Image
        import io

        data = np.random.default_rng(45).uniform(0, 50, (32, 32)).astype(np.float32)
        rgba = encode_float_to_rgba(data, 0.0, 50.0)
        png_bytes = rgba_to_png_bytes(rgba)

        # Decode PNG back
        img = Image.open(io.BytesIO(png_bytes))
        recovered = np.array(img)
        np.testing.assert_array_equal(rgba, recovered)
