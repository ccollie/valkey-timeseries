"""
Integration tests for TS.DECOMPOSE.

Covers:
- STL (single period) reply shape and additivity of components
- MSTL (multiple periods) reply shape
- SEASONALITY AUTO
- Large ranges run on the analysis pool (same reply as inline)
- TIMEOUT: fires on a slow input, accepted otherwise, negative rejected
- Error handling: nonexistent key, insufficient data, bad periods, unknown argument
"""

import math
import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from data_helpers import create_sine_series, create_large_seasonal_series, _add


def as_map(result):
    return dict(zip(result[::2], result[1::2]))


def values_of(component):
    return [float(v) for _, v in component]


class TestDecompose(ValkeyTimeSeriesTestCaseBase):

    def test_stl_components_add_up(self):
        key = "test:decompose:stl"
        create_sine_series(self.client, key, count=240, period=24)

        result = as_map(self.client.execute_command(
            "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24"
        ))
        assert set(result) == {b"original", b"trend", b"seasonal", b"residual"}
        original = values_of(result[b"original"])
        trend = values_of(result[b"trend"])
        seasonal = values_of(result[b"seasonal"])
        residual = values_of(result[b"residual"])
        assert len(original) == len(trend) == len(seasonal) == len(residual) == 240
        for o, t, s, r in zip(original, trend, seasonal, residual):
            assert math.isclose(o, t + s + r, rel_tol=1e-6, abs_tol=1e-6)
        # Every component carries the source timestamps.
        assert [int(ts) for ts, _ in result[b"trend"]] == \
            [int(ts) for ts, _ in result[b"original"]]

    def test_mstl_reply_shape(self):
        key = "test:decompose:mstl"
        create_sine_series(self.client, key, count=600, period=24)

        result = as_map(self.client.execute_command(
            "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24", "48"
        ))
        assert set(result) == {b"original", b"trend", b"seasonal_components", b"residual"}
        components = result[b"seasonal_components"]
        assert [int(period) for period, _ in components] == [24, 48]
        for _, component in components:
            assert len(component) == 600

    def test_seasonality_auto(self):
        key = "test:decompose:auto"
        create_sine_series(self.client, key, count=480, period=24)

        result = as_map(self.client.execute_command(
            "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "AUTO"
        ))
        assert b"original" in result and b"trend" in result and b"residual" in result

    # ── analysis pool ────────────────────────────────────────────────────

    def test_large_range_runs_in_background_with_same_reply(self):
        """Above the inline threshold the work runs on the analysis pool; the
        reply must have the same shape and still be an exact decomposition."""
        key = "test:decompose:large"
        create_large_seasonal_series(self.client, key, count=20000)

        result = as_map(self.client.execute_command(
            "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "44"
        ))
        assert set(result) == {b"original", b"trend", b"seasonal", b"residual"}
        original = values_of(result[b"original"])
        assert len(original) == 20000
        for o, t, s, r in zip(original, values_of(result[b"trend"]),
                              values_of(result[b"seasonal"]),
                              values_of(result[b"residual"])):
            assert math.isclose(o, t + s + r, rel_tol=1e-6, abs_tol=1e-6)

    def test_timeout_fires_on_slow_input(self):
        key = "test:decompose:timeout"
        create_large_seasonal_series(self.client, key, count=20000)
        with pytest.raises(ResponseError, match="timed out before the result was ready"):
            self.client.execute_command(
                "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "AUTO", "TIMEOUT", "1"
            )

    def test_timeout_accepted(self):
        key = "test:decompose:timeout_ok"
        create_sine_series(self.client, key, count=240, period=24)
        result = as_map(self.client.execute_command(
            "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24", "TIMEOUT", "30000"
        ))
        assert b"seasonal" in result

    def test_timeout_negative(self):
        key = "test:decompose:timeout_neg"
        create_sine_series(self.client, key, count=240, period=24)
        with pytest.raises(ResponseError, match="TIMEOUT must be zero or positive"):
            self.client.execute_command(
                "TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24", "TIMEOUT", "-1"
            )

    # ── errors ───────────────────────────────────────────────────────────

    def test_nonexistent_key(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.DECOMPOSE", "test:decompose:missing", "-", "+")

    def test_insufficient_data_for_period(self):
        key = "test:decompose:short"
        _add(self.client, key, 1000, [float(i) for i in range(20)])
        with pytest.raises(ResponseError, match="insufficient data for STL"):
            self.client.execute_command("TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24")

    def test_duplicate_periods_rejected(self):
        key = "test:decompose:dup"
        create_sine_series(self.client, key, count=240, period=24)
        with pytest.raises(ResponseError, match="must be unique"):
            self.client.execute_command("TS.DECOMPOSE", key, "-", "+", "SEASONALITY", "24", "24")

    def test_unknown_argument(self):
        key = "test:decompose:unknown"
        create_sine_series(self.client, key, count=240, period=24)
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.DECOMPOSE", key, "-", "+", "BOGUS")
