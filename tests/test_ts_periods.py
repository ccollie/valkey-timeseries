"""
Integration tests for TS.PERIODS.

Covers:
- Default reply shape (array of [period, power, strength, acf, n_cycles])
- DOMINANT (single integer or nil)
- MIN_STRENGTH validation
- Large ranges run on the analysis pool (same reply as inline)
- TIMEOUT: fires on a slow input, accepted otherwise, negative rejected
- Error handling: nonexistent key, insufficient data, unknown argument
"""

import math
import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from data_helpers import create_sine_series, create_large_seasonal_series, _add


def periods_of(result):
    return [int(entry[0]) for entry in result]


class TestPeriods(ValkeyTimeSeriesTestCaseBase):

    def test_detects_seasonal_period(self):
        key = "test:periods:basic"
        create_sine_series(self.client, key, count=480, period=24)

        result = self.client.execute_command("TS.PERIODS", key, "-", "+")
        assert len(result) >= 1
        for entry in result:
            assert len(entry) == 5, f"expected 5 fields per period, got {entry}"
        assert 24 in periods_of(result)

    def test_dominant_returns_single_period(self):
        key = "test:periods:dominant"
        create_sine_series(self.client, key, count=480, period=24)

        result = self.client.execute_command("TS.PERIODS", key, "-", "+", "DOMINANT")
        assert result == 24

    def test_dominant_is_nil_without_seasonality(self):
        key = "test:periods:dominant_none"
        _add(self.client, key, 1000, [float(i) for i in range(50)])

        result = self.client.execute_command(
            "TS.PERIODS", key, "-", "+", "MIN_STRENGTH", "1", "DOMINANT"
        )
        assert result is None

    def test_min_strength_out_of_range(self):
        key = "test:periods:min_strength"
        create_sine_series(self.client, key, count=100, period=10)
        with pytest.raises(ResponseError, match="MIN_STRENGTH must be between 0 and 1"):
            self.client.execute_command("TS.PERIODS", key, "-", "+", "MIN_STRENGTH", "2")

    # ── analysis pool ────────────────────────────────────────────────────

    def test_large_range_runs_in_background_with_same_reply(self):
        """Above the inline threshold the work runs on the analysis pool; the
        reply must be identical in shape and content to a narrower range."""
        key = "test:periods:large"
        create_large_seasonal_series(self.client, key, count=20000)

        full = self.client.execute_command("TS.PERIODS", key, "-", "+")
        assert len(full) >= 1
        for entry in full:
            assert len(entry) == 5
        # The short ~44-sample cycle dominates the synthetic signal.
        assert self.client.execute_command("TS.PERIODS", key, "-", "+", "DOMINANT") == \
            int(full[0][0])

    def test_timeout_fires_on_slow_input(self):
        key = "test:periods:timeout"
        create_large_seasonal_series(self.client, key, count=20000)
        with pytest.raises(ResponseError, match="timed out before the result was ready"):
            self.client.execute_command("TS.PERIODS", key, "-", "+", "TIMEOUT", "1")

    def test_timeout_accepted(self):
        key = "test:periods:timeout_ok"
        create_sine_series(self.client, key, count=200, period=24)
        result = self.client.execute_command("TS.PERIODS", key, "-", "+", "TIMEOUT", "30000")
        assert 24 in periods_of(result)

    def test_timeout_negative(self):
        key = "test:periods:timeout_neg"
        create_sine_series(self.client, key, count=200, period=24)
        with pytest.raises(ResponseError, match="TIMEOUT must be zero or positive"):
            self.client.execute_command("TS.PERIODS", key, "-", "+", "TIMEOUT", "-1")

    # ── errors ───────────────────────────────────────────────────────────

    def test_nonexistent_key(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.PERIODS", "test:periods:missing", "-", "+")

    def test_insufficient_data(self):
        key = "test:periods:short"
        _add(self.client, key, 1000, [1.0, 2.0, 3.0])
        with pytest.raises(ResponseError, match="insufficient data"):
            self.client.execute_command("TS.PERIODS", key, "-", "+")

    def test_unknown_argument(self):
        key = "test:periods:unknown"
        create_sine_series(self.client, key, count=100, period=10)
        with pytest.raises(ResponseError, match="Unknown argument"):
            self.client.execute_command("TS.PERIODS", key, "-", "+", "BOGUS")
