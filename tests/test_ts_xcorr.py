"""TS.XCORR: cross-correlation between two timestamp-aligned series.

Covers the lag convention (a positive peak lag means key1 leads key2), inner-join alignment,
the reply fields, argument validation and TIMEOUT.
"""
import math
import random

import pytest
import valkey
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

STEP = 1000


def reply_to_dict(reply):
    it = iter(reply)
    return {(k.decode() if isinstance(k, bytes) else k): next(it) for k in it}


def pearson(x, y):
    n = len(x)
    mx, my = sum(x) / n, sum(y) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    return sxy / math.sqrt(sxx * syy)


class TestTsXcorr(ValkeyTimeSeriesTestCaseBase):

    def add(self, key, values, timestamps=None):
        self.client.execute_command("TS.CREATE", key)
        timestamps = timestamps or [(i + 1) * STEP for i in range(len(values))]
        args = []
        for ts, value in zip(timestamps, values):
            args += [key, ts, value]
        self.client.execute_command("TS.MADD", *args)

    def xcorr(self, *args):
        return reply_to_dict(self.client.execute_command("TS.XCORR", *args))

    def random_walk(self, count, seed):
        rng = random.Random(seed)
        value, values = 0.0, []
        for _ in range(count):
            value += rng.gauss(0.0, 1.0)
            values.append(value)
        return values

    def lagged_pair(self, shift, count=300):
        """`up` leads `down` by `shift` samples: down[i] == up[i - shift]."""
        base = self.random_walk(count + shift, seed=7)
        self.add("up", base[shift:])
        self.add("down", base[:count])

    def test_positive_peak_lag_means_key1_leads(self):
        self.lagged_pair(shift=3)
        result = self.xcorr("up", "down", "-", "+", 5)
        # up[i] = base[i + 3] and down[i] = base[i]: down catches up with up three steps
        # later, so key1 (up) leads key2 (down) and the peak is at +3.
        assert result["peak_lag"] == 3
        assert float(result["peak_correlation"]) == pytest.approx(1.0)

    def test_swapping_the_keys_negates_the_peak_lag(self):
        self.lagged_pair(shift=3)
        result = self.xcorr("down", "up", "-", "+", 5)
        assert result["peak_lag"] == -3
        assert float(result["peak_correlation"]) == pytest.approx(1.0)

    def test_reply_fields(self):
        x = self.random_walk(100, seed=1)
        y = self.random_walk(100, seed=2)
        self.add("x", x)
        self.add("y", y)
        result = self.xcorr("x", "y", "-", "+", 4)

        assert result["lags"] == list(range(-4, 5))
        values = [float(v) for v in result["values"]]
        assert len(values) == 9
        assert all(-1.0 <= v <= 1.0 for v in values)
        # The server's SIMD correlation and this reference differ in the last few digits.
        assert values[4] == pytest.approx(pearson(x, y), abs=1e-6)
        # lag +1 pairs x[i] with y[i + 1].
        assert values[5] == pytest.approx(pearson(x[:-1], y[1:]), abs=1e-6)
        peak = max(range(9), key=lambda i: abs(values[i]))
        assert result["peak_lag"] == result["lags"][peak]
        assert float(result["peak_correlation"]) == pytest.approx(values[peak])
        assert result["n"] == 100

    def test_only_matching_timestamps_are_aligned(self):
        x = self.random_walk(50, seed=3)
        self.add("x", x)
        # Every other timestamp of x, plus some that x does not have.
        y_ts = [(i + 1) * STEP for i in range(0, 50, 2)] + [10**9 + i for i in range(5)]
        self.add("y", self.random_walk(len(y_ts), seed=4), y_ts)
        assert self.xcorr("x", "y", "-", "+", 0)["n"] == 25

    def test_range_limits_the_aligned_pairs(self):
        self.add("x", self.random_walk(100, seed=5))
        self.add("y", self.random_walk(100, seed=6))
        assert self.xcorr("x", "y", 11 * STEP, 30 * STEP, 2)["n"] == 20

    def test_zero_max_lag_is_contemporaneous_only(self):
        x = self.random_walk(40, seed=8)
        y = self.random_walk(40, seed=9)
        self.add("x", x)
        self.add("y", y)
        result = self.xcorr("x", "y", "-", "+", 0)
        assert result["lags"] == [0]
        assert result["peak_lag"] == 0
        assert float(result["peak_correlation"]) == pytest.approx(pearson(x, y), abs=1e-6)

    def test_timeout_option(self):
        self.add("x", self.random_walk(50, seed=10))
        self.add("y", self.random_walk(50, seed=11))
        assert self.xcorr("x", "y", "-", "+", 3, "TIMEOUT", 30000)["n"] == 50
        with pytest.raises(ResponseError, match="TIMEOUT"):
            self.client.execute_command("TS.XCORR", "x", "y", "-", "+", 3, "TIMEOUT", -1)

    def test_resp3_reply_is_a_map(self):
        self.add("x", self.random_walk(30, seed=12))
        self.add("y", self.random_walk(30, seed=13))
        c3 = valkey.Valkey(host=self.server.bind_ip, port=self.server.port, protocol=3)
        result = c3.execute_command("TS.XCORR", "x", "y", "-", "+", 2)
        assert isinstance(result, dict)
        assert set(result) == {b"lags", b"values", b"peak_lag", b"peak_correlation", b"n"}
        assert result[b"n"] == 30
        # The connection is still in sync after the map.
        assert c3.ping()

    @pytest.mark.parametrize("args,match", [
        (["x", "missing", "-", "+", 1], "does not exist"),
        (["missing", "x", "-", "+", 1], "does not exist"),
        (["x", "x", "-", "+", 1], "duplicate"),
        (["x", "y", "-", "+", -1], "MAXLAG"),
        (["x", "y", "-", "+", "abc"], "MAXLAG"),
        (["x", "y", "-", "+", 1001], "must not exceed 1000"),
        (["x", "y", "-", "+", 9], "insufficient aligned samples"),
        (["x", "y", "-", "+", 1, "BOGUS"], None),
    ])
    def test_errors(self, args, match):
        self.add("x", self.random_walk(10, seed=14))
        self.add("y", self.random_walk(10, seed=15))
        with pytest.raises(ResponseError, match=match):
            self.client.execute_command("TS.XCORR", *args)

    def test_arity(self):
        with pytest.raises(ResponseError, match="wrong number of arguments"):
            self.client.execute_command("TS.XCORR", "x", "y", "-", "+")
