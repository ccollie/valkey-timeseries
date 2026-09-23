"""Regression tests for analysis bugs found while verifying the command docs against a server.

Each test names the bug it pins down.
"""
import math
import random

import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def as_dict(reply):
    it = iter(reply)
    return {(k.decode() if isinstance(k, bytes) else k): v for k, v in zip(it, it)}


class TestAnalysisRegressions(ValkeyTimeSeriesTestCaseBase):

    def add(self, key, values, step=1000):
        self.client.execute_command("TS.CREATE", key)
        args = []
        for i, value in enumerate(values):
            args += [key, (i + 1) * step, "nan" if value != value else value]
        self.client.execute_command("TS.MADD", *args)

    def trending(self, key, count=120, seed=3):
        rng = random.Random(seed)
        self.add(key, [
            50 + 0.8 * i + 5 * math.sin(i * 2 * math.pi / 12) + rng.gauss(0, 0.5)
            for i in range(count)
        ])

    def metrics(self, entry):
        metrics = as_dict(entry)["metrics"]
        return None if metrics is None else as_dict(metrics)

    # -- METRICS ------------------------------------------------------------------------------

    @pytest.mark.parametrize("spec", ["ARIMA(2,1,0)", "SARIMA(1,1,0,1,1,0,12)", "ARIMA(1,0,0)"])
    def test_arima_family_metrics_are_on_the_series_scale(self, spec):
        """ARIMA with differencing used to score its fitted *differences* (r_squared -27)."""
        self.trending("t")
        [entry] = self.client.execute_command(
            "TS.FORECAST", "t", "-", "+", "MODELS", spec, "HORIZON", 3, "METRICS"
        )
        assert float(self.metrics(entry)["r_squared"]) > 0.95

    def test_arima_metrics_through_a_transform_pipeline(self):
        self.trending("t")
        [entry] = self.client.execute_command(
            "TS.FORECAST", "t", "-", "+", "MODELS", "ARIMA(1,1,0)", "TRANSFORMS", "Log",
            "HORIZON", 3, "METRICS",
        )
        assert float(self.metrics(entry)["r_squared"]) > 0.95

    def test_autoforecast_arima_metrics_are_on_the_series_scale(self):
        self.trending("t")
        entry = self.client.execute_command(
            "TS.AUTOFORECAST", "t", "-", "+", "MODELS", "ARIMA", "HORIZON", 3, "METRICS"
        )
        assert float(self.metrics(entry)["r_squared"]) > 0.95

    def test_metrics_without_an_in_sample_fit_are_null(self):
        """GARCH has no fit of the level; METRICS used to fail the whole command."""
        self.trending("t")
        [entry] = self.client.execute_command(
            "TS.FORECAST", "t", "-", "+", "MODELS", "GARCH", "HORIZON", 3, "METRICS"
        )
        assert self.metrics(entry) is None

    # -- Missing values -----------------------------------------------------------------------

    def seasonal_with_gaps(self, key, seed=1):
        rng = random.Random(seed)
        values = [100 + 10 * math.sin(i * 2 * math.pi / 24) + rng.gauss(0, 1) for i in range(1344)]
        for i in list(range(300, 310)) + [5, 700, 1000]:
            values[i] = float("nan")
        self.add(key, values, step=3_600_000)
        return values

    def test_features_over_a_range_with_missing_values(self):
        """A NaN made a sort inside the feature code panic (TSDB: internal error)."""
        self.seasonal_with_gaps("s")
        for category in ["basic", "distribution", "autocorrelation", "trend"]:
            features = as_dict(self.client.execute_command(
                "TS.FEATURES", "s", "-", "+", "CATEGORY", category
            ))
            assert features
        length = as_dict(self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "length"
        ))["length"]
        assert float(length) == 1344 - 13

    def test_features_without_finite_values(self):
        self.add("empty", [float("nan")] * 5)
        with pytest.raises(ResponseError, match="no finite samples"):
            self.client.execute_command("TS.FEATURES", "empty", "-", "+", "CATEGORY", "basic")

    def test_sanitize_seasonal_auto_with_missing_values(self):
        """Period detection saw the NaNs it was meant to fill and always failed."""
        self.seasonal_with_gaps("s")
        self.client.execute_command("TS.SANITIZE", "s", "-", "+", "POLICY", "SEASONAL", "auto")
        values = [float(v) for _, v in self.client.execute_command("TS.RANGE", "s", "-", "+")]
        assert len(values) == 1344 and all(math.isfinite(v) for v in values)

    def test_interpolate_treats_infinity_as_missing(self):
        self.add("s", [1.0, float("inf"), float("nan"), float("-inf"), 5.0])
        self.client.execute_command("TS.SANITIZE", "s", "-", "+", "POLICY", "INTERPOLATE")
        values = [float(v) for _, v in self.client.execute_command("TS.RANGE", "s", "-", "+")]
        assert values == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_outliers_with_seasonality_and_missing_values(self):
        """One NaN turned the whole seasonal remainder into NaN, so nothing was ever found."""
        rng = random.Random(2)
        values = [100 + 10 * math.sin(i * 2 * math.pi / 24) + rng.gauss(0, 1) for i in range(480)]
        values[100] += 40
        values[250] -= 40
        values[50] = values[300] = float("nan")
        self.add("s", values)
        found = self.client.execute_command(
            "TS.OUTLIERS", "s", "-", "+", "METHOD", "zscore", "SEASONALITY", 24
        )
        assert {row[0] for row in found} >= {101_000, 251_000}

    def test_stationarity_rejects_missing_values(self):
        self.add("s", [1.0, 2.0, float("nan")] + [float(i % 5) for i in range(20)])
        with pytest.raises(ResponseError, match="NaN or infinite"):
            self.client.execute_command("TS.STATIONARITY", "s", "-", "+")

    def test_sanitize_does_not_touch_the_source_when_store_would_fail(self):
        self.add("s", [1.0, float("nan"), 3.0])
        self.client.execute_command("SET", "dst", "not a series")
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            self.client.execute_command(
                "TS.SANITIZE", "s", "-", "+", "POLICY", "INTERPOLATE", "STORE", "dst"
            )
        values = [v for _, v in self.client.execute_command("TS.RANGE", "s", "-", "+")]
        assert values[1].lower() in (b"nan", b"-nan")

    # -- Grids and numbers --------------------------------------------------------------------

    def test_fillgaps_keeps_an_alignment_offset_near_zero(self):
        self.client.execute_command("TS.CREATE", "g")
        self.client.execute_command("TS.ADD", "g", 1000, 1)
        filled = self.client.execute_command(
            "TS.FILLGAPS", "g", 0, 2000, "FREQUENCY", 1000, "ALIGN", 500, "VALUE", 0
        )
        assert [ts for ts, _ in filled] == [500, 1500]

    def test_stats_moments_are_exact_and_ordered(self):
        self.add("s", [1.0, 2.0, 3.0, 4.0, 10.0])
        reply = self.client.execute_command("TS.STATS", "s")
        names = [k.decode() for k in reply[::2]]
        assert names == sorted(names)
        stats = as_dict(reply)
        assert float(stats["mean"]) == 4.0
        # Bias-adjusted G1 / G2 from the sample standard deviation.
        assert float(stats["skewness"]) == pytest.approx(1.697056274847714, rel=1e-12)
        assert float(stats["kurtosis"]) == pytest.approx(3.152, rel=1e-12)

    def test_features_mean_is_double_precision(self):
        self.add("s", [18.26, 18.26, 18.26])
        mean = as_dict(self.client.execute_command("TS.FEATURES", "s", "-", "+", "FEATURE", "mean"))
        assert float(mean["mean"]) == 18.26

    def test_repeated_feature_clauses_are_combined(self):
        self.add("s", [1.0, 2.0, 3.0, 4.0, 10.0])
        features = as_dict(self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "mean", "FEATURE", "maximum",
            "CATEGORY", "trend", "CATEGORY", "distribution",
        ))
        assert {"mean", "maximum", "linear_trend_slope", "skewness"} <= set(features)

    def test_fillgaps_needs_only_read_access_to_the_source(self):
        self.add("s", [1.0, 2.0])
        self.client.execute_command("TS.ADD", "s", 5000, 5)
        self.client.execute_command("ACL", "SETUSER", "reader", "ON", ">pw", "+@all", "%R~s")
        reader = self.server.get_new_client()
        reader.execute_command("AUTH", "reader", "pw")
        filled = reader.execute_command("TS.FILLGAPS", "s", "-", "+", "FREQUENCY", 1000)
        assert [ts for ts, _ in filled] == [3000, 4000]

    # -- Model specs and messages -------------------------------------------------------------

    @pytest.mark.parametrize("spec,match", [
        ("SeasonalNaive(12, period=6)", "both positionally"),
        ("SES(0.3, alpha=0.5)", "both positionally"),
        ("Holt(phi=0.9)", "phi requires alpha and beta"),
    ])
    def test_ambiguous_model_specs_are_rejected(self, spec, match):
        self.trending("t")
        with pytest.raises(ResponseError, match=match):
            self.client.execute_command("TS.FORECAST", "t", "-", "+", "MODELS", spec, "HORIZON", 3)

    def test_keyword_values_are_case_insensitive(self):
        self.trending("t")
        assert self.client.execute_command(
            "TS.FORECAST", "t", "-", "+", "MODELS", "HoltWinters(12, seasonal_type=Additive)",
            "HORIZON", 3,
        )

    @pytest.mark.parametrize("argv,message", [
        (["TS.FORECAST", "t", "-", "+", "MODELS", "Naive", "HORIZON", "abc"],
         "TSDB: invalid forecast horizon, expected an integer"),
        (["TS.FEATURES", "t", "-", "+", "FEATURE", "foo"], "TSDB: Unknown feature 'foo'"),
        (["TS.STATS", "t", "-", "+", "extra"], "TSDB: unknown argument 'extra'"),
        (["TS.BACKTEST", "t", "-", "+", "MODELS", "Naive", "HORIZON", 5, "STEP", "abc"],
         "TSDB: invalid value for STEP, expected an integer"),
        (["TS.TREND", "t", "-", "+", "RECENCY", "bogus"], "Expected AUTO, FULL, WINDOW, or FRACTION"),
    ])
    def test_error_messages(self, argv, message):
        self.trending("t")
        with pytest.raises(ResponseError) as error:
            self.client.execute_command(*argv)
        assert message in str(error.value)
        assert "TSDB: TSDB" not in str(error.value)
