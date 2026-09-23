"""Inputs that used to crash, hang or exhaust memory in the analysis commands.

Each case must come back as an error while the server stays up. Before the limits, these
divided by zero inside STL (period 0), looped for effectively ever (a period whose double
wraps), allocated (lag + 1)^2 doubles for PACF, or handed a model usize::MAX from a
saturating cast.
"""
import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

HUGE = "9223372036854775807"


class TestAnalysisInputLimits(ValkeyTimeSeriesTestCaseBase):

    def make_series(self, key, count=200):
        self.client.execute_command("TS.CREATE", key)
        args = []
        for i in range(count):
            args += [key, (i + 1) * 1000, 10.0 + (i % 7) + i * 0.01]
        self.client.execute_command("TS.MADD", *args)

    def assert_rejected(self, *command, match=None):
        with pytest.raises(ResponseError, match=match):
            self.client.execute_command(*command)
        assert self.client.ping()

    @pytest.mark.parametrize("period", ["0", "1", HUGE])
    def test_decompose_rejects_degenerate_periods(self, period):
        self.make_series("s")
        self.assert_rejected("TS.DECOMPOSE", "s", "-", "+", "SEASONALITY", period)

    @pytest.mark.parametrize("period", ["0", "1", HUGE])
    def test_outliers_rejects_degenerate_periods(self, period):
        self.make_series("s")
        self.assert_rejected(
            "TS.OUTLIERS", "s", "-", "+", "METHOD", "zscore", "SEASONALITY", period
        )

    @pytest.mark.parametrize("period", ["1", "1e30", "2.5", "201"])
    def test_autoforecast_rejects_bad_seasonality(self, period):
        self.make_series("s")
        self.assert_rejected(
            "TS.AUTOFORECAST", "s", "-", "+", "HORIZON", 3, "SEASONALITY", period
        )

    def test_lag_caps(self):
        self.make_series("s", count=1100)
        self.make_series("t", count=1100)
        self.assert_rejected(
            "TS.AUTOCORRELATION", "s", "-", "+", 1001, "PARTIAL", match="must not exceed 1000"
        )
        self.assert_rejected(
            "TS.AUTOCORRELATION", "s", "-", "+", 1001, "AGGREGATED", "mean",
            match="must not exceed 1000",
        )
        # A single-lag statistic stays uncapped.
        assert self.client.execute_command("TS.AUTOCORRELATION", "s", "-", "+", 1001) is not None
        self.assert_rejected(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "pacf:1001", match="must not exceed 1000"
        )
        self.assert_rejected(
            "TS.STATIONARITY", "s", "-", "+", "TEST", "adf", "LAGS", 1001,
            match="must not exceed 1000",
        )
        self.assert_rejected("TS.XCORR", "s", "t", "-", "+", 1001, match="must not exceed 1000")

    def test_trend_predict_is_capped(self):
        self.make_series("s")
        max_horizon = int(
            self.client.execute_command("CONFIG", "GET", "ts.ts-forecast-max-horizon")[1]
        )
        self.assert_rejected(
            "TS.TREND", "s", "-", "+", "PREDICT", max_horizon + 1, match="must not exceed"
        )
        self.assert_rejected("TS.TREND", "s", "-", "+", "PREDICT", HUGE, match="must not exceed")

    @pytest.mark.parametrize("spec,match", [
        ("SES(alhpa=0.3)", "Unsupported keyword"),
        ("ARIMA(21,0,0)", "must not exceed"),
        ("SeasonalNaive(0)", "must be positive"),
        ("SeasonalNaive(1e30)", None),
        ("MFLES(seasonal_period=[[7]])", "Nested lists"),
        ("MFLES(seasonal_period=" + "[" * 100_000 + ")", None),
        ("Theta(3)", None),
    ])
    def test_forecast_rejects_bad_model_specs(self, spec, match):
        self.make_series("s")
        self.assert_rejected(
            "TS.FORECAST", "s", "-", "+", "MODELS", spec, "HORIZON", 3, match=match
        )

    def test_forecast_rejects_bad_transforms(self):
        self.make_series("s")
        for spec in ["Difference(21)", "SeasonalDifference(0)", "Log(base=10)"]:
            self.assert_rejected(
                "TS.FORECAST", "s", "-", "+", "MODELS", "Naive", "HORIZON", 3,
                "TRANSFORMS", spec,
            )

    def test_keywords_are_case_insensitive(self):
        self.make_series("s")
        result = self.client.execute_command(
            "TS.FORECAST", "s", "-", "+", "MODELS", "SES(ALPHA=0.3)", "HORIZON", 3
        )
        assert len(result) == 1
