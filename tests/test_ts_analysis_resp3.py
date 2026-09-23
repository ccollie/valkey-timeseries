"""RESP3 replies of the analysis commands.

Their map replies announce an entry count up front and then write the entries one by one. In
RESP2 a wrong count only changes the length of a flat array, so the RESP2 tests cannot see it;
in RESP3 it desynchronises the connection. Each case therefore checks the reply's shape and then
that the connection still answers a sentinel correctly, with every optional section enabled so
every conditional entry is counted.
"""
import pytest
import valkey

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

COUNT = 240


def keys_of(mapping):
    return {k.decode() if isinstance(k, bytes) else k for k in mapping}


class TestAnalysisResp3(ValkeyTimeSeriesTestCaseBase):

    def resp3(self):
        c3 = valkey.Valkey(host=self.server.bind_ip, port=self.server.port, protocol=3)
        c3.execute_command("TS.CREATE", "s")
        args = []
        for i in range(COUNT):
            value = 10.0 + (i % 24) + i * 0.05
            args += ["s", (i + 1) * 1000, value]
        c3.execute_command("TS.MADD", *args)
        return c3

    def run(self, c3, *argv):
        reply = c3.execute_command(*argv)
        assert c3.execute_command("ECHO", "sync") == b"sync", f"{argv[0]} desynchronised RESP3"
        return reply

    def test_forecast_with_every_section(self):
        c3 = self.resp3()
        [entry] = self.run(
            c3, "TS.FORECAST", "s", "-", "+", "MODELS", "ARIMA(1,0,0)", "HORIZON", 5,
            "LEVEL", 90, "METRICS",
        )
        assert isinstance(entry, dict)
        assert {"model", "horizon", "forecast", "level", "lower_interval", "upper_interval",
                "metrics"} <= keys_of(entry)
        assert isinstance(entry[b"metrics"], dict)

    def test_autoforecast_with_every_section(self):
        c3 = self.resp3()
        entry = self.run(
            c3, "TS.AUTOFORECAST", "s", "-", "+", "HORIZON", 5, "MODELS", "ETS",
            "LEVEL", 90, "METRICS",
        )
        assert isinstance(entry, dict)
        assert {"model", "horizon", "forecast", "metrics"} <= keys_of(entry)

    def test_backtest_with_predictions(self):
        c3 = self.resp3()
        [entry] = self.run(
            c3, "TS.BACKTEST", "s", "-", "+", "MODELS", "Naive", "HORIZON", 12,
            "N_FOLDS", 3, "WITH_PREDICTIONS",
        )
        assert isinstance(entry, dict)
        assert {"model", "horizon", "n_folds", "metrics", "folds"} <= keys_of(entry)
        fold = entry[b"folds"][0]
        assert isinstance(fold, dict)
        assert {"predictions", "actuals", "metrics"} <= keys_of(fold)

    def test_backtest_failed_model(self):
        c3 = self.resp3()
        # SeasonalNaive with a period longer than any training window fails per model.
        entries = self.run(
            c3, "TS.BACKTEST", "s", "-", "+", "MODELS", "Naive,SeasonalNaive(100000)",
            "HORIZON", 12, "N_FOLDS", 3,
        )
        assert all(isinstance(entry, dict) for entry in entries)

    def test_trend_with_every_section(self):
        c3 = self.resp3()
        fit = self.run(
            c3, "TS.TREND", "s", "-", "+", "PREDICT", 5, "FEATURES", "METRICS",
        )
        assert isinstance(fit, dict)
        assert {"model", "fitted_trend", "predicted_trend", "features",
                "accuracy_metrics"} <= keys_of(fit)
        assert isinstance(fit[b"features"], dict)

    def test_features(self):
        c3 = self.resp3()
        features = self.run(
            c3, "TS.FEATURES", "s", "-", "+", "CATEGORY", "basic,distribution,trend",
        )
        assert isinstance(features, dict)
        assert "mean" in keys_of(features)

    @pytest.mark.parametrize("test", ["adf", "kpss", "combined"])
    def test_stationarity(self, test):
        c3 = self.resp3()
        result = self.run(c3, "TS.STATIONARITY", "s", "-", "+", "TEST", test)
        assert isinstance(result, dict)

    def test_stats(self):
        c3 = self.resp3()
        assert isinstance(self.run(c3, "TS.STATS", "s", "-", "+"), dict)

    def test_other_analysis_replies_stay_in_sync(self):
        c3 = self.resp3()
        self.run(c3, "TS.DECOMPOSE", "s", "-", "+", "SEASONALITY", 24)
        self.run(c3, "TS.PERIODS", "s", "-", "+")
        self.run(c3, "TS.AUTOCORRELATION", "s", "-", "+", 3, "PARTIAL")
        self.run(c3, "TS.FILLGAPS", "s", "-", "+", "FREQUENCY", 500)
        self.run(c3, "TS.SANITIZE", "s", "-", "+")
