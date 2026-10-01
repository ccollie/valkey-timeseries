"""Deadlines and the horizon cap across the analysis commands that run on the pool.

TIMEOUT firing was only exercised for AUTOFORECAST, TREND, DECOMPOSE and PERIODS, and the live
`ts-forecast-max-horizon` setting only for FORECAST. These cover the rest. Each timeout case uses
a 1 ms deadline on work that takes far longer, so getting the timeout error at all shows the reply
did not wait for the job, whatever the speed of the machine.
"""
from contextlib import contextmanager

import pytest
from valkey import ResponseError

from common import wait_for_analysis_pool_idle
from data_helpers import create_large_seasonal_series, create_linear_series
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

TIMED_OUT = "timed out before the result was ready"
HORIZON_CONFIG = "ts.ts-forecast-max-horizon"


class TestAnalysisDeadlines(ValkeyTimeSeriesTestCaseBase):

    # Each takes roughly a second on 20k samples (release build, M-series laptop). A fast
    # job can finish and unblock the client before the server processes a 1 ms deadline, so
    # the work must be slow, not just "not instant".
    @pytest.mark.parametrize("argv", [
        ["TS.FORECAST", "s", "-", "+", "MODELS", "AutoTBATS(44)", "HORIZON", 5],
        ["TS.BACKTEST", "s", "-", "+", "MODELS", "MSTL(44,635)", "HORIZON", 24, "N_FOLDS", 5],
        ["TS.FEATURES", "s", "-", "+", "FEATURE",
         "augmented_dickey_fuller,fourier_entropy,pacf:1000,partial_autocorrelation:999"],
    ], ids=lambda argv: argv[0])
    def test_timeout_fires(self, argv):
        create_large_seasonal_series(self.client, "s", count=20000)
        with pytest.raises(ResponseError, match=TIMED_OUT):
            self.client.execute_command(*argv, "TIMEOUT", 1)
        assert self.client.ping()
        wait_for_analysis_pool_idle(self.client)

    @pytest.mark.parametrize("argv", [
        ["TS.AUTOFORECAST", "s", "-", "+", "MODELS", "ETS", "HORIZON"],
        ["TS.BACKTEST", "s", "-", "+", "MODELS", "Naive", "N_FOLDS", 2, "HORIZON"],
    ], ids=lambda argv: argv[0])
    def test_horizon_cap_follows_config_set(self, argv):
        create_linear_series(self.client, "s", count=200)
        default = self.client.execute_command("CONFIG", "GET", HORIZON_CONFIG)[1]
        try:
            self.client.execute_command("CONFIG", "SET", HORIZON_CONFIG, "5")
            with pytest.raises(ResponseError, match="horizon must not exceed 5"):
                self.client.execute_command(*argv, 6)
            assert self.client.execute_command(*argv, 5)
        finally:
            self.client.execute_command("CONFIG", "SET", HORIZON_CONFIG, default)


class SlowlogMixin:
    """The slowlog as a probe for what ran on the main thread.

    It records only the time a command holds the main thread, so work forced inline (Lua cannot
    block) shows up in it and work handed to the pool does not. A deadline cannot play that
    part for work of a few ms: a 1 ms one is processed too coarsely to beat it.
    """

    SLOWLOG_CONFIG = "slowlog-log-slower-than"

    @contextmanager
    def slowlog_threshold(self, micros):
        previous = self.client.execute_command("CONFIG", "GET", self.SLOWLOG_CONFIG)[1]
        self.client.execute_command("CONFIG", "SET", self.SLOWLOG_CONFIG, micros)
        try:
            yield
        finally:
            self.client.execute_command("CONFIG", "SET", self.SLOWLOG_CONFIG, previous)

    def slowlogged(self, name):
        entries = self.client.slowlog_get(128)
        return [e for e in entries if e["command"].lower().startswith(name)]


class TestTrendInlineThreshold(SlowlogMixin, ValkeyTimeSeriesTestCaseBase):
    """TS.TREND leaves the main thread past about 1 ms of work, not 20 ms.

    The default fit costs about 0.4 ms plus 2.4 us a sample, so it runs inline only up to 400
    samples; a 2,000-sample range (about 5 ms, release build) used to run inline too.
    """

    def test_range_between_the_old_and_new_threshold_stays_off_the_main_thread(self):
        create_large_seasonal_series(self.client, "s", count=2000)
        with self.slowlog_threshold(1000):
            # Control: the same fit run inline holds the main thread for several ms, so the
            # slowlog sees it. Without this the absence below would prove nothing.
            self.client.execute_command("SLOWLOG", "RESET")
            self.client.execute_command(
                "EVAL", "return redis.call('TS.TREND', 's', '-', '+')", 0
            )
            assert self.slowlogged(b"eval"), "an inline fit of 2,000 samples should be slow"

            self.client.execute_command("SLOWLOG", "RESET")
            self.client.execute_command("TS.TREND", "s", "-", "+")
            assert not self.slowlogged(b"ts.trend")
            wait_for_analysis_pool_idle(self.client)

    def test_the_same_range_answers_with_the_fit(self):
        create_large_seasonal_series(self.client, "s", count=2000)
        result = self.client.execute_command("TS.TREND", "s", "-", "+")
        fitted = dict(zip(result[::2], result[1::2]))[b"fitted_trend"]
        assert len(fitted) == 2000

    def test_small_range_still_runs_inline_and_ignores_the_deadline(self):
        create_large_seasonal_series(self.client, "s", count=300)
        assert self.client.execute_command("TS.TREND", "s", "-", "+", "TIMEOUT", 1)


class TestStationarityLagsRouting(SlowlogMixin, ValkeyTimeSeriesTestCaseBase):
    """TS.STATIONARITY counts the lags it runs in its work, not just the samples.

    Each lag is a pass over the data, so `TEST adf LAGS 1000` over 50,000 samples is ~170 ms
    (release build) against ~7 ms for the default lags. The work used to be the sample count
    alone, so that call ran inline, on the main thread, like the cheap one.
    """

    COUNT = 50_000

    def test_many_lags_over_a_range_that_used_to_be_inline_go_to_the_pool(self):
        create_large_seasonal_series(self.client, "s", count=self.COUNT)
        argv = ["TS.STATIONARITY", "s", "-", "+", "TEST", "adf", "LAGS", 1000]
        with self.slowlog_threshold(20_000):
            # Control: forced inline it holds the main thread for ~170 ms, so the slowlog
            # sees it. Without this the absence below would prove nothing.
            self.client.execute_command("SLOWLOG", "RESET")
            self.client.execute_command(
                "EVAL", "return redis.call('TS.STATIONARITY','s','-','+','TEST','adf','LAGS',1000)", 0
            )
            assert self.slowlogged(b"eval"), "ADF with 1000 lags over 50,000 samples is slow"

            self.client.execute_command("SLOWLOG", "RESET")
            result = self.client.execute_command(*argv)
            assert not self.slowlogged(b"ts.stationarity")
            wait_for_analysis_pool_idle(self.client)
        assert dict(zip(result[::2], result[1::2]))[b"test"] == b"adf"
