"""Deadlines and the horizon cap across the analysis commands that run on the pool.

TIMEOUT firing was only exercised for AUTOFORECAST, TREND, DECOMPOSE and PERIODS, and the live
`ts-forecast-max-horizon` setting only for FORECAST. These cover the rest. Each timeout case uses
a 1 ms deadline on work that takes far longer, so getting the timeout error at all shows the reply
did not wait for the job, whatever the speed of the machine.
"""
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


class TestTrendInlineThreshold(ValkeyTimeSeriesTestCaseBase):
    """TS.TREND leaves the main thread past about 1 ms of work, not 20 ms.

    The default fit costs about 0.4 ms plus 2.4 us a sample, so it runs inline only up to 400
    samples; a 2,000-sample range (about 5 ms, release build) used to run inline too. The
    slowlog is the observable: it records only the time a command holds the main thread, so a
    fit forced inline (Lua cannot block) shows up in it and one handed to the pool does not.
    A deadline cannot tell the two apart, since a 1 ms one is processed too coarsely to beat a
    5 ms job.
    """

    SLOWLOG_CONFIG = "slowlog-log-slower-than"

    def slowlogged(self, name):
        entries = self.client.slowlog_get(128)
        return [e for e in entries if e["command"].lower().startswith(name)]

    def test_range_between_the_old_and_new_threshold_stays_off_the_main_thread(self):
        create_large_seasonal_series(self.client, "s", count=2000)
        previous = self.client.execute_command("CONFIG", "GET", self.SLOWLOG_CONFIG)[1]
        try:
            self.client.execute_command("CONFIG", "SET", self.SLOWLOG_CONFIG, 1000)

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
        finally:
            self.client.execute_command("CONFIG", "SET", self.SLOWLOG_CONFIG, previous)

    def test_the_same_range_answers_with_the_fit(self):
        create_large_seasonal_series(self.client, "s", count=2000)
        result = self.client.execute_command("TS.TREND", "s", "-", "+")
        fitted = dict(zip(result[::2], result[1::2]))[b"fitted_trend"]
        assert len(fitted) == 2000

    def test_small_range_still_runs_inline_and_ignores_the_deadline(self):
        create_large_seasonal_series(self.client, "s", count=300)
        assert self.client.execute_command("TS.TREND", "s", "-", "+", "TIMEOUT", 1)
