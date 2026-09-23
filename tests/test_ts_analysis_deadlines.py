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
