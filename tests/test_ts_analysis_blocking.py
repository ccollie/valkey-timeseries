"""Analysis commands inside MULTI/EXEC and Lua, where a client cannot be blocked.

Each case is sized past the command's inline threshold, so outside a transaction it would run
on the analysis pool. Inside one it must run inline and return its real result: blocking such a
client would make the server answer with an error while the job still ran (and wrote).
"""
import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

COUNT = 6000

BACKGROUND_COMMANDS = [
    ["TS.FORECAST", "s", "-", "+", "MODELS", "SES", "HORIZON", 3],
    ["TS.AUTOFORECAST", "s", "-", "+", "HORIZON", 3, "MODELS", "ETS"],
    ["TS.BACKTEST", "s", "-", "+", "MODELS", "Naive", "HORIZON", 5, "N_FOLDS", 2],
    ["TS.FEATURES", "s", "-", "+", "CATEGORY", "basic"],
    ["TS.TREND", "s", "-", "+"],
    ["TS.DECOMPOSE", "s", "-", "+", "SEASONALITY", 24],
    ["TS.PERIODS", "s", "-", "+"],
    ["TS.AUTOCORRELATION", "s", "-", "+", 10, "PARTIAL"],
    ["TS.XCORR", "s", "t", "-", "+", 1000],
    ["TS.OUTLIERS", "s", "-", "+", "METHOD", "zscore"],
]


def command_id(argv):
    return argv[0]


class TestAnalysisWhereBlockingIsDenied(ValkeyTimeSeriesTestCaseBase):

    def make_series(self, key):
        self.client.execute_command("TS.CREATE", key)
        for start in range(0, COUNT, 1000):
            args = []
            for i in range(start, min(start + 1000, COUNT)):
                args += [key, (i + 1) * 1000, 10.0 + (i % 24) + i * 0.001]
            self.client.execute_command("TS.MADD", *args)

    def make_both_series(self):
        self.make_series("s")
        self.make_series("t")

    @pytest.mark.parametrize("argv", BACKGROUND_COMMANDS, ids=command_id)
    def test_runs_inline_inside_multi(self, argv):
        self.make_both_series()
        expected = self.client.execute_command(*argv)
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command(*argv)
        [result] = pipe.execute(raise_on_error=False)
        assert not isinstance(result, ResponseError), result
        assert type(result) is type(expected)

    @pytest.mark.parametrize("argv", BACKGROUND_COMMANDS, ids=command_id)
    def test_runs_inline_inside_lua(self, argv):
        self.make_both_series()
        script = "return redis.call(unpack(ARGV))"
        result = self.client.execute_command("EVAL", script, 0, *argv)
        assert result is not None

    def test_store_inside_multi_writes_and_replies_with_the_count(self):
        self.make_both_series()
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command(
            "TS.FORECAST", "s", "-", "+", "MODELS", "SES", "HORIZON", 4, "STORE", "dst"
        )
        pipe.execute_command("TS.RANGE", "dst", "-", "+")
        written, stored = pipe.execute()
        assert written == 4
        assert len(stored) == 4

    def test_timeout_options(self):
        self.make_both_series()
        assert self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "CATEGORY", "basic", "TIMEOUT", 30000
        )
        assert self.client.execute_command("TS.XCORR", "s", "t", "-", "+", 5, "TIMEOUT", 30000)
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.XCORR", "s", "t", "-", "+", 5, "BOGUS")
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.FEATURES", "s", "-", "+", "CATEGORY", "basic",
                                        "TIMEOUT", -1)
