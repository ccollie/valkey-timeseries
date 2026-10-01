"""Analysis commands inside MULTI/EXEC and Lua, where a client cannot be blocked.

Each case is sized past the command's inline threshold, so outside a transaction it would run
on the analysis pool. Inside one it must run inline and return its real result: blocking such a
client would make the server answer with an error while the job still ran (and wrote).

Inline means on the main thread, which nothing can cancel, so a range past the command's ceiling
is refused there instead of freezing the server (`TestOversizedWhereBlockingIsDenied`).
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


TOO_LARGE = "too large to run inside MULTI"
FOURIER_ENTROPY_MAX = 20_000

# Each case is one sample past the command's ceiling in its own measure (see `WorkLimits` in
# the command's source), so a refusal is immediate and the series is the only cost.
# `(argv, samples in "s", samples in "t")`. TS.PERIODS (1M) and the cheap TS.OUTLIERS methods
# (1M) are left out: building a series that large costs more than the case is worth, and they
# share the runner path the others exercise. TS.STATIONARITY is reached through `LAGS`, which
# its work counts (four passes per ADF lag): 250,001 samples at 1,000 lags is 1.001G > 1G.
OVERSIZED = [
    (["TS.TREND", "s", "-", "+"], 40_001, 0),
    (["TS.DECOMPOSE", "s", "-", "+", "SEASONALITY", 24], 100_001, 0),
    (["TS.STATIONARITY", "s", "-", "+", "TEST", "adf", "LAGS", 1000], 250_001, 0),
    (["TS.AUTOCORRELATION", "s", "-", "+", 1000, "PARTIAL"], 100_001, 0),
    (["TS.XCORR", "s", "t", "-", "+", 1000], 100_001, 100_001),
    (["TS.FEATURES", "s", "-", "+", "FEATURE", "pacf:1000"], 100_001, 0),
    (["TS.FEATURES", "s", "-", "+", "FEATURE", "fourier_entropy"], 10_001, 0),
    (["TS.FORECAST", "s", "-", "+", "MODELS", "SES,Naive,SMA", "HORIZON", 3], 7_000, 0),
    (["TS.AUTOFORECAST", "s", "-", "+", "HORIZON", 3, "MODELS", "ETS"], 10_001, 0),
    (["TS.BACKTEST", "s", "-", "+", "MODELS", "Naive", "HORIZON", 5, "N_FOLDS", 12], 5_001, 0),
    (["TS.OUTLIERS", "s", "-", "+", "METHOD", "esd"], 6_001, 0),
    (["TS.OUTLIERS", "s", "-", "+", "METHOD", "rcf"], 10_001, 0),
]


def oversized_id(case):
    argv = case[0]
    method = argv[argv.index("METHOD") + 1] if "METHOD" in argv else ""
    feature = argv[argv.index("FEATURE") + 1] if "FEATURE" in argv else ""
    return "-".join(part for part in (argv[0], method, feature) if part)


class TestOversizedWhereBlockingIsDenied(ValkeyTimeSeriesTestCaseBase):
    """A call that cannot be blocked and is too big is refused, not run on the main thread."""

    def make_sized(self, key, count):
        self.client.execute_command("TS.CREATE", key)
        for start in range(0, count, 2000):
            args = []
            for i in range(start, min(start + 2000, count)):
                args += [key, (i + 1) * 1000, 10.0 + (i % 24) + i * 0.001]
            self.client.execute_command("TS.MADD", *args)

    def make_for(self, case):
        _, s_count, t_count = case
        self.make_sized("s", s_count)
        if t_count:
            self.make_sized("t", t_count)

    @pytest.mark.parametrize("case", OVERSIZED, ids=oversized_id)
    def test_refused_inside_lua(self, case):
        self.make_for(case)
        script = "return redis.call(unpack(ARGV))"
        with pytest.raises(ResponseError, match=TOO_LARGE):
            self.client.execute_command("EVAL", script, 0, *case[0])
        assert self.client.ping()

    @pytest.mark.parametrize("case", OVERSIZED, ids=oversized_id)
    def test_refused_inside_multi(self, case):
        self.make_for(case)
        pipe = self.client.pipeline(transaction=True)
        pipe.execute_command(*case[0])
        pipe.execute_command("PING")
        refused, pong = pipe.execute(raise_on_error=False)
        assert isinstance(refused, ResponseError), refused
        assert TOO_LARGE in str(refused)
        # The rest of the transaction still ran.
        assert pong in (True, b"PONG")

    def test_the_refusal_names_the_size_and_the_limit(self):
        self.make_sized("s", 40_001)
        with pytest.raises(ResponseError) as refused:
            self.client.execute_command(
                "EVAL", "return redis.call(unpack(ARGV))", 0, "TS.TREND", "s", "-", "+"
            )
        assert "40001 samples exceeds the limit of 40000" in str(refused.value)
        assert "outside of it" in str(refused.value)

    def test_a_range_exactly_at_the_limit_still_runs_inline(self):
        # 100,000 samples x lag 1000 is exactly the 100M the features ceiling allows.
        self.make_sized("s", 100_001)
        script = "return redis.call(unpack(ARGV))"
        at_limit = ["TS.FEATURES", "s", 1000, 100_000_000, "FEATURE", "pacf:1000"]
        assert self.client.execute_command("EVAL", script, 0, *at_limit)
        with pytest.raises(ResponseError, match=TOO_LARGE):
            self.client.execute_command(
                "EVAL", script, 0, "TS.FEATURES", "s", "-", "+", "FEATURE", "pacf:1000"
            )

    def test_the_same_range_runs_outside_a_transaction(self):
        """The limit is for the main thread only: with the client blockable it uses the pool."""
        self.make_sized("s", 100_001)
        result = self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "pacf:1000"
        )
        assert result


class TestFourierEntropyLimit(ValkeyTimeSeriesTestCaseBase):
    """`fourier_entropy` is a quadratic DFT: refused past a documented size in every context."""

    def make_sized(self, key, count):
        self.client.execute_command("TS.CREATE", key)
        for start in range(0, count, 2000):
            args = []
            for i in range(start, min(start + 2000, count)):
                args += [key, (i + 1) * 1000, 10.0 + (i % 24) + i * 0.001]
            self.client.execute_command("TS.MADD", *args)

    def test_small_range_computes(self):
        self.make_sized("s", 500)
        result = self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "fourier_entropy"
        )
        assert result[0] == b"fourier_entropy"
        assert float(result[1]) > 0

    def test_refused_past_the_limit_outside_a_transaction(self):
        self.make_sized("s", 20_001)
        with pytest.raises(ResponseError, match="limited to 20000; the range has 20001"):
            self.client.execute_command(
                "TS.FEATURES", "s", "-", "+", "FEATURE", "fourier_entropy"
            )
        assert self.client.ping()

    def test_the_limit_counts_finite_values_only(self):
        """Non-finite samples are dropped before computing, so they do not count towards it."""
        count = FOURIER_ENTROPY_MAX + 10
        self.client.execute_command("TS.CREATE", "s")
        for start in range(0, count, 2000):
            args = []
            for i in range(start, min(start + 2000, count)):
                # The first ten are NaN: 20,010 samples, exactly 20,000 of them finite.
                args += ["s", (i + 1) * 1000, "nan" if i < 10 else 10.0 + (i % 24)]
            self.client.execute_command("TS.MADD", *args)
        result = self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "fourier_entropy"
        )
        assert result[0] == b"fourier_entropy"

    def test_other_features_have_no_such_limit(self):
        self.make_sized("s", 20_001)
        assert self.client.execute_command(
            "TS.FEATURES", "s", "-", "+", "FEATURE", "mean,variance"
        )
