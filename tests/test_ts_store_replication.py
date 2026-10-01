"""STORE writes from the analysis commands: replication, database selection and validation.

The analysis runs on the primary only. FORECAST, AUTOFORECAST, TREND and FILLGAPS replicate the
resulting write as the internal TS._STORE command, so a replica never re-runs the analysis;
SANITIZE is inline, so it replicates itself exactly once, with its range and policy resolved.
"""
import threading
import time

import pytest

from common import SERVER_PATH, get_module_path
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.valkey_test_case import ReplicationTestCase

STEP_MS = 1000
FORECAST_MODEL = "ARIMA(1,0,0)"


class TestTimeSeriesStoreReplication(ReplicationTestCase):
    REPLICAS_COUNT = 1

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        self.args = {"enable-debug-command": "yes", "loadmodule": get_module_path()}
        self.server, self.client = self.create_server(
            testdir=self.testdir, server_path=SERVER_PATH, args=self.args
        )
        self.setup_replication(num_replicas=1)
        self.replica = self.replicas[0].client

    def add_series(self, key, count, value=lambda i: 10.0 + i + (i % 7) * 0.5, client=None):
        client = client or self.client
        client.execute_command("TS.CREATE", key)
        args = []
        for i in range(count):
            args += [key, (i + 1) * STEP_MS, value(i)]
        client.execute_command("TS.MADD", *args)

    def sync(self):
        self.waitForReplicaToSyncUp(self.replicas[0])

    def command_calls(self, client, command):
        stats = client.info("commandstats")
        return stats.get(f"cmdstat_{command}", {}).get("calls", 0)

    def assert_replica_matches(self, *keys):
        self.sync()
        for key in keys:
            primary = self.client.execute_command("TS.RANGE", key, "-", "+")
            replica = self.replica.execute_command("TS.RANGE", key, "-", "+")
            assert primary, f"{key} is empty on the primary"
            assert replica == primary, f"{key} differs on the replica"

    def assert_replicated_as_store(self, command, writes=1):
        """The replica applied the write through TS._STORE and never ran the analysis."""
        assert self.command_calls(self.replica, command) == 0
        assert self.command_calls(self.replica, "ts._store") == writes

    def test_forecast_store_replicates_the_result(self):
        self.add_series("src", 60)
        written = self.client.execute_command(
            "TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 5, "STORE", "dst"
        )
        assert written == 5
        self.assert_replica_matches("dst")
        self.assert_replicated_as_store("ts.forecast")

    def test_autoforecast_store_replicates_the_result(self):
        self.add_series("src", 60)
        self.client.execute_command(
            "TS.AUTOFORECAST", "src", "-", "+", "HORIZON", 3, "MODELS", "ETS", "STORE", "dst"
        )
        self.assert_replica_matches("dst")
        self.assert_replicated_as_store("ts.autoforecast")

    @pytest.mark.parametrize("count", [100, 2500], ids=["inline", "background"])
    def test_trend_store_replicates_the_result(self, count):
        self.add_series("src", count)
        written = self.client.execute_command(
            "TS.TREND", "src", "-", "+", "PREDICT", 3, "STORE", "dst"
        )
        assert written == count + 3
        self.assert_replica_matches("dst")
        self.assert_replicated_as_store("ts.trend")

    def test_fillgaps_store_replicates_the_result(self):
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command("TS.MADD", "src", 1000, 1, "src", 5000, 5)
        written = self.client.execute_command(
            "TS.FILLGAPS", "src", "-", "+", "FREQUENCY", STEP_MS, "VALUE", 0, "STORE", "dst"
        )
        assert written == 3
        self.assert_replica_matches("dst")
        self.assert_replicated_as_store("ts.fillgaps")

    def test_store_merge_and_options_replicate(self):
        self.add_series("src", 60)
        self.client.execute_command("TS.CREATE", "dst", "RETENTION", 0)
        self.client.execute_command("TS.ADD", "dst", 1, 42)
        # MERGE keeps the existing sample; the options only apply to a new destination.
        self.client.execute_command(
            "TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 2,
            "STORE", "dst", "MERGE", "RETENTION", 5000,
        )
        # A new destination is created on the replica with the clause's options.
        self.client.execute_command(
            "TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 2,
            "STORE", "fresh", "RETENTION", 987654, "ENCODING", "UNCOMPRESSED",
        )
        self.assert_replica_matches("dst", "fresh")
        kept = self.replica.execute_command("TS.RANGE", "dst", 1, 1)
        assert [(ts, float(v)) for ts, v in kept] == [(1, 42.0)]
        # Like TS.ADD, an existing destination keeps its own settings: RETENTION is ignored.
        for client in (self.client, self.replica):
            existing = client.execute_command("TS.INFO", "dst")
            assert dict(zip(existing[::2], existing[1::2]))[b"retentionTime"] == 0
        info = self.replica.execute_command("TS.INFO", "fresh")
        info = dict(zip(info[::2], info[1::2]))
        assert info[b"retentionTime"] == 987654
        assert info[b"chunkType"] == b"uncompressed"

    def test_sanitize_replicates_once(self):
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command(
            "TS.MADD", "src", 1000, 1, "src", 2000, "nan", "src", 3000, 3
        )
        self.client.execute_command(
            "TS.SANITIZE", "src", "-", "+", "POLICY", "INTERPOLATE", "STORE", "dst"
        )
        self.assert_replica_matches("src", "dst")
        # Replicated exactly once, covering both the in-place and the STORE write.
        assert self.command_calls(self.replica, "ts.sanitize") == 1
        assert self.command_calls(self.replica, "ts._store") == 0

    def server_time_ms(self):
        seconds, micros = self.client.time()
        return seconds * 1000 + micros // 1000

    def stall_replica(self, seconds):
        """Hold the replica off the replication stream; returns the thread to join."""
        stall = threading.Thread(
            target=self.replica.execute_command, args=("DEBUG", "SLEEP", seconds)
        )
        stall.start()
        time.sleep(0.3)  # let the replica enter the sleep before the primary writes
        return stall

    def test_sanitize_relative_bounds_replicate_resolved(self):
        """A replica applying SANITIZE late must use the window the primary resolved.

        `-2s *` is relative to the clock of whoever runs it. Replayed verbatim after the replica
        has been held back for longer than the window is wide, the window would have slid past
        every sample and the replica would keep the NaNs the primary dropped.
        """
        now = self.server_time_ms()
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command(
            "TS.MADD", "src",
            now - 1500, 1, "src", now - 1200, "nan", "src", now - 900, 3,
            "src", now - 600, "nan", "src", now - 300, 5,
        )
        self.sync()
        assert len(self.replica.execute_command("TS.RANGE", "src", "-", "+")) == 5

        stall = self.stall_replica(3)
        self.client.execute_command("TS.SANITIZE", "src", "-2s", "*")
        stall.join()
        self.sync()

        kept = self.client.execute_command("TS.RANGE", "src", "-", "+")
        assert [ts for ts, _ in kept] == [now - 1500, now - 900, now - 300]
        assert self.replica.execute_command("TS.RANGE", "src", "-", "+") == kept

    def test_sanitize_relative_bounds_replicate_resolved_store(self):
        """The STORE clause is replayed with the resolved window, so the destination matches."""
        now = self.server_time_ms()
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command(
            "TS.MADD", "src", now - 1500, 1, "src", now - 1200, "nan", "src", now - 900, 3
        )
        self.sync()

        stall = self.stall_replica(3)
        written = self.client.execute_command(
            "TS.SANITIZE", "src", "-2s", "*", "POLICY", "INTERPOLATE",
            "STORE", "dst", "RETENTION", 987654,
        )
        stall.join()
        assert written == 3
        self.assert_replica_matches("src", "dst")
        info = self.replica.execute_command("TS.INFO", "dst")
        assert dict(zip(info[::2], info[1::2]))[b"retentionTime"] == 987654

    def test_sanitize_seasonal_auto_replicates_the_detected_period(self):
        """`SEASONAL auto` is replicated as the detected period, and the replica accepts it.

        A guard on the rewritten command rather than a failing-before test: on identical data a
        replica would detect the same period anyway.
        """
        self.client.execute_command("TS.CREATE", "src")
        period = 8
        args = []
        for i in range(160):
            value = "nan" if i % 37 == 5 else float(i % period)
            args += ["src", (i + 1) * STEP_MS, value]
        self.client.execute_command("TS.MADD", *args)
        self.client.execute_command("TS.SANITIZE", "src", "-", "+", "POLICY", "SEASONAL", "auto")
        self.assert_replica_matches("src")
        assert all(
            v != b"nan" for _, v in self.replica.execute_command("TS.RANGE", "src", "-", "+")
        )

    def test_sanitize_rejected_store_is_not_replicated(self):
        """A STORE the command can't carry out changes nothing, here or on the replica.

        The destination's METRIC is held by another series, so it can't be created. The command
        used to rewrite the source and queue itself for replication before finding that out.
        """
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command(
            "TS.MADD", "src", 1000, 1, "src", 2000, "nan", "src", 3000, 3
        )
        self.client.execute_command("TS.CREATE", "holder", "METRIC", "taken")
        self.sync()
        before = self.client.execute_command("TS.RANGE", "src", "-", "+")

        with pytest.raises(Exception, match="duplicate series"):
            self.client.execute_command(
                "TS.SANITIZE", "src", "-", "+", "POLICY", "INTERPOLATE",
                "STORE", "dst", "METRIC", "taken",
            )

        self.sync()
        for client in (self.client, self.replica):
            assert client.execute_command("TS.RANGE", "src", "-", "+") == before
            assert client.execute_command("EXISTS", "dst") == 0
        assert self.command_calls(self.replica, "ts.sanitize") == 0

    def no_output_command(self, command):
        """Creates `src` so that `command` has nothing to store; returns its argv without STORE.

        FILLGAPS finds no gap and SANITIZE drops every sample. (TREND cannot get here: AutoTrend
        fails the command when no candidate wins, so a fit is never empty.)
        """
        if command == "fillgaps":
            self.add_series("src", 20)
            return ["TS.FILLGAPS", "src", "-", "+", "FREQUENCY", STEP_MS]
        self.client.execute_command("TS.CREATE", "src")
        self.client.execute_command("TS.MADD", "src", 1000, "nan", "src", 2000, "nan")
        return ["TS.SANITIZE", "src", "-", "+", "POLICY", "DROP"]

    @pytest.mark.parametrize("command", ["fillgaps", "sanitize"])
    def test_store_overwrite_with_no_output_clears_destination(self, command):
        """Overwrite means the destination holds exactly the output, so no output empties it."""
        argv = self.no_output_command(command)
        self.add_series("dst", 5)
        self.sync()
        assert len(self.replica.execute_command("TS.RANGE", "dst", "-", "+")) == 5

        assert self.client.execute_command(*argv, "STORE", "dst") == 0

        self.sync()
        for client in (self.client, self.replica):
            # Emptied, not deleted: the series and its settings stay.
            assert client.execute_command("EXISTS", "dst") == 1
            assert client.execute_command("TS.RANGE", "dst", "-", "+") == []
        if command == "fillgaps":
            self.assert_replicated_as_store("ts.fillgaps")
        else:
            assert self.command_calls(self.replica, "ts.sanitize") == 1

    @pytest.mark.parametrize("command", ["fillgaps", "sanitize"])
    def test_store_merge_with_no_output_leaves_destination(self, command):
        argv = self.no_output_command(command)
        self.add_series("dst", 5)
        before = self.client.execute_command("TS.RANGE", "dst", "-", "+")
        self.sync()

        assert self.client.execute_command(*argv, "STORE", "dst", "MERGE") == 0

        self.sync()
        for client in (self.client, self.replica):
            assert client.execute_command("TS.RANGE", "dst", "-", "+") == before
        if command == "fillgaps":
            assert self.command_calls(self.replica, "ts._store") == 0

    @pytest.mark.parametrize("command", ["fillgaps", "sanitize"])
    def test_store_with_no_output_does_not_create_destination(self, command):
        argv = self.no_output_command(command)

        assert self.client.execute_command(*argv, "STORE", "dst") == 0

        self.sync()
        for client in (self.client, self.replica):
            assert client.execute_command("EXISTS", "dst") == 0
        if command == "fillgaps":
            assert self.command_calls(self.replica, "ts._store") == 0

    def test_store_overwrite_of_an_empty_destination_is_not_replicated(self):
        """Clearing nothing changes nothing, so there is nothing to send the replica."""
        argv = self.no_output_command("fillgaps")
        self.client.execute_command("TS.CREATE", "dst")
        self.sync()

        assert self.client.execute_command(*argv, "STORE", "dst") == 0

        self.sync()
        assert self.command_calls(self.replica, "ts._store") == 0

    @pytest.mark.parametrize("command", ["fillgaps", "sanitize"])
    def test_store_overwrite_with_no_output_rejects_a_destination_of_another_type(self, command):
        argv = self.no_output_command(command)
        self.client.execute_command("SET", "dst", "not a series")

        with pytest.raises(Exception, match="WRONGTYPE"):
            self.client.execute_command(*argv, "STORE", "dst")

        assert self.client.execute_command("GET", "dst") == b"not a series"

    # ------------------------------------------------------------------
    # STORE feeds the destination's compaction rules
    # ------------------------------------------------------------------

    COMPACTION_POLICY = "count:10s:1h|^dst"
    BUCKET_MS = 10_000

    def use_compaction_policy(self):
        """Give `dst` (only) a default count rule, on the primary and on the replica.

        The configuration is per node, and the replica creates `dst` itself when it applies the
        replicated write, so it needs the policy as much as the primary does.
        """
        for client in (self.client, self.replica):
            client.execute_command("CONFIG", "SET", "ts.ts-compaction-policy", self.COMPACTION_POLICY)

    def compaction_child(self, client, key="dst"):
        info = client.execute_command("TS.INFO", key)
        rules = dict(zip(info[::2], info[1::2]))[b"rules"]
        assert len(rules) == 1, f"{key} should have exactly the one default rule: {rules}"
        return rules[0][0]

    def expected_closed_buckets(self, samples):
        """What a `count` rule holds for `samples`: every bucket but the last one, still open."""
        counts = {}
        for ts, _ in samples:
            bucket = ts // self.BUCKET_MS * self.BUCKET_MS
            counts[bucket] = counts.get(bucket, 0) + 1
        open_bucket = samples[-1][0] // self.BUCKET_MS * self.BUCKET_MS if samples else None
        return [(b, float(n)) for b, n in sorted(counts.items()) if b != open_bucket]

    def assert_compaction_matches_destination(self, expect_buckets):
        """The rule's series holds exactly the buckets of `dst`'s samples, on both nodes."""
        self.sync()
        for client in (self.client, self.replica):
            samples = client.execute_command("TS.RANGE", "dst", "-", "+")
            expected = self.expected_closed_buckets(samples)
            assert len(expected) == expect_buckets, f"expected {expect_buckets} buckets: {expected}"
            child = client.execute_command("TS.RANGE", self.compaction_child(client), "-", "+")
            assert [(ts, float(v)) for ts, v in child] == expected

    def run_store_command(self, command, destination="dst"):
        """One STORE write of ~20-60 samples spanning several 10 s buckets."""
        if command == "sanitize":
            self.add_series("sanitize_src", 60)
            self.client.execute_command("TS.MADD", "sanitize_src", 5000, "nan", "sanitize_src", 6000, "nan")
            return self.client.execute_command(
                "TS.SANITIZE", "sanitize_src", "-", "+", "POLICY", "FILL", 0, "STORE", destination
            )
        if command == "fillgaps":
            self.client.execute_command("TS.CREATE", "gaps_src")
            self.client.execute_command("TS.MADD", "gaps_src", 1000, 1, "gaps_src", 2000, 2, "gaps_src", 60000, 3)
            return self.client.execute_command(
                "TS.FILLGAPS", "gaps_src", "-", "+", "FREQUENCY", STEP_MS, "VALUE", 1, "STORE", destination
            )
        self.add_series("model_src", 60)
        if command == "trend":
            return self.client.execute_command("TS.TREND", "model_src", "-", "+", "STORE", destination)
        return self.client.execute_command(
            "TS.FORECAST", "model_src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 25,
            "STORE", destination,
        )

    # Closed 10 s buckets each command's output fills: all but the last sample's, which is open.
    # sanitize and trend store 1000..60000, fillgaps 3000..59000, forecast 61000..85000.
    @pytest.mark.parametrize("command, buckets", [
        ("sanitize", 6), ("fillgaps", 5), ("trend", 6), ("forecast", 2),
    ])
    def test_store_feeds_the_destinations_compaction_rules(self, command, buckets):
        """A new destination gets the default rules, and what is written to it flows into them.

        The rules used to be attached to the destination and then never fed, so their series
        stayed empty however much was stored.
        """
        self.use_compaction_policy()

        assert self.run_store_command(command) > 0

        self.assert_compaction_matches_destination(expect_buckets=buckets)

    def test_store_overwrite_clears_the_compaction_buckets_too(self):
        """Overwriting empties the destination, and the buckets computed from what was there."""
        self.use_compaction_policy()
        self.run_store_command("trend")  # 1000..60000: six closed buckets
        self.assert_compaction_matches_destination(expect_buckets=6)

        self.add_series("short_src", 30)
        self.client.execute_command("TS.SANITIZE", "short_src", "-", "+", "STORE", "dst")

        # 30 samples: three closed buckets. The stale ones from the first write are gone.
        self.assert_compaction_matches_destination(expect_buckets=3)

    def test_store_overwrite_with_no_output_empties_the_compaction_buckets(self):
        self.use_compaction_policy()
        self.run_store_command("trend")
        self.assert_compaction_matches_destination(expect_buckets=6)

        # No gaps in `model_src` at its own spacing: nothing to store, so `dst` is emptied.
        assert self.client.execute_command(
            "TS.FILLGAPS", "model_src", "-", "+", "FREQUENCY", STEP_MS, "STORE", "dst"
        ) == 0

        self.sync()
        for client in (self.client, self.replica):
            assert client.execute_command("TS.RANGE", "dst", "-", "+") == []
            assert client.execute_command("TS.RANGE", self.compaction_child(client), "-", "+") == []

    def test_store_merge_adds_to_the_compaction_buckets(self):
        """MERGE keeps what is there, and the new samples join the same buckets."""
        self.use_compaction_policy()
        self.run_store_command("trend")  # 1000..60000
        # Past it: 61000..85000. The 60000 and 70000 buckets close, on top of the six.
        self.client.execute_command(
            "TS.FORECAST", "model_src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 25,
            "STORE", "dst", "MERGE",
        )
        self.assert_compaction_matches_destination(expect_buckets=8)

    def test_background_store_writes_to_the_selected_db(self):
        self.client.select(3)
        try:
            self.add_series("src", 60)
            self.client.execute_command(
                "TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 4,
                "STORE", "dst",
            )
            assert self.client.execute_command("EXISTS", "dst") == 1
            self.sync()
            self.replica.select(3)
            assert len(self.replica.execute_command("TS.RANGE", "dst", "-", "+")) == 4
        finally:
            self.client.select(0)
            self.replica.select(0)
        assert self.client.execute_command("EXISTS", "dst") == 0
        assert self.replica.execute_command("EXISTS", "dst") == 0

    @pytest.mark.parametrize("command", [
        ["TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 2],
        ["TS.AUTOFORECAST", "src", "-", "+", "HORIZON", 2],
        ["TS.TREND", "src", "-", "+"],
        ["TS.FILLGAPS", "src", "-", "+", "FREQUENCY", 500],
        ["TS.SANITIZE", "src", "-", "+"],
    ], ids=lambda argv: argv[0])
    def test_store_to_the_source_key_is_rejected(self, command):
        self.add_series("src", 60)
        before = self.client.execute_command("TS.RANGE", "src", "-", "+")
        with pytest.raises(Exception, match="STORE destination must be different"):
            self.client.execute_command(*command, "STORE", "src")
        assert self.client.execute_command("TS.RANGE", "src", "-", "+") == before

    def test_options_after_the_store_clause_are_parsed(self):
        self.add_series("src", 60)
        written = self.client.execute_command(
            "TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL,
            "STORE", "dst", "RETENTION", 0, "HORIZON", 3, "LEVEL", 90,
        )
        assert written == 3
        # SANITIZE rejects what it does not recognise instead of silently ignoring it.
        with pytest.raises(Exception):
            self.client.execute_command("TS.SANITIZE", "src", "-", "+", "STORE", "out", "BOGUS")
        assert self.client.execute_command("EXISTS", "out") == 0

    def test_ts_store_rejects_direct_calls(self):
        with pytest.raises(Exception, match="internal command"):
            self.client.execute_command("TS._STORE", "dst", "")
        assert self.client.execute_command("EXISTS", "dst") == 0

    @pytest.mark.parametrize("command", [
        ["TS.FORECAST", "src", "-", "+", "MODELS", FORECAST_MODEL, "HORIZON", 2],
        ["TS.AUTOFORECAST", "src", "-", "+", "HORIZON", 2],
        ["TS.TREND", "src", "-", "+"],
        ["TS.FILLGAPS", "src", "-", "+", "FREQUENCY", 500],
        ["TS.SANITIZE", "src", "-", "+"],
    ], ids=lambda argv: argv[0])
    def test_store_to_a_key_of_another_type_is_rejected(self, command):
        self.add_series("src", 60)
        self.client.execute_command("SET", "dst", "not a series")
        with pytest.raises(Exception, match="WRONGTYPE"):
            self.client.execute_command(*command, "STORE", "dst")
        assert self.client.execute_command("GET", "dst") == b"not a series"
