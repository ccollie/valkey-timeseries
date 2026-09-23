"""STORE writes from the analysis commands: replication, database selection and validation.

The analysis runs on the primary only. FORECAST, AUTOFORECAST, TREND and FILLGAPS replicate the
resulting write as the internal TS._STORE command, so a replica never re-runs the analysis;
SANITIZE is deterministic and inline, so it replicates itself verbatim, exactly once.
"""
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
        # Replicated verbatim exactly once, covering both the in-place and the STORE write.
        assert self.command_calls(self.replica, "ts.sanitize") == 1
        assert self.command_calls(self.replica, "ts._store") == 0

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
