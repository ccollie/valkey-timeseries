"""Cluster-mode tests for the analysis commands that touch two keys.

The analysis itself is the standalone behaviour and is covered there. What only a cluster can
show is whether the declared key specs name every key: the STORE destination of TS.FORECAST,
TS.AUTOFORECAST, TS.TREND, TS.FILLGAPS and TS.SANITIZE, and both series of TS.XCORR. If a spec
missed a key, a node would run the command against a key it may not own (or write one into the
wrong shard) instead of refusing it with CROSSSLOT.

Each test builds its own cluster (`setup_test` is function-scoped), so the cases are grouped.
"""

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

SRC = "{a}src"
SAME_SLOT_DST = "{a}dst"
OTHER_SLOT_DST = "{b}dst"

STORE_COMMANDS = [
    ["TS.FORECAST", SRC, "-", "+", "MODELS", "SES", "HORIZON", 3],
    ["TS.AUTOFORECAST", SRC, "-", "+", "HORIZON", 3, "MODELS", "ETS"],
    ["TS.TREND", SRC, "-", "+"],
    ["TS.FILLGAPS", SRC, "-", "+", "FREQUENCY", 500],
    ["TS.SANITIZE", SRC, "-", "+"],
]


class TestAnalysisKeySpecsInCluster(ValkeyTimeSeriesClusterTestCase):

    def add_series(self, cluster, key, count=60):
        cluster.execute_command("TS.CREATE", key)
        args = []
        for i in range(count):
            args += [key, (i + 1) * 1000, 10.0 + (i % 7) + i * 0.1]
        cluster.execute_command("TS.MADD", *args)

    def owner_client(self, cluster, key):
        """A direct connection to the primary that owns `key`'s slot, so the server's own
        CROSSSLOT check is what answers, not a client-side redirect."""
        port = cluster.get_node_from_key(key).port
        for index in range(self.CLUSTER_SIZE):
            primary = self.replication_groups[index].primary.server
            if primary.port == port:
                return primary.get_new_client()
        raise AssertionError(f"no primary on port {port}")

    def test_store_destination_in_another_slot_is_refused(self):
        cluster = self.new_cluster_client()
        self.add_series(cluster, SRC)
        node = self.owner_client(cluster, SRC)
        for argv in STORE_COMMANDS:
            with pytest.raises(ResponseError, match="CROSSSLOT"):
                node.execute_command(*argv, "STORE", OTHER_SLOT_DST)
        assert cluster.execute_command("EXISTS", OTHER_SLOT_DST) == 0

    def test_store_destination_in_the_same_slot_is_written(self):
        cluster = self.new_cluster_client()
        self.add_series(cluster, SRC)
        for index, argv in enumerate(STORE_COMMANDS):
            dst = f"{SAME_SLOT_DST}{index}"
            cluster.execute_command(*argv, "STORE", dst)
            assert cluster.execute_command("EXISTS", dst) == 1, argv[0]

    def test_xcorr_needs_both_series_in_one_slot(self):
        cluster = self.new_cluster_client()
        self.add_series(cluster, "{a}x")
        self.add_series(cluster, "{a}y")
        self.add_series(cluster, "{b}y")

        result = cluster.execute_command("TS.XCORR", "{a}x", "{a}y", "-", "+", 2)
        fields = dict(zip(result[::2], result[1::2]))
        assert fields[b"n"] == 60

        with pytest.raises(ResponseError, match="CROSSSLOT"):
            self.owner_client(cluster, "{a}x").execute_command(
                "TS.XCORR", "{a}x", "{b}y", "-", "+", 2
            )
