"""TS.QUERY / TS.QUERYRANGE when a shard fails.

A PromQL query in a cluster reads every shard (or every one its HASHTAG names). It must never
answer from part of them: a shard that is slow past the query's TIMEOUT, or gone, fails the whole
query with an error. Once the cluster has recovered, a failover included, the same query answers
in full again from every live node.
"""
import os
import signal
import threading
import time

import pytest
from valkey import ResponseError, Valkey

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true

QUERIES = [
    ("TS.QUERY", "sum by (job) (req)", "TIME", "60"),
    ("TS.QUERYRANGE", "sum by (job) (rate(req[30s]))", "START", "30", "END", "60",
     "STEP", "10s"),
]


def rendered(raw):
    result = QueryResult.from_raw(raw)
    if result.is_vector():
        return sorted((sorted(s.metric.items()), s.value.value) for s in result.result)
    return sorted((sorted(s.metric.items()), [(p.timestamp, p.value) for p in s.values])
                  for s in result.result)


def node_id(client) -> str:
    value = client.execute_command("CLUSTER MYID")
    return value.decode() if isinstance(value, bytes) else value


class TestQueryShardFailuresCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def get_config_file_lines(self, test_dir, port):
        # Detect a dead node in seconds rather than the default 15 s.
        return super().get_config_file_lines(test_dir, port) + ["cluster-node-timeout 2000"]

    def populate(self):
        """Thirty series spread over every shard; returns each query's full answer."""
        cluster = self.new_cluster_client()
        for i in range(30):
            key = f"req:{i}"
            cluster.execute_command("TS.CREATE", key, "LABELS", "__name__", "req",
                                    "job", "web" if i % 2 else "api", "instance", str(i))
            for t in range(0, 61, 10):
                cluster.execute_command("TS.ADD", key, t * 1000, t * (i + 1))
        for group in self.replication_groups:
            group.wait_for_replica_offset_to_sync_up(0)
        coordinator = self.client_for_primary(0)
        answers = [rendered(coordinator.execute_command(*q)) for q in QUERIES]
        assert all(answers)
        return answers

    def test_a_stalled_shard_fails_the_query_at_its_timeout(self):
        full = self.populate()
        sleeper = threading.Thread(
            target=lambda: self.new_client_for_primary(1).execute_command("DEBUG", "SLEEP", 3))
        sleeper.start()
        try:
            time.sleep(0.2)
            coordinator = self.client_for_primary(0)
            for query in QUERIES:
                started = time.monotonic()
                with pytest.raises(ResponseError, match="timed out"):
                    coordinator.execute_command(*query, "TIMEOUT", "500ms")
                assert time.monotonic() - started < 2.5, query
        finally:
            sleeper.join()
        # The shard is back: the same queries answer in full.
        coordinator = self.client_for_primary(0)
        assert [rendered(coordinator.execute_command(*q)) for q in QUERIES] == full

    def test_a_dead_primary_fails_queries_until_its_replica_takes_over(self):
        full = self.populate()
        failed = self.replication_groups[1]
        old_primary = node_id(failed.get_primary_connection())
        os.kill(failed.primary.server.pid(), signal.SIGKILL)
        failed.primary.server.server.wait()

        # Before the failover: an error, never part of the answer.
        coordinator = self.client_for_primary(0)
        coordinator.execute_command("TS._DEBUG", "REFRESH_CLUSTERMAP")
        for query in QUERIES:
            with pytest.raises(ResponseError):
                coordinator.execute_command(*query, "TIMEOUT", "2s")

        promoted = failed.get_replica_connection(0)
        promoted.execute_command("CLUSTER FAILOVER", "TAKEOVER")
        promoted_id = node_id(promoted)
        live = [self.replication_groups[0].get_primary_connection(), promoted,
                self.replication_groups[2].get_primary_connection()]

        def promoted_everywhere():
            for client in live:
                for node in client.execute_command("CLUSTER NODES").values():
                    if node["node_id"] == promoted_id and "master" not in node["flags"]:
                        return False
            return True
        wait_for_true(promoted_everywhere, timeout=30)

        def answers_in_full(client):
            client.execute_command("TS._DEBUG", "REFRESH_CLUSTERMAP")
            try:
                return [rendered(client.execute_command(*q)) for q in QUERIES] == full
            except ResponseError:
                return False
        for client in live:
            wait_for_true(lambda: answers_in_full(client), timeout=30)
        assert old_primary != promoted_id

    def test_readonly_queries_avoid_a_dead_replica(self):
        full = self.populate()
        group = self.replication_groups[0]
        replica_id = node_id(group.get_replica_connection(0))
        os.kill(group.replicas[0].server.pid(), signal.SIGKILL)
        group.replicas[0].server.server.wait()

        live = [rg.get_primary_connection() for rg in self.replication_groups]

        def marked_failed():
            return all(any(node["node_id"] == replica_id and "fail" in node["flags"]
                           for node in c.execute_command("CLUSTER NODES").values())
                       for c in live)
        wait_for_true(marked_failed, timeout=30)
        for client in live:
            client.execute_command("TS._DEBUG", "REFRESH_CLUSTERMAP")

        primary = self.get_primary(0)
        coordinator = Valkey(host=primary.bind_ip, port=primary.port)
        coordinator.execute_command("READONLY")
        for _ in range(10):
            assert [rendered(coordinator.execute_command(*q)) for q in QUERIES] == full
