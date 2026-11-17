"""TS.QUERY / TS.QUERYRANGE in a cluster with replicas.

The coordinator reads each shard from its primary, unless the client allows replica reads
(`READONLY`): then it may ask a replica instead, as `TS.MRANGE` does. The answer must not depend
on which node of a shard served it.
"""
import pytest
from valkey import Valkey

from query_result import QueryResult
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

QUERIES = [
    ("TS.QUERY", "sum by (job) (req)", "TIME", "60"),
    ("TS.QUERY", "req", "TIME", "60"),
    ("TS.QUERYRANGE", "sum by (job) (rate(req[30s]))", "START", "30", "END", "60",
     "STEP", "10s"),
]


def served(client) -> int:
    """Peer requests this node answered, from its own TS._DEBUG METRICS."""
    flat = client.execute_command('TS._DEBUG', 'METRICS', 'fanout')
    stats = {flat[i].decode(): flat[i + 1] for i in range(0, len(flat), 2)}
    return stats['fanout_served_ok_total']


def rendered(raw):
    result = QueryResult.from_raw(raw)
    if result.is_vector():
        return sorted((sorted(s.metric.items()), s.value.value) for s in result.result)
    return sorted((sorted(s.metric.items()), [(p.timestamp, p.value) for p in s.values])
                  for s in result.result)


class TestQueryWithReplicasCME(ValkeyTimeSeriesClusterTestCaseDebugMode):
    REPLICAS_COUNT = 1

    def populate(self):
        cluster = self.new_cluster_client()
        for i in range(30):
            key = f'req:{i}'
            cluster.execute_command('TS.CREATE', key, 'LABELS', '__name__', 'req',
                                    'job', 'web' if i % 2 else 'api', 'instance', str(i))
            for t in range(0, 61, 10):
                cluster.execute_command('TS.ADD', key, t * 1000, t * (i + 1))
        for shard in range(self.CLUSTER_SIZE):
            self.get_replication_group(shard).wait_for_replica_offset_to_sync_up(0)

    def replicas(self):
        return [self.get_replication_group(i).get_replica_connection(0)
                for i in range(self.CLUSTER_SIZE)]

    def coordinator(self) -> Valkey:
        """A fresh connection to primary 0, so READONLY is set or not per test."""
        primary = self.get_primary(0)
        return Valkey(host=primary.bind_ip, port=primary.port)

    def test_without_readonly_only_primaries_serve(self):
        self.populate()
        expected = [rendered(self.client_for_primary(0).execute_command(*q)) for q in QUERIES]
        before = [served(r) for r in self.replicas()]
        coordinator = self.coordinator()
        for _ in range(10):
            for query, want in zip(QUERIES, expected):
                assert rendered(coordinator.execute_command(*query)) == want, query
        assert [served(r) for r in self.replicas()] == before, "a replica served a query"

    def test_readonly_lets_replicas_serve_with_the_same_answer(self):
        self.populate()
        expected = [rendered(self.client_for_primary(0).execute_command(*q)) for q in QUERIES]
        before = [served(r) for r in self.replicas()]
        coordinator = self.coordinator()
        coordinator.execute_command('READONLY')
        for _ in range(20):
            for query, want in zip(QUERIES, expected):
                assert rendered(coordinator.execute_command(*query)) == want, query
        # One node per shard is picked at random for each read: over 60 queries, every
        # shard's replica serves some of them.
        after = [served(r) for r in self.replicas()]
        assert all(a > b for a, b in zip(after, before)), (before, after)
