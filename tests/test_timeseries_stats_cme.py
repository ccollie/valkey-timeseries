import pytest
from valkeytestframework.conftest import resource_port_tracker
from valkey import ResponseError, ValkeyCluster
from common import parse_stats_response
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

class TestTsStatsCluster(ValkeyTimeSeriesClusterTestCase):
    """Test suite for TS.STATS command in cluster mode."""

    def get_stats(self, limit: int | None = None):
        client = self.new_client_for_primary(0)
        args = ['TS.STATS']
        if limit is not None:
            args.extend(['LIMIT', limit])

        # In cluster mode, the command is sent to one node and fanned out
        result = client.execute_command(*args)
        stats = parse_stats_response(result)
        return stats

    def test_stats_cluster_basic(self):
        """Test TS.STATS aggregates series counts from multiple shards."""
        # Create series on different shards using hash tags to ensure distribution
        cluster: ValkeyCluster = self.new_cluster_client()

        cluster.execute_command('TS.CREATE', 'ts:{1}', 'LABELS', 'metric', 'cpu', 'host', 'h1')
        cluster.execute_command('TS.CREATE', 'ts:{2}', 'LABELS', 'metric', 'cpu', 'host', 'h2')
        cluster.execute_command('TS.CREATE', 'ts:{3}', 'LABELS', 'metric', 'mem', 'host', 'h3')

        stats = self.get_stats()

        # Verify total series count across cluster
        assert stats['numSeries'] == 3

        # Verify total label pairs
        # ts:{1}: metric=cpu, host=h1 (2 pairs)
        # ts:{2}: metric=cpu, host=h2 (2 pairs)
        # ts:{3}: metric=mem, host=h3 (2 pairs)
        # Total: 6 pairs
        assert stats['numLabelPairs'] == 6

    def test_stats_cluster_top_k_aggregation(self):
        """Test TS.STATS aggregates top-k lists across shards correctly."""
        cluster: ValkeyCluster = self.new_cluster_client()

        # Shard 1: 2 series with type=A
        cluster.execute_command('TS.CREATE', 'ts:{1}a', 'LABELS', 'type', 'A')
        cluster.execute_command('TS.CREATE', 'ts:{1}b', 'LABELS', 'type', 'A')

        # Shard 2: 1 series with type=A, 1 with type=B
        cluster.execute_command('TS.CREATE', 'ts:{2}a', 'LABELS', 'type', 'A')
        cluster.execute_command('TS.CREATE', 'ts:{2}b', 'LABELS', 'type', 'B')

        # Shard 3: 2 series with type=B
        cluster.execute_command('TS.CREATE', 'ts:{3}a', 'LABELS', 'type', 'B')
        cluster.execute_command('TS.CREATE', 'ts:{3}b', 'LABELS', 'type', 'B')

        # Total across cluster: type=A (3), type=B (3)

        stats = self.get_stats()

        pair_counts = {item[0]: item[1] for item in stats['seriesCountByLabelPair']}

        assert pair_counts.get('type=A') == 3
        assert pair_counts.get('type=B') == 3

    def test_stats_cluster_limit(self):
        """Test TS.STATS LIMIT parameter in cluster mode."""
        # Create 15 unique label values across shards
        cluster: ValkeyCluster = self.new_cluster_client()
        for i in range(15):
            # Use hash tags to distribute
            tag = f"{{{i}}}"
            cluster.execute_command('TS.CREATE', f'ts:{tag}', 'LABELS', 'id', f'val{i}')

        # Default limit is usually 10
        stats = self.get_stats()
        assert len(stats['seriesCountByLabelPair']) <= 10

        # Explicit limit 5
        stats = self.get_stats(5)
        assert len(stats['seriesCountByLabelPair']) <= 5

    def test_stats_cluster_empty(self):
        """Test TS.STATS on an empty cluster."""
        stats = self.get_stats()
        assert stats['numSeries'] == 0
        assert stats['numLabelPairs'] == 0
        assert stats['seriesCountByLabelPair'] == []
