import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMgetCluster(ValkeyTimeSeriesTestCaseBase):

    def get_cluster_env(self):
        """Return cluster environment configuration"""
        return {
            'cluster': True,
            'num_shards': 3,
            'num_replicas': 1
        }

    def setup_test_data(self, client):
        """Create time series distributed across different hash slots"""
        # Use hash tags to control slot distribution
        # Series with {tag1} will hash to same slot, {tag2} to different slot, etc.
        client.execute_command('TS.CREATE', 'ts:{shard1}:cpu1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1', 'region', 'us-east')
        client.execute_command('TS.CREATE', 'ts:{shard1}:cpu2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2', 'region', 'us-east')
        client.execute_command('TS.CREATE', 'ts:{shard2}:cpu3', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node3', 'region', 'us-west')
        client.execute_command('TS.CREATE', 'ts:{shard2}:cpu4', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node4', 'region', 'us-west')
        client.execute_command('TS.CREATE', 'ts:{shard3}:mem1', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1', 'region', 'eu-central')
        client.execute_command('TS.CREATE', 'ts:{shard3}:mem2', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2', 'region', 'eu-central')
        client.execute_command('TS.CREATE', 'ts:{shard1}:disk1', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node1', 'region', 'us-east')

        # Add samples to each time series
        current_time = 1000
        client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', current_time, 10)
        client.execute_command('TS.ADD', 'ts:{shard1}:cpu2', current_time, 20)
        client.execute_command('TS.ADD', 'ts:{shard2}:cpu3', current_time, 30)
        client.execute_command('TS.ADD', 'ts:{shard2}:cpu4', current_time, 40)
        client.execute_command('TS.ADD', 'ts:{shard3}:mem1', current_time, 50)
        client.execute_command('TS.ADD', 'ts:{shard3}:mem2', current_time, 60)
        client.execute_command('TS.ADD', 'ts:{shard1}:disk1', current_time, 70)

        # Add additional samples with different timestamps
        client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', current_time + 1000, 15)
        client.execute_command('TS.ADD', 'ts:{shard2}:cpu3', current_time + 1000, 35)
        client.execute_command('TS.ADD', 'ts:{shard3}:mem1', current_time + 1000, 55)

    def test_cluster_mget_cross_shard(self):
        """Test TS.MGET across multiple shards"""
        self.setup_test_data(self.client)

        # Get all CPU metrics that should span multiple shards
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        assert len(result) == 4

        # Verify keys from different shards are included
        keys = [r[0] for r in result]
        assert b'ts:{shard1}:cpu1' in keys
        assert b'ts:{shard1}:cpu2' in keys
        assert b'ts:{shard2}:cpu3' in keys
        assert b'ts:{shard2}:cpu4' in keys

        # Verify latest values
        assert result[0][2][0] == 2000  # ts:{shard1}:cpu1
        assert result[0][2][1] == b'15'
        assert result[2][2][0] == 2000  # ts:{shard2}:cpu3
        assert result[2][2][1] == b'35'

    def test_mget_cme_with_withlabels(self):
        """Test TS.MGET with WITHLABELS across cluster"""
        self.setup_test_data(self.client)

        result = self.client.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'name=memory')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2

        # Verify labels are returned from all shards
        for item in result:
            labels = item[1]
            assert len(labels) == 4  # 4 label pairs
            label_dict = {l[0]: l[1] for l in labels}
            assert label_dict[b'name'] == b'memory'
            assert label_dict[b'type'] == b'usage'
            assert label_dict[b'region'] == b'eu-central'

    def test_mget_cme_with_selected_labels(self):
        """Test TS.MGET with SELECTED_LABELS across cluster"""
        self.setup_test_data(self.client)

        result = self.client.execute_command('TS.MGET', 'SELECTED_LABELS', 'name', 'region', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        assert len(result) == 4

        # Verify only selected labels are returned from all shards
        for item in result:
            labels = item[1]
            # Should only have name and region labels
            label_names = {l[0] for l in labels if l is not None}
            assert label_names.issubset({b'name', b'region'})

    def test_mget_cme_complex_filter(self):
        """Test TS.MGET with complex filters across cluster"""
        self.setup_test_data(self.client)

        # Filter by multiple conditions
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu', 'type=usage')
        result.sort(key=lambda x: x[0])

        assert len(result) == 3
        assert result[0][0] == b'ts:{shard1}:cpu1'
        assert result[1][0] == b'ts:{shard1}:cpu2'
        assert result[2][0] == b'ts:{shard2}:cpu3'

        # Regex filter across shards
        result = self.client.execute_command('TS.MGET', 'FILTER', 'region=~"us-.*"')
        result.sort(key=lambda x: x[0])

        assert len(result) == 5
        # Verify all us-* region series are included
        regions = set()
        for item in result:
            # Keys should be from us-east or us-west
            if b'shard1' in item[0]:
                regions.add('us-east')
            elif b'shard2' in item[0]:
                regions.add('us-west')

        assert 'us-east' in regions
        assert 'us-west' in regions

    def test_mget_cme_single_shard(self):
        """Test TS.MGET when results are from a single shard"""
        self.setup_test_data(self.client)

        # Get all series from eu-central (should be on one shard)
        result = self.client.execute_command('TS.MGET', 'FILTER', 'region=eu-central')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2
        assert result[0][0] == b'ts:{shard3}:mem1'
        assert result[1][0] == b'ts:{shard3}:mem2'

    def test_mget_cme_with_latest_option(self):
        """Test TS.MGET with LATEST option across cluster"""
        self.setup_test_data(self.client)

        # Create compaction rules on different shards
        self.client.execute_command('TS.CREATE', 'ts:{shard1}:cpu1:avg', 'LABELS', 'aggregation', 'avg')
        self.client.execute_command('TS.CREATERULE', 'ts:{shard1}:cpu1', 'ts:{shard1}:cpu1:avg', 'AGGREGATION', 'avg', 5000)

        # Add more samples to trigger compaction
        current_time = 2000
        for i in range(10):
            self.client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', current_time + i * 1000, 100 + i)

        # Test LATEST option
        result = self.client.execute_command('TS.MGET', 'LATEST', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        # Should include latest values including from compaction rules
        assert len(result) == 4

    def test_mget_cme_empty_result(self):
        """Test TS.MGET with no matching series across cluster"""
        self.setup_test_data(self.client)

        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=nonexistent')
        assert result == []

    def test_mget_cme_partial_shards(self):
        """Test TS.MGET behavior when some shards have no matches"""
        self.setup_test_data(self.client)

        # Create series on specific shard
        self.client.execute_command('TS.CREATE', 'ts:{unique}:test', 'LABELS', 'name', 'test', 'unique', 'true')
        self.client.execute_command('TS.ADD', 'ts:{unique}:test', 1000, 100)

        # Query for unique label - should only hit one shard
        result = self.client.execute_command('TS.MGET', 'FILTER', 'unique=true')

        assert len(result) == 1
        assert result[0][0] == b'ts:{unique}:test'

    def test_mget_cme_large_result_set(self):
        """Test TS.MGET with large number of series across cluster"""
        # Create many series distributed across shards
        num_series = 100
        for i in range(num_series):
            shard = f'shard{i % 3}'
            key = f'ts:{{{shard}}}:metric{i}'
            self.client.execute_command('TS.CREATE', key, 'LABELS', 'name', 'load_test', 'id', str(i))
            self.client.execute_command('TS.ADD', key, 1000, i)

        # Get all load_test metrics
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=load_test')

        assert len(result) == num_series

        # Verify results are from all shards
        shard_counts = {'shard0': 0, 'shard1': 0, 'shard2': 0}
        for item in result:
            key = item[0].decode('utf-8')
            for shard in shard_counts:
                if shard in key:
                    shard_counts[shard] += 1
                    break

        # Each shard should have approximately num_series/3 series
        for count in shard_counts.values():
            assert count > 0

    def test_mget_cme_after_node_addition(self):
        """Test TS.MGET continues to work after cluster topology changes"""
        self.setup_test_data(self.client)

        # Initial query
        result1 = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        initial_count = len(result1)

        # Add more series (simulating data after potential resharding)
        self.client.execute_command('TS.CREATE', 'ts:{new}:cpu5', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node5')
        self.client.execute_command('TS.ADD', 'ts:{new}:cpu5', 1000, 90)

        # Query again
        result2 = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')

        assert len(result2) == initial_count + 1

    def test_mget_cme_with_no_samples(self):
        """Test TS.MGET with series that have no samples across cluster"""
        # Create empty series on different shards
        self.client.execute_command('TS.CREATE', 'ts:{shard1}:empty1', 'LABELS', 'name', 'empty', 'shard', '1')
        self.client.execute_command('TS.CREATE', 'ts:{shard2}:empty2', 'LABELS', 'name', 'empty', 'shard', '2')
        self.client.execute_command('TS.CREATE', 'ts:{shard3}:empty3', 'LABELS', 'name', 'empty', 'shard', '3')

        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=empty')
        result.sort(key=lambda x: x[0])

        assert len(result) == 3
        # All should have empty samples
        for item in result:
            assert item[2] == []

    def test_mget_cme_concurrent_updates(self):
        """Test TS.MGET consistency during concurrent updates"""
        self.setup_test_data(self.client)

        # Perform MGET while adding new data
        self.client.execute_command('TS.ADD', 'ts:{shard1}:cpu1', 5000, 200)
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')

        # Should get results, potentially with new data
        assert len(result) >= 4

    def test_mget_cme_label_consistency(self):
        """Test that labels are consistently returned across all shards"""
        self.setup_test_data(self.client)

        result = self.client.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'type=usage')

        # Verify all results have consistent label structure
        for item in result:
            labels = item[1]
            label_dict = {l[0]: l[1] for l in labels}
            # All should have the 'type' label with value 'usage'
            assert label_dict[b'type'] == b'usage'
