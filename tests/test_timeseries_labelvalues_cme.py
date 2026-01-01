import pytest
from valkey import ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase, ValkeySearchClusterTestCaseDebugMode

TS1 = 'ts1:{1}'
TS2 = 'ts2:{2}'
TS3 = 'ts3:{3}'
TS4 = 'ts4:{1}'
TS5 = 'ts5:{2}'
TS6 = 'ts6:{3}'
TS7 = 'ts7:{1}'
TS8 = 'ts8:{2}'
TS9 = 'ts9:{3}'

class TestTimeSeriesLabelValues(ValkeySearchClusterTestCaseDebugMode):

    @staticmethod
    def setup_test_data(client):
        """Set up test data with various label values"""
        # Create time series with different label combinations
        client.execute_command('TS.CREATE', TS1, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server1', 'datacenter', 'dc1', "key", "ts1")
        client.execute_command('TS.CREATE', TS2, 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'server1', 'datacenter', 'dc1', "key", "ts2")
        client.execute_command('TS.CREATE', TS3, 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'server2', 'datacenter', 'dc1', "key", "ts3")
        client.execute_command('TS.CREATE', TS4, 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'server2', 'datacenter', 'dc2', "key", "ts4")
        client.execute_command('TS.CREATE', TS5, 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server3', 'datacenter', 'dc2', "key", "ts5")
        client.execute_command('TS.CREATE', TS6, 'LABELS', 'name', 'network', 'type', 'throughput', 'node', 'server3', "key", "ts6")

        # Add some sample data
        now = 1000
        KEYS = [ TS1, TS2, TS3, TS4, TS5, TS6]
        i = 1
        for ts_key in KEYS:
            client.execute_command('TS.ADD', ts_key, now, i * 10)
            client.execute_command('TS.ADD', ts_key, now + 1000, i * 10 + 5)


    def test_label_values_with_filter(self):
        """Test retrieving label values with a filter"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'name' label filtered by type=usage
        # result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        # assert result == [b'cpu', b'disk', b'memory']

        # Get values for the 'type' label filtered by name=cpu
        result = client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=cpu')
        assert result == [b'temperature', b'usage']

    def test_label_values_with_multiple_filters(self):
        """Test retrieving label values with multiple filters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'node' label with multiple filters
        result = client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=cpu', 'type=usage')
        assert result == [b'server1', b'server3']

        # Get values for the 'datacenter' label with multiple filters
        result = client.execute_command('TS.LABELVALUES', 'datacenter', 'FILTER', 'name=cpu', 'type=usage')
        assert result == [b'dc1', b'dc2']

    def test_label_values_with_regex_filters(self):
        """Test retrieving label values with regex filters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'node' label with regex filter
        result = client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=~"c.*"')
        assert result == [b'server1', b'server3']

        # Get values for the 'type' label with regex filter
        result = client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=~"(memory|disk)"')
        assert result == [b'usage']

    def test_label_values_with_time_range(self):
        """Test retrieving label values with time range filtering"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Add data with specific timestamps for time range testing
        cluster.execute_command('TS.CREATE', 'ts_old', 'LABELS', 'name', 'archive', 'age', 'old', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_old', 500, 100)

        cluster.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'recent', 'age', 'new', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_new', 2500, 200)

        # Get values for the 'age' label with time range
        # First timestamp should exclude the 'old' series
        result = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', 1000, "+", "FILTER", 'common=1')
        assert result == [b'new']

        # Get values with end time range
        result = client.execute_command('TS.LABELVALUES', 'age', 'FILTER_BY_RANGE', '-', 1500, "FILTER", 'common=1')
        assert result == [b'old']

        # Get values with both start and end time range
        cluster.execute_command('TS.CREATE', 'ts_1', 'LABELS', 'name', 'alice', 'age', '32', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_1', 1000, 200)

        cluster.execute_command('TS.CREATE', 'ts_2', 'LABELS', 'name', 'bob', 'age', '45', 'common', '1')
        cluster.execute_command('TS.ADD', 'ts_2', 1700, 200)

        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 900, 2000, "FILTER", 'common=1')
        assert result == [b'alice', b'bob']

    def test_label_values_with_limit(self):
        """Test retrieving label values with LIMIT parameter"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values for the 'name' label with limit
        result = client.execute_command('TS.LABELVALUES', 'name', 'LIMIT', 2, 'FILTER', 'type=usage')
        assert len(result) == 2
        assert all(val in [b'cpu', b'disk', b'memory'] for val in result)

        # Get values for the 'node' label with limit
        result = client.execute_command('TS.LABELVALUES', 'node', 'LIMIT', 1, "FILTER", 'datacenter=dc2')
        assert result == [b'server2']

    def test_label_values_with_combined_parameters(self):
        """Test retrieving label values with combined parameters"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Get values with filter, time range, and limit
        result = client.execute_command(
            'TS.LABELVALUES', 'type',
            'FILTER_BY_RANGE', 500, 2500,
            'LIMIT', 1,
            'FILTER', 'name=~"c.*"',
        )
        assert len(result) == 1
        assert result[0] in [b'temperature', b'usage']

        # Different combination of parameters
        result = client.execute_command(
            'TS.LABELVALUES', 'node',
            'FILTER_BY_RANGE', 900, '+',
            'FILTER', 'datacenter=dc1'
        )
        assert result == [b'server1', b'server2']

    def test_label_values_empty_result(self):
        """Test retrieving label values with no matching results"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        # Filter that doesn't match any series
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'nonexistent=value')
        assert result == []

        # Filter with a time range that excludes all series
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER_BY_RANGE', 5000, '+', "FILTER", 'type=usage')
        assert result == []

    def test_label_values_after_series_deletion(self):
        """Test retrieving label values after series deletion"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Verify initial state
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        assert b'network' in result

        # Delete a time series
        client.execute_command('DEL', TS6)  # ts6 has name=network

        # Verify the deleted label value is no longer returned
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        assert b'network' not in result

    def test_label_values_with_nonexistent_label(self):
        """Test retrieving values for a non-existent label"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Query for a label that doesn't exist
        result = client.execute_command('TS.LABELVALUES', 'nonexistent_label', 'FILTER', 'name=cpu')
        assert result == []

    def test_label_values_after_label_update(self):
        """Test retrieving values after updating labels"""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        self.setup_test_data(cluster)

        # Verify initial state
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        assert b'updated' not in result

        # Create a new time series with a new label value
        cluster.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'updated', 'type', 'usage')

        # Verify the new label value is included
        result = client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        assert b'updated' in result
