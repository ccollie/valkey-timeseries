import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryIndex(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'cpu', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'type', 'usage')  # No name label

    def test_basic_query(self):
        """Test basic TS.QUERYINDEX functionality"""
        self.setup_test_data(self.client)

        # Query for all CPU metrics
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=cpu'))
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6']

        # Query for all metrics from node1
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'node=node1'))
        assert result == [b'ts1', b'ts3', b'ts5']

    def test_compound_filters(self):
        """Test querying with multiple label conditions"""
        self.setup_test_data(self.client)

        # Query for CPU usage metrics
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type=usage'))
        assert result == [b'ts1', b'ts2']

        # Query for all usage metrics from node2
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'type=usage', 'node=node2'))
        assert result == [b'ts2', b'ts4']

    def test_regex_matching(self):
        """Test querying with regex patterns"""
        self.setup_test_data(self.client)

        # Query for all metrics with name matching 'c.*'
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=~"c.*"'))
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6']

        # Query for all metrics with node matching 'node[12]'
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'node=~"node[12]"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5']

        # Match using alternation
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=~"cpu|disk"'))
        assert result == [b'ts1', b'ts2', b'ts5', b'ts6', b'ts7']

    def test_negative_matching(self):
        """Test querying with negation patterns"""
        self.setup_test_data(self.client)

        # Query for all metrics with name not equal to cpu
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name!=cpu'))
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Query for all metrics with type not matching 'usage'
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'type!=usage'))
        assert result == [b'ts5', b'ts6']

    def test_prometheus_not_regex_matcher(self):
        """Test Prometheus-style regex negation matchers (label!~"regex")"""
        self.setup_test_data(self.client)

        # Not matching regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name!~"c.*"'))
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Not matching regex alternation
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name!~"cpu|memory"'))
        assert result == [b'ts7', b'ts8']

        # Not matching using character class
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'node!~"node[12]"'))
        assert result == [b'ts6', b'ts7', b'ts8']

    def test_complex_queries(self):
        """Test more complex query combinations"""
        self.setup_test_data(self.client)

        # CPU metrics that are not usage type
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'type!=usage'))
        assert result == [b'ts5', b'ts6']

        # Non-CPU metrics that are usage type
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name!=cpu', 'type=usage'))
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

        # Regex not matching
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name!~"c.*"'))
        assert result == [b'ts3', b'ts4', b'ts7', b'ts8']

    def test_missing_labels(self):
        """Test querying for metrics with missing labels"""
        self.setup_test_data(self.client)

        # Find series without the 'name' label
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name='))
        assert result == [b'ts8']

        # Find series without the 'node' label
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'node='))
        assert result == [b'ts8']

    def test_combined_operations(self):
        """Test combination of different operations"""
        self.setup_test_data(self.client)

        # Find series that match regex but don't match another condition
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=~".*"', 'type!=usage'))
        assert result == [b'ts5', b'ts6']

        # Mix of equals, not equals, and regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'node!=node1', 'type=~".*"'))
        assert result == [b'ts2', b'ts6']

    def test_error_cases(self):
        """Test error conditions"""
        self.setup_test_data(self.client)

        # Empty query should return error
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.QUERYINDEX')
        assert "wrong number of arguments for 'ts.queryindex' command" in str(excinfo.value).lower()


    def test_no_results(self):
        """Test queries that should return no results"""
        self.setup_test_data(self.client)

        # Query for non-existent label value
        result = self.client.execute_command('TS.QUERYINDEX', 'name=nonexistent')
        assert result == []

        # Query with impossible combination
        result = self.client.execute_command('TS.QUERYINDEX', 'name=cpu', 'name=memory')
        assert result == []

    def test_multiple_queries(self):
        """Test executing multiple QUERYINDEX commands sequentially"""
        self.setup_test_data(self.client)

        # First query
        result1 = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=cpu'))
        assert result1 == [b'ts1', b'ts2', b'ts5', b'ts6']

        # Second query with a different filter
        result2 = sorted(self.client.execute_command('TS.QUERYINDEX', 'type=usage'))
        assert result2 == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts7', b'ts8']

    def test_list_include_queries(self):
        """Test executing multiple QUERYINDEX commands sequentially"""
        self.setup_test_data(self.client)

        # First query
        result1 = sorted(self.client.execute_command('TS.QUERYINDEX', 'name=(cpu,disk)'))
        assert result1 == [b'ts1', b'ts2', b'ts5', b'ts6', b'ts7']
