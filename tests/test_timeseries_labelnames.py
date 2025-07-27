import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesLabelNames(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create multi series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'cpu', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'type', 'usage')  # No name label
        client.execute_command('TS.CREATE', 'ts9', 'LABELS', 'location', 'datacenter', 'rack', 'rack1')  # Different labels

    def test_labelnames_with_filter(self):
        """Test TS.LABELNAMES with FILTER parameter"""
        self.setup_test_data(self.client)

        # Get label names for CPU series
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'name=cpu')
        assert result == [b'name', b'node', b'type']

        # Get label names for node1 series
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'node=node1')
        assert result == [b'name', b'node', b'type']

    def test_labelnames_with_multiple_filters(self):
        """Test TS.LABELNAMES with multiple filter conditions"""
        self.setup_test_data(self.client)

        # Get label names for CPU usage series
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'name=cpu', 'type=usage')
        assert result == [b'name', b'node', b'type']

        # Get label names for series with location label
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'location=datacenter')
        assert result == [b'location', b'rack']

    def test_labelnames_with_regex_filters(self):
        """Test TS.LABELNAMES with regex filter expressions"""
        self.setup_test_data(self.client)

        # Get label names for series where name matches regex
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'name=~"c.*"')
        assert result == [b'name', b'node', b'type']

        # Get label names for series where node matches pattern
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'node=~"node[12]"')
        assert result == [b'name', b'node', b'type']

    def test_labelnames_with_time_range(self):
        """Test TS.LABELNAMES with time range filters"""

        # Add samples with different timestamps
        now = 1000

        # Add samples to different series at different times
        self.client.execute_command('TS.ADD', 'ts1', now, 10, 'LABELS', 'name', 'cpu', 'ts1', '1')
        self.client.execute_command('TS.ADD', 'ts2', now + 100, 20, 'LABELS', 'name', 'cpu', 'ts2', '2')
        self.client.execute_command('TS.ADD', 'ts3', now + 200, 30, 'LABELS', 'name', 'cpu', 'ts3', '3')
        self.client.execute_command('TS.ADD', 'ts9', now + 500, 40, 'LABELS', 'name', 'cpu', 'ts9', '9')

        # Query with START parameter only
        result = self.client.execute_command('TS.LABELNAMES', 'START', now + 150, 'FILTER', 'name=cpu')
        assert result == [b'name', b'ts3', b'ts9']

        # Query with END parameter only
        result = self.client.execute_command('TS.LABELNAMES', 'END', now + 150, 'FILTER', 'name=cpu')
        assert result == [b'name', b'ts1', b'ts2']

        # Query with both START and END
        result = self.client.execute_command('TS.LABELNAMES', 'START', now + 50, 'END', now + 250, 'FILTER', 'name=cpu')
        assert result == [b'name', b'ts2', b'ts3']

    def test_labelnames_with_combined_parameters(self):
        """Test TS.LABELNAMES with combined filter and time range parameters"""
        self.setup_test_data(self.client)

        # Add samples with different timestamps
        now = 1000

        # Add samples to different series at different times
        self.client.execute_command('TS.ADD', 'ts1', now, 10)
        self.client.execute_command('TS.ADD', 'ts2', now + 100, 20)
        self.client.execute_command('TS.ADD', 'ts3', now + 200, 30)
        self.client.execute_command('TS.ADD', 'ts9', now + 500, 40)

        # Query with filter and time range
        result = self.client.execute_command('TS.LABELNAMES',
                                                    'START', now,
                                                    'END', now + 150,
                                                    'FILTER', 'name=~"c.*"',
                                                    )
        assert result == [b'name', b'node', b'type']

    def test_labelnames_empty_result(self):
        """Test TS.LABELNAMES when no series match the criteria"""
        self.setup_test_data(self.client)

        # Filter that doesn't match any series
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'nonexistent=value')
        assert result == []

        # Time range that doesn't match any samples
        now = 1000
        self.client.execute_command('TS.ADD', 'ts1', now, 10)
        result = self.client.execute_command('TS.LABELNAMES', 'START', now + 2000, 'FILTER', 'name=cpu')
        assert result == []

    def test_labelnames_with_limit(self):
        """Test TS.LABELNAMES with LIMIT parameter"""
        self.setup_test_data(self.client)

        # Get top 2 label names
        result = self.client.execute_command('TS.LABELNAMES', 'LIMIT', 2, 'FILTER', 'name=cpu')
        assert len(result) == 2

        # Verify sorted order (alphabetical)
        all_labels = [b'name', b'node', b'type']
        assert result == all_labels[:2]

        result = self.client.execute_command('TS.LABELNAMES', 'LIMIT', 5, 'FILTER', 'name=cpu')
        assert result == all_labels

    def test_labelnames_error_cases(self):
        """Test error conditions for TS.LABELNAMES"""
        self.setup_test_data(self.client)

        # Invalid filter format
        # This is invalid in redis, but this is a valid Prometheus filter (equivalent to {__name__="invalid_filter"})
        # or invalid_filter{}
        # self.verify_error_response(self.client, 'TS.LABELNAMES FILTER invalid_filter',
        #                            "Invalid filter: invalid_filter")

        # Invalid time format
        self.verify_error_response(self.client, 'TS.LABELNAMES START invalid_time',
                                   "TSDB: invalid START timestamp")

        # Invalid limit format
        self.verify_error_response(self.client, 'TS.LABELNAMES LIMIT invalid_limit',
                                   "TSDB: invalid LIMIT value")

    def test_labelnames_after_series_deletion(self):
        """Test TS.LABELNAMES after deleting time series"""
        self.setup_test_data(self.client)

        # Delete series with unique labels
        self.client.execute_command('DEL', 'ts9')

        # Verify unique labels are no longer returned
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'location=datacenter')
        assert result == []

    def test_labelnames_with_complex_filters(self):
        """Test TS.LABELNAMES with complex filter combinations"""
        self.setup_test_data(self.client)

        # Complex filter: CPU metrics that are not a usage type
        result = sorted(self.client.execute_command('TS.LABELNAMES',
                                                    'FILTER', 'name=cpu', 'type!=usage'))
        assert result == [b'name', b'node', b'type']

        # Complex filter with regex: nodes that don't match the pattern
        result = sorted(self.client.execute_command('TS.LABELNAMES',
                                                    'FILTER', 'node!~"node[12]"'))
        assert result == [b'location', b'name', b'node', b'rack', b'type']

    def test_labelnames_with_empty_database(self):
        """Test TS.LABELNAMES with an empty database"""
        # Ensure that the database is empty
        self.client.execute_command('FLUSHALL')

        # Verify no labels are returned
        result = self.client.execute_command('TS.LABELNAMES', 'FILTER', 'name=cpu')
        assert result == []