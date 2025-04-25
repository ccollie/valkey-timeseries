import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesLabelValues(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Set up test data with various label values"""
        # Create time series with different label combinations
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server1', 'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'server1', 'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'server2', 'datacenter', 'dc1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'server2', 'datacenter', 'dc2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'server3', 'datacenter', 'dc2')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'network', 'type', 'throughput', 'node', 'server3')

        # Add some sample data
        now = 1000
        for i in range(1, 7):
            ts_key = f'ts{i}'
            client.execute_command('TS.ADD', ts_key, now, i * 10)
            client.execute_command('TS.ADD', ts_key, now + 1000, i * 10 + 5)

    def test_label_values_with_filter(self):
        """Test retrieving label values with a filter"""
        self.setup_test_data(self.client)

        # Get values for the 'name' label filtered by type=usage
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage'))
        assert result == [b'cpu', b'disk', b'memory']

        # Get values for the 'type' label filtered by name=cpu
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=cpu'))
        assert result == [b'temperature', b'usage']

    def test_label_values_with_multiple_filters(self):
        """Test retrieving label values with multiple filters"""
        self.setup_test_data(self.client)

        # Get values for the 'node' label with multiple filters
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=cpu', 'type=usage'))
        assert result == [b'server1', b'server3']

        # Get values for the 'datacenter' label with multiple filters
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'datacenter', 'FILTER', 'name=cpu', 'type=usage'))
        assert result == [b'dc1', b'dc2']

    def test_label_values_with_regex_filters(self):
        """Test retrieving label values with regex filters"""
        self.setup_test_data(self.client)

        # Get values for the 'node' label with regex filter
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'node', 'FILTER', 'name=~"c.*"'))
        assert result == [b'server1', b'server3']

        # Get values for the 'type' label with regex filter
        result = sorted(self.client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=~"(memory|disk)"'))
        assert result == [b'usage']

    def test_label_values_with_time_range(self):
        """Test retrieving label values with time range filtering"""
        self.setup_test_data(self.client)

        # Add data with specific timestamps for time range testing
        self.client.execute_command('TS.CREATE', 'ts_old', 'LABELS', 'name', 'archive', 'age', 'old', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_old', 500, 100)

        self.client.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'recent', 'age', 'new', 'common', '1')
        self.client.execute_command('TS.ADD', 'ts_new', 2500, 200)

        # Get values for the 'age' label with time range
        # First timestamp should exclude the 'old' series
        result = self.client.execute_command('TS.LABELVALUES', 'age', 'START', 1000, "FILTER", 'common=1')
        assert result == [b'new']

        # Get values with end time range
        result = self.client.execute_command('TS.LABELVALUES', 'age', 'END', 1500, "FILTER", 'common=1')
        assert result == [b'old']

        # Get values with both start and end time range
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'START', 900, 'END', 2000, "FILTER", 'common=1')
        assert result == [b'cpu', b'disk', b'memory', b'network']

    def test_label_values_with_limit(self):
        """Test retrieving label values with LIMIT parameter"""
        self.setup_test_data(self.client)

        # Get values for the 'name' label with limit
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'LIMIT', 2, 'FILTER', 'type=usage')
        assert len(result) == 2
        assert all(val in [b'cpu', b'disk', b'memory'] for val in result)

        # Get values for the 'node' label with limit
        result = self.client.execute_command('TS.LABELVALUES', 'node', 'LIMIT', 1, "FILTER", 'datacenter=dc2')
        assert result == [b'server2']

    def test_label_values_with_combined_parameters(self):
        """Test retrieving label values with combined parameters"""
        self.setup_test_data(self.client)

        # Get values with filter, time range, and limit
        result = self.client.execute_command(
            'TS.LABELVALUES', 'type',
            'START', 500,
            'END', 2500,
            'LIMIT', 1,
            'FILTER', 'name="~c.*"',
        )
        assert len(result) == 1
        assert result[0] in [b'temperature', b'usage']

        # Different combination of parameters
        result = sorted(self.client.execute_command(
            'TS.LABELVALUES', 'node',
            'START', 900,
            'LIMIT', 5,
            'FILTER', 'datacenter=dc1'
        ))
        assert result == [b'server1', b'server2']

    def test_label_values_empty_result(self):
        """Test retrieving label values with no matching results"""
        self.setup_test_data(self.client)

        # Filter that doesn't match any series
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'nonexistent=value')
        assert result == []

        # Filter with time range that excludes all series
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'START', 5000, "FILTER", 'type=usage')
        assert result == []

    def test_label_values_error_cases(self):
        """Test error cases for TS.LABELVALUES"""
        self.setup_test_data(self.client)

        # Missing label argument
        self.verify_error_response(
            self.client, 'TS.LABELVALUES',
            "wrong number of arguments for 'TS.LABELVALUES' command"
        )

        # Invalid filter format
        self.verify_error_response(
            self.client, 'TS.LABELVALUES name FILTER invalid-filter',
            "Invalid filter: invalid-filter"
        )

        # Invalid time format
        self.verify_error_response(
            self.client, 'TS.LABELVALUES name START invalid-time',
            "Invalid time argument"
        )

        # Invalid limit format
        self.verify_error_response(
            self.client, 'TS.LABELVALUES name LIMIT invalid-limit',
            "Invalid limit argument, must be a positive integer"
        )

    def test_label_values_after_series_deletion(self):
        """Test retrieving label values after series deletion"""
        self.setup_test_data(self.client)

        # Verify initial state
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        assert b'network' in result

        # Delete a time series
        # self.client.execute_command('DEL', 'ts6')  # ts6 has name=network

        # Verify the deleted label value is no longer returned
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'name=network')
        assert b'network' not in result

    def test_label_values_with_nonexistent_label(self):
        """Test retrieving values for a non-existent label"""
        self.setup_test_data(self.client)

        # Query for a label that doesn't exist
        result = self.client.execute_command('TS.LABELVALUES', 'nonexistent_label', 'FILTER', 'name=cpu')
        assert result == []

    def test_label_values_after_label_update(self):
        """Test retrieving values after updating labels"""
        self.setup_test_data(self.client)

        # Verify initial state
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        assert b'updated' not in result

        # Create a new time series with a new label value
        self.client.execute_command('TS.CREATE', 'ts_new', 'LABELS', 'name', 'updated', 'type', 'usage')

        # Verify the new label value is included
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        assert b'updated' in result

        # todo: add test for label update of existing series

    def test_label_values_with_empty_database(self):
        """Test TS.LABELVALUES with an empty database"""
        # Ensure database is empty
        self.client.execute_command('FLUSHALL')

        # Verify no values are returned for any label
        result = self.client.execute_command('TS.LABELVALUES', 'name', 'FILTER', 'type=usage')
        assert result == []

        result = self.client.execute_command('TS.LABELVALUES', 'type', 'FILTER', 'name=cpu')
        assert result == []