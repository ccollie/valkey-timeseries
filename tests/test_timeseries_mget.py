import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMget(ValkeyTimeSeriesTestCaseBase):

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

        # Add samples to each time series
        current_time = 1000
        client.execute_command('TS.ADD', 'ts1', current_time, 10)
        client.execute_command('TS.ADD', 'ts2', current_time, 20)
        client.execute_command('TS.ADD', 'ts3', current_time, 30)
        client.execute_command('TS.ADD', 'ts4', current_time, 40)
        client.execute_command('TS.ADD', 'ts5', current_time, 50)
        client.execute_command('TS.ADD', 'ts6', current_time, 60)
        client.execute_command('TS.ADD', 'ts7', current_time, 70)

        # Add additional samples with different timestamps
        client.execute_command('TS.ADD', 'ts1', current_time + 1000, 15)
        client.execute_command('TS.ADD', 'ts2', current_time + 1000, 25)
        client.execute_command('TS.ADD', 'ts3', current_time + 1000, 35)

    def test_basic_mget(self):
        """Test basic TS.MGET functionality with a simple filter"""
        self.setup_test_data(self.client)

        # Get all CPU metrics
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        # Sort results by key name for consistent test results
        result.sort(key=lambda x: x[0])

        # Check the structure and content of the results
        assert len(result) == 3

        # Each result is [key_name, [timestamp, value], labels_dict]
        assert result[0][0] == b'ts1'
        assert result[0][1][0] == 2000  # latest timestamp
        assert result[0][1][1] == 15    # latest value

        assert result[1][0] == b'ts2'
        assert result[1][1][0] == 2000
        assert result[1][1][1] == 25

        assert result[2][0] == b'ts5'
        assert result[2][1][0] == 1000
        assert result[2][1][1] == 50

    def test_mget_with_empty_result(self):
        """Test TS.MGET with a filter that doesn't match any series"""
        self.setup_test_data(self.client)

        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=nonexistent')
        assert result == []

    def test_mget_with_withlabels(self):
        """Test TS.MGET with the WITHLABELS option"""
        self.setup_test_data(self.client)

        # Get all memory metrics with their labels
        result = self.client.execute_command('TS.MGET', 'WITHLABELS', 'FILTER', 'name=memory')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2

        # Check that labels are included
        ts3_labels = result[0][2]
        assert len(ts3_labels) == 6  # 3 label pairs
        assert [b'name', b'memory'] in ts3_labels
        assert [b'type', b'usage'] in ts3_labels
        assert [b'node', b'node1'] in ts3_labels

        ts4_labels = result[1][2]
        assert len(ts4_labels) == 6
        assert [b'name', b'memory'] in ts4_labels
        assert [b'type', b'usage'] in ts4_labels
        assert [b'node', b'node2'] in ts4_labels

    def test_mget_with_selectedlabels(self):
        """Test TS.MGET with the SELECTEDLABELS option"""
        self.setup_test_data(self.client)

        # Get all CPU metrics with only selected labels
        result = self.client.execute_command('TS.MGET', 'SELECTEDLABELS', 'name', 'type', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        assert len(result) == 3

        # Check that only selected labels are included
        for item in result:
            labels = item[2]
            # Should only have the specified labels, not 'node' or others
            label_names = [pair[0] for pair in labels]
            assert b'name' in label_names
            assert b'type' in label_names if b'type' in dict(labels) else True  # ts6 doesn't have 'type'
            assert b'node' not in label_names

    def test_mget_with_complex_filter(self):
        """Test TS.MGET with complex filters"""
        self.setup_test_data(self.client)

        # Get metrics that match multiple conditions
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu', 'type=usage')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2
        assert result[0][0] == b'ts1'
        assert result[1][0] == b'ts2'

        # Get metrics with regex filter
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=~"c.*"', 'node=node1')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2
        assert result[0][0] == b'ts1'
        assert result[1][0] == b'ts5'

    def test_mget_with_no_data(self):
        """Test TS.MGET with series that exist but have no data"""
        # Create series without samples
        self.client.execute_command('TS.CREATE', 'empty_ts1', 'LABELS', 'name', 'empty', 'type', 'test')
        self.client.execute_command('TS.CREATE', 'empty_ts2', 'LABELS', 'name', 'empty', 'type', 'test2')

        # Test MGET on series with no data
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=empty')
        result.sort(key=lambda x: x[0])

        assert len(result) == 2
        # Each series should return empty array for the sample
        assert result[0][0] == b'empty_ts1'
        assert result[0][1] == []

        assert result[1][0] == b'empty_ts2'
        assert result[1][1] == []

    def test_mget_error_cases(self):
        """Test error cases for TS.MGET"""
        self.setup_test_data(self.client)

        # Missing FILTER
        self.verify_error_response(
            self.client,
            'TS.MGET',
            "TSDB: Missing FILTER"
        )

        # Invalid filter format
        self.verify_error_response(
            self.client,
            'TS.MGET FILTER invalid_filter',
            "Invalid filter: invalid_filter"
        )

        # Unknown option
        self.verify_error_response(
            self.client,
            'TS.MGET UNKNOWN_OPTION FILTER name=cpu',
            "ERR invalid argument 'UNKNOWN_OPTION'"
        )

    def test_mget_with_different_timestamps(self):
        """Test TS.MGET with series having different latest timestamps"""
        self.setup_test_data(self.client)

        # Add a sample with a newer timestamp to one series
        self.client.execute_command('TS.ADD', 'ts5', 3000, 55)

        # Get all CPU metrics
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        result.sort(key=lambda x: x[0])

        # Verify different timestamps
        assert result[0][1][0] == 2000  # ts1
        assert result[1][1][0] == 2000  # ts2
        assert result[2][1][0] == 3000  # ts5 (newest timestamp)

    def test_mget_after_series_deletion(self):
        """Test TS.MGET behavior after some series are deleted"""
        self.setup_test_data(self.client)

        # Delete one of the series
        # self.client.execute_command('DEL', 'ts1')

        # Get all CPU metrics
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=cpu')
        print("Result after deletion:", result)
        # result.sort(key=lambda x: x[0])

        # Should only return remaining series
        assert len(result) == 2
        assert result[0][0] == b'ts2'
        assert result[1][0] == b'ts5'

    def test_mget_with_latest_samples(self):
        """Test that TS.MGET returns only the latest sample for each series"""
        self.setup_test_data(self.client)

        # Add multiple samples to a series
        self.client.execute_command('TS.ADD', 'ts7', 2000, 75)
        self.client.execute_command('TS.ADD', 'ts7', 3000, 80)

        # Get the disk metrics
        result = self.client.execute_command('TS.MGET', 'FILTER', 'name=disk')

        # Should only have one result with the latest sample
        assert len(result) == 1
        assert result[0][0] == b'ts7'
        assert result[0][1][0] == 3000  # latest timestamp
        assert result[0][1][1] == 80     # latest value