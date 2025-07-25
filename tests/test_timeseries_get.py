import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

class TestTsGet(ValkeyTimeSeriesTestCaseBase):

    def test_get_basic(self):
        """Test basic TS.GET functionality with a single sample"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.5)

        # Get the latest sample
        result = self.client.execute_command('TS.GET', 'ts1')
        assert result == [1000, b'10.5']

    def test_get_multiple_samples(self):
        """Test TS.GET returns the latest sample when multiple samples exist"""
        # Create a time series and add multiple samples
        self.client.execute_command('TS.CREATE', 'ts2')
        self.client.execute_command('TS.ADD', 'ts2', 1000, 10.5)
        self.client.execute_command('TS.ADD', 'ts2', 2000, 20.5)
        self.client.execute_command('TS.ADD', 'ts2', 3000, 30.5)

        # Get the latest sample
        result = self.client.execute_command('TS.GET', 'ts2')
        assert result == [3000, b'30.5']

    def test_get_empty_series(self):
        """Test TS.GET returns empty array for a series with no samples"""
        # Create an empty time series
        self.client.execute_command('TS.CREATE', 'empty_ts')

        # Get sample from empty series
        result = self.client.execute_command('TS.GET', 'empty_ts')
        assert result == []

    def test_get_nonexistent_key(self):
        """Test TS.GET behavior with a nonexistent key"""
        # Attempt to get sample from nonexistent time series
        with pytest.raises(Exception) as excinfo:
            self.client.execute_command('TS.GET', 'nonexistent_ts')
        assert "the key does not exist" in str(excinfo.value)

    def test_get_after_del(self):
        """Test TS.GET after deleting samples"""
        # Create a time series and add samples
        self.client.execute_command('TS.CREATE', 'ts_del')
        self.client.execute_command('TS.ADD', 'ts_del', 1000, 10.5)
        self.client.execute_command('TS.ADD', 'ts_del', 2000, 20.5)
        self.client.execute_command('TS.ADD', 'ts_del', 3000, 30.5)

        # Delete the latest sample
        self.client.execute_command('TS.DEL', 'ts_del', 3000, 3000)

        # Get the latest sample (should be the second-latest)
        result = self.client.execute_command('TS.GET', 'ts_del')
        assert result == [2000, b'20.5']

    def test_get_after_all_deleted(self):
        """Test TS.GET after deleting all samples"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts_all_del')
        self.client.execute_command('TS.ADD', 'ts_all_del', 1000, 10.5)

        # Delete the only sample
        self.client.execute_command('TS.DEL', 'ts_all_del', 0, 2000)

        # Get the latest sample (should be empty)
        result = self.client.execute_command('TS.GET', 'ts_all_del')
        assert result == []

    def test_get_after_add_with_same_timestamp(self):
        """Test TS.GET after adding a sample with the same timestamp"""
        # Create a time series and add a sample
        self.client.execute_command('TS.CREATE', 'ts_same_ts', 'DUPLICATE_POLICY', 'LAST')
        self.client.execute_command('TS.ADD', 'ts_same_ts', 1000, 10.5)

        # Add another sample with the same timestamp
        self.client.execute_command('TS.ADD', 'ts_same_ts', 1000, 20.5)

        # Get the latest sample (should return the most recently added value)
        result = self.client.execute_command('TS.GET', 'ts_same_ts')
        assert result == [1000, b'20.5']

    def test_get_no_args(self):
        """Test TS.GET with no arguments"""
        # Test missing key argument
        self.verify_error_response(self.client, 'TS.GET',
                                   "wrong number of arguments for 'TS.GET' command")

    def test_get_too_many_args(self):
        """Test TS.GET with too many arguments"""
        # Create a time series
        self.client.execute_command('TS.CREATE', 'ts_extra')

        # Test with extra arguments
        self.verify_error_response(self.client, 'TS.GET ts_extra latest other',
                                   "wrong number of arguments for 'TS.GET' command")