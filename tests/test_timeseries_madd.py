import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

class TestTsMadd(ValkeyTimeSeriesTestCaseBase):

    def test_madd_basic(self):
        """Test basic functionality of TS.MADD command"""
        # Create a time series
        self.client.execute_command('TS.CREATE', 'ts1')

        # Add multiple samples
        result = self.client.execute_command('TS.MADD',
                                             'ts1', 1000, 10.0,
                                             'ts1', 2000, 20.0,
                                             'ts1', 3000, 30.0)

        # Verify result contains the timestamps
        assert result == [1000, 2000, 3000]

        # Verify samples were added correctly
        range_result = self.client.execute_command('TS.RANGE', 'ts1', 0, 4000)
        expected_result = [[1000, b'10.0'], [2000, b'20.0'], [3000, b'30.0']]
        assert range_result == expected_result

    def test_madd_multiple_series(self):
        """Test adding samples to multiple time series in one command"""
        # Create multiple time series
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.CREATE', 'ts2')

        # Add samples to both time series
        result = self.client.execute_command('TS.MADD',
                                             'ts1', 1000, 10.0,
                                             'ts2', 1000, 100.0,
                                             'ts1', 2000, 20.0,
                                             'ts2', 2000, 200.0)

        # Verify timestamps
        assert result == [1000, 1000, 2000, 2000]

        # Check ts1 data
        range_ts1 = self.client.execute_command('TS.RANGE', 'ts1', 0, 3000)
        assert len(range_ts1) == 2
        assert float(range_ts1[0][1]) == 10.0
        assert float(range_ts1[1][1]) == 20.0

        # Check ts2 data
        range_ts2 = self.client.execute_command('TS.RANGE', 'ts2', 0, 3000)
        assert len(range_ts2) == 2
        assert float(range_ts2[0][1]) == 100.0
        assert float(range_ts2[1][1]) == 200.0

    def test_madd_auto_creation(self):
        """Test that TS.MADD automatically creates time series if they don't exist"""
        # Add samples to non-existent time series
        result = self.client.execute_command('TS.MADD',
                                             'ts_auto1', 1000, 10.0,
                                             'ts_auto2', 2000, 20.0)

        # Verify timestamps
        assert result == [1000, 2000]

        # Verify time series were created
        assert self.client.execute_command('EXISTS', 'ts_auto1') == 1
        assert self.client.execute_command('EXISTS', 'ts_auto2') == 1

        # Verify data
        assert float(self.client.execute_command('TS.GET', 'ts_auto1')[1]) == 10.0
        assert float(self.client.execute_command('TS.GET', 'ts_auto2')[1]) == 20.0

    def test_madd_with_labels(self):
        """Test TS.MADD with pre-created time series with labels"""
        # Create time series with labels
        self.client.execute_command('TS.CREATE', 'ts_labels', 'LABELS', 'sensor', 'temp', 'location', 'room1')

        # Add samples
        self.client.execute_command('TS.MADD', 'ts_labels', 1000, 22.5, 'ts_labels', 2000, 23.1)

        # Verify data was added
        range_result = self.client.execute_command('TS.RANGE', 'ts_labels', 0, 3000)
        assert len(range_result) == 2

        # Verify labels are maintained
        info = self.ts_info('ts_labels')
        assert info[b'labels'][b'sensor'] == b'temp'
        assert info[b'labels'][b'location'] == b'room1'

    def test_madd_duplicate_timestamp(self):
        """Test behavior with duplicate timestamps"""
        # Create time series
        self.client.execute_command('TS.CREATE', 'ts_dup')

        # Add a sample
        self.client.execute_command('TS.ADD', 'ts_dup', 1000, 10.0)

        # Try to add samples with duplicate timestamp
        result = self.client.execute_command('TS.MADD',
                                             'ts_dup', 1000, 20.0,  # Duplicate
                                             'ts_dup', 2000, 30.0)  # New

        # Verify timestamps (should fail for duplicate)
        assert result[0] == -1  # Error code for duplicate timestamp
        assert result[1] == 2000  # Success for new timestamp

        # Verify data - original sample should remain unchanged
        range_result = self.client.execute_command('TS.RANGE', 'ts_dup', 0, 3000)
        assert len(range_result) == 2
        assert float(range_result[0][1]) == 10.0  # Original value preserved
        assert float(range_result[1][1]) == 30.0  # New value added

    def test_madd_with_duplicate_policy(self):
        """Test with duplicate policy for handling duplicate timestamps"""
        # Create time series with duplicate policy
        self.client.execute_command('TS.CREATE', 'ts_dup_policy', 'DUPLICATE_POLICY', 'MAX')

        # Add initial sample
        self.client.execute_command('TS.ADD', 'ts_dup_policy', 1000, 10.0)

        # Add duplicate sample with higher value
        result = self.client.execute_command('TS.MADD',
                                             'ts_dup_policy', 1000, 20.0,  # Higher value
                                             'ts_dup_policy', 2000, 30.0)

        # Verify timestamps
        assert result == [1000, 2000]

        # Verify MAX policy was applied
        range_result = self.client.execute_command('TS.RANGE', 'ts_dup_policy', 0, 3000)
        assert len(range_result) == 2
        assert float(range_result[0][1]) == 20.0  # Value was updated to higher value per MAX policy
        assert float(range_result[1][1]) == 30.0

    def test_madd_with_retention(self):
        """Test adding samples with retention period"""
        # Create time series with retention
        self.client.execute_command('TS.CREATE', 'ts_retention', 'RETENTION', 3000)

        # Add samples with timestamps far apart
        now = 10000
        self.client.execute_command('TS.MADD',
                                    'ts_retention', now - 5000, 10.0,
                                    'ts_retention', now, 20.0)

        # Add another sample
        self.client.execute_command('TS.ADD', 'ts_retention', now + 5000, 30.0)

        # Verify older samples are removed due to retention
        range_result = self.client.execute_command('TS.RANGE', 'ts_retention', 0, now + 10000)

        # First sample should be removed due to retention (3000ms)
        assert len(range_result) == 2
        assert float(range_result[0][1]) == 20.0
        assert float(range_result[1][1]) == 30.0

    def test_madd_large_batch(self):
        """Test adding a large number of samples in one command"""
        # Create time series
        self.client.execute_command('TS.CREATE', 'ts_large')

        # Prepare a large batch of samples
        args = ['TS.MADD']
        expected_timestamps = []

        for i in range(1000):
            args.extend([f'ts_large', 1000 + i, i * 1.5])
            expected_timestamps.append(1000 + i)

        # Add all samples at once
        result = self.client.execute_command(*args)

        # Verify all timestamps were added
        assert len(result) == 1000
        assert result == expected_timestamps

        # Verify sample count
        info = self.ts_info('ts_large')
        assert info[b'total_samples'] == 1000

    def test_madd_errors(self):
        """Test error cases for TS.MADD"""
        # Create a regular key (not a time series)
        self.client.execute_command('SET', 'string_key', 'hello')

        # Add with invalid timestamp format
        self.verify_error_response(
            self.client, 'TS.MADD ts1 abc 10.0',
            "TSDB: invalid timestamp."
        )

        # Add with invalid value format
        self.verify_error_response(
            self.client, 'TS.MADD ts1 1000 invalid',
            "Invalid value specified"
        )

        # Add to a regular string key
        self.verify_error_response(
            self.client, 'TS.MADD string_key 1000 10.0',
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )

        # Not enough arguments
        self.verify_error_response(
            self.client, 'TS.MADD ts1 1000',
            "wrong number of arguments for 'TS.MADD' command"
        )

    def test_madd_with_millisecond_values(self):
        """Test TS.MADD with millisecond timestamp values"""
        # Create time series
        self.client.execute_command('TS.CREATE', 'ts_millis')

        # Add with millisecond timestamps
        result = self.client.execute_command('TS.MADD',
                                             'ts_millis', 1614556800000, 10.0,  # 2021-03-01 00:00:00.000
                                             'ts_millis', 1614556800001, 10.1,  # +1ms
                                             'ts_millis', 1614556800002, 10.2)  # +2ms

        # Verify timestamps
        assert result == [1614556800000, 1614556800001, 1614556800002]

        # Verify data with millisecond precision
        range_result = self.client.execute_command('TS.RANGE', 'ts_millis', 0, 1614556800999)
        assert len(range_result) == 3
        assert range_result[0][0] == 1614556800000
        assert range_result[1][0] == 1614556800001
        assert range_result[2][0] == 1614556800002

    def test_madd_with_uncompressed(self):
        """Test TS.MADD with uncompressed series"""
        # Create uncompressed time series
        self.client.execute_command('TS.CREATE', 'ts_uncompressed', 'UNCOMPRESSED')

        # Add samples
        self.client.execute_command('TS.MADD',
                                    'ts_uncompressed', 1000, 10.0,
                                    'ts_uncompressed', 2000, 20.0,
                                    'ts_uncompressed', 3000, 30.0)

        # Verify data was added correctly
        range_result = self.client.execute_command('TS.RANGE', 'ts_uncompressed', 0, 4000)
        assert len(range_result) == 3

        # Verify uncompressed flag in info
        info = self.ts_info('ts_uncompressed')
        assert info[b'chunkType'] == b'uncompressed'