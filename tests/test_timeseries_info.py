import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

class TestTimeseriesInfo(ValkeyTimeSeriesTestCaseBase):
    def test_info_basic(self):
        """Test TS.INFO on a basic time series with data"""
        key = 'ts_basic'
        self.client.execute_command('TS.CREATE', key)
        ts1 = self.client.execute_command('TS.ADD', key, 1000, 10.1)
        ts2 = self.client.execute_command('TS.ADD', key, 2000, 20.2)

        info = self.ts_info(key)

        assert info['totalSamples'] == 2
        assert info['memoryUsage'] > 0
        assert info['retentionTime'] == 0 # Default
        assert len(info['chunks']) >= 1
        assert info['firstTimestamp'] == ts1
        assert info['lastTimestamp'] == ts2
        assert info['labels'] == []
        assert info['rules'] == []
        assert info['duplicatePolicy'] is None # Default is BLOCK, not explicitly stored unless set

    def test_info_with_options(self):
        """Test TS.INFO on a time series created with options"""
        key = 'ts_options'
        retention = 60000 # 1 minute
        chunk_size = 128
        labels = ['sensor', 'temp', 'area', 'A1']
        duplicate_policy = 'LAST'

        self.client.execute_command(
            'TS.CREATE', key,
            'RETENTION', retention,
            'CHUNK_SIZE', chunk_size,
            'LABELS', *labels,
            'DUPLICATE_POLICY', duplicate_policy
        )
        ts1 = self.client.execute_command('TS.ADD', key, 3000, 30.3)

        info = self.ts_info(key)

        assert info['total_samples'] == 1
        assert info['retention_msecs'] == retention
        assert info['chunk_count'] == 1
        assert info['max_samples_per_chunk'] == chunk_size
        assert info['first_timestamp'] == ts1
        assert info['last_timestamp'] == ts1
        assert info['labels'] == [['sensor', 'temp'], ['area', 'A1']]
        assert info['rules'] == []
        assert info['duplicate_policy'] == duplicate_policy

    def test_info_empty_series(self):
        """Test TS.INFO on an existing but empty time series"""
        key = 'ts_empty'
        self.client.execute_command('TS.CREATE', key, 'LABELS', 'status', 'init')

        info = self.ts_info(key)

        assert info['totalSamples'] == 0
        assert info['memoryUsage'] > 0 # Metadata still uses memory
        assert info['retentionTime'] == 0
        assert info['chunks'] == [] # No data chunks yet
        assert info['firstTimestamp'] == 0
        assert info['lastTimestamp'] == 0
        assert info['labels'] == {'status': 'init'}
        assert info['rules'] == []
        assert info['duplicatePolicy'] is None

    def test_info_non_existent_key(self):
        """Test TS.INFO on a non-existent key"""
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.INFO', 'ts_nonexistent')
        # Error message might vary slightly based on Valkey version/implementation
        assert "key does not exist" in str(excinfo.value).lower() or \
               "ERR TSDB: the key does not exist" in str(excinfo.value) # Common variations

    def test_info_wrong_type(self):
        """Test TS.INFO on a key of the wrong type"""
        key = 'string_key'
        self.client.set(key, 'hello world')

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.INFO', key)
        assert "WRONGTYPE" in str(excinfo.value)

    def test_info_debug(self):
        """Test TS.INFO DEBUG option"""
        key = 'ts_debug'
        chunk_size = 128 # Use a smaller chunk size for easier testing
        self.client.execute_command('TS.CREATE', key, 'CHUNK_SIZE', chunk_size)

        # Add enough samples to potentially create more than one chunk
        for i in range(int(chunk_size * 1.5)):
            self.client.execute_command('TS.ADD', key, 1000 + i * 10, i)

        info = self.ts_info(key, True)

        assert 'chunks' in info
        assert isinstance(info['chunks'], list)
        assert len(info['chunks']) >= 1 # Should have at least one chunk

        # Check details of the first chunk (keys might vary slightly)
        first_chunk = info['chunks'][0]
        assert 'startTimestamp' in first_chunk
        assert 'endTimestamp' in first_chunk
        assert 'samples' in first_chunk
        assert 'size' in first_chunk
        assert 'encoding' in first_chunk
        assert first_chunk['samples'] > 0
        assert first_chunk['size'] > 0