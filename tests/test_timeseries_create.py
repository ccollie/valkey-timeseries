import pytest
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestTimeSeriesBasic(ValkeyTimeSeriesTestCaseBase):
    def test_create_basic(self):
        """Test basic TS.CREATE functionality"""
        client = self.server.get_new_client()
        # Basic create with no parameters
        assert client.execute_command("TS.CREATE", "ts1") == b'OK'

        # Create with retention
        assert client.execute_command("TS.CREATE", "ts2", "RETENTION", "60000") == b'OK'
        info = self.ts_info("ts2")
        assert info[b'retentionTime'] == b'60000'

        # Create with labels
        assert client.execute_command("TS.CREATE", "ts3", "LABELS", "sensor_id", "2", "area_id", "32") == b'OK'
        info = self.ts_info("ts3")
        print(info)
        labels = info[b'labels']
        assert labels['sensor_id'] == '2'
        assert labels['area_id'] == '32'

    def test_create_with_labels(self):
        client = self.server.get_new_client()
        assert client.execute_command("ts.create time-series-2 labels hello world") == b'OK'
        info = self.ts_info("time-series-2")
        labels = info[b'labels']
        print(labels)
        assert "world" == labels["hello"]


    def test_create_with_duplicate_policy(self):
        """Test creating time series with different duplicate policies"""
        client = self.server.get_new_client()

        policies = ["BLOCK", "FIRST", "LAST", "MIN", "MAX", "SUM"]
        for i, policy in enumerate(policies):
            key = f"ts_policy_{i}"
            assert client.execute_command("TS.CREATE", key, "DUPLICATE_POLICY", policy) == b'OK'
            info = self.ts_info(key)
            print(info)
            assert info[b'duplicatePolicy'] == policy.lower().encode()

    def test_create_with_encoding(self):
        """Test creating time series with different encoding options"""
        client = self.server.get_new_client()

        encodings = ["UNCOMPRESSED", "COMPRESSED"]
        for i, encoding in enumerate(encodings):
            key = f"ts_encoding_{i}"
            assert client.execute_command("TS.CREATE", key, "ENCODING", encoding) == b'OK'
            info = self.ts_info(key)
            # Check encoding in info response
            if encoding == "UNCOMPRESSED":
                assert info[b'chunkType'] == b'uncompressed'
            else:
                assert info[b'chunkType'] == b'compressed'

    def test_create_with_chunk_size(self):
        """Test creating time series with different chunk sizes"""
        client = self.server.get_new_client()

        # Test with valid chunk sizes (must be multiples of 2)
        chunk_sizes = [128, 256, 512, 1024]
        for i, size in enumerate(chunk_sizes):
            key = f"ts_chunk_{i}"
            assert client.execute_command("TS.CREATE", key, "CHUNK_SIZE", size) == b'OK'
            info = self.ts_info(key)
            assert info[b'chunkSize'] == size

    def test_create_with_rounding(self):
        """Test creating time series with rounding options"""
        client = self.server.get_new_client()

        # Test DECIMAL_DIGITS
        assert client.execute_command("TS.CREATE", "ts_decimal", "DECIMAL_DIGITS", 2) == b'OK'
        info = self.ts_info("ts_decimal")
        assert info[b'rounding'] == [b'decimalDigits', 2]

        # Test SIGNIFICANT_DIGITS
        assert client.execute_command("TS.CREATE", "ts_significant", "SIGNIFICANT_DIGITS", 3) == b'OK'
        info = self.ts_info("ts_significant")
        assert info[b'rounding'] == [b'significantDigits', 3]

    def test_create_metric_name(self):
        """Test creating time series with metric name"""
        client = self.server.get_new_client()

        assert client.execute_command("TS.CREATE", "ts_metric", "METRIC", 'temperature{city="CDMX"}') == b'OK'
        info = self.ts_info("ts_metric")
        # The metric should be parsed into labels
        labels = info[b'labels']
        assert '__name__' in labels
        assert 'city' in labels
        assert labels['__name__'] == 'temperature'
        assert labels['city'] == 'CDMX'

    def test_create_errors(self):
        """Test error cases for TS.CREATE"""
        client = self.server.get_new_client()

        # Create a time series first
        assert client.execute_command("TS.CREATE", "ts_error") == b'OK'

        # Attempt to create with the same key should fail
        with pytest.raises(ResponseError) as excinfo:
            client.execute_command("TS.CREATE", "ts_error")
        assert "key already exists" in str(excinfo.value).lower()

        # Invalid chunk size (not a multiple of 2)
        with pytest.raises(ResponseError) as excinfo:
            client.execute_command("TS.CREATE", "ts_invalid_chunk", "CHUNK_SIZE", 123)
        assert "chunk_size" in str(excinfo.value).lower()

        # Invalid duplicate policy
        with pytest.raises(ResponseError) as excinfo:
            client.execute_command("TS.CREATE", "ts_invalid_policy", "DUPLICATE_POLICY", "INVALID")
        assert "invalid duplicate policy" in str(excinfo.value).lower()

        # Both DECIMAL_DIGITS and SIGNIFICANT_DIGITS (only one allowed)
        with pytest.raises(ResponseError) as excinfo:
            client.execute_command("TS.CREATE", "ts_both_roundings",
                                   "DECIMAL_DIGITS", 2, "SIGNIFICANT_DIGITS", 3)
        assert "rounding" in str(excinfo.value).lower()

    def test_create_with_ignore_options(self):
        """Test creating time series with IGNORE options"""
        client = self.server.get_new_client()

        assert client.execute_command("TS.CREATE", "ts_ignore",
                                      "IGNORE", "1000", "0.5") == b'OK'
        info = self.ts_info("ts_ignore")
        print(info)
        assert info[b'ignoreMaxTimeDiff'] == b'1000'
        assert info[b'ignoreMaxValDiff'] == b'0.5'

    def test_create_multiple_series(self):
        """Test creating many time series and verify they all exist"""
        client = self.server.get_new_client()

        # Create 10 time series
        for i in range(10):
            assert client.execute_command("TS.CREATE", f"multi_ts_{i}",
                                          "LABELS", "idx", str(i)) == b'OK'

        # Verify all time series exist
        for i in range(10):
            key = f"multi_ts_{i}"
            assert client.execute_command("EXISTS", key) == 1
            info = self.ts_info(key)
            labels = info[b'labels']
            assert labels['idx'] == str(i)

        # Verify count in database
        assert client.execute_command("DBSIZE") == 10