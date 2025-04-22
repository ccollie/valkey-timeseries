import pytest
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeseriesAdd(ValkeyTimeSeriesTestCaseBase):
    def test_basic_add(self):
        """Test basic TS.ADD command functionality"""
        # Create a timeseries
        assert self.client.execute_command("TS.CREATE", "ts1") == b"OK"

        # Add a sample
        timestamp = 16000
        value = 10.5
        result = self.client.execute_command("TS.ADD", "ts1", timestamp, value)
        assert result == timestamp

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts1", "-", "+")
        assert len(samples) == 1
        assert samples[0][0] == timestamp
        assert samples[0][1] == b'10.5'

    def test_add_with_labels(self):
        """Test adding samples with labels"""
        # Create timeseries with labels
        self.client.execute_command("TS.CREATE", "ts2", "LABELS", "sensor", "temp", "location", "room1")

        # Add a sample
        timestamp = 160000
        result = self.client.execute_command("TS.ADD", "ts2", timestamp, 22.5)
        assert result == timestamp

        # Verify the labels were preserved
        info = self.ts_info("ts2")
        labels = info[b"labels"]
        assert labels["sensor"] == "temp"
        assert labels["location"] == "room1"

    def test_add_auto_timestamp(self):
        """Test TS.ADD with '*' automatic timestamp"""
        self.client.execute_command("TS.CREATE", "ts3")

        # Add a sample with automatic timestamp
        result = self.client.execute_command("TS.ADD", "ts3", "*", 33.5)

        # Verify a timestamp was generated (should be a recent timestamp)
        assert isinstance(result, int)
        assert result > 160000  # Some time after 2020

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts3", "-", "+")
        assert samples == [[result, b'33.5']]

    def test_add_creates_key(self):
        """Test TS.ADD creates a new timeseries if it doesn't exist"""
        # Add to a non-existent timeseries
        timestamp = 160000
        result = self.client.execute_command("TS.ADD", "ts_auto_create", timestamp, 44.5)
        assert result == timestamp

        # Verify the timeseries was created
        assert self.client.execute_command("EXISTS", "ts_auto_create") == 1

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts_auto_create", "-", "+")
        print(samples)

        assert samples == [[timestamp, b'44.5']]

    def test_add_with_retention(self):
        """Test TS.ADD with retention option"""
        # Add to a non-existent timeseries with retention
        timestamp = 160000
        retention = 10000  # 10 seconds
        result = self.client.execute_command(
            "TS.ADD", "ts_retention", timestamp, 55.5, "RETENTION", retention
        )
        assert result == timestamp

        # Verify the retention was set
        info = self.ts_info("ts_retention")
        assert info["retentionTime"] == retention

    def test_add_with_encoding(self):
        """Test TS.ADD with different encoding options"""
        # Test with UNCOMPRESSED encoding
        timestamp = 160000
        self.client.execute_command(
            "TS.ADD", "ts_uncompressed", timestamp, 66.5, "ENCODING", "UNCOMPRESSED"
        )

        info = self.client.execute_command("TS.INFO", "ts_uncompressed")
        assert info[b"encoding"] == b"UNCOMPRESSED"

        # Test with COMPRESSED encoding
        self.client.execute_command(
            "TS.ADD", "ts_compressed", timestamp, 77.5, "ENCODING", "COMPRESSED"
        )

        self.validate_ts_info_values("ts_compressed", {b"encoding": b"COMPRESSED"})

    def test_add_with_chunk_size(self):
        """Test TS.ADD with chunk size option"""
        timestamp = 160000
        chunk_size = 128  # Small chunk size for testing

        self.client.execute_command(
            "TS.ADD", "ts_chunk_size", timestamp, 88.5, "CHUNK_SIZE", chunk_size
        )

        self.validate_ts_info_values("ts_chunk_size", {b"chunk_size": chunk_size})

    def test_add_with_duplicate_policy(self):
        """Test TS.ADD with different duplicate policies"""
        # Create a timeseries
        self.client.execute_command("TS.CREATE", "ts_dup")

        # Add first sample
        timestamp = 160000
        self.client.execute_command("TS.ADD", "ts_dup", timestamp, 10.0)

        # Add duplicate with BLOCK policy (should fail)
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command(
                "TS.ADD", "ts_dup", timestamp, 20.0, "ON_DUPLICATE", "BLOCK"
            )
        assert "sample blocked" in str(excinfo.value)

        # Add duplicate with FIRST policy (should keep first value)
        self.client.execute_command(
            "TS.ADD", "ts_dup", timestamp, 30.0, "ON_DUPLICATE", "FIRST"
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup", "-", "+")
        assert samples[0][1] == b'10.0'  # First value preserved

        # Add duplicate with LAST policy (should update to new value)
        self.client.execute_command(
            "TS.ADD", "ts_dup", timestamp, 40.0, "ON_DUPLICATE", "LAST"
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup", "-", "+")
        assert samples[0][1] == b'40.0'  # Last value used

        # Add duplicate with MAX policy
        self.client.execute_command(
            "TS.ADD", "ts_dup", timestamp, 30.0, "ON_DUPLICATE", "MAX"
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup", "-", "+")
        assert samples[0][1] == b'40.0'  # Higher value kept

        # Add duplicate with MIN policy
        self.client.execute_command(
            "TS.ADD", "ts_dup", timestamp, 20.0, "ON_DUPLICATE", "MIN"
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup", "-", "+")
        assert samples[0][1] == b'20.0' # Lower value kept

        # Add duplicate with SUM policy
        self.client.execute_command(
            "TS.ADD", "ts_dup", timestamp, 5.0, "ON_DUPLICATE", "SUM"
        )
        samples = self.client.execute_command("TS.RANGE", "ts_dup", "-", "+")
        assert samples[0][1] == b'25.0'  # Sum of values

    def test_add_with_labels_creation(self):
        """Test TS.ADD with labels when creating a new timeseries"""
        timestamp = 160000

        # Add with labels to a non-existent timeseries
        self.client.execute_command(
            "TS.ADD", "ts_with_labels", timestamp, 99.5,
            "LABELS", "sensor", "humidity", "location", "outside"
        )

        # Verify the labels were set
        info = self.ts_info("ts_with_labels")
        labels = info(b'labels')
        assert labels["sensor"] == "humidity"
        assert labels["location"] == "outside"

    def test_add_multiple_samples(self):
        """Test adding multiple samples to a timeseries"""
        self.client.execute_command("TS.CREATE", "ts_multi")

        # Add multiple samples with different timestamps
        base_ts = 160000
        for i in range(10):
            ts = base_ts + (i * 1000)
            result = self.client.execute_command("TS.ADD", "ts_multi", ts, i * 1.5)
            assert result == ts

        # Verify all samples were added
        samples = self.client.execute_command("TS.RANGE", "ts_multi", "-", "+")
        assert len(samples) == 10
        for i, sample in enumerate(samples):
            value = i * 1.5
            actual = sample[1]
            if value == 0.0:
                assert actual == b'0.0' or actual == b'0'
            else:
                expected_value = str(value).encode()
                assert sample[0] == base_ts + (i * 1000)
                assert actual == expected_value

    def test_add_invalid_values(self):
        """Test TS.ADD with invalid values and parameters"""
        self.client.execute_command("TS.CREATE", "ts_invalid")

        # Test with invalid timestamp
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.ADD", "ts_invalid", "invalid_ts", 10.0)

        # Test with invalid value
        with pytest.raises(ResponseError):
            self.client.execute_command("TS.ADD", "ts_invalid", 160000, "not_a_number")

        # Test with invalid chunk size
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "CHUNK_SIZE", "invalid"
            )

        # Test with invalid duplicate policy
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "ON_DUPLICATE", "INVALID_POLICY"
            )

        # Test with odd number of label pairs
        with pytest.raises(ResponseError):
            self.client.execute_command(
                "TS.ADD", "ts_invalid", 160000, 10.0, "LABELS", "key1", "val1", "key2"
            )

    def test_add_with_decimal_digits(self):
        """Test TS.ADD with decimal digits option"""
        timestamp = 160000

        # Add with decimal digits precision
        self.client.execute_command(
            "TS.ADD", "ts_decimal", timestamp, 123.456789, "DECIMAL_DIGITS", 2
        )

        # Verify the value was rounded to the specified precision
        samples = self.client.execute_command("TS.RANGE", "ts_decimal", "-", "+")
        assert samples[0][1] == b'123.46'  # Rounded to 2 decimal places

    def test_add_with_significant_digits(self):
        """Test TS.ADD with significant digits option"""
        timestamp = 160000

        # Add with significant digits precision
        self.client.execute_command(
            "TS.ADD", "ts_significant", timestamp, 123.456789, "SIGNIFICANT_DIGITS", 3
        )

        # Verify the value was rounded to the specified precision
        samples = self.client.execute_command("TS.RANGE", "ts_significant", "-", "+")
        assert samples[0][1] == b'123'  # 3 significant digits

    def test_add_sample_before_first(self):
        """Test adding a sample before the first sample in the timeseries"""
        self.client.execute_command("TS.CREATE", "ts_before_first")

        # Add a sample with a timestamp
        timestamp1 = 10000
        self.client.execute_command("TS.ADD", "ts_before_first", timestamp1, 20.0)

        # Add a sample with a timestamp before the first sample
        timestamp2 = 5000
        self.client.execute_command("TS.ADD", "ts_before_first", timestamp2, 10.0)

        # Verify the sample was added
        samples = self.client.execute_command("TS.RANGE", "ts_before_first", "-", "+")
        assert len(samples) == 1
        assert samples[0][0] == timestamp2
        assert samples[0][1] == b'10.0'

    def test_add_out_of_range_values(self):
        """Test TS.ADD with extreme values"""
        self.client.execute_command("TS.CREATE", "ts_extreme")

        # Test with very large timestamp
        max_timestamp = 9223372036854775807  # i64::MAX
        self.client.execute_command("TS.ADD", "ts_extreme", max_timestamp, 100.0)

        # Test with very large value
        large_value = 1.7976931348623157e+308  # close to f64::MAX
        self.client.execute_command("TS.ADD", "ts_extreme", 160000, large_value)

        # Verify the samples
        samples = self.client.execute_command("TS.RANGE", "ts_extreme", "-", "+")
        assert len(samples) == 2
        assert samples[1][0] == max_timestamp
        assert abs(samples[0][1] - large_value) < 1e300