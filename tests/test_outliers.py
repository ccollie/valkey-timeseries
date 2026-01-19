import math

import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


def create_normal_series(client, key, start_time=1000, count=100):
    """Create a time series with normal data points."""
    for i in range(count):
        client.execute_command(
            'TS.ADD', key, start_time + i * 1000,
            math.sin(i * 0.1)
        )
    return start_time, count


def create_series_with_outliers(client, key, start_time=1000):
    """Create a time series with clear outliers."""
    values = [1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9, 1.7, 50.0]
    for i, val in enumerate(values):
        client.execute_command(
            'TS.ADD', key, start_time + i * 1000, val
        )
    return start_time, len(values)


class TestOutliersBasic(ValkeyTimeSeriesTestCaseBase):
    """Basic functionality tests for TS.OUTLIERS"""

    def test_outliers_zscore_default(self):
        """Test outlier detection with default z-score method."""
        key = 'test:outliers:zscore'
        client = self.client
        start_time, count = create_series_with_outliers(client, key)

        result = client.execute_command('TS.OUTLIERS', key, "-", "+", "method", "zscore")

        # Should return [timestamps, values, scores, signals]
        assert len(result) == 4
        timestamps, values, scores, signals = result

        assert len(timestamps) == count
        assert len(values) == count
        assert len(scores) == count
        assert len(signals) == count

        # Check that outliers are detected (values 10.0 and 50.0)
        outlier_count = sum(1 for s in signals if s != 0)
        assert outlier_count >= 2

    def test_outliers_with_threshold(self):
        """Test outlier detection with a custom threshold."""
        key = 'test:outliers:threshold'
        start_time, count = create_series_with_outliers(self.client, key)

        # A lower threshold should detect more outliers
        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'zscore', 'THRESHOLD', 2.0
        )

        print("threshold result", result)
        timestamps, values, scores, signals = result[0]
        outlier_count = sum(1 for s in signals if s != 0)
        assert outlier_count >= 2

    def test_outliers_empty_series(self):
        """Test outlier detection on empty series."""
        key = 'test:outliers:empty'
        self.client.execute_command('TS.CREATE', key)

        with pytest.raises(Exception) as exc_info:
            self.client.execute_command('TS.OUTLIERS', key, '-', '+', 'METHOD', 'zscore')
        assert 'insufficient data' in str(exc_info.value).lower()

    def test_outliers_nonexistent_key(self):
        """Test outlier detection on non-existent key."""
        with pytest.raises(ResponseError, match="the key does not exist") as exc_info:
            self.client.execute_command('TS.OUTLIERS', 'nonexistent', "-", "+", "method", "zscore")


class TestOutliersMethods(ValkeyTimeSeriesTestCaseBase):
    """Test different outlier detection methods."""

    def test_method_zscore(self):
        """Test z-score method."""
        key = 'test:outliers:method:zscore'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'zscore'
        )
        print("zscore result", result)
        assert len(result) == 4

    def test_method_modified_zscore(self):
        """Test modified z-score method."""
        key = 'test:outliers:method:modified'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, "-", "+", 'METHOD', 'modified-zscore'
        )

        print("modified-zscore result", result)
        assert len(result) == 4

        timestamps, values, scores, signals = result[0]
        outlier_count = sum(1 for s in signals if s != 0)
        assert outlier_count >= 1

    def test_method_iqr(self):
        """Test interquartile range method."""
        key = 'test:outliers:method:iqr'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'iqr', 'threshold', 1.5
        )
        print("iqr result", result)
        assert len(result) == 4

    def test_method_mad(self):
        """Test median absolute deviation method."""
        key = 'test:outliers:method:mad'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'mad', 'threshold', 3.0
        )
        print("mad result", result)
        assert len(result) == 4

    def test_method_double_mad(self):
        """Test double Mad method."""
        key = 'test:outliers:method:double-mad'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'double-mad', 'threshold', 3.0
        )
        print("double result", result)
        assert len(result) == 4

    def test_method_spc_shewhart(self):
        """Test SPC Shewhart control chart method."""
        key = 'test:outliers:method:spc'
        create_normal_series(self.client, key, count=100)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'shewhart'
        )
        print("shewhart result", result)
        assert len(result) == 4

    def test_method_spc_cusum(self):
        """Test SPC CUSUM control chart method."""
        key = 'test:outliers:method:cusum'
        create_normal_series(self.client, key, count=100)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'cusum'
        )
        print("cusum result", result)
        assert len(result) == 4

    def test_method_spc_ewma(self):
        """Test SPC EWMA control chart method."""
        key = 'test:outliers:method:ewma'
        create_normal_series(self.client, key, count=100)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'ewma',
            'ALPHA', 0.3
        )
        print("ewma result", result)
        assert len(result) == 4

    def test_method_smoothed_zscore(self):
        """Test smoothed z-score method."""
        key = 'test:outliers:method:smoothed'
        create_series_with_outliers(self.client, key)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'smoothed-zscore',
            'LAG', 5, 'THRESHOLD', 3.5, 'INFLUENCE', 0.5
        )
        print("smoothed-zscore result", result)
        assert len(result) == 4

    def test_method_isolation_forest(self):
        """Test isolation forest method."""
        key = 'test:outliers:method:iforest'
        create_normal_series(self.client, key, count=100)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'isolation-forest',
            'NUM_TREES', 50, 'CONTAMINATION', 0.1
        )
        print("isolation-forest result", result)
        assert len(result) == 4

    def test_method_rcf(self):
        """Test random cut forest method."""
        key = 'test:outliers:method:rcf'
        create_normal_series(self.client, key, count=100)

        result = self.client.execute_command(
            'TS.OUTLIERS', key, '-', '+', 'METHOD', 'rcf',
            'SHINGLE_SIZE', 8, 'SAMPLE_SIZE', 256
        )
        print("rcf result", result)
        assert len(result) == 4

    def test_invalid_method(self):
        """Test with an invalid method name."""
        key = 'test:outliers:invalid'
        create_series_with_outliers(self.client, key)

        with pytest.raises(ResponseError, match='TSDB: unknown outlier detection method'):
            self.client.execute_command('TS.OUTLIERS', key, '-', '+', 'METHOD', 'invalid')


class TestTimeSeriesAnomalyDetection(ValkeyTimeSeriesTestCaseBase):
    """Tests for TS.OUTLIERS command"""

    def test_zscore_anomaly_detection(self):
        """Test Z-score based anomaly detection"""
        key = "zscore_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a time series with clear anomalies
        for i in range(100):
            value = (i / 10.0) if i != 25 and i != 75 else (5.0 if i == 25 else -5.0)
            self.client.execute_command("TS.ADD", key, i, value)

        # Detect anomalies using the Z-score method
        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "zscore",
            "threshold", 3.0
        )

        print("zscore result", result)
        assert result is not None
        assert "scores" in result
        assert "anomalies" in result
        assert len(result["anomalies"]) == 100

        # Verify anomalies at indices 25 and 75
        anomaly_count = sum(1 for x in result["anomalies"] if x != 0)
        assert anomaly_count >= 2

    def test_modified_zscore_detection(self):
        """Test Modified Z-score using median absolute deviation"""
        key = "modified_zscore_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with outliers
        values = [1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]
        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "modified-zscore",
            "threshold", 3.5
        )
        print("modified-zscore result", result)
        assert result["anomalies"][5] != 0  # Outlier at index 5

    def test_iqr_anomaly_detection(self):
        """Test Interquartile Range (IQR) anomaly detection"""
        key = "iqr_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with outliers
        for i in range(100):
            value = 1.0 if i != 50 else 10.0
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "iqr",
            "threshold", 1.5
        )

        print("iqr result", result)

        assert result["anomalies"][50] != 0  # Outlier detected

    def test_spc_shewhart_detection(self):
        """Test Statistical Process Control (Shewhart) detection"""
        key = "spc_shewhart_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with mean shift
        for i in range(100):
            value = 1.0 + 0.1 * (i * 0.1) if i < 50 else 5.0 + 0.1 * (i * 0.1)
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "shewhart"
        )

        print("shewhart result", result)
        # Expect anomalies in the second half
        second_half_anomalies = sum(1 for x in result["anomalies"][50:] if x != 0)
        assert second_half_anomalies > 10

    def test_isolation_forest_detection(self):
        """Test Isolation Forest anomaly detection"""
        key = "isolation_forest_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with anomalies
        for i in range(50):
            value = (i / 5.0) if i != 25 else 10.0
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command(
            "TS.OUTLIERS", key, "-", "+",
            "method", "isolation-forest",
            "num_trees", 10,
            "contamination", 0.1,
            "window_size", 5
        )

        print("isolation-forest result", result)

        anomaly_count = sum(1 for x in result["anomalies"] if x != 0)
        assert anomaly_count > 0

    def test_smoothed_zscore_detection(self):
        """Test Smoothed Z-Score anomaly detection"""
        key = "smoothed_zscore_test"

        self.client.execute_command("TS.CREATE", key)

        # Create a series with peaks
        initial = [1.0] * 20
        for i, value in enumerate(initial):
            self.client.execute_command("TS.ADD", key, i, value)

        # Add test values
        test_values = [1.0, 5.0, 1.0, -3.0, 1.0]
        for i, value in enumerate(test_values, start=20):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "smoothed-zscore",
                                             "lag", 20,
                                             "threshold", 2.0,
                                             "influence", 0.0
                                             )

        print("smoothed-zscore result", result)

        assert len(result["anomalies"]) == 25

    def test_mad_detection(self):
        """Test Median Absolute Deviation (Mad) detection"""
        key = "mad_test"

        self.client.execute_command("TS.CREATE", key)

        values = [1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]
        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "mad",
                                             "threshold", 3.0,
                                             "estimator", "simple"
                                             )

        print("mad result", result)

        assert any(x != 0 for x in result["anomalies"])

    def test_double_mad_detection(self):
        """Test Double Median Absolute Deviation (Double Mad) outlier detection"""
        key = "double_mad_test"

        self.client.execute_command("TS.CREATE", key)

        values = [1.0] * 10 + [5.0] + [1.0] * 10
        for i, value in enumerate(values):
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "double-mad",
                                             "threshold", 3.0,
                                             "estimator", "harrell-davis"
                                             )

        print("double mad result", result)

        assert result["anomalies"][10] != 0  # Outlier at index 10

    def test_anomaly_direction_filtering(self):
        """Test filtering anomalies by direction"""
        key = "direction_test"

        self.client.execute_command("TS.CREATE", key)

        for i in range(50):
            value = 1.0
            if i == 20:
                value = 5.0  # Positive anomaly
            elif i == 30:
                value = -3.0  # Negative anomaly
            self.client.execute_command("TS.ADD", key, i, value)

        # Detect only positive anomalies
        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 2.0,
                                             "direction", "positive"
        )

        assert result["anomalies"][20] == 1  # Positive signal
        assert result["anomalies"][30] == 0  # Negative filtered out

    def test_seasonal_adjustment(self):
        """Test anomaly detection with seasonal adjustment"""
        key = "seasonal_test"

        self.client.execute_command("TS.CREATE", key)

        # Create series with a seasonal pattern
        for i in range(100):
            seasonal = (i % 7) * 0.5
            value = 1.0 + seasonal
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0,
                                             "seasonal_period", 7
        )

        assert result is not None
        assert "anomalies" in result

    def test_insufficient_data_error(self):
        """Test error handling with insufficient data"""
        key = "insufficient_test"

        self.client.execute_command("TS.CREATE", key)

        # Add only 2 points
        self.client.execute_command("TS.ADD", key, 0, 1.0)
        self.client.execute_command("TS.ADD", key, 1, 2.0)

        with pytest.raises(Exception):
            self.client.execute_command("TS.OUTLIERS", key, method="zscore")

    def test_constant_series(self):
        """Test anomaly detection on constant series"""
        key = "constant_test"

        self.client.execute_command("TS.CREATE", key)

        # Add constant values
        for i in range(50):
            self.client.execute_command("TS.ADD", key, i, 1.0)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "zscore",
                                             "threshold", 3.0
        )

        # No anomalies in constant series
        anomaly_count = sum(1 for x in result["anomalies"] if x != 0)
        assert anomaly_count == 0

    def test_rcf_detection(self):
        """Test Random Cut Forest (Rcf) anomaly detection"""
        key = "rcf_test"

        self.client.execute_command("TS.CREATE", key)

        for i in range(100):
            value = (i / 10.0) if i != 50 else 10.0
            self.client.execute_command("TS.ADD", key, i, value)

        result = self.client.execute_command("TS.OUTLIERS",
                                             key, "-", "+",
                                             "method", "rcf",
                                             "threshold", 2.5
        )

        assert any(x != 0 for x in result["anomalies"])
