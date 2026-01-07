import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from common import parse_stats_response
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsStats(ValkeyTimeSeriesTestCaseBase):
    """Test suite for TS.STATS command."""

    def get_stats(self, limit: int | None = None):
        args = ['TS.STATS']
        if limit is not None:
            args.extend(['LIMIT', limit])

        result = self.client.execute_command(*args)
        stats = parse_stats_response(result)
        return stats


    def test_stats_empty_index(self):
        """Test TS.STATS on an empty index returns zero counts."""
        stats = self.get_stats()

        assert stats['numSeries'] == 0
        assert stats['numLabels'] == 0
        assert stats['numLabelPairs'] == 0
        assert stats['seriesCountByMetricName'] == []
        assert stats['labelValueCountByLabelName'] == []
        assert stats['memoryInBytesByLabelPair'] == []
        assert stats['seriesCountByLabelPair'] == []

    def test_stats_single_series(self):
        """Test TS.STATS with a single time series."""
        self.client.execute_command(
            'TS.CREATE', 'temperature',
            'LABELS', 'sensor', 'temp1', 'location', 'room1'
        )

        result = self.get_stats()

        assert result['numSeries'] == 1
        assert result['numLabels'] == 2  # sensor and location
        assert result['numLabelPairs'] == 2

    def test_stats_multiple_series_same_labels(self):
        """Test TS.STATS with multiple series sharing labels."""
        self.client.execute_command(
            'TS.CREATE', 'temp1',
            'LABELS', 'sensor', 'temp', 'location', 'room1'
        )
        self.client.execute_command(
            'TS.CREATE', 'temp2',
            'LABELS', 'sensor', 'temp', 'location', 'room2'
        )
        self.client.execute_command(
            'TS.CREATE', 'temp3',
            'LABELS', 'sensor', 'temp', 'location', 'room3'
        )

        result = self.get_stats()

        assert result['numSeries'] == 3
        assert result['numLabels'] == 2
        assert result['numLabelPairs'] == 4

    def test_stats_with_limit_parameter(self):
        """Test TS.STATS with LIMIT parameter."""
        # Create multiple series with different metric names
        for i in range(15):
            self.client.execute_command(
                'TS.CREATE', f'metric{i}',
                'LABELS', 'type', f'type{i}'
            )

        # Default limit is 10
        result = self.get_stats()
        assert len(result['seriesCountByMetricName']) <= 10
        assert len(result['labelValueCountByLabelName']) <= 10
        assert len(result['memoryInBytesByLabelPair']) <= 10
        assert len(result['seriesCountByLabelPair']) <= 10

        # Custom limit of 5
        result = self.get_stats(5)
        assert len(result['seriesCountByMetricName']) <= 5
        assert len(result['labelValueCountByLabelName']) <= 5
        assert len(result['memoryInBytesByLabelPair']) <= 5
        assert len(result['seriesCountByLabelPair']) <= 5

    def test_stats_series_count_by_metric_name(self):
        """Test that seriesCountByMetricName returns correct counts."""
        # Create multiple series with the same metric name
        for i in range(3):
            self.client.execute_command(
                'TS.CREATE', f'temperature:{i}',
                'LABELS', '__name__', 'temperature', 'id', f'{i}'
            )

        for i in range(2):
            self.client.execute_command(
                'TS.CREATE', f'pressure:{i}',
                'LABELS', '__name__', 'pressure', 'id', f'{i}'
            )

        result = self.get_stats()

        # Check that temperature has a count of 3 and pressure has a count of 2
        metric_stats = {item[0]: item[1]
                    for item in result['seriesCountByMetricName']}

        assert metric_stats.get('temperature') == 3
        assert metric_stats.get('pressure') == 2

    def test_stats_label_value_count(self):
        """Test labelValueCountByLabelName statistics."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'region', 'us-east', 'env', 'prod'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'region', 'us-west', 'env', 'prod'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts3',
            'LABELS', 'region', 'eu-west', 'env', 'dev'
        )

        result = self.get_stats()

        label_counts = result['labelValueCountByLabelName']
        print("labelValueCountByLabelName =", label_counts)

        # region has 3 values, env has 2 values
        label_counts = {item[0]: item[1]
                        for item in label_counts}

        assert label_counts.get('region') == 3
        assert label_counts.get('env') == 3

    def test_stats_series_count_by_label_pair(self):
        """Test seriesCountByLabelPair statistics."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'status', '200', 'method', 'GET'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'status', '200', 'method', 'POST'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts3',
            'LABELS', 'status', '404', 'method', 'GET'
        )

        result = self.get_stats()
        label_pair_counts = result['seriesCountByLabelPair']    
        print("seriesCountByLabelPair =", label_pair_counts)

        # status=200 appears in 2 series, status=404 in 1
        pair_counts = {item[0]: item[1]
                       for item in label_pair_counts}

        assert pair_counts.get('status=200') == 2
        assert pair_counts.get('status=404') == 1
        assert pair_counts.get('method=GET') == 2
        assert pair_counts.get('method=POST') == 1

    def test_stats_invalid_limit(self):
        """Test TS.STATS with invalid LIMIT values."""
        # LIMIT must be greater than 0
        with pytest.raises(ResponseError, match='LIMIT must be greater than 0'):
            self.client.execute_command('TS.STATS', 'LIMIT', 0)

        # LIMIT cannot be negative
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.STATS', 'LIMIT', -1)

    def test_stats_wrong_arity(self):
        """Test TS.STATS with wrong number of arguments."""
        with pytest.raises(ResponseError, match='wrong number of arguments'):
            self.client.execute_command('TS.STATS', 'LIMIT', 10, 20)

    def test_stats_memory_usage(self):
        """Test that memoryInBytesByLabelPair returns reasonable values."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'label1', 'value1', 'label2', 'value2'
        )

        result = self.get_stats()
        memory_stats = result['memoryInBytesByLabelPair']

        # Should have entries for label pairs
        assert len(memory_stats) > 0

        # Each entry should have a positive memory value
        for item in memory_stats:
            value = item[1]
            assert value > 0

    def test_stats_after_series_deletion(self):
        """Test TS.STATS after deleting series."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', 'type', 'test'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', 'type', 'test'
        )

        result = self.get_stats()
        assert result['numSeries'] == 2

        # Delete one series
        self.client.delete('ts1')

        result = self.get_stats()
        assert result['numSeries'] == 1

    def test_stats_with_metric_name_label(self):
        """Test TS.STATS with __name__ label (metric name)."""
        self.client.execute_command(
            'TS.CREATE', 'ts1',
            'LABELS', '__name__', 'http_requests', 'status', '200'
        )
        self.client.execute_command(
            'TS.CREATE', 'ts2',
            'LABELS', '__name__', 'http_requests', 'status', '404'
        )

        result = self.get_stats()

        # Should count __name__ as a label, along with status
        assert result['numLabels'] == 2

    def test_stats_sorted_by_count(self):
        """Test that stats results are sorted by count in descending order."""
        # Create series with different frequencies
        for i in range(5):
            self.client.execute_command(
                'TS.CREATE', f'common:{i}',
                'LABELS', 'type', 'common'
            )

        for i in range(2):
            self.client.execute_command(
                'TS.CREATE', f'rare:{i}',
                'LABELS', 'type', 'rare'
            )

        result = self.get_stats()
        pair_counts = result['seriesCountByMetricName']

        print("seriesCountByMetricName =", pair_counts)

        # Check that label pair counts are sorted descending
        pair_counts = [item[1]
                       for item in result['seriesCountByLabelPair']]

        for i in range(len(pair_counts) - 1):
            assert pair_counts[i] >= pair_counts[i + 1]
