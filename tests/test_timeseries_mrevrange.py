from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import pytest


class TestTimeSeriesMRevRange(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        # Create test time series with different labels
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'sensor', 'temp', 'location', 'kitchen')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'sensor', 'temp', 'location', 'living_room')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'sensor', 'humid', 'location', 'kitchen')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'sensor', 'humid', 'location', 'living_room')

        # Add data points
        now = 1000
        self.start_ts = now

        for i in range(0, 100, 10):
            # Add temperature readings (incrementing)
            self.client.execute_command('TS.ADD', 'ts1', self.start_ts + i, 20 + i/10)
            self.client.execute_command('TS.ADD', 'ts2', self.start_ts + i, 25 + i/10)

            # Add humidity readings (fluctuating)
            self.client.execute_command('TS.ADD', 'ts3', self.start_ts + i, 50 + (i % 20))
            self.client.execute_command('TS.ADD', 'ts4', self.start_ts + i, 60 + (i % 15))

    def test_mrevrange_basic(self):
        """Test basic TS.MREVRANGE functionality with filters"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=temp')

        # Should return 2 time series
        assert len(result) == 2

        # Each time series should have a key, labels and values
        for series in result:
            assert series[0] in [b'ts1', b'ts2']
            assert isinstance(series[1], list)  # Labels
            assert isinstance(series[2], list)  # Values
            assert len(series[2]) == 10

            # Verify timestamps are in descending order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_order_verification(self):
        """Test that TS.MREVRANGE returns samples in reverse chronological order"""
        self.setup_data()

        # Get both forward and reverse results
        forward_result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                                     'FILTER', 'sensor=temp')
        reverse_result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                                     'FILTER', 'sensor=temp')

        assert len(forward_result) == len(reverse_result)

        # Compare that the reverse result is exactly the reverse of the forward result
        for fwd_series, rev_series in zip(forward_result, reverse_result):
            assert fwd_series[0] == rev_series[0]  # Same key
            fwd_samples = fwd_series[2]
            rev_samples = rev_series[2]
            assert len(fwd_samples) == len(rev_samples)
            assert list(reversed(fwd_samples)) == rev_samples

    def test_mrevrange_withlabels(self):
        """Test TS.MREVRANGE with the WITHLABELS option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'WITHLABELS', 'FILTER', 'location=kitchen')

        assert len(result) == 2  # Should return ts1 and ts3

        # Check that labels are returned and timestamps are reversed
        for series in result:
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert labels_dict['location'] == 'kitchen'
            assert labels_dict['sensor'] in ['temp', 'humid']

            # Verify reverse order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_selected_labels(self):
        """Test TS.MREVRANGE with the SELECTED_LABELS option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=humid', 'SELECTED_LABELS', 'sensor')

        assert len(result) == 2  # Should return ts3 and ts4

        # Check that only selected labels are returned
        for series in result:
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert len(labels_dict) == 1
            assert labels_dict['sensor'] == 'humid'

            # Verify reverse order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_filter_by_value(self):
        """Test TS.MREVRANGE with the FILTER_BY_VALUE option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER_BY_VALUE', 25, 30, 'FILTER', 'sensor=temp')

        assert len(result) == 2
        for series in result:
            assert series[0] in [b'ts1', b'ts2']
            # All values should be within the filter range
            assert any(25 <= float(sample[1]) <= 30 for sample in series[2])

            # Verify reverse order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_count(self):
        """Test TS.MREVRANGE with the COUNT option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'COUNT', 5, 'FILTER', 'sensor=temp')

        # Should return 2 time series with 5 samples each
        assert len(result) == 2
        for series in result:
            assert len(series[2]) == 5

            # Verify we get the last 5 samples in reverse order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)
            # The first timestamp should be the largest (most recent)
            assert timestamps[0] >= self.start_ts + 50

    def test_mrevrange_aggregation(self):
        """Test TS.MREVRANGE with the AGGREGATION option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'FILTER', 'sensor=temp')

        # Should return 2 time series with ~5 aggregated samples each
        assert len(result) == 2
        for series in result:
            assert len(series[2]) in [5, 6]

            # Verify timestamps are in descending order
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_groupby(self):
        """Test TS.MREVRANGE with the GROUPBY option"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'WITHLABELS',
                                             'AGGREGATION', 'avg', 20,
                                             'FILTER', 'sensor=temp',
                                             'GROUPBY', 'sensor',
                                             'REDUCE', 'sum')

        # Should return 1 grouped time series
        assert len(result) == 1

        # Check labels include the groupby and reducer info
        labels_dict = {item[0].decode(): item[1].decode() for item in result[0][1]}
        assert labels_dict['sensor'] == 'temp'
        assert labels_dict['__reducer__'] == 'sum'

        # Verify timestamps are in descending order
        timestamps = [sample[0] for sample in result[0][2]]
        assert timestamps == sorted(timestamps, reverse=True)

        # Check values are aggregated (sum of both sensors)
        for ts, val in result[0][2]:
            val = float(val.decode())
            assert val > 40

    def test_mrevrange_empty(self):
        """Test TS.MREVRANGE with empty filter results"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=nonexistent')

        # Should return an empty list
        assert len(result) == 0

    def test_mrevrange_complex_filter(self):
        """Test TS.MREVRANGE with complex filter expressions"""
        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=temp', 'location!=kitchen')

        # Should return just ts2 (temp sensor in living room)
        assert len(result) == 1
        assert result[0][0] == b'ts2'

        # Verify reverse order
        timestamps = [sample[0] for sample in result[0][2]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_mrevrange_with_filter_by_ts(self):
        """Test TS.MREVRANGE with FILTER_BY_TS option"""
        self.setup_data()

        # Get specific timestamps only (1010, 1030, 1050)
        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER_BY_TS', self.start_ts + 10, self.start_ts + 30, self.start_ts + 50,
                                             'FILTER', 'sensor=temp')

        assert len(result) == 2
        for series in result:
            # Should have exactly 3 samples
            assert len(series[2]) == 3

            # Verify timestamps match and are in reverse order
            timestamps = [sample[0] for sample in series[2]]
            expected_ts = [self.start_ts + 50, self.start_ts + 30, self.start_ts + 10]
            assert timestamps == expected_ts

    def test_mrevrange_single_sample(self):
        """Test TS.MREVRANGE with a single sample result"""
        self.client.execute_command('TS.CREATE', 'single', 'LABELS', 'type', 'single')
        self.client.execute_command('TS.ADD', 'single', 1000, 42)

        result = self.client.execute_command('TS.MREVRANGE', 999, 1001, 'FILTER', 'type=single')

        assert len(result) == 1
        assert len(result[0][2]) == 1
        assert result[0][2][0][0] == 1000
        assert float(result[0][2][0][1]) == 42.0
