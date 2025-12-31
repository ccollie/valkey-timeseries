from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import time
import pytest
class TestTimeSeriesMRange(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):
        # Create test time series with different labels
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'sensor', 'temp', 'location', 'kitchen')
        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'sensor', 'temp', 'location', 'living_room')
        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'sensor', 'humid', 'location', 'kitchen')
        self.client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'sensor', 'humid', 'location', 'living_room')

        # Add data points
        now = 1000
        self.start_ts = now # - 100

        for i in range(0, 100, 10):
            # Add temperature readings (incrementing)
            self.client.execute_command('TS.ADD', 'ts1', self.start_ts + i, 20 + i/10)
            self.client.execute_command('TS.ADD', 'ts2', self.start_ts + i, 25 + i/10)

            # Add humidity readings (fluctuating)
            self.client.execute_command('TS.ADD', 'ts3', self.start_ts + i, 50 + (i % 20))
            self.client.execute_command('TS.ADD', 'ts4', self.start_ts + i, 60 + (i % 15))

    def test_mrange_basic(self):
        """Test basic TS.MRANGE functionality with filters"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=temp')

        # Should return 2 time series
        assert len(result) == 2

        # Each time series should have a key, labels and values
        for series in result:
            assert series[0] in [b'ts1', b'ts2']

            assert isinstance(series[1], list)  # Labels
            assert isinstance(series[2], list)  # values
            # Each series should have 10 data points (0, 10, 20, ..., 100)
            # print(series[1])
            # print(series[2])
            assert len(series[2]) == 10

    def test_mrange_withlabels(self):
        """Test TS.MRANGE with WITHLABELS option"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'WITHLABELS', 'FILTER', 'location=kitchen')

        assert len(result) == 2  # Should return ts1 and ts3

        # Check that labels are returned
        for series in result:
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert labels_dict['location'] == 'kitchen'
            assert labels_dict['sensor'] in ['temp', 'humid']

    def test_mrange_selected_labels(self):
        """Test TS.MRANGE with the SELECTED_LABELS option"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=humid', 'SELECTED_LABELS', 'sensor')

        assert len(result) == 2  # Should return ts3 and ts4

        # Check that only selected labels are returned
        for series in result:
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert len(labels_dict) == 1  # Only the 'sensor' label should be returned
            assert labels_dict['sensor'] == 'humid'

    def test_mrange_filter_by_value(self):
        """Test TS.MRANGE with the FILTER_BY_VALUE option"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER_BY_VALUE', 25, 30, 'FILTER', 'sensor=temp')
        print(result)

        # Should only return ts2 as ts1 values start at 20
        assert len(result) == 2
        for series in result:
            assert series[0] in [b'ts1', b'ts2']
            assert any(25 <= float(sample[1]) <= 30 for sample in series[2])

    def test_mrange_aggregation(self):
        """Test TS.MRANGE with the AGGREGATION option"""
        # Get average temperatures in 20-second buckets

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'FILTER', 'sensor=temp')

        # Should return 2 time series with ~5 aggregated samples each (100/20=5)
        assert len(result) == 2
        for series in result:
            # Might be 5 or 6 samples depending on the exact bucket alignment
            assert len(series[2]) in [5, 6]

    def test_mrange_groupby(self):
        """Test TS.MRANGE with GROUPBY option"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'FILTER', 'sensor=temp',
                                             'GROUPBY', 'sensor',
                                             'REDUCE', 'sum')

        ts1 = self.client.execute_command('TS.RANGE', 'ts1', self.start_ts, self.start_ts + 100)
        ts2 = self.client.execute_command('TS.RANGE', 'ts2', self.start_ts, self.start_ts + 100)
        print("TS1:", ts1)
        print("TS2:", ts2)
        res_agg = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'FILTER', 'sensor=temp')
        print("Aggregated Result:", res_agg)

        # Should return just 1 time series that groups both temperature sensors
        assert len(result) == 1
        print("Grouped Result:", result)

        # Check values are aggregated (sum of both sensors)
        for ts, val in result[0][2]:
            val = float(val.decode())
            assert val > 40  # Sum of two temp sensors should be > 40

    def test_mrange_empty(self):

        self.setup_data()

        """Test TS.MRANGE with empty filter results"""
        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=nonexistent')

        # Should return an empty list
        assert len(result) == 0

    def test_mrange_complex_filter(self):
        """Test TS.MRANGE with complex filter expressions"""

        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=temp', 'location!=kitchen')

        # Should return just ts2 (temp sensor in living room)
        assert len(result) == 1
        assert result[0][0] == b'ts2'

    def test_mrevrange(self):
        """Test TS.MREVRANGE (reverse order)"""

        self.setup_data()

        result = self.client.execute_command('TS.MREVRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER', 'sensor=temp')
        # Should return 2 time series
        assert len(result) == 2

        # Check that timestamps are in descending order
        for series in result:
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps == sorted(timestamps, reverse=True)

    def test_mrange_count_basic(self):
        """Test TS.MRANGE COUNT returns exactly the requested number of samples"""
        self.setup_data()

        # Request only 3 samples
        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'COUNT', 3, 'FILTER', 'sensor=temp')

        assert len(result) == 2  # Two temperature series
        for series in result:
            assert len(series[2]) == 3  # Exactly 3 samples
            # Verify timestamps are sequential from the start
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps[0] == self.start_ts
            assert timestamps[1] == self.start_ts + 10
            assert timestamps[2] == self.start_ts + 20

    def test_mrange_count_exceeds_available(self):
        """Test TS.MRANGE COUNT when the requested count exceeds available samples"""
        self.setup_data()

        # Request more samples than exist (we have 10, request 20)
        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'COUNT', 20, 'FILTER', 'sensor=humid')

        assert len(result) == 2
        for series in result:
            # Should return all 10 available samples, not fail
            assert len(series[2]) == 10

    def test_mrange_count_with_aggregation_avg(self):
        """Test TS.MRANGE COUNT combined with AGGREGATION avg"""
        self.setup_data()

        # Get average in 20-second buckets, but limit to 2 buckets
        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'COUNT', 2,
                                             'FILTER', 'sensor=temp')

        assert len(result) == 2  # Two temperature series
        for series in result:
            print("series:", series)
            # Should return exactly 2 aggregated samples
            assert len(series[2]) == 2
            # Verify the samples are aggregated values
            timestamps = [sample[0] for sample in series[2]]
            assert timestamps[0] == self.start_ts

    def test_mrange_count_with_aggregation_sum(self):
        """Test TS.MRANGE COUNT combined with the AGGREGATION sum"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'sum', 30,
                                             'COUNT', 3,
                                             'FILTER', 'sensor=humid')

        assert len(result) == 2
        for series in result:
            print("series:", series)
            assert len(series[2]) == 3
            # Verify values are sums (should be larger than individual readings)
            for ts, val in series[2]:
                val = float(val.decode())
                assert val > 50  # Sum of multiple readings

    def test_mrange_count_with_aggregation_max(self):
        """Test TS.MRANGE COUNT combined with AGGREGATION max"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'max', 25,
                                             'COUNT', 2,
                                             'FILTER', 'location=kitchen')

        assert len(result) == 2  # ts1 and ts3
        for series in result:
            print("series:", series)
            assert len(series[2]) == 2

    def test_mrange_count_with_groupby(self):
        """Test TS.MRANGE COUNT combined with GROUPBY"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'COUNT', 5,
                                             'FILTER', 'sensor=temp',
                                             'GROUPBY', 'sensor',
                                             'REDUCE', 'sum')

        # Should return 1 grouped series
        assert len(result) == 1
        # Should have exactly 5 samples due to COUNT
        assert len(result[0][2]) == 5

        # Verify timestamps are from the beginning
        timestamps = [sample[0] for sample in result[0][2]]
        expected_timestamps = [self.start_ts + i * 10 for i in range(5)]
        assert timestamps == expected_timestamps

    def test_mrange_count_with_groupby_and_aggregation(self):
        """Test TS.MRANGE COUNT combined with both GROUPBY and AGGREGATION"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'AGGREGATION', 'avg', 20,
                                             'COUNT', 2,
                                             'WITHLABELS',
                                             'FILTER', 'sensor=humid',
                                             'GROUPBY', 'location',
                                             'REDUCE', 'max')

        # Should return 2 grouped series (one per location)
        assert len(result) == 2

        for series in result:
            # Each should have exactly 2 aggregated samples
            assert len(series[2]) == 2

            # Verify groupby labels
            labels_dict = {item[0].decode(): item[1].decode() for item in series[1]}
            assert labels_dict['location'] in ['kitchen', 'living_room']
            assert labels_dict['__reducer__'] == 'max'

    def test_mrange_count_zero(self):
        """Test TS.MRANGE with COUNT 0 (should return empty results)"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'COUNT', 0,
                                             'FILTER', 'sensor=temp')

        assert len(result) == 2  # Series are returned
        for series in result:
            # But no data points
            assert len(series[2]) == 0

    def test_mrange_count_with_filter_by_value(self):
        """Test TS.MRANGE COUNT combined with FILTER_BY_VALUE"""
        self.setup_data()

        result = self.client.execute_command('TS.MRANGE', self.start_ts, self.start_ts + 100,
                                             'FILTER_BY_VALUE', 20, 30,
                                             'COUNT', 3,
                                             'FILTER', 'sensor=temp')

        for series in result:
            # Should have at most 3 samples
            assert len(series[2]) <= 3
            # All values should be within the filter range
            for ts, val in series[2]:
                val = float(val.decode())
                assert 20 <= val <= 30