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

    def test_mrevrange_latest_with_partial_bucket(self):
        """Test TS.MREVRANGE LATEST with partial (unclosed) compaction buckets"""
        base_ts = 1000

        # Create source and compaction series with 60-second buckets
        self.client.execute_command('TS.CREATE', 'source:partial')
        self.client.execute_command('TS.CREATE', 'compact:partial',
                                    'LABELS', 'type', 'partial')
        self.client.execute_command('TS.CREATERULE', 'source:partial', 'compact:partial',
                                    'AGGREGATION', 'avg', 60000, base_ts)

        # Add samples that span multiple buckets
        for i in range(8):
            self.client.execute_command('TS.ADD', 'source:partial', base_ts + i * 10000, i * 5)

        # Add a few more samples in a new bucket that isn't closed yet
        # This creates a partial bucket at base_ts + 120000
        partial_bucket_start = base_ts + 120000
        self.client.execute_command('TS.ADD', 'source:partial', partial_bucket_start, 100)
        self.client.execute_command('TS.ADD', 'source:partial', partial_bucket_start + 10000, 110)
        self.client.execute_command('TS.ADD', 'source:partial', partial_bucket_start + 20000, 120)

        # Query with LATEST - should include the partial bucket's latest sample
        result = self.client.execute_command('TS.MREVRANGE', base_ts, partial_bucket_start + 30000,
                                             'LATEST',
                                             'FILTER', 'type=partial')
        assert len(result) == 1
        samples = result[0][2]
        print("Samples from only partial bucket test:", samples)
        assert len(samples) >= 2  # At least one complete bucket + partial bucket

        # Verify reverse order
        timestamps = [sample[0] for sample in samples]
        assert timestamps == sorted(timestamps, reverse=True)

        # The most recent timestamp should be from the partial bucket
        assert timestamps[0] >= partial_bucket_start

    def test_mrevrange_latest_multiple_partial_buckets(self):
        """Test TS.MREVRANGE LATEST with multiple series having partial buckets"""
        # Create multiple compaction series
        base_ts = 5000
        for i in range(3):
            source = f'source:multi:{i}'
            compact = f'compact:multi:{i}'
            self.client.execute_command('TS.CREATE', source)
            self.client.execute_command('TS.CREATE', compact,
                                        'LABELS', 'group', 'multi', 'id', str(i))
            self.client.execute_command('TS.CREATERULE', source, compact,
                                        'AGGREGATION', 'sum', 50000)
            # Add complete buckets
            for j in range(5):
                self.client.execute_command('TS.ADD', source, base_ts + j * 15000, j * 10)

            # Add partial bucket
            partial_ts = base_ts + 100000
            self.client.execute_command('TS.ADD', source, partial_ts, 200 + i)
            self.client.execute_command('TS.ADD', source, partial_ts + 5000, 210 + i)

        # Query with LATEST
        result = self.client.execute_command('TS.MREVRANGE', 5000, 120000,
                                             'LATEST',
                                             'FILTER', 'group=multi')

        assert len(result) == 3

        for series in result:
            samples = series[2]
            assert len(samples) >= 1

            # Verify reverse order
            timestamps = [sample[0] for sample in samples]
            assert timestamps == sorted(timestamps, reverse=True)

            # Should include samples from partial bucket
            assert any(ts >= 100000 for ts in timestamps)

    def test_mrevrange_latest_without_compaction(self):
        """Test TS.MREVRANGE LATEST on non-compacted series has no effect"""
        self.setup_data()

        # Query regular series with LATEST flag
        result_with_latest = self.client.execute_command('TS.MREVRANGE', self.start_ts,
                                                         self.start_ts + 100,
                                                         'LATEST',
                                                         'FILTER', 'sensor=temp')

        result_without_latest = self.client.execute_command('TS.MREVRANGE', self.start_ts,
                                                            self.start_ts + 100,
                                                            'FILTER', 'sensor=temp')

        # Results should be identical for non-compacted series
        assert len(result_with_latest) == len(result_without_latest)
        for i in range(len(result_with_latest)):
            assert result_with_latest[i][0] == result_without_latest[i][0]
            assert len(result_with_latest[i][2]) == len(result_without_latest[i][2])


    def test_mrevrange_latest_only_partial_bucket(self):
        """Test TS.MREVRANGE LATEST when only a partial bucket exists"""
        # Create compaction series
        self.client.execute_command('TS.CREATE', 'source:only_partial')
        self.client.execute_command('TS.CREATE', 'compact:only_partial',
                                    'LABELS', 'bucket', 'partial')
        self.client.execute_command('TS.CREATERULE', 'source:only_partial', 'compact:only_partial',
                                    'AGGREGATION', 'avg', 100000)

        base_ts = 50000

        # Add samples in the first bucket [0..100000).
        # Then add a sample at the next bucket boundary (100000) to force the first bucket to close
        # and emit at least one persisted compaction sample. After that, add samples in the new
        # (still-open) bucket so LATEST has an "in-progress" bucket to include.
        self.client.execute_command('TS.ADD', 'source:only_partial', base_ts, 10)
        self.client.execute_command('TS.ADD', 'source:only_partial', base_ts + 20000, 20)
        self.client.execute_command('TS.ADD', 'source:only_partial', base_ts + 40000, 30)

        partial_bucket_start = 100000  # bucket boundaries are aligned to epoch (0), not base_ts
        self.client.execute_command('TS.ADD', 'source:only_partial', partial_bucket_start, 40)
        self.client.execute_command('TS.ADD', 'source:only_partial', partial_bucket_start + 20000, 50)
        self.client.execute_command('TS.ADD', 'source:only_partial', partial_bucket_start + 40000, 60)

        # Query with LATEST
        result = self.client.execute_command(
            'TS.MREVRANGE',
            base_ts,
            partial_bucket_start + 50000,
            'LATEST',
            'FILTER', 'bucket=partial'
        )

        assert len(result) == 1
        samples = result[0][2]

        # Should have at least one sample from the (now-present) open bucket
        assert len(samples) >= 1

        # Verify reverse order
        timestamps = [sample[0] for sample in samples]
        assert timestamps == sorted(timestamps, reverse=True)



    def test_mrevrange_latest_partial_bucket_with_filter_by_ts(self):
        """Test TS.MREVRANGE LATEST with partial buckets and FILTER_BY_TS"""
        # Create compaction series
        self.client.execute_command('TS.CREATE', 'source:filtered')
        self.client.execute_command('TS.CREATE', 'compact:filtered',
                                    'LABELS', 'filtered', 'yes')
        self.client.execute_command('TS.CREATERULE', 'source:filtered', 'compact:filtered',
                                    'AGGREGATION', 'sum', 45000)

        base_ts = 15000
        # Add complete buckets
        for i in range(8):
            self.client.execute_command('TS.ADD', 'source:filtered', base_ts + i * 12000, i * 6)

        # Add partial bucket
        partial_ts = base_ts + 110000
        self.client.execute_command('TS.ADD', 'source:filtered', partial_ts, 300)
        self.client.execute_command('TS.ADD', 'source:filtered', partial_ts + 10000, 310)

        # Query with LATEST and FILTER_BY_TS including partial bucket timestamp
        specific_timestamps = [base_ts, base_ts + 45000, partial_ts]
        result = self.client.execute_command('TS.MREVRANGE', base_ts, partial_ts + 15000,
                                             'LATEST',
                                             'FILTER_BY_TS', *specific_timestamps,
                                             'FILTER', 'filtered=yes')

        assert len(result) == 1
        samples = result[0][2]

        # Should only return samples at specified timestamps in reverse order
        timestamps = [sample[0] for sample in samples]
        assert timestamps == sorted(timestamps, reverse=True)
        for ts in timestamps:
            assert ts in specific_timestamps

    def test_mrevrange_latest_empty_range(self):
        """Test TS.MREVRANGE LATEST with time range that has no compacted data"""
        # Create compaction series
        self.client.execute_command('TS.CREATE', 'source:test')
        self.client.execute_command('TS.CREATE', 'compact:test',
                                    'LABELS', 'status', 'active')
        self.client.execute_command('TS.CREATERULE', 'source:test', 'compact:test',
                                    'AGGREGATION', 'avg', 50000)

        # Add data in a different time range
        base_ts = 100000
        for i in range(5):
            self.client.execute_command('TS.ADD', 'source:test', base_ts + i * 10000, i)

        # Query a range with no data
        result = self.client.execute_command('TS.MREVRANGE', 1000, 5000,
                                             'LATEST',
                                             'FILTER', 'status=active')

        assert len(result) == 1
        # Should return empty data
        assert len(result[0][2]) == 0
