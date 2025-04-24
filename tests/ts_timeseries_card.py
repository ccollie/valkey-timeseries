import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsCard(ValkeyTimeSeriesTestCaseBase):
    def setup_test(self):
        # Setup some time series data
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'sensor', 'temp', 'area', 'A')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 25)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 26)

        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'sensor', 'temp', 'area', 'B')
        self.client.execute_command('TS.ADD', 'ts2', 1500, 30)
        self.client.execute_command('TS.ADD', 'ts2', 2500, 31)

        self.client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'sensor', 'humidity', 'area', 'A')
        self.client.execute_command('TS.ADD', 'ts3', 1200, 60)
        self.client.execute_command('TS.ADD', 'ts3', 2200, 65)

        self.client.execute_command('TS.CREATE', 'ts4_nodata', 'LABELS', 'sensor', 'pressure', 'area', 'C')
        # ts4 has no data points

    def test_card_no_filters(self):
        """Test TS.CARD with no filters - should return total count"""
        result = self.client.execute_command('TS.CARD')
        assert result == 4 # ts1, ts2, ts3, ts4_nodata

    def test_card_single_filter(self):
        """Test TS.CARD with a single label filter"""
        # Filter by sensor=temp
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp')
        assert result == 2 # ts1, ts2

        # Filter by area=A
        result = self.client.execute_command('TS.CARD', 'FILTER', 'area=A')
        assert result == 2 # ts1, ts3

        # Filter by non-existent label value
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=light')
        assert result == 0

    def test_card_multiple_filters(self):
        """Test TS.CARD with multiple label filters"""
        # Filter by sensor=temp AND area=A
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=A')
        assert result == 1 # ts1

        # Filter by sensor=temp AND area=C (ts4 has no data, but should still match labels)
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=pressure', 'area=C')
        assert result == 1 # ts4_nodata

        # Filter with mismatch
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp', 'area=C')
        assert result == 0

    def test_card_filter_with_date_range(self):
        """Test TS.CARD with filters and a date range"""
        # Filter sensor=temp, range includes data for both ts1 and ts2
        result = self.client.execute_command('TS.CARD', 'START', 1000, 'END', 2500, 'FILTER', 'sensor=temp')
        assert result == 2 # ts1 and ts2 have data in this range

        # Filter sensor=temp, range includes only ts1's first point
        result = self.client.execute_command('TS.CARD', 'START', 500, 'END', 1100, 'FILTER', 'sensor=temp')
        assert result == 1 # Only ts1 has data in this range

        # Filter sensor=temp, range includes only ts2's second point
        result = self.client.execute_command('TS.CARD', 'START', 2400, 'END', 2600, 'FILTER', 'sensor=temp')
        assert result == 1 # Only ts2 has data in this range

        # Filter area=A, range includes data for ts1 and ts3
        result = self.client.execute_command('TS.CARD', 'START', 1000, 'END', 2200, 'FILTER', 'area=A')
        assert result == 2 # ts1 and ts3 have data in this range

        # Filter area=A, range with no data for matching series
        result = self.client.execute_command('TS.CARD', 'START', 3000, 'END', 4000, 'FILTER', 'area=A')
        assert result == 0

        # Filter matching ts4_nodata, but with a date range (should return 0 as no data)
        result = self.client.execute_command('TS.CARD', 'START', 0, 'END', 3000, 'FILTER', 'sensor=pressure')
        assert result == 0

    def test_card_date_range_no_filter_error(self):
        """Test TS.CARD with only date range (should error)"""
        # Based on the Rust code, START/END without FILTER is an error
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.CARD', 'START', 1000, 'END', 2000)
        # The error message might vary slightly, check for key parts
        assert "requires at least one matcher" in str(excinfo.value).lower() or \
               "wrong number of arguments" in str(excinfo.value).lower() # Depending on exact parsing

    def test_card_empty_database(self):
        """Test TS.CARD on an empty database"""
        # Flush the database first
        self.client.flushdb()
        result = self.client.execute_command('TS.CARD')
        assert result == 0

        # With filter on empty db
        result = self.client.execute_command('TS.CARD', 'FILTER', 'sensor=temp')
        assert result == 0

    def test_card_invalid_syntax(self):
        """Test TS.CARD with invalid syntax"""
        # Invalid filter format
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'FILTER', 'invalid-filter')

        # Invalid date range
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'START', 'not-a-time', 'END', 2000, 'FILTER', 'sensor=temp')
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'START', 1000, 'END', 'not-a-time', 'FILTER', 'sensor=temp')

        # Wrong number of args for filter
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.CARD', 'FILTER')