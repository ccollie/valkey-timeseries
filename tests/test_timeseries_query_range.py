import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesQueryRange(ValkeyTimeSeriesTestCaseBase):
    def setup_data(self):
        # Setup some time series data
        self.client.execute_command('TS.CREATE', 'ts1', 'LABELS', '__name__', 'temperature', 'sensor', 'temp', 'area', 'living_room')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.1)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 20.2)
        self.client.execute_command('TS.ADD', 'ts1', 3000, 30.3)
        self.client.execute_command('TS.ADD', 'ts1', 4000, 40.4)
        self.client.execute_command('TS.ADD', 'ts1', 5000, 50.5)

        self.client.execute_command('TS.CREATE', 'ts2', 'LABELS', '__name__', 'temperature', 'sensor', 'temp', 'area', 'den')
        self.client.execute_command('TS.ADD', 'ts2', 1000, 15.5)
        self.client.execute_command('TS.ADD', 'ts2', 2000, 25.5)
        self.client.execute_command('TS.ADD', 'ts2', 3000, 35.5)
        self.client.execute_command('TS.ADD', 'ts2', 4000, 45.5)
        self.client.execute_command('TS.ADD', 'ts2', 5000, 55.5)

    def test_query_basic(self):
        """Test basic TS.QUERY"""

        self.client.execute_command('TS.CREATE', 'foo_bar', 'LABELS', '__name__', 'foo_bar')
        # foo_bar 1.00 1652169600000 # 2022-05-10T08:00:00Z
        # foo_bar 2.00 1652169660000 # 2022-05-10T08:01:00Z
        # foo_bar 3.00 1652169720000 # 2022-05-10T08:02:00Z
        # foo_bar 5.00 1652169840000 # 2022-05-10T08:04:00Z, one point missed
        # foo_bar 5.50 1652169960000 # 2022-05-10T08:06:00Z, one point missed
        # foo_bar 5.50 1652170020000 # 2022-05-10T08:07:00Z
        # foo_bar 4.00 1652170080000 # 2022-05-10T08:08:00Z
        # foo_bar 3.50 1652170260000 # 2022-05-10T08:11:00Z, two points missed
        # foo_bar 3.25 1652170320000 # 2022-05-10T08:12:00Z
        # foo_bar 3.00 1652170380000 # 2022-05-10T08:13:00Z
        # foo_bar 2.00 1652170440000 # 2022-05-10T08:14:00Z
        # foo_bar 1.00 1652170500000 # 2022-05-10T08:15:00Z
        # foo_bar 4.00 1652170560000 # 2022-05-10T08:16:00Z
        self.client.execute_command('TS.ADD', 'foo_bar', 1652169600000, 1.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652169660000, 2.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652169720000, 3.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652169840000, 5.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652169960000, 5.50)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170020000, 5.50)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170080000, 4.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170260000, 3.50)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170320000, 3.25)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170380000, 3.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170440000, 2.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170500000, 1.00)
        self.client.execute_command('TS.ADD', 'foo_bar', 1652170560000, 4.00)

        result = self.client.execute_command('TS.QUERY_RANGE', 'foo_bar', '2022-05-10T07:59:00.000Z', '2022-05-10T08:17:00.000Z', 'STEP', '1m')
        assert result == [[2000, b'20.2'], [3000, b'30.3'], [4000, b'40.4']]