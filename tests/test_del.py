import time

from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestDel(ValkeyTimeSeriesTestCaseBase):
    """Test DEL command."""

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        self.start_ts = timestamp or 1577836800  # Default to 2020-01-01 00:00:00
        if timestamp is None:
            timestamp = self.start_ts

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def test_delete(self):
        """Test that a deleted series is properly removed from the index."""

        key = "ts:delete"
        self.create_ts(key)

        # Delete the key
        self.client.delete(key)

        # The key should be removed
        assert not self.client.exists(key)

        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0

        # Create the key again - should succeed without index interference
        self.create_ts(key, self.start_ts + 1, 2.0)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1