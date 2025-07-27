from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestMove(ValkeyTimeSeriesTestCaseBase):
    """Test cases for server event handling in the TimeSeries module."""

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""

        self.start_ts = timestamp or 1577836800

        if timestamp is None:
            timestamp = self.start_ts

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key


    def test_move_key_between_dbs(self):
        """Test that a key moved between databases is correctly re-indexed."""

        # ensure we are in db 0
        self.client.select(0)

        key = "ts:move"
        self.create_ts(key)

        # Move key to db 1
        assert self.client.move(key, 1) == 1, "Move command should return 1 on success"

        self.client.select(0)

        # Key should not exist in db 0
        assert not self.client.exists(key), "Key should not exist in db 0 after moving"

        # should not be able to find it in db 0 by index
        key_from_index = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(key_from_index) == 0

        key_from_index = self.client.execute_command("TS.QUERYINDEX", "sensor=sensor")
        assert len(key_from_index) == 0

        # Key should exist in db 1 and be queryable
        self.client.select(1)
        assert self.client.exists(key), "Key should exist in db 1 after moving"

        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts

        # Verify the key is indexed correctly in db 1
        keys = self.client.execute_command("TS.QUERYINDEX", f"key={key}")
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == key

        keys = self.client.execute_command("TS.QUERYINDEX", "sensor=temp")
        assert len(keys) == 1
        assert keys[0].decode('utf-8') == key
