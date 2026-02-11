import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMdel(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'name', 'memory', 'type', 'usage', 'node', 'node2')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'name', 'cpu', 'type', 'temperature', 'node', 'node1')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'name', 'cpu', 'node', 'node3')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'name', 'disk', 'type', 'usage', 'node', 'node3')

    def _create_series_with_data(self, key: str, labels=None, start_ts=1, count=20, value_base=1):
        args = ['TS.CREATE', key]
        if labels:
            args += ['LABELS']
            for k, v in labels.items():
                args += [k, v]
        self.client.execute_command(*args)

        for i in range(count):
            ts = start_ts + i
            val = value_base + i
            self.client.execute_command('TS.ADD', key, ts, val)

    def test_basic_mdel_without_range(self):
        """Test basic TS.MDEL functionality with a simple filter"""
        self.setup_test_data(self.client)

        # Delete all CPU metrics
        deleted_count = self.client.execute_command('TS.MDEL', 'FILTER', 'name=cpu')
        assert deleted_count == 4

        # Verify deletion by attempting to get the deleted series
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.GET', 'ts1')
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.GET', 'ts2')
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.GET', 'ts5')
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.GET', 'ts6')

        # Verify that non-deleted series still exist
        result = self.client.execute_command('TS.GET', 'ts3')
        assert result is not None
        result = self.client.execute_command('TS.GET', 'ts4')
        assert result is not None
        result = self.client.execute_command('TS.GET', 'ts7')
        assert result is not None

    def test_mdel_with_timerange(self):
        self.setup_test_data(self.client)

        # Add a sample to all created series so they are non-empty and comparable.
        for k in ['ts1', 'ts2', 'ts3', 'ts4', 'ts5', 'ts6', 'ts7']:
            self.client.execute_command('TS.ADD', k, 100, 1)

        # Delete CPU metrics in the time range 50 to 150. Since each series has only one sample at timestamp 100,
        # we should expect that 4 samples are deleted.
        deleted_count = self.client.execute_command('TS.MDEL', 50, 150, 'FILTER', 'name=cpu')
        assert deleted_count == 4

    def test_compaction_rule_updates_after_range_delete(self):
        # Source series with SUM compaction into dest series.
        self.client.execute_command('TS.CREATE', 'src', 'LABELS', 'name', 'cpu', 'type', 'usage', 'node', 'node1')
        self.client.execute_command('TS.CREATE', 'dst')
        self.client.execute_command('TS.CREATERULE', 'src', 'dst', 'AGGREGATION', 'sum', 10)

        for i in range(40):
            ts = 1 + i
            val = 1 + i
            self.client.execute_command('TS.ADD', 'src', ts, val)

        self._create_series_with_data(
            'other',
            labels={'name': 'battery', 'type': 'usage', 'node': 'node1'},
            start_ts=1,
            count=40,
            value_base=1,
        )

        # Baseline: 40 samples @ 1..40 should produce 4 buckets (1-10, 11-20, 21-30, 31-40).
        baseline = self.client.execute_command('TS.RANGE', 'dst', '-', '+')
        assert len(baseline) == 4

        # Delete the source series via TS.MDEL using a time range and label filter.
        deleted_count = self.client.execute_command('TS.MDEL', 1, 40, 'FILTER', 'name=cpu', 'type=usage', 'node=node1')
        assert deleted_count == 40

        data = self.client.execute_command('TS.RANGE', 'dst', 1, 40)
        assert len(data) == 0
