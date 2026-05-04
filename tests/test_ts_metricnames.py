import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from common import LabelValue, LabelSearchResponse
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMetricNames(ValkeyTimeSeriesTestCaseBase):
    def setup_test_data(self, client):
        client.execute_command(
            "TS.CREATE", "ts:cpu:prod",
            "METRIC", 'cpu_usage_total{env="prod",node="node-1"}',
        )
        client.execute_command(
            "TS.CREATE", "ts:mem:prod",
            "METRIC", 'mem_usage_bytes{env="prod",node="node-1"}',
        )
        client.execute_command(
            "TS.CREATE", "ts:cpu:dev",
            "METRIC", 'cpu_idle_total{env="dev",node="node-2"}',
        )

    def exec_sorted_values(self, name, *args):
        """Helper method to execute a command and return sorted results"""
        result = self.client.execute_command('TS.METRICNAMES', name, *args)
        parsed = LabelValue.parse_response(result)
        return sorted([lv.value for lv in parsed])

    def test_metricnames_with_filter(self):
        self.setup_test_data(self.client)

        names = self.exec_sorted_values("FILTER", "env=prod")

        assert names == [b"cpu_usage_total", b"mem_usage_bytes"]

    def test_metricnames_with_search(self):
        self.setup_test_data(self.client)

        names = self.exec_sorted_values(
            "SEARCH", "idle",
            "FILTER", 'env=~"(prod|dev)"',
        )

        assert names == [b"cpu_idle_total"]

    def test_metricnames_with_include_score(self):
        self.setup_test_data(self.client)

        # The server now exposes metadata when the INCLUDE_METADATA token is provided.
        result = self.client.execute_command(
            "TS.METRICNAMES",
            "SEARCH", "cpu_usage_total",
            "INCLUDE_METADATA",
            "FILTER", 'env=~"(prod|dev)"',
        )

        # Parse using LabelValuesResponse to normalize metadata
        parsed = LabelSearchResponse.parse(result)
        assert len(parsed.results) == 1
        item = parsed.results[0]
        # item is a LabelValue with value, score, cardinality
        assert item.value == b"cpu_usage_total"
        # ensure cardinality present and is an int
        assert item.cardinality is not None and isinstance(item.cardinality, int)
