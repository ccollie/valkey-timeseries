from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesMetricNames(ValkeyTimeSeriesTestCaseBase):
    def setup_test_data(self, client):
        client.execute_command(
            "TS.CREATE",
            "ts:cpu:prod",
            "METRIC",
            "cpu_usage_total",
            "LABELS",
            "env",
            "prod",
            "node",
            "node-1",
        )
        client.execute_command(
            "TS.CREATE",
            "ts:mem:prod",
            "METRIC",
            "mem_usage_bytes",
            "LABELS",
            "env",
            "prod",
            "node",
            "node-1",
        )
        client.execute_command(
            "TS.CREATE",
            "ts:cpu:dev",
            "METRIC",
            "cpu_idle_total",
            "LABELS",
            "env",
            "dev",
            "node",
            "node-2",
        )

    def test_metricnames_with_filter(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command("TS.METRICNAMES", "FILTER", "env=prod")
        assert result == [b"cpu_usage_total", b"mem_usage_bytes"]

    def test_metricnames_with_search(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command(
            "TS.METRICNAMES",
            "SEARCH",
            "idle",
            "FILTER",
            'env=~"(prod|dev)"',
        )
        assert result == [b"cpu_idle_total"]

    def test_metricnames_with_include_score(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command(
            "TS.METRICNAMES",
            "SEARCH",
            "cpu_usage_total",
            "INCLUDE_SCORE",
            "true",
            "FILTER",
            'env=~"(prod|dev)"',
        )

        assert len(result) == 1
        assert result[0][0] == b"cpu_usage_total"
