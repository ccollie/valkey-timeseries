"""TS.QUERY over classic histogram buckets, on a real server."""
from __future__ import annotations

import math

import pytest
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import QueryResult

EXPERIMENTAL = "ts-promql-enable-experimental-functions"
# A realistic millisecond timestamp: TIME infers its unit from the magnitude.
AT = 1_700_000_000_000


class TestTsQueryHistogram(ValkeyTimeSeriesTestCaseBase):
    def setup_buckets(self):
        # Midpoints 0.5, 1.5, 3 with weights 2, 4, 2.
        for le, count in [("1", 2), ("2", 6), ("4", 8), ("+Inf", 8)]:
            key = f"lat:{le}"
            self.client.execute_command(
                "TS.CREATE", key, "LABELS", "__name__", "lat_bucket", "job", "a", "le", le
            )
            self.client.execute_command("TS.ADD", key, AT, count)

    def query(self, promql):
        raw = self.client.execute_command("TS.QUERY", promql, "TIME", AT)
        return QueryResult.from_raw(raw).result

    def test_statistics(self):
        self.setup_buckets()
        for promql, expected in [
            ("histogram_avg(lat_bucket)", 1.625),
            ("histogram_stdvar(lat_bucket)", 0.796875),
            ("histogram_stddev(lat_bucket)", math.sqrt(0.796875)),
        ]:
            result = self.query(promql)
            assert len(result) == 1, promql
            assert result[0].value.value == pytest.approx(expected), promql
            assert result[0].metric == {"job": "a"}, promql

    def test_histogram_quantiles_is_experimental(self):
        self.setup_buckets()
        promql = 'histogram_quantiles(lat_bucket, "q", 0, 0.5, 1)'
        with pytest.raises(ResponseError, match="not enabled"):
            self.query(promql)
        self.client.execute_command("CONFIG", "SET", f"ts.{EXPERIMENTAL}", "yes")
        try:
            result = {s.metric["q"]: s.value.value for s in self.query(promql)}
            assert result == pytest.approx({"0.0": 0.0, "0.5": 1.5, "1.0": 4.0})
            for labels in (s.metric for s in self.query(promql)):
                assert "le" not in labels and "__name__" not in labels
            with pytest.raises(ResponseError):
                self.query('histogram_quantiles(lat_bucket, "q", 0.5, 0.5)')
        finally:
            self.client.execute_command("CONFIG", "SET", f"ts.{EXPERIMENTAL}", "no")

    def test_a_filter_on_le_does_not_narrow_the_buckets(self):
        self.setup_buckets()
        self.client.execute_command(
            "TS.CREATE", "other", "LABELS", "__name__", "other", "job", "a", "le", "1"
        )
        self.client.execute_command("TS.ADD", "other", AT, 0)
        # Narrowed to le="1", the mean would be 0.5.
        result = self.query("histogram_avg(lat_bucket) + on(job) other")
        assert len(result) == 1
        assert result[0].value.value == pytest.approx(1.625)
