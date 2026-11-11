"""PromQL identifies a series by its label set: a query that reads two keys with the
same labels is refused, on every path a single node takes. TS.CREATE already refuses
such a pair on one node when the labels include `__name__`, so the duplicates here
have none. TS.MRANGE, which keeps series apart by key, still returns both."""
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from query_result import QueryResult

DUPLICATE = "more than one series has the labels"


class TestTsQueryDuplicateLabels(ValkeyTimeSeriesTestCaseBase):

    def setup_duplicates(self):
        """Two keys with identical labels, and a third that differs by one label."""
        for key, job in [("dup:a", "api"), ("dup:b", "api"), ("dup:c", "web")]:
            self.client.execute_command("TS.CREATE", key, "LABELS", "kind", "dup", "job", job)
            for t in [1000, 2000, 3000, 4000, 5000]:
                self.client.execute_command("TS.ADD", key, t, t / 1000)

    def range_query(self, query: str):
        return self.client.execute_command(
            "TS.QUERYRANGE", query, "STEP", "1000", "START", 3000, "END", 5000
        )

    @pytest.mark.parametrize(
        "query",
        [
            pytest.param('{kind="dup"}', id="selector"),
            pytest.param('{kind="dup", job="api"}', id="narrowed_selector"),
            pytest.param('sum({kind="dup"})', id="aggregation"),
            pytest.param('{kind="dup"} + 1', id="binary_operation"),
            pytest.param('max_over_time({kind="dup"}[3s])', id="rollup"),
        ],
    )
    def test_instant_query_refuses_two_series_with_the_same_labels(self, query):
        self.setup_duplicates()
        with pytest.raises(ResponseError, match=DUPLICATE):
            self.client.execute_command("TS.QUERY", query, "TIME", "5")

    @pytest.mark.parametrize(
        "query",
        [
            pytest.param('{kind="dup"}', id="selector"),
            pytest.param('rate({kind="dup"}[3s])', id="rollup"),
            pytest.param('sum by (job) (rate({kind="dup"}[3s]))', id="aggregated_rollup"),
            pytest.param('sum({kind="dup"})', id="aggregation"),
        ],
    )
    def test_range_query_refuses_two_series_with_the_same_labels(self, query):
        self.setup_duplicates()
        with pytest.raises(ResponseError, match=DUPLICATE):
            self.range_query(query)

    def test_a_selector_that_matches_only_distinct_series_is_answered(self):
        self.setup_duplicates()
        result = QueryResult.from_raw(
            self.client.execute_command("TS.QUERY", '{kind="dup", job="web"}', "TIME", "5")
        )
        assert result.is_vector()
        assert [s.metric["job"] for s in result.result] == ["web"]

    def test_mrange_still_returns_every_key(self):
        """The rule is PromQL's: TS.MRANGE identifies series by key."""
        self.setup_duplicates()
        reply = self.client.execute_command("TS.MRANGE", "-", "+", "FILTER", "kind=dup", "job=api")
        assert len(reply) == 2

    def test_create_refuses_a_second_key_with_the_same_named_labels(self):
        """With `__name__`, a node never holds the pair a query would have to refuse."""
        self.client.execute_command("TS.CREATE", "named:a", "LABELS", "__name__", "m", "job", "api")
        with pytest.raises(ResponseError, match="duplicate series"):
            self.client.execute_command("TS.CREATE", "named:b", "LABELS", "__name__", "m", "job", "api")
