"""PromQL identifies a series by its label set, in a cluster too: two keys with the
same labels are refused whether they share a shard or not, with one documented
exception. An aggregation pushed down to the shards sees only per-group partials
from each, so two such keys on *different* shards are aggregated as two series
(see "Series identity" in docs/topics/promql.md).

The pairs have no `__name__`: with one, TS.CREATE refuses the second key on the
node that holds the first, and only shards apart could hold both."""
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from query_result import QueryResult

# Hash tags whose slots land on primaries 0, 1 and 2 of the 3-node test cluster.
TAG_BY_NODE = {0: 'h2', 1: 'h1', 2: 'h0'}

DUPLICATE = "more than one series has the labels"
TIMES = [f"2026-04-06T20:0{m}:00Z" for m in range(5)]
PAIRS = [pytest.param((1, 1), id="same_shard"), pytest.param((1, 2), id="different_shards")]


class TestTsQueryDuplicateLabelsCluster(ValkeyTimeSeriesClusterTestCase):

    def create(self, name: str, node: int, *labels: str):
        key = f"{name}:{{{TAG_BY_NODE[node]}}}"
        client = self.new_client_for_primary(node)
        client.execute_command("TS.CREATE", key, "LABELS", *labels)
        for i, t in enumerate(TIMES):
            client.execute_command("TS.ADD", key, t, 10 * (i + 1))

    def setup_pair(self, nodes):
        """Two keys with identical labels on `nodes`, plus a distinct series."""
        self.create("dup-a", nodes[0], "kind", "dup", "job", "api")
        self.create("dup-b", nodes[1], "kind", "dup", "job", "api")
        self.create("other", 0, "kind", "dup", "job", "web")

    def instant(self, query: str):
        return self.new_client_for_primary(0).execute_command(
            "TS.QUERY", query, "TIME", TIMES[-1]
        )

    def range(self, query: str):
        return self.new_client_for_primary(0).execute_command(
            "TS.QUERYRANGE", query, "STEP", "60s", "START", TIMES[2], "END", TIMES[-1]
        )

    @pytest.mark.parametrize("nodes", PAIRS)
    @pytest.mark.parametrize(
        "kind,query",
        [
            pytest.param("instant", '{kind="dup"}', id="instant_selector"),
            pytest.param("instant", 'max_over_time({kind="dup", job="api"}[3m])', id="instant_rollup"),
            pytest.param("range", '{kind="dup"}', id="range_selector"),
            pytest.param("range", 'rate({kind="dup"}[3m])', id="range_rollup"),
        ],
    )
    def test_series_reads_refuse_two_series_with_the_same_labels(self, nodes, kind, query):
        self.setup_pair(nodes)
        with pytest.raises(ResponseError, match=DUPLICATE):
            getattr(self, kind)(query)

    @pytest.mark.parametrize(
        "kind,query",
        [
            pytest.param("instant", 'sum({kind="dup", job="api"})', id="instant_sum"),
            pytest.param("range", 'sum(rate({kind="dup", job="api"}[3m]))', id="range_sum_of_rate"),
        ],
    )
    def test_a_shard_refuses_its_own_duplicates_under_aggregation_push_down(self, kind, query):
        self.setup_pair((1, 1))
        with pytest.raises(ResponseError, match=DUPLICATE):
            getattr(self, kind)(query)

    def test_pushed_down_aggregation_counts_duplicates_on_different_shards(self):
        """The documented exception: the coordinator sees only each shard's group."""
        self.setup_pair((1, 2))
        result = QueryResult.from_raw(self.instant('sum({kind="dup", job="api"})'))
        assert result.is_vector()
        assert [s.value.value for s in result.result] == [100.0]

        result = QueryResult.from_raw(self.range('sum({kind="dup", job="api"})'))
        assert result.is_matrix()
        assert len(result.result) == 1
