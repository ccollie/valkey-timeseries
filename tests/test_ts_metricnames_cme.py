from valkey import Valkey, ValkeyCluster
import pytest

from common import LabelSearchResponse
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

TS1 = "ts1:{1}"
TS2 = "ts2:{2}"
TS3 = "ts3:{3}"


class TestTimeSeriesMetricNamesCME(ValkeyTimeSeriesClusterTestCase):
    @staticmethod
    def setup_test_data(client: ValkeyCluster):
        client.execute_command("TS.CREATE", TS1, "METRIC", 'cpu_usage_total{env="prod"}')
        client.execute_command("TS.CREATE", TS2, "METRIC", 'mem_usage_bytes{env="prod"}')
        client.execute_command("TS.CREATE", TS3, "METRIC", 'cpu_idle_total{env="dev"}')

    def test_metricnames_cluster_fanout(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        result = client.execute_command(
            "TS.METRICNAMES",
            "FILTER",
            'env=~"(prod|dev)"',
        )

        labels = LabelSearchResponse.parse(result)
        names = [item.value for item in labels.results]

        assert sorted(names) == sorted([b"cpu_idle_total", b"cpu_usage_total", b"mem_usage_bytes"])
