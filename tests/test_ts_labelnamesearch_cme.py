from valkey import Valkey, ValkeyCluster
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

TS1 = "ts1:{1}"
TS2 = "ts2:{2}"
TS3 = "ts3:{3}"


class TestTimeSeriesLabelNameSearchCME(ValkeyTimeSeriesClusterTestCase):
    @staticmethod
    def setup_test_data(client: ValkeyCluster):
        client.execute_command("TS.CREATE", TS1, "LABELS", "name", "cpu", "node", "node-1", "region", "us-east")
        client.execute_command("TS.CREATE", TS2, "LABELS", "name", "cpu", "node", "node-2", "role", "db")
        client.execute_command("TS.CREATE", TS3, "LABELS", "name", "mem", "namespace", "default", "region", "us-west")

    def test_labelnamesearch_cluster_fanout(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        result = client.execute_command(
            "TS.LABELNAMESEARCH",
            "SEARCH",
            "reg",
            "FILTER",
            'name=~"(cpu|mem)"',
        )

        assert result == [b"region"]

    def test_labelnamesearch_cluster_limit(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self.setup_test_data(cluster)

        result = client.execute_command(
            "TS.LABELNAMESEARCH",
            "SEARCH",
            "e",
            "LIMIT",
            2,
            "FILTER",
            'name=~"(cpu|mem)"',
        )

        assert result == [b"name", b"namespace"]

    def test_labelnamesearch_cluster_requires_matcher(self):
        client: Valkey = self.new_client_for_primary(0)

        self.verify_error_response(
            client,
            "TS.LABELNAMESEARCH SEARCH node",
            "TS.LABELNAMESEARCH in cluster mode requires at least one matcher",
        )
