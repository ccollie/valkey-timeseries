from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesLabelNameSearch(ValkeyTimeSeriesTestCaseBase):
    def setup_test_data(self, client):
        client.execute_command(
            "TS.CREATE",
            "cpu:1",
            "LABELS",
            "name",
            "cpu",
            "node",
            "node-1",
            "region",
            "us-east",
        )
        client.execute_command(
            "TS.CREATE",
            "cpu:2",
            "LABELS",
            "name",
            "cpu",
            "node",
            "node-2",
            "role",
            "db",
        )

    def test_labelnamesearch_substring(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command(
            "TS.LABELNAMESEARCH",
            "SEARCH",
            "nod",
            "FILTER",
            'name="cpu"',
        )

        assert result == [b"node"]

    def test_labelnamesearch_sort_desc(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command(
            "TS.LABELNAMESEARCH",
            "SEARCH",
            "e",
            "SORT_BY",
            "alpha",
            "SORT_DIR",
            "dsc",
            "FILTER",
            'name="cpu"',
        )

        assert result == [b"role", b"region", b"node", b"name"]

    def test_labelnamesearch_limit(self):
        self.setup_test_data(self.client)

        result = self.client.execute_command(
            "TS.LABELNAMESEARCH",
            "SEARCH",
            "e",
            "LIMIT",
            2,
            "FILTER",
            'name="cpu"',
        )

        assert result == [b"name", b"node"]

    def test_labelnamesearch_invalid_threshold(self):
        self.setup_test_data(self.client)

        self.verify_error_response(
            self.client,
            "TS.LABELNAMESEARCH FUZZ_THRESHOLD 101 FILTER name=cpu",
            "TSDB: FUZZ_THRESHOLD must be in [0..100]",
        )
