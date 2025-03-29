from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTimeSeriesBasic(ValkeyTimeSeriesTestCaseBase):
    def test_create_basic(self):
        client = self.server.get_new_client()
        assert client.execute_command("ts.create 1") == "OK"
        assert client.execute_command("ts.create 2 retention 5ms") == "OK"
        assert client.execute_command("ts.create 3 labels Redis Labs") == "OK"
        assert client.execute_command("ts.create 4 retention 20ms labels Time Series") == "OK"
        info = client.execute_command("info 4")

        # assert_resp_response(
        #     client, 20, info.get("retention_msecs"), info.get("retentionTime")
        # )
        assert "Series" == info["labels"]["Time"]

        # Test for a chunk size of 128 Bytes
        assert client.execute_command("create time-serie-1 chunk_size 128") == "OK"
        info = client.execute_command("time-serie-1")
        # todo
        # assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))