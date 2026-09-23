"""ACL checks on the STORE destination of the analysis commands.

The destination is a key the command writes, so a user needs write access to it as well as read
access to the source. The check must use the calling user even when the write happens on the
analysis pool, where the worker's context has no user of its own.
"""
import pytest
from valkey import ResponseError

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

PASSWORD = "password123"

STORE_COMMANDS = [
    ["TS.FORECAST", "src", "-", "+", "MODELS", "SES", "HORIZON", 3],
    ["TS.AUTOFORECAST", "src", "-", "+", "HORIZON", 3, "MODELS", "ETS"],
    ["TS.TREND", "src", "-", "+"],
    ["TS.FILLGAPS", "src", "-", "+", "FREQUENCY", 500],
    ["TS.SANITIZE", "src", "-", "+"],
]


def command_id(argv):
    return argv[0]


class TestStoreDestinationAcls(ValkeyTimeSeriesTestCaseBase):

    def make_source(self, client=None):
        client = client or self.client
        client.execute_command("TS.CREATE", "src")
        args = []
        for i in range(60):
            args += ["src", (i + 1) * 1000, 10.0 + (i % 7) + i * 0.1]
        client.execute_command("TS.MADD", *args)

    def user(self, name, *rules):
        self.client.execute_command("ACL", "SETUSER", name, "ON", f">{PASSWORD}", *rules)
        client = self.server.get_new_client()
        client.execute_command("AUTH", name, PASSWORD)
        return client

    @pytest.mark.parametrize("argv", STORE_COMMANDS, ids=command_id)
    def test_destination_outside_the_users_keys_is_refused(self, argv):
        self.make_source()
        client = self.user("src_only", "+@all", "~src*")
        with pytest.raises(ResponseError, match="NOPERM|permission"):
            client.execute_command(*argv, "STORE", "other")
        assert self.client.execute_command("EXISTS", "other") == 0

    @pytest.mark.parametrize("argv", STORE_COMMANDS, ids=command_id)
    def test_read_only_destination_is_refused(self, argv):
        self.make_source()
        client = self.user("reader", "+@all", "~src*", "%R~out*")
        with pytest.raises(ResponseError, match="NOPERM|permission"):
            client.execute_command(*argv, "STORE", "out")
        assert self.client.execute_command("EXISTS", "out") == 0

    @pytest.mark.parametrize("argv", STORE_COMMANDS, ids=command_id)
    def test_writable_destination_is_written(self, argv):
        self.make_source()
        client = self.user("writer", "+@all", "~src*", "~out*")
        client.execute_command(*argv, "STORE", "out")
        assert self.client.execute_command("EXISTS", "out") == 1

    def test_background_store_checks_the_calling_user(self):
        """The pool worker must not fall back to another user's permissions.

        With the `default` user stripped of all keys, a background STORE by a user who owns the
        keys must still succeed: the destination is checked as the caller, on the main thread.
        """
        admin = self.user("admin", "+@all", "~*")
        self.make_source(admin)
        admin.execute_command("ACL", "SETUSER", "default", "resetkeys")
        try:
            written = admin.execute_command(
                "TS.FORECAST", "src", "-", "+", "MODELS", "SES", "HORIZON", 4, "STORE", "out"
            )
            assert written == 4
            assert admin.execute_command("EXISTS", "out") == 1
        finally:
            admin.execute_command("ACL", "SETUSER", "default", "~*")
