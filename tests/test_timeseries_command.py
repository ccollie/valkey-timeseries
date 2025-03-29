import pytest

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestTimeSeriesCommand(ValkeyTimeSeriesTestCaseBase):

    @pytest.fixture(autouse=True)
    def setup(self, resource_port_tracker):
        self.port_tracker = resource_port_tracker
        super().setup(resource_port_tracker)

    def verify_command_arity(self, command, expected_arity): 
        command_info = self.client.execute_command('COMMAND', 'INFO', command)
        actual_arity = command_info.get(command).get('arity')
        assert actual_arity == expected_arity, f"Arity mismatch for command '{command}'"

    def test_command_arity(self):
        self.verify_command_arity('TS.CREATE', -1)
        self.verify_command_arity('TS.ADD', -1)
        self.verify_command_arity('TS.MADD', -1)
        self.verify_command_arity('TS.DEL', -1)
        self.verify_command_arity('TS.CARD', -1)
        self.verify_command_arity('TS.RESERVE', -1)
        self.verify_command_arity('TS.INFO', -1)
        self.verify_command_arity('TS.INSERT', -1)
