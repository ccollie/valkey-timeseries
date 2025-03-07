from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestTimeSeriesCommand(ValkeyTimeSeriesTestCaseBase):

    def verify_command_arity(self, command, expected_arity): 
        command_info = self.client.execute_command('COMMAND', 'INFO', command)
        actual_arity = command_info.get(command).get('arity')
        assert actual_arity == expected_arity, f"Arity mismatch for command '{command}'"

    def test_bloom_command_arity(self):
        self.verify_command_arity('TS.CREATE', -1)
        self.verify_command_arity('TS.ADD', -1)
        self.verify_command_arity('TS.MADD', -1)
        self.verify_command_arity('TS.DEL', -1)
        self.verify_command_arity('TS.CARD', -1)
        self.verify_command_arity('TS.RESERVE', -1)
        self.verify_command_arity('TS.INFO', -1)
        self.verify_command_arity('TS.INSERT', -1)
