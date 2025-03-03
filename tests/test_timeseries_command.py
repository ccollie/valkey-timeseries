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

    def test_bloom_command_error(self):
        # test set up
        assert self.client.execute_command('TS.ADD key item') == 1
        assert self.client.execute_command('TS.RESERVE bf 0.01 1000') == b'OK'

        basic_error_test_cases = [
            # not found
            ('TS.INFO TEST404', 'not found'),
            # incorrect syntax and argument usage
            ('bf.info key item', 'invalid information value'),
            ('bf.insert key CAPACITY 10000 ERROR 0.01 EXPANSION 0.99 NOCREATE NONSCALING ITEMS test1 test2 test3', 'bad expansion'),
            ('TS.INSERT KEY HELLO WORLD', 'unknown argument received'),
            ('TS.INSERT KEY error 2 ITEMS test1', '(0 < error rate range < 1)'),
            ('TS.INSERT KEY ERROR err ITEMS test1', 'bad error rate'),
            ('TS.INSERT KEY TIGHTENING tr ITEMS test1', 'bad tightening ratio'),
            ('TS.INSERT KEY TIGHTENING 2 ITEMS test1', '(0 < tightening ratio range < 1)'),
            ('TS.INSERT TEST_LIMIT ERROR 0.99999999999999999 ITEMS ERROR_RATE', '(0 < error rate range < 1)'),
            ('TS.INSERT TEST_LIMIT TIGHTENING 0.99999999999999999 ITEMS ERROR_RATE', '(0 < tightening ratio range < 1)'),
            ('TS.INSERT TEST_LIMIT CAPACITY 9223372036854775808 ITEMS CAP', 'bad capacity'),
            ('TS.INSERT TEST_LIMIT CAPACITY 0 ITEMS CAP0', '(capacity should be larger than 0)'),
            ('TS.INSERT TEST_LIMIT EXPANSION 4294967299 ITEMS EXPAN', 'bad expansion'),
            ('TS.INSERT TEST_NOCREATE NOCREATE ITEMS A B', 'not found'),
            ('TS.INSERT KEY HELLO', 'unknown argument received'),
            ('TS.INSERT KEY CAPACITY 1 ERROR 0.0000000001 VALIDATESCALETO 10000000 EXPANSION 1', 'provided VALIDATESCALETO causes false positive to degrade to 0'),
            ('TS.INSERT KEY VALIDATESCALETO 1000000000000', 'provided VALIDATESCALETO causes bloom object to exceed memory limit'),
            ('TS.INSERT KEY VALIDATESCALETO 1000000000000 NONSCALING', 'cannot use NONSCALING and VALIDATESCALETO options together'),
            ('TS.RESERVE KEY String 100', 'bad error rate'),
            ('TS.RESERVE KEY 0.99999999999999999 3000', '(0 < error rate range < 1)'),
            ('TS.RESERVE KEY 2 100', '(0 < error rate range < 1)'),
            ('TS.RESERVE KEY 0.01 String', 'bad capacity'),
            ('TS.RESERVE KEY 0.01 0.01', 'bad capacity'),
            ('TS.RESERVE KEY 0.01 -1', 'bad capacity'),
            ('TS.RESERVE KEY 0.01 9223372036854775808', 'bad capacity'),
            ('TS.RESERVE bf 0.01 1000', 'item exists'),
            ('TS.RESERVE TEST_CAP 0.50 0', '(capacity should be larger than 0)'),

            # wrong number of arguments
            ('TS.ADD TEST', 'wrong number of arguments for \'TS.ADD\' command'),
            ('TS.ADD', 'wrong number of arguments for \'TS.ADD\' command'),
            ('TS.ADD HELLO TEST WORLD', 'wrong number of arguments for \'TS.ADD\' command'),
            ('TS.CARD KEY ITEM', 'wrong number of arguments for \'TS.CARD\' command'),
            ('bf.card', 'wrong number of arguments for \'TS.CARD\' command'),
            ('TS.EXISTS', 'wrong number of arguments for \'TS.EXISTS\' command'),
            ('bf.exists item', 'wrong number of arguments for \'TS.EXISTS\' command'),
            ('bf.exists key item hello', 'wrong number of arguments for \'TS.EXISTS\' command'),
            ('TS.INFO', 'wrong number of arguments for \'TS.INFO\' command'),
            ('bf.info key capacity size', 'wrong number of arguments for \'TS.INFO\' command'),
            ('TS.INSERT', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.INSERT MISS_ITEM EXPANSION 2 ITEMS', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.INSERT MISS_VAL ERROR 0.5 EXPANSION', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.INSERT MISS_VAL ERROR 0.5 CAPACITY', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.INSERT MISS_VAL EXPANSION 2 EXPANSION', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.INSERT MISS_VAL EXPANSION 1 error', 'wrong number of arguments for \'TS.INSERT\' command'),
            ('TS.MADD', 'wrong number of arguments for \'TS.MADD\' command'),
            ('TS.MADD KEY', 'wrong number of arguments for \'TS.MADD\' command'),
            ('TS.MEXISTS', 'wrong number of arguments for \'TS.MEXISTS\' command'),
            ('TS.MEXISTS INFO', 'wrong number of arguments for \'TS.MEXISTS\' command'),
            ('TS.RESERVE', 'wrong number of arguments for \'TS.RESERVE\' command'),
            ('TS.RESERVE KEY', 'wrong number of arguments for \'TS.RESERVE\' command'),
            ('TS.RESERVE KEY SSS', 'wrong number of arguments for \'TS.RESERVE\' command'),
            ('TS.RESERVE TT1 0.01 1 NONSCALING test1 test2 test3', 'wrong number of arguments for \'TS.RESERVE\' command'),
            ('TS.RESERVE TT 0.01 1 NONSCALING EXPANSION 1', 'wrong number of arguments for \'TS.RESERVE\' command'),
        ]

        for test_case in basic_error_test_cases:
            cmd = test_case[0]
            expected_err_reply = test_case[1]
            self.verify_error_response(self.client, cmd, expected_err_reply)

    def test_bloom_command_behavior(self):
        basic_behavior_test_case = [
            ('TS.ADD key item', 1),
            ('TS.ADD key item', 0),
            ('TS.EXISTS key item', 1),
            ('TS.MADD key item item2', 2),
            ('TS.EXISTS key item', 1),
            ('TS.EXISTS key item2', 1),
            ('TS.MADD hello world1 world2 world3', 3),
            ('TS.MADD hello world1 world2 world3 world4', 4),
            ('TS.MEXISTS hello world5', 1),
            ('TS.MADD hello world5', 1),
            ('TS.MEXISTS hello world5 world6 world7', 3),
            ('TS.INSERT TEST ITEMS ITEM', 1),
            ('TS.INSERT TEST CAPACITY 1000 ITEMS ITEM', 1),
            ('TS.INSERT TEST CAPACITY 200 error 0.50 ITEMS ITEM ITEM1 ITEM2', 3),
            ('TS.INSERT TEST CAPACITY 300 ERROR 0.50 EXPANSION 1 ITEMS ITEM FOO', 2),
            ('TS.INSERT TEST ERROR 0.50 EXPANSION 3 NOCREATE items BOO', 1), 
            ('TS.INSERT TEST ERROR 0.50 EXPANSION 1 NOCREATE NONSCALING items BOO', 1),
            ('TS.INSERT TEST_EXPANSION EXPANSION 9 ITEMS ITEM', 1),
            ('TS.INSERT TEST_CAPACITY CAPACITY 2000 ITEMS ITEM', 1),
            ('TS.INSERT TEST_ITEMS ITEMS 1 2 3 EXPANSION 2', 5),
            ('TS.INSERT TEST_VAL_SCALE_1 CAPACITY 200 VALIDATESCALETO 1000000 error 0.0001 ITEMS ITEM ITEM1 ITEM2', 3),
            ('TS.INSERT TEST_VAL_SCALE_2 CAPACITY 20000 VALIDATESCALETO 10000000 error 0.5 EXPANSION 4 ITEMS ITEM ITEM1 ITEM2', 3),
            ('TS.INSERT TEST_VAL_SCALE_3 CAPACITY 10400 VALIDATESCALETO 10410 error 0.0011 EXPANSION 1 ITEMS ITEM ITEM1 ITEM2', 3),
            ('TS.INSERT KEY', 0),
            ('TS.INSERT KEY EXPANSION 2', 0),
            ('TS.INFO TEST Capacity', 100),
            ('TS.INFO TEST ITEMS', 5),
            ('TS.INFO TEST filters', 1),
            ('bf.info TEST expansion', 2),
            ('TS.INFO TEST_EXPANSION EXPANSION', 9),
            ('TS.INFO TEST_CAPACITY CAPACITY', 2000),
            ('TS.INFO TEST MAXSCALEDCAPACITY', 26214300),
            ('TS.INFO TEST_VAL_SCALE_1 ERROR', b'0.0001'),
            ('TS.INFO TEST_VAL_SCALE_2 ERROR', b'0.5'),
            ('TS.CARD key', 3),
            ('TS.CARD hello', 5),
            ('TS.CARD TEST', 5),
            ('bf.card HELLO', 0),
            ('TS.RESERVE bf 0.01 1000', b'OK'),
            ('TS.EXISTS bf non_existant', 0),
            ('TS.RESERVE bf_exp 0.01 1000 EXPANSION 2', b'OK'),
            ('TS.RESERVE bf_non 0.01 1000 NONSCALING', b'OK'),
            ('bf.info bf_exp expansion', 2),
            ('TS.INFO bf_non expansion', None),
        ]

        for test_case in basic_behavior_test_case:
            cmd = test_case[0]
            # For non multi commands, this is the verbatim expected result. 
            # For multi commands, test_case[1] contains the number of item add/exists results which are expected to be 0 or 1.
            expected_result = test_case[1]
            # For Cardinality commands we want to add items till we are at the number of items we expect then check Cardinality worked
            if cmd.upper().startswith("TS.CARD"):
                self.add_items_till_capacity(self.client, cmd.split()[-1], expected_result, 1, "item_prefix")
            # For multi commands expected result is actually the length of the expected return. While for other commands this we have the literal
            # expected result
            self.verify_command_success_reply(self.client, cmd, expected_result)

        # test bf.info
        assert self.client.execute_command('TS.RESERVE TS_INFO 0.50 2000 NONSCALING') == b'OK'
        bf_info = self.client.execute_command('TS.INFO TS_INFO')
        capacity_index = bf_info.index(b'Capacity') + 1
        filter_index = bf_info.index(b'Number of filters') + 1
        item_index = bf_info.index(b'Number of items inserted') + 1
        expansion_index = bf_info.index(b'Expansion rate') + 1
        assert bf_info[capacity_index] == self.client.execute_command('TS.INFO TS_INFO CAPACITY') == 2000
        assert bf_info[filter_index] == self.client.execute_command('TS.INFO TS_INFO FILTERS') == 1
        assert bf_info[item_index] == self.client.execute_command('TS.INFO TS_INFO ITEMS') == 0
        assert bf_info[expansion_index] == self.client.execute_command('TS.INFO TS_INFO EXPANSION') == None
