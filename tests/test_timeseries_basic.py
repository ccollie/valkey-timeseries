import time
from valkeytests.util.waiters import *
from valkey import ResponseError
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestTimeSeriesBasic(ValkeyTimeSeriesTestCaseBase):

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-bloom module is loaded.
        module_list_data = client.execute_command('MODULE LIST')
        module_list_count = len(module_list_data)
        assert module_list_count == 1
        module_loaded = False
        for module in module_list_data:
            if module[b'name'] == b'ts':
                module_loaded = True
                break
        assert(module_loaded)
        # Validate that all the BF.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        ts_cmds = ["TS.CREATE", "TS.ALTER", "TS.ADD", "TS.MADD", "TS.DEL", "TS.GET", "TS.MGET", "TS.RANGE", "TS.MRANGE", "TS.CARD", "TS.QUERYINDEX", "TS.STATS", "TS.LABELNAMES", "TS.LABELVALUES", "TS.INFO"]
        assert all(item in command_cmd_result for item in ts_cmds)
        # Basic bloom filter create, item add and item exists validation.
        bf_add_result = client.execute_command('TS.ADD series1 1000 102')
        assert bf_add_result == 1
        bf_exists_result = client.execute_command('TS.GET series1 1000')
        assert bf_exists_result == 1

    def test_timeseries_create_cmd(self):
        client = self.server.get_new_client()
        # cmd create
        assert client.execute_command('TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32') == 1
        assert client.execute_command('TS.ADD temperature:2:32 * 1000') == 1
        
    def test_copy_and_exists_cmd(self):
        client = self.server.get_new_client()
        madd_result = client.execute_command('BF.MADD series item1 item2 item3 item4')
        assert client.execute_command('EXISTS series') == 1
        mexists_result = client.execute_command('BF.MEXISTS series item1 item2 item3 item4')
        assert len(madd_result) == 4 and len(mexists_result) == 4
        # cmd debug digest
        server_digest = client.debug_digest()
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = client.execute_command('DEBUG DIGEST-VALUE filter')
        assert client.execute_command('COPY series new_filter') == 1
        copied_server_digest = client.debug_digest()
        assert copied_server_digest != None or 0000000000000000000000000000000000000000
        copied_object_digest = client.execute_command('DEBUG DIGEST-VALUE series')
        assert client.execute_command('EXISTS new_filter') == 1
        copy_mexists_result = client.execute_command('BF.MEXISTS new_filter item1 item2 item3 item4')
        assert mexists_result == copy_mexists_result
        assert server_digest != copied_server_digest
        assert copied_object_digest == object_digest
    
    def test_memory_usage_cmd(self):
        client = self.server.get_new_client()
        assert client.execute_command('BF.ADD series * 1000') == 1
        memory_usage = client.execute_command('MEMORY USAGE series')
        info_size = client.execute_command('TS.INFO series SIZE')
        assert memory_usage >= info_size and info_size > 0

    def test_module_data_type(self):
        # Validate the name of the Module data type.
        client = self.server.get_new_client()
        assert client.execute_command('TS.ADD series * 2000') == 1
        type_result = client.execute_command('TYPE series')
        assert type_result == b"vktseries"
        # Validate the name of the Module data type.
        encoding_result = client.execute_command('OBJECT ENCODING series')
        assert encoding_result == b"raw"

    def test_timeseries_obj_access(self):
        client = self.server.get_new_client()
        # check timeseries with basic valkey command
        # cmd touch
        assert client.execute_command('TS.ADD key1 val1') == 1
        assert client.execute_command('TS.ADD key2 val2') == 1
        assert client.execute_command('TOUCH key1 key2') == 2
        assert client.execute_command('TOUCH key3') == 0
        self.verify_server_key_count(client, 2)
        assert client.execute_command('DBSIZE') == 2
        random_key = client.execute_command('RANDOMKEY')
        assert random_key == b"key1" or random_key == b"key2"

    def test_timeseries_lua(self):
        client = self.server.get_new_client()
        # lua
        load_filter = """
        redis.call('TS.ADD', 'LUA1', 'ITEM1');
        redis.call('TS.ADD', 'LUA2', 'ITEM2');
        redis.call('TS.MADD', 'LUA2', 'ITEM3', 'ITEM4', 'ITEM5');
        """
        client.eval(load_filter, 0)
        assert client.execute_command('BF.MEXISTS LUA2 ITEM1 ITEM3 ITEM4') == [0, 1, 1]
        self.verify_server_key_count(client, 2)

    def test_timeseries_deletes(self):
        client = self.server.get_new_client()
        # delete
        assert client.execute_command('TS.ADD series1 item1') == 1
        self.verify_timeseries_filter_item_existence(client, 'series1', 'item1')
        self.verify_server_key_count(client, 1)
        assert client.execute_command('DEL series1') == 1
        self.verify_timeseries_filter_item_existence(client, 'series1', 'item1', should_exist=False)
        self.verify_server_key_count(client, 0)

        # flush
        self.create_timeseries_filters_and_add_items(client, number_of_bf=10)
        self.verify_server_key_count(client, 10)
        assert client.execute_command('FLUSHALL')
        self.verify_server_key_count(client, 0)

        # unlink
        assert client.execute_command('BF.ADD A ITEMA') == 1
        assert client.execute_command('BF.ADD B ITEMB') == 1
        self.verify_timeseries_filter_item_existence(client, 'A', 'ITEMA')
        self.verify_timeseries_filter_item_existence(client, 'B', 'ITEMB')
        self.verify_timeseries_filter_item_existence(client, 'C', 'ITEMC', should_exist=False)
        self.verify_server_key_count(client, 2)
        assert client.execute_command('UNLINK A B C') == 2
        assert client.execute_command('BF.MEXISTS A ITEMA ITEMB') == [0, 0]
        self.verify_timeseries_filter_item_existence(client, 'A', 'ITEMA', should_exist=False)
        self.verify_timeseries_filter_item_existence(client, 'B', 'ITEMB', should_exist=False)
        self.verify_server_key_count(client, 0)

    def test_timeseries_expiration(self):
        client = self.server.get_new_client()
        # expiration
        # cmd object idletime
        self.verify_server_key_count(client, 0)
        assert client.execute_command('BF.ADD TEST_IDLE val3') == 1
        self.verify_timeseries_filter_item_existence(client, 'TEST_IDLE', 'val3')
        self.verify_server_key_count(client, 1)
        time.sleep(1)
        assert client.execute_command('OBJECT IDLETIME test_idle') == None
        assert client.execute_command('OBJECT IDLETIME TEST_IDLE') > 0
        # cmd ttl, expireat
        assert client.execute_command('BF.ADD TEST_EXP ITEM') == 1
        assert client.execute_command('TTL TEST_EXP') == -1
        self.verify_timeseries_filter_item_existence(client, 'TEST_EXP', 'ITEM')
        self.verify_server_key_count(client, 2)
        curr_time = int(time.time())
        assert client.execute_command(f'EXPIREAT TEST_EXP {curr_time + 5}') == 1
        wait_for_equal(lambda: client.execute_command('BF.EXISTS TEST_EXP ITEM'), 0)
        self.verify_server_key_count(client, 1)
        # cmd persist
        assert client.execute_command('BF.ADD TEST_PERSIST ITEM') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1
        self.verify_timeseries_filter_item_existence(client, 'TEST_PERSIST', 'ITEM')
        self.verify_server_key_count(client, 2)
        assert client.execute_command(f'EXPIREAT TEST_PERSIST {curr_time + 100000}') == 1
        assert client.execute_command('TTL TEST_PERSIST') > 0
        assert client.execute_command('PERSIST TEST_PERSIST') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1

    def test_debug_cmd(self):
        client = self.server.get_new_client()
        default_obj = client.execute_command('BF.RESERVE default_obj 0.001 1000')
        default_object_digest = client.execute_command('DEBUG DIGEST-VALUE default_obj')

        # scenario1 validates that digest differs on bloom objects (with same properties) when different items are added.
        scenario1_obj = client.execute_command('BF.INSERT scenario1 error 0.001 capacity 1000 items 1')
        scenario1_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario1')
        assert scenario1_obj != default_obj
        assert scenario1_object_digest != default_object_digest
        
        # scenario2 validates that digest differs on bloom objects with different false positive rate.
        scenario2_obj = client.execute_command('BF.INSERT scenario2 error 0.002 capacity 1000 items 1')
        scenario2_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario2')
        assert scenario2_obj != default_obj
        assert scenario2_object_digest != default_object_digest

        # scenario3 validates that digest differs on bloom objects with different expansion.
        scenario3_obj = client.execute_command('BF.INSERT scenario3 error 0.002 capacity 1000 expansion 3 items 1')
        scenario3_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario3')
        assert scenario3_obj != default_obj
        assert scenario3_object_digest != default_object_digest


        # scenario4 validates that digest differs on bloom objects with different capacity.
        scenario4_obj = client.execute_command('BF.INSERT scenario4 error 0.001 capacity 2000 items 1')
        scenario4_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario4')
        assert scenario4_obj != default_obj
        assert scenario4_object_digest != default_object_digest

        # scenario5 validates that digest is equal on bloom objects with same properties and same items only when we are
        # using a fixed seed. Not when we are using a random seed.
        is_random_seed = client.execute_command('CONFIG GET bf.bloom-use-random-seed')
        scenario5_obj = client.execute_command('BF.INSERT scenario5 error 0.001 capacity 1000 items 1')
        scenario5_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario5')
        assert scenario5_obj != default_obj
        assert scenario5_object_digest != default_object_digest

        # Add the same items to both the original and the new bloom object.
        client.execute_command('BF.MADD default_obj 1 2 3')
        client.execute_command('BF.MADD scenario5 2 3')
        madd_default_object_digest = client.execute_command('DEBUG DIGEST-VALUE default_obj')
        madd_scenario_object_digest = client.execute_command('DEBUG DIGEST-VALUE scenario5')
        if is_random_seed[1] == b'yes':
            assert madd_scenario_object_digest != madd_default_object_digest
        else:
            madd_scenario_object_digest == madd_default_object_digest

        # scenario 6 validates that digest differs on bloom objects after changing the tightening_ratio config
        client.execute_command('BF.RESERVE tightening_ratio 0.001 1000')
        assert self.client.execute_command('CONFIG SET bf.bloom-tightening-ratio 0.75') == b'OK'
        client.execute_command('BF.RESERVE tightening_ratio2 0.001 1000')
        scenario_tightening_ratio_object_digest = client.execute_command('DEBUG DIGEST-VALUE tightening_ratio')
        scenario_tightening_ratio2_digest = client.execute_command('DEBUG DIGEST-VALUE tightening_ratio2')
        assert scenario_tightening_ratio_object_digest != scenario_tightening_ratio2_digest

        # scenario 7 validates that digest differs on bloom objects after changing the fp_rate config
        client.execute_command('BF.INSERT fp_rate capacity 1000 items 1')
        assert self.client.execute_command('CONFIG SET bf.bloom-fp-rate 0.5') == b'OK'
        client.execute_command('BF.INSERT fp_rate2 capacity 1000 items 1')
        fp_rate_object_digest = client.execute_command('DEBUG DIGEST-VALUE fp_rate')
        scenario_fp_rate2_digest = client.execute_command('DEBUG DIGEST-VALUE fp_rate2')
        assert fp_rate_object_digest != scenario_fp_rate2_digest

    def test_timeseries_wrong_type(self):
        # List of all bloom commands
        bloom_commands = [
            'BF.ADD key item',
            'BF.EXISTS key item',
            'BF.MADD key item1 item2 item3',
            'BF.MEXISTS key item2 item3 item4',
            'BF.INSERT key ITEMS item',
            'BF.INFO key filters',
            'BF.CARD key',
            'BF.RESERVE key 0.01 1000',
        ]
        client = self.server.get_new_client()
        # Set the key we try to perform bloom commands on
        client.execute_command("set key value")
        # Run each command and check we get the correct error returned
        for cmd in bloom_commands:
            cmd_name = cmd.split()[0]
            try:
                result = client.execute_command(cmd)
                assert False, f"{cmd_name} on existing non bloom object should fail, instead: {result}"
            except Exception as e:
                
                assert str(e) == f"WRONGTYPE Operation against a key holding the wrong kind of value"
    
    def test_timeseries_string_config_set(self):
        """
        This is a test that validates the bloom string configuration set logic.
        """     
        assert self.client.execute_command('CONFIG SET bf.bloom-fp-rate 0.1') == b'OK'
        assert self.client.execute_command('CONFIG SET bf.bloom-tightening-ratio 0.75') == b'OK'
        
        assert self.client.execute_command('CONFIG GET bf.bloom-fp-rate')[1] == b'0.1'
        assert self.client.execute_command('CONFIG GET bf.bloom-tightening-ratio')[1] == b'0.75'
        try:
            assert self.client.execute_command('CONFIG SET bf.bloom-fp-rate 1.1') == b'ERR (0 < error rate range < 1)'
        except ResponseError as e:
            assert str(e) == f"CONFIG SET failed (possibly related to argument 'bf.bloom-fp-rate') - ERR (0 < error rate range < 1)"
        try:
            assert self.client.execute_command('CONFIG SET bf.bloom-tightening-ratio 1.75') == b'ERR (0 < tightening ratio range < 1)'
        except ResponseError as e:
            assert str(e) == f"CONFIG SET failed (possibly related to argument 'bf.bloom-tightening-ratio') - ERR (0 < tightening ratio range < 1)"

    def test_timeseries_dump_and_restore(self):
        """
        This is a test that validates the bloom data has same debug digest value before and after using restore command
        """     
        client = self.server.get_new_client()
        client.execute_command('BF.INSERT original error 0.001 capacity 2000 items 1')
        dump = client.execute_command('DUMP original')
        dump_digest = client.execute_command('DEBUG DIGEST-VALUE original')
        client.execute_command('RESTORE', 'copy', 0, dump)
        restore_digest = client.execute_command('DEBUG DIGEST-VALUE copy')
        assert restore_digest == dump_digest
