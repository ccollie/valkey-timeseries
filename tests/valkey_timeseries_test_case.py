import os
import pytest
from valkeytests.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import random
import string

class ValkeyTimeSeriesTestCaseBase(ValkeyTestCase):

    # Global Parameterized Configs
    use_random_seed = 'no'

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg
            return str(e)

    def verify_command_success_reply(self, client, cmd, expected_result):
        cmd_actual_result = client.execute_command(cmd)
        assert_error_msg = f"Actual command response '{cmd_actual_result}' is different from expected response '{expected_result}'"
        assert cmd_actual_result == expected_result, assert_error_msg


    def verify_key_exists(self, client, key, value, should_exist=True):
        if should_exist:
            assert client.execute_command(f'EXISTS {key}') == 1, f"Item {key} {value} doesn't exist"
        else:
            assert client.execute_command(f'EXISTS {key}') == 0, f"Item {key} {value} exists"

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = client.info_obj().num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def generate_random_string(self, length=7):
        """ Creates a random string with specified length.
        """
        characters = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

    def validate_copied_series_correctness(self, client, original_filter_name, item_prefix, add_operation_idx, expected_fp_rate, fp_margin, original_info_dict):
        """ Validate correctness on a copy of the provided timeseries.
        """
        copy_filter_name = "filter_copy"
        assert client.execute_command(f'COPY {original_filter_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        copy_info = client.execute_command(f'TS.INFO {copy_filter_name}')
        copy_it = iter(copy_info)
        copy_info_dict = dict(zip(copy_it, copy_it))
        assert copy_info_dict[b'Capacity'] == original_info_dict[b'Capacity']
        assert copy_info_dict[b'Number of items inserted'] == original_info_dict[b'Number of items inserted']
        assert copy_info_dict[b'Number of filters'] == original_info_dict[b'Number of filters']
        assert copy_info_dict[b'Size'] == original_info_dict[b'Size']
        assert copy_info_dict[b'Expansion rate'] == original_info_dict[b'Expansion rate']

    """
    This method will parse the return of an INFO command and return a python dict where each metric is a key value pair.
    We can pass in specific sections in order to not have the dict store irrelevant fields related to what we want to check.
    Example of parsing the returned dict:
        stats = self.parse_valkey_info("STATS")
        stats.get('active_defrag_misses')
    """
    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command('INFO ' + section)
        lines = mem_info.decode('utf-8').split('\r\n')        
        stats_dict = {}
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict
