import os
import re

import pytest
from valkey.commands.timeseries.utils import list_to_dict
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import random
import string
import logging

class CompactionRule:
    """Represents a compaction rule for time series."""
    def __init__(self, dest_key, bucket_duration, aggregation, alignment):
        self.dest_key = dest_key.decode('utf-8') if isinstance(dest_key, bytes) else dest_key
        self.bucket_duration = int(bucket_duration)
        self.aggregation = aggregation.decode('utf-8') if isinstance(aggregation, bytes) else aggregation
        if alignment is None:
            self.alignment = 0
        else:
            self.alignment = int(alignment)

    def __eq__(self, other):
        if isinstance(other, list):
            if len(other) != 4:
                return False
            other = CompactionRule(other[0], other[1], other[2], other[3])
        elif isinstance(other, object):
            if not hasattr(other, 'dest_key') or not hasattr(other, 'bucket_duration') or \
               not hasattr(other, 'aggregation') or not hasattr(other, 'alignment'):
                return False
            other = CompactionRule(other.dest_key, other.bucket_duration, other.aggregation, other.alignment)
        elif not isinstance(other, CompactionRule):
            return False
        return (self.dest_key == other.dest_key and
                self.bucket_duration == other.bucket_duration and
                self.aggregation == other.aggregation and
                self.alignment == other.alignment)
    def __repr__(self):
        return f"CompactionRule(dest_key={self.dest_key}, bucket_duration={self.bucket_duration}, " \
               f"aggregation={self.aggregation}, alignment={self.alignment})"

class ValkeyTimeSeriesTestCaseBase(ValkeyTestCase):

    # Global Parameterized Configs
    use_random_seed = 'no'

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        args = {"enable-debug-command":"yes", 'loadmodule': os.getenv('MODULE_PATH')}
        server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"

        self.server, self.client = self.create_server(testdir = self.testdir,  server_path=server_path, args=args)
        logging.info("startup args are: %s", args)

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
        actual_num_keys = self.server.num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def generate_random_string(self, length=7):
        """ Creates a random string with a specified length.
        """
        characters = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

    def ts_info(self, key, debug = False):
        """ Get the info of the given key.
        """
        debug_str = 'DEBUG' if debug else ''
        info = self.client.execute_command(f'TS.INFO {key} {debug_str}')
        info_dict = parse_info_response(info)

        return info_dict

    def validate_ts_info_values(self, key, expected_info_dict):
        """ Validate the values of the timeseries info.
        """
        info_dict = self.ts_info(key)
        for k, v in expected_info_dict.items():
            if k == 'labels':
                assert info_dict[k] == v
            else:
                assert info_dict[k] == v, f"Expected {k} to be {v}, but got {info_dict[k]}"


    def validate_copied_series_correctness(self, client, original_name):
        """ Validate correctness on a copy of the provided timeseries.
        """
        copy_filter_name = f"{original_name}_copy"
        assert client.execute_command(f'COPY {original_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        original_info_dict = self.ts_info(original_name)
        copy_info_dict = self.ts_info(copy_filter_name)

        assert copy_info_dict == original_info_dict, f"Expected {copy_info_dict} to be equal to {original_info_dict}"

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


class ValkeyInfo:
    """Contains information about a point in time of Valkey"""
    def __init__(self, info):
        self.info = info

    def is_save_in_progress(self):
        """Return True if there is a save in progress."""
        return self.info['rdb_bgsave_in_progress'] == 1

    def is_aof_rewrite_in_progress(self):
        """Return True if there is a aof rewrite in progress."""
        return self.info['aof_rewrite_in_progress'] == 1

    def num_keys(self, db=0):
        if 'db{}'.format(db) in self.info:
            return self.info['db{}'.format(db)]['keys']
        return 0

    def get_master_repl_offset(self):
        return self.info['master_repl_offset']

    def get_master_replid(self):
        return self.info['master_replid']

    def get_replica_repl_offset(self):
        return self.info['slave_repl_offset']

    def is_master_link_up(self):
        """Returns True if role is slave and master_link_status is up"""
        if self.info['role'] == 'slave' and self.info['master_link_status'] == 'up':
            return True
        return False

    def num_replicas(self):
        return self.info['connected_slaves']

    def num_replicas_online(self):
        count=0
        for k,v in self.info.items():
            if re.match('^slave[0-9]', k) and v['state'] == 'online':
                count += 1
        return count

    def was_save_successful(self):
        return self.info['rdb_last_bgsave_status'] == 'ok'

    def was_aofrewrite_successful(self):
        return self.info['aof_last_bgrewrite_status'] == 'ok'

    def used_memory(self):
        return self.info['used_memory']

    def maxmemory(self):
        return self.info['maxmemory']

    def maxmemory_policy(self):
        return self.info['maxmemory_policy']

    def uptime_in_secs(self):
        return self.info['uptime_in_seconds']


def parse_info_response(response):
    """Helper function to parse TS.INFO list response into a dictionary."""
    info_dict = {}
    it = iter(response)
    for key in it:
        key_str = key.decode('utf-8')
        value = next(it)
        if key_str == 'rules':
            # Handle rules separately
            info_dict[key_str] = []
            for rule in value:
                if isinstance(rule, list):
                    # Convert each rule to a CompactionRule object
                    data = CompactionRule(rule[0], rule[1], rule[2], rule[3])
                    info_dict[key_str].append(data)
            continue
        if isinstance(value, list):
            # Handle nested structures like labels and chunks
            if key_str == 'labels':
                # Convert the labels from a list to a dictionary
                if value is None or len(value) == 0:
                    info_dict['labels'] = {}
                else:
                    info_dict['labels'] = list_to_dict(value)
            elif key_str == 'rules':
                # Convert the rules from a list to a dictionary
                if value is None or len(value) == 0:
                    info_dict[key_str] = []
                else:
                    info_dict[key_str] = []
                    for rule in value:
                        rule_dict = {}
                        for i in range(0, len(rule), 2):
                            rule_dict[rule[i].decode('utf-8')] = rule[i + 1]
                        info_dict[key_str].append(rule_dict)
            elif key_str == 'Chunks':
                if value is None or len(value) == 0:
                    info_dict['chunks'] = []
                else:
                    info_dict['chunks'] = []
                    for chunk in value:
                        chunk_dict = {}
                        for i in range(0, len(chunk), 2):
                            chunk_dict[chunk[i].decode('utf-8')] = chunk[i + 1]
                        info_dict['chunks'].append(chunk_dict)
            else: # Fallback for unknown list types
                info_dict[key_str] = value
        elif isinstance(value, bytes):
            info_dict[key_str] = value.decode('utf-8')
        else:
            info_dict[key_str] = value
    return info_dict
