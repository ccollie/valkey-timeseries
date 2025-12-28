import os
import re
from sys import platform

from valkey.commands.timeseries.utils import list_to_dict

CWD = os.path.dirname(os.path.realpath(__file__))
ROOT_PATH = os.path.abspath(os.path.join(CWD, "../.."))

WORK_DIR = 'work'
RDB_PATH = os.path.join(CWD, 'rdbs')

PORT = 6379
SERVER_VERSION = os.environ.get('SERVER_VERSION', 'unstable')
SERVER_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{SERVER_VERSION}/valkey-server"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SERVER_VERSION = os.environ.get("SERVER_VERSION", "unstable")
VALKEY_SERVER_PATH = f"{SCRIPT_DIR}/build/binaries/{SERVER_VERSION}/valkey-server"
TEST_DIR = f"{ROOT_PATH}/test-data"
LOGS_DIR = f"{TEST_DIR}/logs"

if "VALKEY_SERVER_PATH" in os.environ:
    VALKEY_SERVER_PATH = os.environ["VALKEY_SERVER_PATH"]

if "LOGS_DIR" in os.environ:
    LOGS_DIR = os.environ["LOGS_DIR"]

def get_platform():
    return platform.lower()

def get_dynamic_lib_extension():
    system = get_platform()

    if system == "windows":
        return ".dll"
    elif system == "darwin":
        return ".dylib"
    elif system == "linux":
        return ".so"
    else:
        raise Exception(f"Unsupported platform: {system}")

PLATFORM = get_platform()
MODULE_PATH = os.path.abspath("{}/target/debug/libvalkey_timeseries{}".format(ROOT_PATH, get_dynamic_lib_extension()))

def get_server_version():
    version = os.getenv('VALKEY_VERSION')
    if version is None:
        version = "unstable"

    return version

def get_server_path(version):
    if version is None:
        version = get_server_version()
    path = os.path.join(CWD, "..", ".build/binaries/{}/valkey-server".format(version))
    return os.path.abspath(path)

def get_module_path():
    path = os.environ.get('MODULE_PATH')
    if path is None:
        path = os.path.join(ROOT_PATH, "target/debug/libvalkey_metrics{}".format(get_dynamic_lib_extension()))
        path = os.path.abspath(path)

    return path

def parse_info_response(response):
    """Helper function to parse TS.INFO list response into a dictionary."""

    # Keys that contain integer values
    integer_keys = {
        'totalSamples',
        'memoryUsage',
        'firstTimestamp',
        'lastTimestamp',
        'retentionTime',
        'chunkCount',
        'chunkSize'
    }

    info_dict = {}
    it = iter(response)
    for key in it:
        key_str = key.decode('utf-8')
        value = next(it)
        if key_str in integer_keys:
            # Convert to integer if the key is in the integer keys set
            if isinstance(value, bytes):
                info_dict[key_str] = int(value.decode('utf-8'))
            else:
                info_dict[key_str] = int(value)
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


class CompactionRule:
    """Represents a compaction rule for time series."""
    def __init__(self, dest_key, bucket_duration, aggregation, alignment = 0):
        self.dest_key = dest_key.decode('utf-8') if isinstance(dest_key, bytes) else dest_key
        self.bucket_duration = int(bucket_duration)
        self.aggregation = aggregation.decode('utf-8') if isinstance(aggregation, bytes) else aggregation
        if alignment is None:
            self.alignment = 0
        else:
            self.alignment = int(alignment)

    def __key(self):
        return self.dest_key, self.bucket_duration, self.aggregation, self.alignment

    def __hash__(self):
        return hash(self.__key())

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