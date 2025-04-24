import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

# todo: validate index changes when labels are altered
class TestTimeSeriesAlter(ValkeyTimeSeriesTestCaseBase):

    def setup_data(self):

        # Create a base series for alteration tests
        self.key = 'ts_alter_test'
        self.initial_retention = 0
        self.initial_labels = ['sensor', 'temp', 'area', 'A1']
        self.initial_chunk_size = 4096 # Default chunk size might vary, get from INFO if needed
        self.initial_policy = None # Default BLOCK

        self.client.execute_command(
            'TS.CREATE', self.key,
            'LABELS', *self.initial_labels
        )
        self.client.execute_command('TS.ADD', self.key, 1000, 25)

        # Verify initial state (especially default chunk size)
        info = self.ts_info(self.key)
        print(info)
        self.initial_chunk_size = info[b'chunkSize'] # Get actual default
        self.initial_retention = info[b'retentionTime']

    def test_alter_retention(self):
        """Test altering the retention period"""
        self.setup_data()

        new_retention = 60000 # 1 minute
        assert self.client.execute_command('TS.ALTER', self.key, 'RETENTION', new_retention) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']
        assert info[b'retentionTime'] == str(new_retention).encode()
        # Other properties should remain unchanged
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    def test_alter_chunk_size(self):
        """Test altering the chunk size"""
        self.setup_data()

        new_chunk_size = 128
        assert self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', new_chunk_size) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert info[b'chunkSize'] == new_chunk_size
        # Other properties should remain unchanged
        assert info[b'retentionTime'] == self.initial_retention
        # Other properties should remain unchanged
        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'

    def test_alter_labels_add(self):
        """Test altering labels by adding a new one"""
        self.setup_data()

        new_labels = self.initial_labels + ['status', 'active']
        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS', *new_labels) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'
        assert labels['status'] == 'active'

        # Other properties should remain unchanged
        assert info[b'retentionTime'] == self.initial_retention
        assert info[b'chunkSize'] == self.initial_chunk_size

    def test_alter_labels_remove(self):
        """Test altering labels by removing one"""
        self.setup_data()

        new_labels = ['sensor', 'temp'] # Remove area=A1
        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS', *new_labels) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert labels['sensor'] == 'temp'

        # Other properties should remain unchanged
        assert info[b'retentionTime'] == self.initial_retention
        assert info[b'chunkSize'] == self.initial_chunk_size

    def test_alter_labels_change_value(self):
        """Test altering labels by changing a value"""
        self.setup_data()

        new_labels = ['sensor', 'temp', 'area', 'B2'] # Change area A1 -> B2
        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS', *new_labels) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'B2'

    def test_alter_labels_clear(self):
        """Test altering labels to an empty set"""
        self.setup_data()

        assert self.client.execute_command('TS.ALTER', self.key, 'LABELS') == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert len(labels) == 0

    def test_alter_duplicate_policy(self):
        """Test altering the duplicate policy"""
        self.setup_data()

        new_policy = 'SUM'

        # Use ON_DUPLICATE or DUPLICATE_POLICY based on command implementation
        assert self.client.execute_command('TS.ALTER', self.key, 'DUPLICATE_POLICY', new_policy) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert info[b'duplicatePolicy'] == new_policy.lower().encode()
        # Other properties should remain unchanged
        assert info[b'retentionTime'] == self.initial_retention
        assert info[b'chunkSize'] == self.initial_chunk_size

        assert labels['sensor'] == 'temp'
        assert labels['area'] == 'A1'


    def test_alter_multiple_properties(self):
        """Test altering multiple properties at once"""
        self.setup_data()

        new_retention = 30000
        new_labels = ['location', 'server_room']
        new_policy = 'MAX'

        assert self.client.execute_command(
            'TS.ALTER', self.key,
            'RETENTION', new_retention,
            'DUPLICATE_POLICY', new_policy,
            'LABELS', *new_labels,
        ) == b'OK'

        info = self.ts_info(self.key)
        labels = info[b'labels']

        assert info[b'retentionTime'] == str(new_retention).encode()
        assert info[b'duplicatePolicy'] == new_policy.lower().encode()
        assert info[b'chunkSize'] == self.initial_chunk_size

        assert labels['location'] == 'server_room'

    def test_alter_non_existent_key(self):
        """Test TS.ALTER on a non-existent key"""
        self.setup_data()

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.ALTER', 'non_existent_key', 'RETENTION', 1000)
        assert "key does not exist" in str(excinfo.value).lower()

    def test_alter_wrong_type(self):
        """Test TS.ALTER on a key of the wrong type"""
        self.setup_data()

        string_key = 'my_string_key'
        self.client.set(string_key, 'hello')
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.ALTER', string_key, 'RETENTION', 1000)
        assert "the key is not a TSDB key" in str(excinfo.value)

    def test_alter_invalid_arguments(self):
        """Test TS.ALTER with invalid argument values"""
        self.setup_data()

        # Invalid retention
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'RETENTION', -10)
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'RETENTION', 'not_a_number')

        # Invalid chunk size
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', 0) # Must be > 0
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'CHUNK_SIZE', 'not_a_number')

        # Invalid duplicate policy
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'DUPLICATE_POLICY', 'INVALID_POLICY')

        # Odd number of labels
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'LABELS', 'label1')

        # Unknown option
        with pytest.raises(ResponseError):
            self.client.execute_command('TS.ALTER', self.key, 'UNKNOWN_OPTION', 'value')