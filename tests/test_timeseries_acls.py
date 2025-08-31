import os

import pytest
import time

from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesACL(ValkeyTimeSeriesTestCaseBase):
    """Integration tests for TimeSeries ACL validation"""

    def create_test_user(self, username: str, password: str, acl_rules: list) -> None:
        """Create a test user with specific ACL permissions"""

        acl_command = ['ACL', 'SETUSER', username, 'ON', f'>{password}'] + acl_rules
        print(f"Creating user {username} with rules: {acl_rules}")
        self.client.execute_command(*acl_command)

    def get_user_client(self, username: str, password: str):
        """Get a client authenticated as a specific user"""
        client = self.server.get_new_client()
        client.execute_command('AUTH', username, password)
        return client

    def test_commands_have_acl_permissions(self):
        """Test that all TimeSeries commands have ACL permissions"""
        # Get all TS commands
        client = self.server.get_new_client()
        ts_commands = client.execute_command('COMMAND', 'LIST', 'FILTERBY', 'ACLCAT', 'timeseries')
        assert isinstance(ts_commands, list)
        assert len(ts_commands) > 0, "No TimeSeries commands found with ACL category"

        ALL_COMMANDS = set([
            'TS.CREATE',
            'TS.ALTER',
            'TS.DEL',
            'TS.GET',
            'TS.JOIN',
            'TS.MGET',
            'TS.RANGE',
            'TS.REVRANGE',
            'TS.INCRBY',
            'TS.DECRBY',
            'TS.MADD',
            'TS.MRANGE',
            'TS.MREVRANGE',
            'TS.DELETERULE',
            'TS.INFO',
            'TS.CARD',
            'TS.CREATERULE',
            'TS.QUERYINDEX',
            'TS.STATS',
        ])

        # Check each command has the correct ACL category
        for cmd in ts_commands:
            assert 'timeseries' in cmd[2], f"Command {cmd[1]} does not have timeseries ACL category"
            ALL_COMMANDS.remove(cmd[1])

        assert len(ALL_COMMANDS) == 0, f"Some TimeSeries commands are missing ACL category: {ALL_COMMANDS}"

    def test_ts_add_acl_permissions(self):
        """Test TS.ADD command with various ACL permissions"""
        # User with full TS permissions
        self.create_test_user('ts_full', 'password123', [
            '+@all', '+@timeseries', '+@keyspace', '~*'
        ])

        # User with only TS.ADD permission
        self.create_test_user('ts_add_only', 'password123', ['+@read', '+ts.add'])

        # User with no TS permissions
        self.create_test_user('no_ts', 'password123', [
            '+@read', '-@timeseries'
        ])

        # Test full permissions user
        ts_full_client = self.get_user_client('ts_full', 'password123')

        result = ts_full_client.execute_command('TS.ADD', 'ts:acl:full', '*', 100.5)
        assert result is not None

        # Test add-only user
        ts_add_client = self.get_user_client('ts_add_only', 'password123')
        result = ts_add_client.execute_command('TS.ADD', 'ts:acl:add_only', '*', 200.5)
        assert result is not None

        # Test user without TS permissions
        no_ts_client = self.get_user_client('no_ts', 'password123')

        with pytest.raises(Exception, match="No permissions to access a key"):
            no_ts_client.execute_command('TS.ADD', 'ts:acl:denied', '*', 300.5)

    def test_ts_range_acl_permissions(self):
        """Test TS.RANGE command with various ACL permissions"""
        # Setup test data as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:range_test')
        timestamp = int(time.time() * 1000)
        self.client.execute_command('TS.ADD', 'ts:acl:range_test', timestamp, 42.0)

        # User with read permissions
        self.create_test_user('ts_reader', 'password123', [
            '+@read', '+ts.range', '+ts.get'
        ])

        # User without read permissions
        self.create_test_user('no_read', 'password123', [
            '+ts.add', '-@read', '-ts.range'
        ])

        # Test reader can access TS.RANGE
        reader_client = self.get_user_client('ts_reader', 'password123')

        result = reader_client.execute_command('TS.RANGE', 'ts:acl:range_test', '-', '+')
        assert len(result) > 0

        # Test user without read permissions
        no_read_client = self.get_user_client('no_read', 'password123')

        with pytest.raises(Exception, match="No permissions to access a key"):
            no_read_client.execute_command('TS.RANGE', 'ts:acl:range_test', '-', '+')

    def test_compaction_rule_acl_permissions(self):
        """Test compaction rule operations with ACL permissions"""
        # Setup source and destination series as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:source')
        self.client.execute_command('TS.CREATE', 'ts:acl:dest')
        timestamp = int(time.time() * 1000)
        self.client.execute_command('TS.ADD', 'ts:acl:source', timestamp, 100.0)

        # User with compaction permissions
        self.create_test_user('ts_compaction', 'password123', [
            '+@all', '+@timeseries'
        ])

        # User without compaction permissions
        self.create_test_user('no_compaction', 'password123', [
            '+@read', '+ts.add', '+ts.range', '-ts.createrule', '-ts.deleterule'
        ])

        # Test user with compaction permissions
        compaction_client = self.get_user_client('ts_compaction', 'password123')
        result = compaction_client.execute_command(
            'TS.CREATERULE', 'ts:acl:source', 'ts:acl:dest',
            'AGGREGATION', 'avg', 60000
        )
        assert result == 'OK' or result == b'OK'

        # Test user without compaction permissions
        no_compaction_client = self.get_user_client('no_compaction', 'password123')
        with pytest.raises(Exception, match="No permissions to access a key"):
            no_compaction_client.execute_command(
                'TS.CREATERULE', 'ts:acl:source', 'ts:acl:dest2',
                'AGGREGATION', 'sum', 60000
            )

    def test_key_pattern_acl_restrictions(self):
        """Test ACL restrictions based on key patterns"""
        # User restricted to specific key patterns
        self.create_test_user('pattern_user', 'password123', [
            '+@all', '~ts:allowed:*'
        ])

        pattern_client = self.get_user_client('pattern_user', 'password123')

        # Should succeed for an allowed pattern
        result = pattern_client.execute_command('TS.ADD', 'ts:allowed:metric1', '*', 100.0)
        assert result is not None

        # Should fail for a disallowed pattern
        with pytest.raises(Exception, match="No permissions to access a key"):
            pattern_client.execute_command('TS.ADD', 'ts:forbidden:metric1', '*', 100.0)

    def test_acl_with_compaction_workflow(self):
        """Test ACL permissions in a complete compaction workflow"""
        # Setup users with specific roles
        self.create_test_user('data_producer', 'password123', [
            '+ts.add', '+ts.create', '+ping'
        ])

        self.create_test_user('rule_manager', 'password123', [
            '+ts.createrule', '+ts.deleterule', '+@read', '+ping'
        ])

        self.create_test_user('data_consumer', 'password123', [
            '+@read', '+ts.range', '+ts.get', '+ping'
        ])

        producer = self.get_user_client('data_producer', 'password123')
        manager = self.get_user_client('rule_manager', 'password123')
        consumer = self.get_user_client('data_consumer', 'password123')

        # Producer creates series and adds data
        producer.execute_command('TS.CREATE', 'ts:acl:workflow:source')
        producer.execute_command('TS.CREATE', 'ts:acl:workflow:dest')

        timestamp = int(time.time() * 1000)
        for i in range(5):
            producer.execute_command('TS.ADD', 'ts:acl:workflow:source', timestamp + i*1000, i*10.0)

        # Manager sets up compaction rule
        manager.execute_command(
            'TS.CREATERULE', 'ts:acl:workflow:source', 'ts:acl:workflow:dest',
            'AGGREGATION', 'avg', 5000
        )

        # Consumer reads aggregated data
        result = consumer.execute_command('TS.RANGE', 'ts:acl:workflow:dest', '-', '+')

        # Verify the workflow succeeded (a result may be empty if aggregation window not complete)
        assert isinstance(result, list)

        # Verify role separation - producer can't create rules
        with pytest.raises(Exception, match="No permissions to access a key"):
            producer.execute_command(
                'TS.CREATERULE', 'ts:acl:workflow:source', 'ts:acl:workflow:dest2',
                'AGGREGATION', 'sum', 5000
            )

        # Consumer can't modify data
        with pytest.raises(Exception, match="No permissions to access a key"):
            consumer.execute_command('TS.ADD', 'ts:acl:workflow:source', '*', 999.0)

    def test_acl_command_category_restrictions(self):
        """Test ACL restrictions using command categories"""
        # Setup test data as admin first
        self.client.execute_command('TS.CREATE', 'ts:acl:categories')
        self.client.execute_command('TS.ADD', 'ts:acl:categories', '*', 42.0)

        # User with only read category
        self.create_test_user('read_only', 'password123', [
            '+@read', '+ping', '-@write'
        ])

        # User with timeseries category but no dangerous commands
        self.create_test_user('ts_safe', 'password123', [
            '+@timeseries', '+ping', '-flushdb', '-flushall'
        ])

        read_only_client = self.get_user_client('read_only', 'password123')
        ts_safe_client = self.get_user_client('ts_safe', 'password123')

        # Read-only user can read but not write
        result = read_only_client.execute_command('TS.RANGE', 'ts:acl:categories', '-', '+')
        assert len(result) > 0

        with pytest.raises(Exception, match="No permissions to access a key"):
            read_only_client.execute_command('TS.ADD', 'ts:acl:categories', '*', 100.0)

        # TS safe user can use timeseries commands
        result = ts_safe_client.execute_command('TS.ADD', 'ts:acl:categories', '*', 200.0)
        assert result is not None

    def test_acl_with_multiple_series(self):
        """Test ACL validation with multiple timeseries operations"""
        # Create a user with limited permissions
        self.create_test_user('multi_user', 'password123', [
            '+ts.add', '+ts.range', '+ts.create', '+ping'
        ])

        multi_client = self.get_user_client('multi_user', 'password123')

        # Create multiple series
        series_names = ['ts:acl:multi1', 'ts:acl:multi2', 'ts:acl:multi3']

        for series_name in series_names:
            multi_client.execute_command('TS.CREATE', series_name)

        # Add data to each series
        base_timestamp = int(time.time() * 1000)
        for i, series_name in enumerate(series_names):
            for j in range(10):
                timestamp = base_timestamp + j * 1000
                value = float(i * 10 + j)
                result = multi_client.execute_command('TS.ADD', series_name, timestamp, value)
                assert result is not None

        # Verify data can be read back
        for series_name in series_names:
            result = multi_client.execute_command('TS.RANGE', series_name, '-', '+')
            assert len(result) == 10

    def test_acl_info_command_permissions(self):
        """Test TS.INFO command with ACL permissions"""
        # Setup test data as admin
        self.client.execute_command('TS.CREATE', 'ts:acl:info_test',
                                    'RETENTION', 10000,
                                    'LABELS', 'sensor', 'temperature')
        self.client.execute_command('TS.ADD', 'ts:acl:info_test', '*', 25.5)

        # User with info permissions
        self.create_test_user('info_user', 'password123', ['+@read', '+ts.info'])

        # User without info permissions
        self.create_test_user('no_info', 'password123', ['+ts.add', '-ts.info'])

        info_client = self.get_user_client('info_user', 'password123')
        no_info_client = self.get_user_client('no_info', 'password123')

        # User with info permissions can access TS.INFO
        result = info_client.execute_command('TS.INFO', 'ts:acl:info_test')
        assert isinstance(result, list)
        assert len(result) > 0

        # User without info permissions gets denied
        with pytest.raises(Exception, match="No permissions to access a key"):
            no_info_client.execute_command('TS.INFO', 'ts:acl:info_test')

    def test_acl_queryindex_permissions(self):
        """Test TS.QUERYINDEX command with ACL permissions"""
        # Setup test data with labels
        self.client.execute_command('TS.CREATE', 'ts:acl:sensor1',
                                    'LABELS', 'type', 'temperature', 'location', 'room1')
        self.client.execute_command('TS.CREATE', 'ts:acl:sensor2',
                                    'LABELS', 'type', 'humidity', 'location', 'room1')

        # User with query permissions
        self.create_test_user('query_user', 'password123', [
            '+@read', '+ts.queryindex'
        ])

        # User without query permissions
        self.create_test_user('no_query', 'password123', [
            '+ts.add', '-ts.queryindex'
        ])

        query_client = self.get_user_client('query_user', 'password123')
        no_query_client = self.get_user_client('no_query', 'password123')

        # User with query permissions can use TS.QUERYINDEX
        result = query_client.execute_command('TS.QUERYINDEX', 'location=room1')
        assert isinstance(result, list)
        assert len(result) >= 2  # Should find both sensors

        # User without query permissions gets denied
        with pytest.raises(Exception) as exc_info:
            no_query_client.execute_command('TS.QUERYINDEX', 'location=room1')
        assert 'NOPERM' in str(exc_info.value) or 'permission' in str(exc_info.value).lower()