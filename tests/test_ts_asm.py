"""
Integration tests for Atomic Slot Migration (ASM) index consistency.

Tests verify that the timeseries index is properly maintained during slot migrations:
1. All timeseries are removed from the source index and cannot be queried
2. Source replica nodes are consistent with the primary node after migration 
3. The migrated series are visible in the target shard after migration
4. Indexes on target replicas are consistent with the target primary

Reference: https://valkey.io/topics/atomic-slot-migration/

These tests require Valkey >= 9.0 (ASM minimum version).
"""

import logging
import time
import pytest
from valkey import Valkey, ValkeyCluster
from valkeytestframework.util.waiters import wait_for_true
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

logger = logging.getLogger(__name__)


def get_server_version(client: Valkey) -> tuple:
    """Extract server version as (major, minor, patch) from INFO server."""
    info = client.info("server")
    version_str = info.get("valkey_version") or info.get("redis_version", "0.0.0")
    parts = version_str.split(".")
    return tuple(int(p.split("-")[0]) for p in parts[:3])


class TestAtomicSlotMigration(ValkeyTimeSeriesClusterTestCase):
    """Test Atomic Slot Migration index consistency."""

    # Use 3 shards with 1 replica each for comprehensive migration testing
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def _skip_if_asm_not_supported(self, client: Valkey):
        """Skip test if server version < 9.0 (ASM minimum version)."""
        version = get_server_version(client)
        if version < (9, 0, 0):
            pytest.skip(f"ASM requires Valkey >= 9.0, found {'.'.join(map(str, version))}")

    def _skip_if_asm_commands_not_supported(self, client: Valkey):
        """Skip when this server binary does not expose ASM cluster migration commands."""
        help_reply = client.execute_command("CLUSTER", "HELP")
        lines = []
        for item in help_reply:
            if isinstance(item, bytes):
                lines.append(item.decode("utf-8"))
            else:
                lines.append(str(item))
        joined = "\n".join(lines).upper()
        has_migrate_cmd = "MIGRATESLOTS" in joined or " MIGRATE " in joined
        has_getslotmigrations = "GETSLOTMIGRATIONS" in joined
        if not has_migrate_cmd or not has_getslotmigrations:
            pytest.skip("ASM migration command not available in this server build")

    def _get_node_id(self, client: Valkey) -> str:
        """Get the cluster node ID for a client."""
        node_id = client.execute_command("CLUSTER MYID")
        return node_id.decode("utf-8") if isinstance(node_id, bytes) else node_id

    def _get_key_slot(self, client: Valkey, key: str) -> int:
        """Get the hash slot for a key."""
        slot = client.execute_command("CLUSTER KEYSLOT", key)
        return int(slot)

    def _shard_index_for_slot(self, slot: int) -> int:
        """Determine which shard owns a given slot."""
        ranges = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)
        for idx, (start, end) in enumerate(ranges):
            if start <= slot < end:
                return idx
        raise ValueError(f"Slot {slot} not found in any shard range")

    def _wait_for_no_migrations(self, client: Valkey, timeout: int = 30):
        """Wait for all migrations to complete (CLUSTER GETSLOTMIGRATIONS returns empty)."""
        def check_migrations():
            try:
                result = client.execute_command("CLUSTER GETSLOTMIGRATIONS")
                # Result is a dict/map when there are no migrations
                if isinstance(result, dict):
                    return len(result) == 0
                # Or empty list
                return len(result) == 0
            except Exception as e:
                logger.warning(f"Error checking migrations: {e}")
                return False

        wait_for_true(check_migrations, timeout=timeout)

    def _assert_queryindex_empty(self, client: Valkey, label_filter: str):
        """Assert TS.QUERYINDEX returns empty list for the given filter."""
        result = client.execute_command("TS.QUERYINDEX", label_filter)
        assert result == [], f"Expected empty queryindex result, got {result}"

    def _assert_queryindex_contains(self, client: Valkey, label_filter: str, expected_keys: list):
        """Assert TS.QUERYINDEX returns exactly the expected keys."""
        result = client.execute_command("TS.QUERYINDEX", label_filter)
        result_set = set(result)
        expected_set = set(k.encode() if isinstance(k, str) else k for k in expected_keys)
        assert result_set == expected_set, f"Expected {expected_set}, got {result_set}"

    def _create_series_in_slot(self, cluster_client: ValkeyCluster, hash_tag: str, count: int = 5):
        """Create multiple time series with a common hash tag and migration label."""
        keys = []
        base_ts = 1000
        
        for i in range(count):
            key = f"ts:{hash_tag}:series{i}"
            keys.append(key)
            cluster_client.execute_command(
                "TS.CREATE", key, 
                "LABELS", "migration", "yes", "series_id", str(i), "tag", hash_tag
            )
            
            # Add some sample data
            for j in range(10):
                cluster_client.execute_command(
                    "TS.ADD", key, base_ts + j * 100, 10.0 + i + j * 0.1
                )
        
        return keys

    def _migrate_slot(self, source_client: Valkey, target_node_id: str, slot: int, timeout: int = 30):
        """Perform slot migration and wait for completion."""
        self._skip_if_asm_commands_not_supported(source_client)
        logger.info(f"Migrating slot {slot} to node {target_node_id}")
        
        # Initiate migration
        source_client.execute_command("CLUSTER MIGRATE", target_node_id, slot, slot)
        
        # Wait for migration to complete
        self._wait_for_no_migrations(source_client, timeout=timeout)
        
        # Give a bit of time for background index processing
        time.sleep(2)

    def _get_slot_migrations(self, client: Valkey) -> list[dict]:
        """Normalize CLUSTER GETSLOTMIGRATIONS output into list[dict]."""
        raw = client.execute_command("CLUSTER", "GETSLOTMIGRATIONS")
        if not raw:
            return []

        def to_str(v):
            if isinstance(v, bytes):
                return v.decode("utf-8")
            return v

        # RESP3 map/list style from cluster clients
        if isinstance(raw, dict):
            values = raw.values()
            return [v for v in values if isinstance(v, dict)]

        # RESP2 flattened pairs, where each entry is an array [k1,v1,k2,v2,...]
        out: list[dict] = []
        if isinstance(raw, list):
            for entry in raw:
                if isinstance(entry, dict):
                    out.append(entry)
                    continue
                if not isinstance(entry, list):
                    continue
                m = {}
                for i in range(0, len(entry) - 1, 2):
                    k = to_str(entry[i])
                    m[k] = entry[i + 1]
                out.append(m)
        return out

    def _find_migration_job_for_slot(self, client: Valkey, slot: int):
        """Return migration job name for a slot if present."""
        migrations = self._get_slot_migrations(client)
        slot_token = str(slot)
        slot_range_token = f"{slot}-{slot}"

        for migration in migrations:
            name = migration.get("name") or migration.get(b"name")
            slot_ranges = migration.get("slot_ranges") or migration.get(b"slot_ranges")
            if isinstance(slot_ranges, bytes):
                slot_ranges = slot_ranges.decode("utf-8")

            if not slot_ranges or not name:
                continue

            slot_ranges = str(slot_ranges)
            if slot_token in slot_ranges or slot_range_token in slot_ranges:
                return name.decode("utf-8") if isinstance(name, bytes) else str(name)

        return None

    def _migrate_slot_range(self, source_client: Valkey, target_node_id: str, start_slot: int, end_slot: int):
        """Start slot migration without waiting, with command compatibility fallback."""
        self._skip_if_asm_commands_not_supported(source_client)
        attempts = [
            ("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", start_slot, end_slot, "NODE", target_node_id),
            ("CLUSTER", "MIGRATE", target_node_id, start_slot, end_slot),
        ]

        last_error = None
        for attempt in attempts:
            try:
                source_client.execute_command(*attempt)
                return
            except Exception as e:
                last_error = e
                continue

        raise AssertionError(f"Failed to start slot migration: {last_error}")

    def _abort_migration(self, source_client: Valkey, target_client: Valkey, slot: int, job_name: str):
        """Abort a migration job.

        First attempts explicit CLUSTER MIGRATE ABORT forms for forward compatibility,
        then falls back to CLUSTER SYNCSLOTS FINISH STATE failed NAME <job>.
        """
        # Try explicit ABORT forms first (syntax may differ by server revision).
        abort_attempts = [
            ("CLUSTER", "MIGRATE", "ABORT", job_name),
            ("CLUSTER", "MIGRATESLOTS", "ABORT", job_name),
            ("CLUSTER", "MIGRATE", "SLOTSRANGE", slot, slot, "NODE", self._get_node_id(target_client), "ABORT"),
        ]

        for attempt in abort_attempts:
            try:
                source_client.execute_command(*attempt)
                return
            except Exception:
                continue

        # Force failed finish on both sides so ImportAborted is emitted on target.
        # This maps to finishSlotMigrationJob(..., FAILED, ...).
        target_client.execute_command(
            "CLUSTER",
            "SYNCSLOTS",
            "FINISH",
            "STATE",
            "failed",
            "NAME",
            job_name,
            "MESSAGE",
            "test-induced-abort",
        )

        try:
            source_client.execute_command(
                "CLUSTER",
                "SYNCSLOTS",
                "FINISH",
                "STATE",
                "failed",
                "NAME",
                job_name,
                "MESSAGE",
                "test-induced-abort",
            )
        except Exception:
            # Source may have already transitioned; target-side abort is enough for ImportAborted coverage.
            pass

    def test_source_index_cleared_after_migration(self):
        """Test that source primary index is cleared after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create test data with controlled slot placement
        hash_tag = "asm_test_src"
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)
        
        # Determine slot and validate it's in source shard
        slot = self._get_key_slot(source_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"
        
        # Verify keys are indexed on source before migration
        self._assert_queryindex_contains(source_client, "migration=yes", keys)
        
        # Get target node ID
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        
        # Perform migration
        self._migrate_slot(source_client, target_node_id, slot)
        
        # Assert: source index should be empty for migrated keys
        self._assert_queryindex_empty(source_client, "migration=yes")
        
        # Verify keys no longer exist on source
        for key in keys:
            exists = source_client.execute_command("EXISTS", key)
            assert exists == 0, f"Key {key} still exists on source after migration"

    def test_source_replicas_consistent_after_migration(self):
        """Test that source replicas are consistent with primary after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_rg = self.get_replication_group(0)
        source_primary_client = source_rg.get_primary_connection()
        self._skip_if_asm_not_supported(source_primary_client)
        self._skip_if_asm_commands_not_supported(source_primary_client)

        # Create test data
        hash_tag = "asm_test_replica"
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)
        
        slot = self._get_key_slot(source_primary_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"
        
        # Wait for initial replication
        source_rg.wait_for_replica_offset_to_sync_up(0)
        
        # Verify keys are on replica before migration
        source_replica_client = source_rg.get_replica_connection(0)
        for key in keys:
            exists = source_replica_client.execute_command("EXISTS", key)
            assert exists == 1, f"Key {key} not replicated to source replica"
        
        # Perform migration
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_primary_client, target_node_id, slot)
        
        # Wait for DEL commands to replicate to source replicas
        source_rg.wait_for_replica_offset_to_sync_up(0)
        time.sleep(2)  # Additional time for index cleanup
        
        # Assert: source replica index should also be empty
        self._assert_queryindex_empty(source_replica_client, "migration=yes")
        
        # Verify keys are deleted on replica
        for key in keys:
            exists = source_replica_client.execute_command("EXISTS", key)
            assert exists == 0, f"Key {key} still exists on source replica after migration"

    def test_target_index_populated_after_migration(self):
        """Test that the target primary index is populated after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create test data
        hash_tag = "asm_test_target"
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)
        
        slot = self._get_key_slot(source_client, keys[0])
        
        # Perform migration
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)
        
        # Assert: target index should contain all migrated keys
        self._assert_queryindex_contains(target_client, "migration=yes", keys)
        
        # Verify keys exist on target with correct data
        for key in keys:
            exists = target_client.execute_command("EXISTS", key)
            assert exists == 1, f"Key {key} not found on target after migration"
            
            # Verify data integrity
            result = target_client.execute_command("TS.RANGE", key, "-", "+")
            assert len(result) == 10, f"Expected 10 samples in {key}, got {len(result)}"
        
        # Verify MRANGE works across the migrated keys
        mrange_result = target_client.execute_command(
            "TS.MRANGE", "-", "+", "FILTER", "migration=yes"
        )
        assert len(mrange_result) == 5, f"Expected 5 series in MRANGE, got {len(mrange_result)}"

    def test_target_replicas_index_consistent(self):
        """Test that target replica indexes are consistent with primary after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create test data
        hash_tag = "asm_test_target_replica"
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=5)
        
        slot = self._get_key_slot(source_client, keys[0])
        
        # Perform migration to shard 1
        target_rg = self.get_replication_group(1)
        target_primary_client = target_rg.get_primary_connection()
        target_node_id = self._get_node_id(target_primary_client)
        self._migrate_slot(source_client, target_node_id, slot)
        
        # Wait for replication to target replicas
        target_rg.wait_for_replica_offset_to_sync_up(0)
        time.sleep(2)  # Additional time for delayed indexing to complete
        
        # Get primary index result
        primary_result = target_primary_client.execute_command("TS.QUERYINDEX", "migration=yes")
        primary_keys = set(primary_result)
        
        # Assert: target replica index matches primary
        target_replica_client = target_rg.get_replica_connection(0)
        replica_result = target_replica_client.execute_command("TS.QUERYINDEX", "migration=yes")
        replica_keys = set(replica_result)
        
        assert replica_keys == primary_keys, \
            f"Target replica index mismatch. Primary: {primary_keys}, Replica: {replica_keys}"
        
        # Verify all keys exist on replica
        for key in keys:
            exists = target_replica_client.execute_command("EXISTS", key)
            assert exists == 1, f"Key {key} not found on target replica after migration"
            
            # Verify data integrity on replica
            result = target_replica_client.execute_command("TS.RANGE", key, "-", "+")
            assert len(result) == 10, f"Expected 10 samples in replica {key}, got {len(result)}"

    def test_multiple_slots_migration(self):
        """Test migration of multiple series across different slots."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create series in multiple slots (all should be in shard 0)
        all_keys = []
        slots_created = set()
        
        for i in range(3):
            hash_tag = f"asm_multi_{i}"
            keys = self._create_series_in_slot(cluster_client, hash_tag, count=3)
            all_keys.extend(keys)
            
            slot = self._get_key_slot(source_client, keys[0])
            slots_created.add(slot)
            assert self._shard_index_for_slot(slot) == 0, f"Keys not in source shard for tag {hash_tag}"
        
        logger.info(f"Created series in slots: {slots_created}")
        
        # Verify all keys indexed on source
        source_result = source_client.execute_command("TS.QUERYINDEX", "migration=yes")
        assert len(source_result) == 9, f"Expected 9 keys on source, got {len(source_result)}"
        
        # Migrate all slots to target
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        
        for slot in sorted(slots_created):
            self._migrate_slot(source_client, target_node_id, slot)
        
        # Assert: source should have no migration=yes keys
        self._assert_queryindex_empty(source_client, "migration=yes")
        
        # Assert: target should have all keys
        self._assert_queryindex_contains(target_client, "migration=yes", all_keys)

    def test_migration_with_label_filtering(self):
        """Test that label filtering works correctly after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create series with different labels in same slot
        hash_tag = "asm_labels"
        keys_migrated = []
        keys_other = []
        
        for i in range(3):
            key_migrated = f"ts:{hash_tag}:mig{i}"
            keys_migrated.append(key_migrated)
            cluster_client.execute_command(
                "TS.CREATE", key_migrated,
                "LABELS", "migration", "yes", "type", "migrated"
            )
            
            key_other = f"ts:{hash_tag}:other{i}"
            keys_other.append(key_other)
            cluster_client.execute_command(
                "TS.CREATE", key_other,
                "LABELS", "migration", "no", "type", "other"
            )
        
        slot = self._get_key_slot(source_client, keys_migrated[0])
        
        # Migrate the slot
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)
        
        # Assert: filtering by migration=yes on target returns only migrated keys
        self._assert_queryindex_contains(target_client, "migration=yes", keys_migrated)
        
        # Assert: filtering by migration=no on target returns other keys
        self._assert_queryindex_contains(target_client, "migration=no", keys_other)
        
        # Assert: source has no keys from this slot
        self._assert_queryindex_empty(source_client, "migration=yes")
        self._assert_queryindex_empty(source_client, "migration=no")

    def test_mget_after_migration(self):
        """Test TS.MGET works correctly after migration."""
        # Setup
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        # Create test data with latest values
        hash_tag = "asm_mget"
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=3)
        
        # Add a recent sample to each
        latest_ts = 10000
        for i, key in enumerate(keys):
            cluster_client.execute_command("TS.ADD", key, latest_ts, 100.0 + i)
        
        slot = self._get_key_slot(source_client, keys[0])
        
        # Migrate
        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)
        self._migrate_slot(source_client, target_node_id, slot)
        
        # Assert: MGET on target returns correct latest values
        result = target_client.execute_command("TS.MGET", "FILTER", "migration=yes")
        assert len(result) == 3, f"Expected 3 results from MGET, got {len(result)}"
        
        for series_result in result:
            key = series_result[0]
            labels = series_result[1]
            latest = series_result[2]
            
            # Verify latest timestamp and value
            assert latest[0] == latest_ts, f"Wrong timestamp for {key}"
            assert 100.0 <= float(latest[1]) < 103.0, f"Wrong value for {key}"

    def test_import_aborted_clears_delayed_keys(self):
        """Exercise ImportAborted and verify target index stays clean after abort."""
        cluster_client = self.new_cluster_client()
        source_client = self.new_client_for_primary(0)
        self._skip_if_asm_not_supported(source_client)
        self._skip_if_asm_commands_not_supported(source_client)

        target_client = self.new_client_for_primary(1)
        target_node_id = self._get_node_id(target_client)

        # Use enough keys to keep migration in-flight long enough to abort.
        hash_tag = self._find_hash_tag_for_shard(source_client, "asm_abort", shard_index=0)
        keys = self._create_series_in_slot(cluster_client, hash_tag, count=128)

        slot = self._get_key_slot(source_client, keys[0])
        assert self._shard_index_for_slot(slot) == 0, "Keys not in source shard"

        # Start migration and wait until job appears, then abort.
        self._migrate_slot_range(source_client, target_node_id, slot, slot)

        wait_for_true(
            lambda: self._find_migration_job_for_slot(target_client, slot) is not None,
            timeout=20,
        )
        job_name = self._find_migration_job_for_slot(target_client, slot)
        assert job_name is not None, "Migration job not found on target"

        self._abort_migration(source_client, target_client, slot, job_name)

        self._wait_for_no_migrations(source_client, timeout=30)
        self._wait_for_no_migrations(target_client, timeout=30)
        time.sleep(2)

        # ImportAborted path should clear delayed keys and keep target index clean.
        self._assert_queryindex_empty(target_client, "tag=asm_abort")
        self._assert_queryindex_empty(target_client, "migration=yes")

        # No migrated keys should be visible on target after abort.
        target_exists_count = sum(target_client.execute_command("EXISTS", key) for key in keys)
        assert target_exists_count == 0, f"Expected no keys on target after abort, found {target_exists_count}"

    def _find_hash_tag_for_shard(self, client: Valkey, prefix: str, shard_index: int, max_attempts: int = 512) -> str:
        """Find a hash tag whose slot is owned by the requested shard."""
        for i in range(max_attempts):
            candidate = f"{prefix}_{i}"
            probe_key = f"ts:{{{candidate}}}:probe"
            slot = self._get_key_slot(client, probe_key)
            if self._shard_index_for_slot(slot) == shard_index:
                return candidate
        raise AssertionError(f"Failed to find hash tag for shard {shard_index} after {max_attempts} attempts")
