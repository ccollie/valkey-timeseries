import logging
import os

import pytest

from valkey_timeseries_test_case import parse_info_response, ValkeyTimeSeriesClusterTestCase
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from typing import List

logger = logging.getLogger(__name__)

REPLICATION_TIMEOUT = 3

class TestTimeSeriesReplication(ValkeyTimeSeriesClusterTestCase):
    """Test replication functionality"""
    
    # Override default cluster size to 1 primary
    CLUSTER_SIZE = 1
    # Override default replica count for replication testing
    REPLICAS_COUNT = 1

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        return [
            "enable-debug-command yes",
            f"dir {test_dir}",
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "cluster-enabled yes",
            f"cluster-config-file nodes_{port}.conf",
            f"loadmodule {os.getenv('MODULE_PATH')}",
        ]

    # Add client property for convenience
    @property
    def client(self):
        """Get the primary client"""
        return self.client_for_primary(0)

    @property
    def replicas(self):
        """Get replica nodes for backward compatibility"""
        return self.get_replication_group(0).replicas

    # Wait for replication to propagate
    def wait_for_key_exists(self, key = None):
        rg = self.get_replication_group(0)
        rg.wait_for_replica_offset_to_sync_up(0)
        client = rg.get_replica_connection(0)

        if key is not None:
            # wait for any key to exist
            waiters.wait_for_true(
                lambda: client.execute_command(f"EXISTS {key}") == 1,
                timeout=REPLICATION_TIMEOUT,
            )
    
    def wait_for_replication(self):
        rg = self.get_replication_group(0)
        rg.wait_for_replica_offset_to_sync_up(0)

    def test_basic_replication(self):
        """Test that basic time series operations replicate to replicas"""
        # Create a time series on primary
        key = "ts:basic_repl"
        client = self.client
        
        assert client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Add samples to primary
        timestamps = [1000, 2000, 3000]
        values = [10.5, 20.3, 30.7]

        for ts, val in zip(timestamps, values):
            client.execute_command(f"TS.ADD {key} {ts} {val}")

        # Wait for replication to propagate
        self.wait_for_key_exists(key)

        # Verify data on replica
        rg = self.get_replication_group(0)
        replica_client = rg.get_replica_connection(0)
        
        result = replica_client.execute_command(f"TS.RANGE {key} - +")
        assert len(result) == 3
        for i, (ts, val) in enumerate(result):
            assert ts == timestamps[i]
            assert float(val) == values[i]

    def test_replication_ts_create_with_labels(self):
        """Test that time series with labels replicate correctly"""
        key = "ts:labeled"
        labels = {"sensor": "temp", "location": "room1", "unit": "celsius"}

        # Create time series with labels on primary
        labels_str = " ".join([f"{k} {v}" for k, v in labels.items()])
        assert self.client.execute_command(
            f"TS.CREATE {key} LABELS {labels_str}"
        ) == b"OK"

        # Add data
        self.client.execute_command(f"TS.ADD {key} 1000 25.5")
        self.client.execute_command(f"TS.ADD {key} 2000 26.8")

        exists = self.replicas[0].client.execute_command(f"EXISTS {key}")
        print(f"EXISTS {key} == {exists}")
        # Wait for replication
        self.wait_for_key_exists(key)

        # Verify labels on replicas
        for replica in self.replicas:
            info = replica.client.execute_command(f"TS.INFO {key}")
            info_dict = parse_info_response(info)
            assert info_dict["labels"] == labels
            assert info_dict["totalSamples"] == 2

    def test_replication_ts_create_with_retention(self):
        """Test that time series retention replicates correctly"""
        key = "ts:retention"
        retention_ms = 100000

        # Create time series with retention on primary
        assert self.client.execute_command(
            f"TS.CREATE {key} RETENTION {retention_ms}"
        ) == b"OK"

        # Add samples
        for i in range(10):
            self.client.execute_command(f"TS.ADD {key} {1000 + i * 1000} {i}")

        # Wait for replication
        self.wait_for_key_exists(key)

        # Verify retention on replicas
        for replica in self.replicas:
            info = replica.client.execute_command(f"TS.INFO {key}")
            info_dict = parse_info_response(info)
            assert info_dict["retentionTime"] == retention_ms
            assert info_dict["totalSamples"] == 10

    def test_replication_ts_createrule(self):
        """Test that compaction rules replicate correctly"""
        source_key = "ts:source"
        dest_key = "ts:dest"

        # Create source and destination time series
        assert self.client.execute_command(f"TS.CREATE {source_key}") == b"OK"
        assert self.client.execute_command(f"TS.CREATE {dest_key}") == b"OK"

        # Create a compaction rule
        assert self.client.execute_command(
            f"TS.CREATERULE {source_key} {dest_key} AGGREGATION avg 60000"
        ) == b"OK"

        # Add samples to the source
        for i in range(10):
            self.client.execute_command(
                f"TS.ADD {source_key} {1000 + i * 10000} {i * 10}"
            )

        # Wait for replication
        waiters.wait_for_true(
            lambda: all(
                replica.client.execute_command(f"EXISTS {source_key}") == 1
                and replica.client.execute_command(f"EXISTS {dest_key}") == 1
                for replica in self.replicas
            ),
            timeout=10,
        )

        # Verify compaction rules on replicas
        for replica in self.replicas:
            info = replica.client.execute_command(f"TS.INFO {source_key}")
            info_dict = parse_info_response(info)

            assert "rules" in info_dict
            assert len(info_dict["rules"]) == 1

            rule = info_dict["rules"][0]
            assert rule.dest_key == dest_key
            assert rule.bucket_duration == 60000
            assert rule.aggregation == "avg"

    def test_replication_ts_del(self):
        """Test that deletions replicate correctly"""
        key = "ts:to_delete"

        # Create time series and add data
        assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"
        self.client.execute_command(f"TS.ADD {key} 1000 10")
        self.client.execute_command(f"TS.ADD {key} 2000 20")

        # Wait for replication
        self.wait_for_key_exists(key)

        # Delete the key on primary
        assert self.client.execute_command(f"DEL {key}") == 1

        # Wait for deletion to replicate
        rg = self.get_replication_group(0)
        rg.wait_for_replica_offset_to_sync_up(0)
        
        # Verify key doesn't exist on replicas
        replica_client = rg.get_replica_connection(0)
        waiters.wait_for_true(
            lambda: replica_client.execute_command(f"EXISTS {key}") == 0,
            timeout=REPLICATION_TIMEOUT,
        )

    def test_replication_ts_del_range(self):
        """Test that TS.DEL replicates correctly"""
        key = "ts:del_range"

        # Create time series and add samples
        assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"

        timestamps = [1000, 2000, 3000, 4000, 5000]
        for ts in timestamps:
            self.client.execute_command(f"TS.ADD {key} {ts} {ts}")

        # Wait for replication
        self.wait_for_key_exists(key)

        # Delete range on primary
        deleted = self.client.execute_command(f"TS.DEL {key} 2000 3000")
        assert deleted == 2

        # Wait for the replicas to sync
        rg = self.get_replication_group(0)
        rg.wait_for_replica_offset_to_sync_up(0)

        # Verify deletion on replicas
        replica_client = rg.get_replica_connection(0)
        result = replica_client.execute_command(f"TS.RANGE {key} - +")
        assert len(result) == 3
        assert result[0][0] == 1000
        assert result[1][0] == 4000
        assert result[2][0] == 5000

    def test_replication_ts_alter(self):
        """Test that TS.ALTER replicates correctly"""
        key = "ts:alter"

        # Create time series
        assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Wait for replication
        self.wait_for_key_exists(key)

        # Alter retention and labels on primary
        new_retention = 50000
        assert self.client.execute_command(
            f"TS.ALTER {key} RETENTION {new_retention} LABELS sensor temp location room2"
        ) == b"OK"

        # Wait for the replicas to sync
        rg = self.get_replication_group(0)
        rg.wait_for_replica_offset_to_sync_up(0)

        # Verify changes on replicas
        replica_client = rg.get_replica_connection(0)
        info = replica_client.execute_command(f"TS.INFO {key}")
        info_dict = parse_info_response(info)
        assert info_dict["retentionTime"] == new_retention
        assert info_dict["labels"]["sensor"] == "temp"
        assert info_dict["labels"]["location"] == "room2"

    def test_replication_multiple_keys(self):
        """Test replication with multiple time series keys"""
        num_keys = 10
        keys = [f"ts:{{node:{i % 2}}}:multi_{i}" for i in range(num_keys)]

        # Create multiple time series on primary
        for key in keys:
            assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"
            # Add some data
            for j in range(5):
                self.client.execute_command(
                    f"TS.ADD {key} {1000 + j * 1000} {j}"
                )

        # Wait for all keys to replicate
        waiters.wait_for_true(
            lambda: all(
                replica.client.execute_command(f"DBSIZE") == num_keys
                for replica in self.replicas
            ),
            timeout=10,
        )

        # Verify all keys on replicas
        for replica in self.replicas:
            for key in keys:
                result = replica.client.execute_command(f"TS.RANGE {key} - +")
                assert len(result) == 5

    def test_replication_ts_madd(self):
        """Test that TS.MADD replicates correctly"""
        keys = ["ts:madd1", "ts:madd2", "ts:madd3"]

        # Create time series
        for key in keys:
            assert self.client.execute_command(f"TS.CREATE {key}") == b"OK"

        # Use TS.MADD to add samples to multiple keys
        madd_cmd = "TS.MADD"
        for i, key in enumerate(keys):
            madd_cmd += f" {key} {1000 + i * 100} {10 + i}"

        result = self.client.execute_command(madd_cmd)
        assert len(result) == len(keys)

        # Wait for replication
        waiters.wait_for_true(
            lambda: all(
                replica.client.execute_command(f"DBSIZE") == len(keys)
                for replica in self.replicas
            ),
            timeout=10,
        )

        # Verify data on replicas
        for replica in self.replicas:
            for i, key in enumerate(keys):
                result = replica.client.execute_command(f"TS.RANGE {key} - +")
                assert len(result) == 1
                assert result[0][0] == 1000 + i * 100
                assert float(result[0][1]) == 10 + i

    def test_replication_ts_deleterule(self):
        """Test that TS.DELETERULE replicates correctly"""
        source_key = "ts:delrule_src"
        dest_key = "ts:delrule_dst"

        # Create time series with rule
        assert self.client.execute_command(f"TS.CREATE {source_key}") == b"OK"
        assert self.client.execute_command(f"TS.CREATE {dest_key}") == b"OK"
        assert self.client.execute_command(
            f"TS.CREATERULE {source_key} {dest_key} AGGREGATION sum 10000"
        ) == b"OK"

        # Wait for replication
        self.wait_for_key_exists(source_key)

        # Delete the rule on primary
        assert self.client.execute_command(
            f"TS.DELETERULE {source_key} {dest_key}"
        ) == b"OK"

        # Wait for replicas to sync
        for replica in self.replicas:
            self.waitForReplicaToSyncUp(replica)

        # Verify rule deletion on replicas
        for replica in self.replicas:
            info = replica.client.execute_command(f"TS.INFO {source_key}")
            info_dict = parse_info_response(info)
            # Rules should be empty or not present
            assert "rules" not in info_dict or len(info_dict["rules"]) == 0