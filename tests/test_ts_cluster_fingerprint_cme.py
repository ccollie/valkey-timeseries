"""
Integration tests for the cluster-map fingerprint validation feature.

Every fanout request carries the sender's cluster-map fingerprint (a hash of all
shard slot fingerprints). The receiving node compares it against its own view of
the cluster topology and rejects the request with a "cluster topology changed"
error when they disagree, so that a topology change between request and response
cannot produce an inconsistent aggregated result.

A node builds/refreshes its cluster map from CLUSTER NODES (whose reply does not
depend on a client), so a receiver can refresh to ground truth on the fanout
worker thread and validate authoritatively — including forcing a refresh when
its own map might merely be stale. These tests cover:

1. Happy path: in a stable cluster, fingerprints computed independently on every
   node agree, so cross-shard fanout succeeds (receivers refresh on demand) and
   no node rejects a request.
2. Rejection + recovery: a requester holding a stale cluster map is rejected
   once the topology changes, then succeeds after refreshing.
"""

import time

import pytest
from valkey import ResponseError, Valkey, ValkeyCluster
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase

# Hash tags {1}/{2}/{3} spread these keys across the three primary shards.
CPU1 = b"fp:cpu1:{1}"
CPU2 = b"fp:cpu2:{2}"
CPU3 = b"fp:cpu3:{3}"

CONFIG_EXPIRATION = "ts.ts-cluster-map-expiration-ms"
LONG_EXPIRATION = 600_000  # 10 minutes: a warmed cache stays valid for the test.

# Slot 12000 lives in the third shard's range for an even 3-way split of the
# 16384 slots ( [10923, 16383] ), and none of the hash-tagged keys above map to
# it, so it is safe to reassign.
MIGRATED_SLOT = 12000

MISMATCH_LOG_MARKER = "cluster topology has changed"


class TestClusterFingerprint(ValkeyTimeSeriesClusterTestCase):
    # ── helpers ──────────────────────────────────────────────────────────────

    def _set_expiration(self, client: Valkey, millis: int):
        client.execute_command("CONFIG", "SET", CONFIG_EXPIRATION, str(millis))

    def _node_id(self, client: Valkey) -> str:
        return client.execute_command("CLUSTER", "MYID").decode()

    def _warm_cluster_map(self, client: Valkey):
        """Force `client`'s node to build and cache its cluster map by running a
        fanout command. The result is irrelevant; the map is refreshed as a side
        effect of target selection."""
        try:
            client.execute_command("TS.QUERYINDEX", "name=__warm__")
        except ResponseError:
            pass

    def _slot_owner_id(self, client: Valkey, slot: int):
        """Return the primary node id that owns `slot`, per this node's view.

        Parses raw CLUSTER NODES output to avoid depending on client-side
        parsing of CLUSTER SLOTS."""
        raw = client.execute_command("CLUSTER", "NODES")
        if isinstance(raw, bytes):
            raw = raw.decode()
        for line in raw.splitlines():
            fields = line.split()
            if len(fields) < 8 or "master" not in fields[2]:
                continue
            node_id = fields[0]
            for spec in fields[8:]:
                if spec.startswith("["):  # importing/migrating placeholder
                    continue
                if "-" in spec:
                    start, end = spec.split("-")
                    if int(start) <= slot <= int(end):
                        return node_id
                elif int(spec) == slot:
                    return node_id
        return None

    def _wait_until(self, predicate, timeout=25, interval=0.2, msg=""):
        deadline = time.time() + timeout
        last_exc = None
        while time.time() < deadline:
            try:
                if predicate():
                    return
            except Exception as e:  # noqa: BLE001 - polling; remember last error
                last_exc = e
            time.sleep(interval)
        raise AssertionError(f"Timeout waiting for: {msg} (last error: {last_exc})")

    def _any_node_log_contains(self, marker: str) -> bool:
        return any(rg.primary.does_logfile_contains(marker) for rg in self.replication_groups)

    # ── tests ────────────────────────────────────────────────────────────────

    def test_fanout_consistent_across_stable_cluster(self):
        """In a stable cluster, cross-shard fanout succeeds and no node rejects a
        request for a fingerprint mismatch.

        Receivers here start with no cached map and must refresh to ground truth
        on the very first request; the fanout succeeding proves both that the
        refresh works from the receive path and that fingerprints computed
        independently on different nodes agree."""
        cluster: ValkeyCluster = self.new_cluster_client()
        client = self.new_client_for_primary(0)

        cluster.execute_command("TS.CREATE", CPU1, "LABELS", "name", "cpu")
        cluster.execute_command("TS.CREATE", CPU2, "LABELS", "name", "cpu")
        cluster.execute_command("TS.CREATE", CPU3, "LABELS", "name", "cpu")

        # No warming: the first fanout forces every receiver to build its map.
        for _ in range(5):
            result = client.execute_command("TS.QUERYINDEX", "name=cpu")
            assert sorted(result) == sorted([CPU1, CPU2, CPU3])

        # And it works regardless of which node originates the fanout.
        for i in range(self.CLUSTER_SIZE):
            result = self.new_client_for_primary(i).execute_command("TS.QUERYINDEX", "name=cpu")
            assert sorted(result) == sorted([CPU1, CPU2, CPU3])

        assert not self._any_node_log_contains(MISMATCH_LOG_MARKER), (
            "a stable cluster must not produce fingerprint-mismatch rejections"
        )

    def test_fanout_rejected_on_topology_change_then_recovers(self):
        """A requester holding a stale cluster map is rejected once the topology
        changes, and recovers on retry after refreshing.

        Node 0 (the requester) is pinned to the pre-change map with a long
        expiration and warmed before the change. Receivers 1 and 2 never cache
        (expiration 0), so they always validate against current ground truth.
        After migrating an empty slot between the receivers, node 0 still sends
        the old fingerprint and is rejected; the rejection marks its map stale,
        so the retry rebuilds and succeeds."""
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        node2 = self.new_client_for_primary(2)

        # Requester pins its (soon to be stale) map; receivers always refresh.
        self._set_expiration(node0, LONG_EXPIRATION)
        self._set_expiration(node1, 0)
        self._set_expiration(node2, 0)

        node1_id = self._node_id(node1)
        node2_id = self._node_id(node2)

        self._wait_until(
            lambda: self._slot_owner_id(node2, MIGRATED_SLOT) == node2_id,
            msg="node2 initially owns the slot to migrate",
        )

        # Pin node 0's cache to the pre-change fingerprint.
        self._warm_cluster_map(node0)

        # Migrate the (empty) slot from node 2 to node 1 using the canonical
        # empty-slot handoff, bumping node 1's config epoch and gossiping the new
        # ownership. This changes both shards' slot fingerprints, hence the
        # cluster-level fingerprint.
        try:
            node2.execute_command("CLUSTER", "SETSLOT", MIGRATED_SLOT, "MIGRATING", node1_id)
            node1.execute_command("CLUSTER", "SETSLOT", MIGRATED_SLOT, "IMPORTING", node2_id)
            node1.execute_command("CLUSTER", "SETSLOT", MIGRATED_SLOT, "NODE", node1_id)
            node2.execute_command("CLUSTER", "SETSLOT", MIGRATED_SLOT, "NODE", node1_id)
        except ResponseError as e:
            pytest.skip(f"slot migration unsupported in this environment: {e}")

        # Wait until the receivers observe the new ownership, so their refresh
        # yields the new fingerprint.
        self._wait_until(
            lambda: self._slot_owner_id(node1, MIGRATED_SLOT) == node1_id
            and self._slot_owner_id(node2, MIGRATED_SLOT) == node1_id,
            msg="receivers observe the migrated slot ownership",
        )

        # Node 0 still holds the pre-change map (long expiration, not yet marked
        # stale), so this fanout carries the stale fingerprint and is rejected.
        with pytest.raises(ResponseError, match=r"(?i)topology( has)? changed"):
            node0.execute_command("TS.QUERYINDEX", "name=cpu")

        self._wait_until(
            lambda: self._any_node_log_contains(MISMATCH_LOG_MARKER),
            msg="a receiver logs the fingerprint-mismatch rejection",
        )

        # The rejection marked node 0's map stale; once its own view converges,
        # the retry rebuilds the map and the fanout succeeds.
        self._wait_until(
            lambda: self._slot_owner_id(node0, MIGRATED_SLOT) == node1_id,
            msg="node0's own view converges on the new ownership",
        )
        self._wait_until(
            lambda: node0.execute_command("TS.QUERYINDEX", "name=cpu") == [],
            msg="node0 recovers after refreshing its cluster map",
        )
