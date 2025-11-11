use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use crate::fanout::cluster_api::{get_cluster_shards, NodeId};
use crate::fanout::utils::current_time_millis;
use ahash::AHashMap;
use rand::{rng, Rng};
use range_set_blaze::{RangeMapBlaze, RangeSetBlaze, RangesIter};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv6Addr;
use std::sync::Arc;
use valkey_module::{Context, REDISMODULE_NODE_MASTER, ValkeyResult};

pub const VALKEYMODULE_NODE_MASTER: u32 = REDISMODULE_NODE_MASTER;
// Constants
pub const NUM_SLOTS: usize = 16384;

/// Enumeration for fanout target modes
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FanoutTargetMode {
    // Default: randomly select one node per shard
    #[default]
    Random,
    // Select only replicas, one per shard
    ReplicasOnly,
    // Select all primary (master) nodes
    Primary,
    // Select all nodes (both primary and replica)
    All,
}

/// Node role enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
}

impl Display for NodeRole {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "Primary"),
            NodeRole::Replica => write!(f, "Replica"),
        }
    }
}

/// Node location enumeration
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum NodeLocation {
    #[default]
    Local,
    Remote,
}

impl Display for NodeLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NodeLocation::Local => write!(f, "Local"),
            NodeLocation::Remote => write!(f, "Remote"),
        }
    }
}

/// Socket address for a node. Derived Eq/Hash so it can be used as a map key.
#[derive(Copy, Clone, Debug, Eq)]
pub struct SocketAddress {
    pub primary_endpoint: Ipv6Addr,
    pub port: u16,
}

impl PartialEq for SocketAddress {
    fn eq(&self, other: &Self) -> bool {
        self.primary_endpoint == other.primary_endpoint && self.port == other.port
    }
}

impl Display for SocketAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.primary_endpoint, self.port)
    }
}

impl Hash for SocketAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.primary_endpoint.hash(state);
        self.port.hash(state);
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, Eq)]
pub struct SlotRangeSet {
    ranges: RangeSetBlaze<u16>,
}

impl SlotRangeSet {
    pub fn new() -> Self {
        Self {
            ranges: RangeSetBlaze::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ranges.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn contains(&self, slot: u16) -> bool {
        self.ranges.contains(slot)
    }

    pub fn insert(&mut self, slot: u16) {
        self.ranges.insert(slot);
    }

    pub fn insert_range(&mut self, start: u16, end: u16) {
        assert!(start <= end, "Invalid range: start ({start}) > end ({end})");
        self.ranges.ranges_insert(start..=end);
    }

    pub fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.ranges.iter()
    }

    pub fn range_iter(&self) -> RangesIter<'_, u16> {
        self.ranges.ranges()
    }

    pub fn append(&mut self, other: &SlotRangeSet) {
        for range in other.range_iter() {
            self.ranges.ranges_insert(range.clone());
        }
    }

    pub fn first(&self) -> Option<u16> {
        self.ranges.first()
    }

    pub fn last(&self) -> Option<u16> {
        self.ranges.last()
    }
}

impl Display for SlotRangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.ranges.fmt(f)
    }
}

/// Information about a cluster node
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub socket_address: SocketAddress,
    pub role: NodeRole,
    pub location: NodeLocation,
}

impl NodeInfo {
    pub fn create(
        id: NodeId,
        addr: Ipv6Addr,
        port: u16,
        role: NodeRole,
        location: NodeLocation,
    ) -> Self {
        let socket_address = SocketAddress {
            primary_endpoint: addr,
            port,
        };

        Self {
            id,
            role,
            location,
            socket_address,
        }
    }

    pub fn address(&self) -> String {
        if self.location == NodeLocation::Local {
            String::new()
        } else {
            self.socket_address.to_string()
        }
    }
}

impl PartialOrd for NodeInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for NodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Information about a shard in the cluster
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    /// shard_id is the primary node id
    pub shard_id: String,
    /// the primary node can be None
    pub primary: Option<NodeInfo>,
    pub replicas: Vec<NodeInfo>,
    pub owned_slots: SlotRangeSet,
    /// Whether this shard is local
    pub is_local: bool,
    /// Hash of an owned_slots set
    pub slots_fingerprint: u64,
    pub is_consistent: bool,
}

impl ShardInfo {
    pub fn get_random_target(&self) -> NodeInfo {
        let mut rng_ = rng();
        let mut total_nodes = self.replicas.len();
        let mut has_primary = false;
        if self.primary.is_some() {
            has_primary = true;
            total_nodes += 1;
        }
        assert!(total_nodes > 0, "Shard has no nodes to select from");
        let index = rng_.random_range(0..total_nodes);
        if has_primary && self.replicas.is_empty() {
            // Only primary exists, always return it
            debug_assert!(index == 0, "Index should be 0 when only primary exists");
            return self.primary.unwrap();
        }
        if index == 0 && has_primary {
            self.primary.unwrap()
        } else {
            let replica_index = if has_primary { index - 1 } else { index };
            // Defensive: check bounds before accessing
            if replica_index >= self.replicas.len() {
                panic!("Replica index {} out of bounds for replicas of length {}", replica_index, self.replicas.len());
            }
            self.replicas[replica_index]
        }
    }
}

/// Main cluster map structure
#[derive(Debug, Default, Clone)]
pub struct ClusterMap {
    /// Slot set for slots owned by the cluster
    owned_slots: SlotRangeSet,
    shards: AHashMap<String, Arc<ShardInfo>>,
    /// A range map, mapping(start slot, end slot) => ShardInfo
    slot_to_shard_map: RangeMapBlaze<u16, Arc<ShardInfo>>,
    /// Cluster-level fingerprint (hash of all shard fingerprints)
    cluster_slots_fingerprint: u64,
    /// Pre-computed target lists
    primary_targets: Arc<Vec<NodeInfo>>,
    replica_targets: Arc<Vec<NodeInfo>>,
    all_targets: Arc<Vec<NodeInfo>>,
    /// expiration timestamp in ms (since epoch)
    expiration_ts: i64,
    /// is the current map consistent (no collisions/inconsistencies found while building)
    pub is_consistent: bool,
    /// Whether the cluster map covers all slots consecutively
    pub is_cluster_map_full: bool,
}

impl ClusterMap {
    /// Return pre-generated target vectors
    pub fn primary_targets(&self) -> &[NodeInfo] {
        &self.primary_targets
    }

    pub fn replica_targets(&self) -> &[NodeInfo] {
        &self.replica_targets
    }

    pub fn all_targets(&self) -> &[NodeInfo] {
        &self.all_targets
    }

    /// Generate random targets vector from the cluster bus
    pub fn get_random_targets(&self) -> Arc<Vec<NodeInfo>> {
        self.get_targets(FanoutTargetMode::Random)
    }

    /// Slot ownership checks
    pub fn i_own_slot(&self, slot: u16) -> bool {
        self.owned_slots.contains(slot)
    }

    /// Get the count of owned slots
    pub fn owned_slot_count(&self) -> usize {
        self.owned_slots.len()
    }

    /// Look up a shard by id. Will return None if shard does not exist
    pub fn get_shard_by_id(&self, shard_id: &str) -> Option<&ShardInfo> {
        self.shards.get(shard_id).map(|shard_info| shard_info.as_ref())
    }

    pub fn get_shard_by_slot(&self, slot: u16) -> Option<&ShardInfo> {
        self.slot_to_shard_map.get(slot).map(|shard| shard.as_ref())
    }

    /// Get all shards
    pub fn all_shards(&self) -> &AHashMap<String, Arc<ShardInfo>> {
        &self.shards
    }

    /// Get cluster level slot fingerprint
    pub fn cluster_slots_fingerprint(&self) -> u64 {
        self.cluster_slots_fingerprint
    }

    /// Helper function to refresh targets in CreateNewClusterMap
    pub fn get_targets(&self, target_mode: FanoutTargetMode) -> Arc<Vec<NodeInfo>> {
        match target_mode {
            FanoutTargetMode::Primary => self.primary_targets.clone(),
            FanoutTargetMode::ReplicasOnly => self.replica_targets.clone(),
            FanoutTargetMode::All => self.all_targets.clone(),
            FanoutTargetMode::Random => {
                // generate a random targets vector with one node from each shard
                let mut targets: Vec<NodeInfo> = self.all_shards()
                    .values()
                    .map(|shard| shard.get_random_target())
                    .collect();
                // sort targets for consistency
                targets.sort();
                Arc::new(targets)
            }
        }
    }

    /// Builder method to create a ClusterMap (alternative to CreateNewClusterMap)
    pub fn build_from_shards(shards: Vec<ShardInfo>) -> Self {
        let mut owned_slots = SlotRangeSet::new();
        let mut shards_map = AHashMap::new();
        let mut slot_to_shard_map: RangeMapBlaze<u16, Arc<ShardInfo>> = RangeMapBlaze::new();
        let mut primary_targets = Vec::new();
        let mut replica_targets = Vec::new();
        let mut all_targets = Vec::new();
        let mut is_consistent = true;

        // Process each shard
        for shard in shards {
            let shard = Arc::new(shard);
            // Mark owned slots
            if shard.is_local {
                owned_slots.append(&shard.owned_slots);
            }

            // Update slot to shard map.
            for slot in shard.owned_slots.range_iter() {
                slot_to_shard_map.ranges_insert(slot, Arc::clone(&shard));
            }

            // Add to targets
            if let Some(primary) = shard.primary {
                primary_targets.push(primary);
                all_targets.push(primary);
            } else {
                is_consistent = false;
            }

            for replica in shard.replicas.iter() {
                replica_targets.push(*replica);
                all_targets.push(*replica);
            }

            shards_map.insert(shard.shard_id.clone(), shard);
        }

        // Calculate cluster fingerprint
        let cluster_slots_fingerprint = {
            let mut hasher = DefaultHasher::new();
            let mut shard_fingerprints: Vec<u64> = shards_map
                .values()
                .map(|shard| shard.slots_fingerprint)
                .collect();
            shard_fingerprints.sort_unstable();
            shard_fingerprints.hash(&mut hasher);
            hasher.finish()
        };

        // Check if the cluster has all slots assigned
        let is_cluster_map_full = slot_to_shard_map.len() == NUM_SLOTS as u32;

        // Sort targets for consistency
        primary_targets.sort();
        replica_targets.sort();
        all_targets.sort();

        let expiration_ms = CLUSTER_MAP_EXPIRATION_MS.load(std::sync::atomic::Ordering::Relaxed);
        let expiration_ts = current_time_millis() + expiration_ms as i64;

        Self {
            owned_slots,
            slot_to_shard_map,
            shards: shards_map,
            cluster_slots_fingerprint,
            primary_targets: Arc::new(primary_targets),
            replica_targets: Arc::new(replica_targets),
            all_targets: Arc::new(all_targets),
            expiration_ts,
            is_consistent,
            is_cluster_map_full,
        }
    }

    /// Create a new cluster map using the CLUSTER SHARDS command
    pub fn create(ctx: &Context) -> ValkeyResult<Self> {
        let (shards, consistent) = get_cluster_shards(ctx)?;

        let mut new_map = Self::build_from_shards(shards);

        #[cfg(debug_assertions)]
        {
            // Log cluster map state
            log::info!("=== Cluster Map Created (CLUSTER SHARDS) ===");
            log::info!("is_cluster_map_full: {}", new_map.is_cluster_map_full);
            log::info!("owned_slots count: {}", new_map.owned_slots.len());

            if new_map.owned_slot_count() > 0 {
                log::info!("owned_slots ranges: {}", &new_map.owned_slots);
            }

            log::info!("shards count: {}", new_map.shards.len());
            for (shard_id, shard_info) in &new_map.shards {
                log::info!("Shard ID: {shard_id}");
                log::info!("  owned_slots count: {}", shard_info.owned_slots.len());
                if !shard_info.owned_slots.is_empty() {
                    log::info!("  slot ranges: {}", shard_info.owned_slots);
                }
            }

            log::info!("=== End Cluster Map ===");
        }

        new_map.is_consistent = consistent;

        Ok(new_map)
    }

    /// Convenience: is the cluster map expired?
    pub fn is_expired(&self) -> bool {
        current_time_millis() >= self.expiration_ts
    }
}

// Unit tests
#[cfg(test)]
mod tests {}