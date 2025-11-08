use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use crate::fanout::cluster_api::get_cluster_shards;
use crate::fanout::utils::current_time_millis;
use ahash::AHashMap;
use hi_sparse_bitset::BitSet;
use rand::prelude::IndexedRandom;
use rand::rng;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_char;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv6Addr;
use std::sync::Arc;
use valkey_module::{Context, REDISMODULE_NODE_MASTER, VALKEYMODULE_NODE_ID_LEN, ValkeyResult};

pub const VALKEYMODULE_NODE_MASTER: u32 = REDISMODULE_NODE_MASTER;
// Constants
pub const NUM_SLOTS: usize = 16384;
pub const INET6_ADDR_STR_LEN: u32 = 46; // Max length for IPv6 string representation

pub type NodeIdBuf = [u8; (VALKEYMODULE_NODE_ID_LEN as usize) + 1]; // +1 for null terminator

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

/// Information about a cluster node
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeIdBuf,
    pub socket_address: SocketAddress,
    pub role: NodeRole,
    pub location: NodeLocation,
}

impl NodeInfo {
    pub fn local(node_id: String, role: NodeRole) -> Self {
        let mut id_buf = [0u8; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
        let bytes = node_id.as_bytes();
        let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
        id_buf[..len].copy_from_slice(&bytes[..len]);
        let addr = SocketAddress {
            primary_endpoint: Ipv6Addr::LOCALHOST,
            port: 6379,
        };

        Self {
            role,
            location: NodeLocation::Local,
            socket_address: addr,
            id: id_buf,
        }
    }

    pub fn address(&self) -> String {
        if self.location == NodeLocation::Local {
            String::new()
        } else {
            self.socket_address.to_string()
        }
    }

    pub fn node_id(&self) -> &str {
        // SAFETY: id_buf is always valid UTF-8 as it is copied from valid node ID strings
        unsafe { std::str::from_utf8_unchecked(&self.id[..VALKEYMODULE_NODE_ID_LEN as usize]) }
    }

    pub(super) fn raw_id_ptr(&self) -> *const c_char {
        // SAFETY: id is a null-terminated byte array
        self.id.as_ptr() as *const c_char
    }
}

impl Display for NodeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeInfo{{role: {:?}, location: {:?}, address: {}}}",
            self.role,
            self.location,
            self.address()
        )
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
#[derive(Debug, Default, Clone)]
pub struct ShardInfo {
    /// shard_id is the primary node id
    pub shard_id: String,
    /// the primary node can be None
    pub primary: Option<NodeInfo>,
    pub replicas: Vec<NodeInfo>,
    pub owned_slots: BTreeSet<u16>,
    /// Whether this shard is local
    pub is_local: bool,
    /// Hash of an owned_slots set
    pub slots_fingerprint: u64,
}

pub type SparseBitSetIter<'a> =
    hi_sparse_bitset::iter::IndexIter<&'a BitSet<hi_sparse_bitset::config::_64bit>>;

/// A dynamic sparse bitset to track owned slots across the cluster
#[derive(Debug, Clone)]
pub struct SparseBitSet(BitSet<hi_sparse_bitset::config::_64bit>, usize);

impl SparseBitSet {
    pub fn new() -> Self {
        Self(BitSet::new(), 0)
    }

    pub fn len(&self) -> usize {
        self.1
    }

    pub fn is_empty(&self) -> bool {
        self.1 == 0
    }

    pub fn insert(&mut self, index: usize) {
        if !self.0.contains(index) {
            self.0.insert(index);
            self.1 += 1;
        }
    }

    pub fn remove(&mut self, index: usize) -> bool {
        if self.0.remove(index) {
            self.1 -= 1;
            return true;
        }
        false
    }

    pub fn contains(&self, index: usize) -> bool {
        self.0.contains(index)
    }

    pub fn iter(&'_ self) -> SparseBitSetIter<'_> {
        self.0.iter()
    }
}

impl Default for SparseBitSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for SparseBitSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut ranges = Vec::new();
        let mut range_start = 0;
        let mut range_end = 0;

        let mut first = false;
        for i in self.iter() {
            if !first {
                first = true;
                range_start = i;
                range_end = i;
            } else if i == range_end + 1 {
                range_end = i;
                continue;
            } else {
                ranges.push((range_start as u16, range_end as u16));
                range_start = i;
                range_end = i;
            }
        }

        if first {
            ranges.push((range_start as u16, (NUM_SLOTS - 1) as u16));
        }

        write!(f, "{}", format_ranges(&ranges))
    }
}

/// Main cluster map structure
#[derive(Debug, Default, Clone)]
pub struct ClusterMap {
    /// Bit vector where 1: slot is owned by this cluster, 0: slot is not owned by this cluster
    owned_slots: SparseBitSet,
    shards: AHashMap<String, ShardInfo>,
    // An ordered map, key is start slot, value is end slot and ShardInfo
    slot_to_shard_map: BTreeMap<u16, (u16, Arc<ShardInfo>)>,
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
        self.owned_slots.contains(slot as usize)
    }

    /// Get the count of owned slots
    pub fn owned_slot_count(&self) -> usize {
        self.owned_slots.len()
    }

    /// Look up a shard by id. Will return None if shard does not exist
    pub fn get_shard_by_id(&self, shard_id: &str) -> Option<&ShardInfo> {
        self.shards.get(shard_id)
    }

    pub fn get_shard_by_slot(&self, slot: u16) -> Option<&ShardInfo> {
        self.slot_to_shard_map
            .range(..=slot)
            .next_back()
            .and_then(|(start, (end, shard))| {
                if slot >= *start && slot <= *end {
                    Some(shard.as_ref())
                } else {
                    None
                }
            })
    }

    /// Get all shards
    pub fn all_shards(&self) -> &AHashMap<String, ShardInfo> {
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
                let shards = self.all_shards();
                let mut rng_ = rng();
                let mut targets: Vec<NodeInfo> = Vec::with_capacity(shards.len());
                for shard in shards.values() {
                    if shard.replicas.is_empty() {
                        // return primary if no replicas
                        if let Some(primary) = shard.primary {
                            targets.push(primary);
                        }
                    } else if let Some(choice) = shard.replicas.as_slice().choose(&mut rng_) {
                        targets.push(*choice);
                    }
                }
                // sort targets for consistency
                targets.sort();
                Arc::new(targets)
            }
        }
    }

    /// Builder method to create a ClusterMap (alternative to CreateNewClusterMap)
    pub fn build_from_shards(shards: Vec<ShardInfo>) -> Self {
        let mut owned_slots = SparseBitSet::new();
        let mut shards_map = AHashMap::new();
        let mut slot_to_shard_map = BTreeMap::new();
        let mut primary_targets = Vec::new();
        let mut replica_targets = Vec::new();
        let mut all_targets = Vec::new();

        // Process each shard
        for shard in shards {
            let shard_ = Arc::new(shard.clone());
            // Mark owned slots
            if shard.is_local {
                for &slot in &shard.owned_slots {
                    owned_slots.insert(slot as usize);
                }
            }
            let start_slot = *shard.owned_slots.iter().next().unwrap_or(&0);
            let end_slot = *shard.owned_slots.iter().last().unwrap_or(&0);
            // Update slot to shard map
            slot_to_shard_map.insert(start_slot, (end_slot, Arc::clone(&shard_)));

            // Add to targets
            if let Some(primary) = shard.primary {
                primary_targets.push(primary);
                all_targets.push(primary);
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
        let is_cluster_map_full = owned_slots.len() == NUM_SLOTS;

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
            is_consistent: true,
            is_cluster_map_full,
        }
    }

    /// Create a new cluster map using the CLUSTER SHARDS command
    pub fn create(ctx: &Context) -> ValkeyResult<Self> {
        let shards = get_cluster_shards(ctx)?;

        let new_map = Self::build_from_shards(shards);

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
                    let slot_ranges = format_slot_set_ranges(&shard_info.owned_slots);
                    log::info!("  slot ranges: {slot_ranges}");
                }
            }

            log::info!("=== End Cluster Map ===");
        }

        Ok(new_map)
    }

    /// Convenience: is the cluster map expired?
    pub fn is_expired(&self) -> bool {
        current_time_millis() >= self.expiration_ts
    }
}

/// Helper to format slot set ranges for logging
fn format_slot_set_ranges(slots: &BTreeSet<u16>) -> String {
    let mut sorted_slots: Vec<u16> = slots.iter().copied().collect();
    sorted_slots.sort_unstable();

    let mut ranges: Vec<(u16, u16)> = Vec::new();
    let range_start: Option<(u16, u16)> = None;

    for &slot in &sorted_slots {
        match range_start {
            Some((_start, end)) if slot == end + 1 => {
                ranges.last_mut().unwrap().1 = slot;
            }
            _ => {
                ranges.push((slot, slot));
            }
        }
    }

    format_ranges(&ranges)
}

fn format_ranges(ranges: &[(u16, u16)]) -> String {
    ranges
        .iter()
        .map(|&(start, end)| {
            if start == end {
                format!("{start}")
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// Unit tests
#[cfg(test)]
mod tests {}
