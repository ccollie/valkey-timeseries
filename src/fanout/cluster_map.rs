use crate::common::time::current_time_millis;
use crate::config::CLUSTER_MAP_EXPIRATION_MS;
use crate::fanout::cluster_api::{CURRENT_NODE_ID, NodeId};
use ahash::AHashMap;
use log::warn;
use rand::{Rng, rng};
use range_set_blaze::{RangeMapBlaze, RangeSetBlaze, RangesIter};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ffi::c_char;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv6Addr;
use std::sync::Arc;
use valkey_module::{CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context};

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

    /// Helper method to calculate slot fingerprint
    fn calculate_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
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
    pub shard_id: NodeId,
    pub socket_address: SocketAddress,
    pub role: NodeRole,
    pub location: NodeLocation,
}

impl NodeInfo {
    pub fn address(&self) -> String {
        if self.location == NodeLocation::Local {
            String::new()
        } else {
            self.socket_address.to_string()
        }
    }
}

impl PartialOrd for NodeInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeInfo {
    fn cmp(&self, other: &Self) -> Ordering {
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
    /// id is the primary node id
    pub id: NodeId,
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
            self.replicas[replica_index]
        }
    }
}

impl PartialOrd<ShardInfo> for ShardInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShardInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Borrow<NodeId> for ShardInfo {
    fn borrow(&self) -> &NodeId {
        &self.id
    }
}

impl Borrow<str> for ShardInfo {
    fn borrow(&self) -> &str {
        self.id.as_ref()
    }
}

/// Helper describing a parsed slot range before building the slot -> shard map.
#[derive(Copy, Clone)]
struct SlotRangeInfo {
    pub start_slot: u16,
    pub end_slot: u16,
    pub shard_id: NodeId,
}

/// Main cluster map structure
#[derive(Debug, Default, Clone)]
pub struct ClusterMap {
    /// Slot set for slots owned by the cluster
    owned_slots: SlotRangeSet,
    shards: BTreeSet<ShardInfo>,
    /// A range map, mapping(start slot, end slot) => shard id
    slot_to_shard_map: RangeMapBlaze<u16, NodeId>,
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
        self.shards.get(shard_id)
    }

    pub fn get_shard_by_slot(&self, slot: u16) -> Option<&ShardInfo> {
        let shard_id = self.slot_to_shard_map.get(slot)?;
        self.shards.get(shard_id)
    }

    /// Get all shards
    pub fn all_shards(&self) -> &BTreeSet<ShardInfo> {
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
                let mut targets: Vec<NodeInfo> = self
                    .shards
                    .iter()
                    .map(|shard| shard.get_random_target())
                    .collect();
                // sort targets for consistency
                targets.sort();
                Arc::new(targets)
            }
        }
    }

    /// Build a new ClusterMap by calling CLUSTER SLOTS through the provided API
    /// implementation. Returns a ClusterMap.
    pub fn create(ctx: &Context) -> Self {
        let mut new_map = ClusterMap {
            is_consistent: true,
            ..Default::default()
        };

        // Call CLUSTER NODES from Valkey Module API
        let call_options = CallOptionsBuilder::new()
            .resp(CallOptionResp::Resp3)
            .errors_as_replies()
            .build();

        let res: CallResult = ctx.call_ext::<_, CallResult>("CLUSTER", &call_options, &["SLOTS"]);

        let reply = match res {
            Err(e) => {
                panic!("Error calling CLUSTER SLOTS: {e}");
            }
            Ok(CallReply::Array(arr)) => arr,
            _ => {
                panic!("CLUSTER SLOTS did not return an array");
            }
        };

        let my_node_id = &CURRENT_NODE_ID;

        let mut socket_addr_to_node_map: AHashMap<SocketAddress, NodeId> =
            AHashMap::with_capacity(reply.len() / 2 + 1);
        let mut shard_map: AHashMap<NodeId, ShardInfo> =
            AHashMap::with_capacity(reply.len() / 2 + 1);
        let mut slot_ranges_parsed = Vec::new();

        for slot_range in reply.iter().flatten() {
            if !new_map.process_slot_range(
                slot_range,
                my_node_id,
                &mut slot_ranges_parsed,
                &mut shard_map,
                &mut socket_addr_to_node_map,
            ) {
                // dropped range; continue
                continue;
            }
        }

        let mut shard_set: BTreeSet<ShardInfo> = BTreeSet::new();

        // Populate target lists
        let mut primary_targets = Vec::new();
        let mut replica_targets = Vec::new();
        let mut all_targets = Vec::new();

        // Fix shard id references on nodes (back-pointers)
        for (_, shard) in shard_map.into_iter() {
            let mut shard = shard;
            if let Some(primary) = shard.primary.as_mut() {
                primary.shard_id = shard.id;
                primary_targets.push(*primary);
                all_targets.push(*primary);
            }
            for replica in shard.replicas.iter_mut() {
                replica.shard_id = shard.id;
                replica_targets.push(*replica);
                all_targets.push(*replica);
            }

            // compute fingerprint for each shard
            shard.slots_fingerprint = shard.owned_slots.calculate_fingerprint();

            shard_set.insert(shard);
        }

        // Build slot-to-shard map
        new_map.build_slot_to_shard_map(&slot_ranges_parsed);

        // Sort targets for consistency
        primary_targets.sort();
        replica_targets.sort();
        all_targets.sort();

        new_map.primary_targets = Arc::new(primary_targets);
        new_map.replica_targets = Arc::new(replica_targets);
        new_map.all_targets = Arc::new(all_targets);

        // Check if a cluster map is full
        new_map.is_consistent &= new_map.check_cluster_map_full();

        // Compute cluster-level fingerprint
        new_map.cluster_slots_fingerprint = new_map.compute_cluster_fingerprint();

        // Set expiration time
        let expiration_ms = CLUSTER_MAP_EXPIRATION_MS.load(std::sync::atomic::Ordering::Relaxed);
        new_map.expiration_ts = current_time_millis() + expiration_ms as i64;

        new_map
    }

    /// Convenience: is the cluster map expired?
    pub fn is_expired(&self) -> bool {
        current_time_millis() >= self.expiration_ts
    }

    /// Process a single slot range reply. On success, appends a SlotRangeInfo
    /// to `out_slot_ranges` and updates internal structures such as owned_slots
    /// and the shard map. Returns `true` on success and `false` if the slot range
    /// should be skipped.
    fn process_slot_range(
        &mut self,
        slot_range_reply: CallReply,
        my_node_id: &NodeId,
        slot_ranges: &mut Vec<SlotRangeInfo>,
        shard_map: &mut AHashMap<NodeId, ShardInfo>,
        socket_addr_to_node_map: &mut AHashMap<SocketAddress, NodeId>,
    ) -> bool {
        let slot_range_arr = match &slot_range_reply {
            CallReply::Array(arr) => arr,
            _ => {
                warn!("Invalid slot range reply type");
                self.is_consistent = false;
                return false;
            }
        };

        if slot_range_arr.len() < 3 {
            warn!("Slot range reply too short");
            self.is_consistent = false;
            return false;
        }

        // Parse start and end slots (elements 0 and 1)
        let start_reply = slot_range_arr.get(0).unwrap();
        let end_reply = slot_range_arr.get(1).unwrap();

        let start = match reply_as_integer(start_reply) {
            Some(s) if s >= 0 && s <= u16::MAX as i64 => s,
            _ => {
                warn!("Invalid start slot");
                self.is_consistent = false;
                return false;
            }
        };

        let end = match reply_as_integer(end_reply) {
            Some(e) if e >= 0 && e <= u16::MAX as i64 => e,
            _ => {
                warn!("Invalid end slot");
                self.is_consistent = false;
                return false;
            }
        };

        let is_shard_local = is_local_shard(&slot_range_reply, my_node_id);

        // Parse primary node at index 2
        let Some(Ok(primary_arr)) = slot_range_arr.get(2) else {
            warn!("Missing primary node info in slot range [{start}-{end}]");
            self.is_consistent = false;
            return false;
        };

        let Some(mut primary_node) =
            self.parse_node_info(&primary_arr, is_shard_local, true, socket_addr_to_node_map)
        else {
            warn!("Dropping slot range [{start}-{end}] due to invalid primary node");
            self.is_consistent = false;
            return false;
        };

        // Parse replicas
        let mut replicas = Vec::new();
        let slot_len = slot_range_arr.len();
        for j in 3..slot_len {
            if let Some(Ok(replica_arr)) = slot_range_arr.get(j) {
                match self.parse_node_info(
                    &replica_arr,
                    is_shard_local,
                    false,
                    socket_addr_to_node_map,
                ) {
                    Some(replica) => replicas.push(replica),
                    None => {
                        warn!("Skipping invalid replica in slot range [{start}-{end}]");
                        self.is_consistent = false;
                        return false;
                    }
                }
            }
        }

        // Mark owned slots if local
        if is_shard_local {
            if end >= NUM_SLOTS as i64 {
                warn!("Invalid end slot number {end}");
                self.is_consistent = false;
                return false;
            }
            self.owned_slots.insert_range(start as u16, end as u16);
        }

        let shard_id = primary_node.id;
        let shard_entry = shard_map.entry(shard_id).or_insert_with(|| ShardInfo {
            id: shard_id,
            ..Default::default()
        });

        shard_entry
            .owned_slots
            .insert_range(start as u16, end as u16);

        if shard_entry.primary.is_none() {
            // new shard initial fill
            primary_node.id = shard_id;
            shard_entry.primary = Some(primary_node);
            shard_entry.replicas = replicas;
        } else {
            // existing shard -> verify consistency
            let consistent = is_existing_shard_consistent(
                shard_entry,
                shard_entry.primary.as_ref().unwrap(),
                &replicas,
            );
            if !consistent {
                warn!("Inconsistency shard info found on existing slot ranges!");
                self.is_consistent = false;
            }
        }

        slot_ranges.push(SlotRangeInfo {
            start_slot: start as u16,
            end_slot: end as u16,
            shard_id,
        });

        true
    }

    /// Parse node info from the Valkey reply.
    fn parse_node_info(
        &mut self,
        node_reply: &CallReply,
        is_local_shard: bool,
        is_primary: bool,
        socket_addr_to_node_map: &mut AHashMap<SocketAddress, NodeId>,
    ) -> Option<NodeInfo> {
        // Expecting an array-like reply with 4 elements.
        let arr = match node_reply {
            CallReply::Array(arr) => arr,
            _ => {
                warn!("Invalid node reply type while parsing node info");
                return None;
            }
        };
        let len = arr.len();
        assert_eq!(len, 4, "Invalid node reply length while parsing node info");

        // element 0: primary endpoint (string)
        let endpoint_reply = arr.get(0)?;
        let endpoint = reply_as_string(endpoint_reply)?;
        if endpoint.is_empty() || endpoint == "?" {
            warn!("Invalid node primary endpoint");
            return None;
        }

        // element 1: port (integer)
        let port_reply = arr.get(1)?;
        let port_i = reply_as_integer(port_reply)?;
        if port_i <= 0 || port_i > u16::MAX as i64 {
            warn!("Invalid port in node info");
            return None;
        }
        let port = port_i as u16;

        // element 2: node id (string)
        let node_id_reply = arr.get(2)?;
        let node_id = reply_as_node_id(node_id_reply)?;
        if node_id.is_empty() {
            warn!("Invalid node id");
            return None;
        }

        let primary_endpoint = match endpoint.parse::<Ipv6Addr>() {
            Ok(ip) => ip,
            Err(_) => {
                warn!("Invalid IP address format in node info");
                return None;
            }
        };

        let socket_address = SocketAddress {
            primary_endpoint,
            port,
        };

        // Check for duplicate socket addresses across different nodes
        if let Some(existing) = socket_addr_to_node_map.get(&socket_address) {
            if existing != &node_id {
                warn!(
                    "Socket address collision between nodes {node_id} and {existing} at {socket_address}"
                );
                self.is_consistent = false;
            }
        } else {
            socket_addr_to_node_map.insert(socket_address, node_id);
        }

        let location = if is_local_shard {
            NodeLocation::Local
        } else {
            NodeLocation::Remote
        };

        let role = if is_primary {
            NodeRole::Primary
        } else {
            NodeRole::Replica
        };

        let node = NodeInfo {
            id: node_id,
            shard_id: NodeId::default(),
            socket_address,
            role,
            location,
        };

        Some(node)
    }

    fn check_cluster_map_full(&self) -> bool {
        // Ensure the first start is 0
        if self.slot_to_shard_map.is_empty() {
            return false;
        }
        let mut expected_next: u16 = 0;
        for range in self.slot_to_shard_map.ranges() {
            let start_slot = *range.start();
            let end_slot = *range.end();
            if start_slot < expected_next {
                panic!(
                    "Slot {start_slot} overlaps with previous range ending at {}",
                    expected_next - 1
                );
            }
            if start_slot != expected_next {
                warn!(
                    "Slot gap found: slots {expected_next} to {} are not covered",
                    start_slot - 1
                );
                return false;
            }
            expected_next = end_slot.wrapping_add(1);
        }
        expected_next as usize == NUM_SLOTS
    }

    fn build_slot_to_shard_map(&mut self, slot_ranges: &[SlotRangeInfo]) {
        for r in slot_ranges {
            // sanity: shard must exist
            if !self.shards.contains(&r.shard_id) {
                panic!("Shard not found when building slot map: {}", r.shard_id);
            }
            self.slot_to_shard_map
                .ranges_insert(r.start_slot..=r.end_slot, r.shard_id);
        }
    }

    fn compute_cluster_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for shard in self.shards.iter() {
            hasher.write(shard.id.as_bytes());
            shard.slots_fingerprint.hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn is_existing_shard_consistent(
    existing_shard: &ShardInfo,
    new_primary: &NodeInfo,
    new_replicas: &[NodeInfo],
) -> bool {
    if let Some(existing_primary) = &existing_shard.primary {
        if existing_primary.id != new_primary.id {
            return false;
        }
        if existing_primary.socket_address != new_primary.socket_address {
            return false;
        }
        if existing_primary.location != new_primary.location {
            return false;
        }
    }
    if existing_shard.replicas.len() != new_replicas.len() {
        return false;
    }
    for (existing_replica, new_replica) in existing_shard.replicas.iter().zip(new_replicas.iter()) {
        if existing_replica.id != new_replica.id {
            return false;
        }
        if existing_replica.socket_address != new_replica.socket_address {
            return false;
        }
    }
    true
}

fn reply_as_string(call_reply: CallResult) -> Option<String> {
    match call_reply {
        Ok(CallReply::String(v)) => v.to_string(),
        _ => None, // TODO: throw error?
    }
}

fn reply_as_node_id(call_reply: CallResult) -> Option<NodeId> {
    match reply_as_string(call_reply) {
        Some(node_id) => {
            let raw_id = node_id.as_ptr() as *const c_char;
            let id: NodeId = NodeId::from_raw(raw_id);
            Some(id)
        }
        None => None,
    }
}

fn reply_as_integer(call_reply: CallResult) -> Option<i64> {
    match call_reply {
        Ok(CallReply::I64(v)) => Some(v.to_i64()),
        _ => None, // TODO: throw error?
    }
}

/// Returns true if any node listed for the slot range has the local node id.
fn is_local_shard(slot_range_reply: &CallReply, my_node_id: &NodeId) -> bool {
    let CallReply::Array(arr) = slot_range_reply else {
        return false;
    };

    // assumes index 2.. are nodes: 2=primary, 3..=replicas
    let id_bytes = my_node_id.as_bytes();
    for i in 2..arr.len() {
        if let Some(Ok(CallReply::Array(node_arr))) = arr.get(i) {
            if let Some(Ok(CallReply::String(node_id_reply))) = node_arr.get(2) {
                if node_id_reply.as_bytes() == id_bytes {
                    return true;
                }
            }
        }
    }
    false
}

// Unit tests
#[cfg(test)]
mod tests {}
