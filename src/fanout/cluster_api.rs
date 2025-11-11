use std::borrow::Borrow;
use std::fmt;
use std::fmt::Display;
use crate::fanout::cluster_map::{
    NodeInfo, NodeLocation, NodeRole, ShardInfo, SlotRangeSet,
};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv6Addr;
use std::os::raw::{c_char, c_int};
use std::sync::LazyLock;
use valkey_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, VALKEYMODULE_NODE_FAIL,
    VALKEYMODULE_NODE_ID_LEN, VALKEYMODULE_NODE_MYSELF, VALKEYMODULE_NODE_PFAIL,
    VALKEYMODULE_NODE_PRIMARY, VALKEYMODULE_OK, ValkeyError, ValkeyModule_GetClusterNodeInfo,
    ValkeyModule_GetMyClusterID, ValkeyModuleCtx, ValkeyResult,
};

/// Maximum length of an IPv6 address string
pub const INET6_ADDR_STR_LEN: usize = 46;

pub type NodeIdBuf = [u8; (VALKEYMODULE_NODE_ID_LEN as usize) + 1]; // +1 for null terminator

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct NodeId(NodeIdBuf);

impl NodeId {
    pub fn from_raw(node_id_ptr: *const c_char) -> Self {
        let mut buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
        // SAFETY: node_id_ptr is expected to be a valid pointer to a 40-byte node ID
        let bytes = if node_id_ptr.is_null() {
            &[]
        } else {
            unsafe {
                std::slice::from_raw_parts(node_id_ptr as *const u8, VALKEYMODULE_NODE_ID_LEN as usize)
            }
        };
        let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
        buf[..len].copy_from_slice(&bytes[..len]);
        NodeId(buf)
    }

    pub(super) fn from_bytes(bytes: &[u8]) -> Self {
        let mut buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
        let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
        buf[..len].copy_from_slice(&bytes[..len]);
        NodeId(buf)
    }

    pub fn raw_ptr(&self) -> *const c_char {
        // SAFETY: self.0 is a null-terminated byte array
        self.0.as_ptr() as *const c_char
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: self.0 is always valid UTF-8 as it is copied from valid node ID strings
        unsafe { std::str::from_utf8_unchecked(&self.0[..VALKEYMODULE_NODE_ID_LEN as usize]) }
    }

    pub fn is_empty(&self) -> bool {
        self.0[0] == 0
    } 
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        // SAFETY: self.0 is always valid UTF-8 as it is copied from valid node ID strings
        unsafe { std::str::from_utf8_unchecked(&self.0[..VALKEYMODULE_NODE_ID_LEN as usize]) }
    }
}

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        &self.0[..VALKEYMODULE_NODE_ID_LEN as usize]
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        NodeId([0; VALKEYMODULE_NODE_ID_LEN as usize + 1])
    }
}


/// Static buffer holding the current node's ID
pub static CURRENT_NODE_ID: LazyLock<NodeId> = LazyLock::new(||
    // Safety: We ensure that the buffer is properly initialized with the current node ID
    unsafe {
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();
        NodeId::from_raw(node_id)
    });

#[derive(Debug, Clone)]
pub(super) struct RawNodeInfo {
    pub node_id: NodeId,
    ip_buf: [u8; INET6_ADDR_STR_LEN],
    pub master_id: NodeId,
    #[allow(unused_variables)]
    pub flags: u32,
    pub port: u32,
    pub role: NodeRole,
    pub location: NodeLocation,
}

impl Hash for RawNodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

impl RawNodeInfo {
    pub fn is_failed(&self) -> bool {
        self.flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL) != 0
    }

    pub fn addr(&self) -> Ipv6Addr {
        let ip_str = self.ip();
        // Parse the string as an IPv6 address
        ip_str.parse::<Ipv6Addr>().unwrap_or(Ipv6Addr::LOCALHOST)
    }

    pub fn ip(&self) -> &str {
        // Find the null terminator or use the full length
        let end = self
            .ip_buf
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(INET6_ADDR_STR_LEN);
        // Convert bytes to string slice
        std::str::from_utf8(&self.ip_buf[..end]).unwrap_or("::1")
    }
}

/// Fetches detailed information about a specific cluster node given its ID.
/// Returns `None` if the node information cannot be retrieved.
pub fn get_cluster_node_info(ctx: &Context, node_id: *const c_char) -> Option<RawNodeInfo> {
    let mut master_buf: [u8; VALKEYMODULE_NODE_ID_LEN as usize + 1] =
        [0; VALKEYMODULE_NODE_ID_LEN as usize + 1];
    let mut ip_buf: [u8; INET6_ADDR_STR_LEN] = [0; INET6_ADDR_STR_LEN];
    let mut port: c_int = 0;
    let mut flags_: c_int = 0;

    let added = unsafe {
        ValkeyModule_GetClusterNodeInfo
            .expect("ValkeyModule_GetClusterNodeInfo function is unavailable")(
            ctx.ctx as *mut ValkeyModuleCtx,
            node_id,
            ip_buf.as_mut_ptr() as *mut c_char,
            master_buf.as_mut_ptr() as *mut c_char,
            &mut port,
            &mut flags_,
        ) == VALKEYMODULE_OK as c_int
    };

    if !added {
        log::debug!("Failed to get node info for node {node_id:?}, skipping node...");
        return None;
    }

    let flags = flags_ as u32;
    let port = port as u32;
    let role = if flags & VALKEYMODULE_NODE_PRIMARY != 0 {
        NodeRole::Primary
    } else {
        NodeRole::Replica
    };

    let location = if flags & VALKEYMODULE_NODE_MYSELF != 0 {
        NodeLocation::Local
    } else {
        NodeLocation::Remote
    };

    Some(RawNodeInfo {
        node_id: NodeId::from_raw(node_id),
        ip_buf,
        port,
        master_id: NodeId::from_raw(master_buf.as_ptr() as *const c_char),
        flags,
        role,
        location,
    })
}

/// Fetches the cluster shard information by calling the `CLUSTER SHARDS` command.
///
/// https://valkey.io/commands/cluster-shards/
///
pub fn get_cluster_shards(ctx: &Context) -> ValkeyResult<(Vec<ShardInfo>, bool)> {
    // Call CLUSTER SHARDS from Valkey Module API
    let call_options = CallOptionsBuilder::new()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();

    let res: CallReply = ctx
        .call_ext::<_, CallResult>("CLUSTER", &call_options, &["SHARDS"])
        .map_err(|e| -> ValkeyError { e.into() })?;

    let mut shard_infos: Vec<ShardInfo> = Vec::with_capacity(16);
    let mut map_consistent = true;

    assert!(
        !matches!(res, CallReply::Null(_)),
        "CLUSTER_MAP_ERROR: CLUSTER SHARDS call returns null"
    );

    let shards_array = match res {
        CallReply::Array(arr) => arr,
        _ => {
            log::warn!("CLUSTER_MAP_ERROR: CLUSTER SHARDS call returns non-array reply");
            return Ok((shard_infos, false));
        }
    };

    // Get local node ID to identify which shard we belong to
    let my_node_id =
        get_current_node_id().expect("CLUSTER_MAP_ERROR: Failed to get current node ID");

    // Parse each shard in the response
    // Format: Array of maps/arrays, each containing:
    //   - "slots": array of slot ranges [start, end]
    //   - "nodes": array of node information
    //   - "id": shard ID
    for shard_reply in shards_array.iter() {
        let Ok(shard_reply) = shard_reply else {
            log::warn!("CLUSTER_MAP_ERROR: error getting shard reply");
            map_consistent = false;
            continue;
        };

        let mut is_consistent = true;

        // Extract shard ID
        let shard_id = get_map_field_as_string(&shard_reply, "id")
            .expect("CLUSTER_MAP_ERROR: Shard entry missing required 'id' field");

        // Extract slots
        let slots_array = match get_map_entry(&shard_reply, "slots")
            .and_then(|reply| {
                if let CallReply::Array(arr) = reply {
                    Some(arr)
                } else {
                    None
                }
            }) {
            Some(arr) => arr,
            None => {
                log::warn!("CLUSTER_MAP_ERROR: Shard entry missing 'slots' field");
                map_consistent = false;
                continue;
            }
        };

        // Parse slot ranges
        let mut owned_slots = SlotRangeSet::new();
        let mut i = 0;
        while i < slots_array.len() {
            // Slots are pairs of [start, end]
            let start = get_reply_as_integer(slots_array.get(i), -1);
            let end = get_reply_as_integer(slots_array.get(i + 1), -1);

            if start >= 0 && end >= 0 && start <= end {
                owned_slots.insert_range(start as u16, end as u16);
            } else {
                log::warn!(
                    "CLUSTER_MAP_ERROR: Invalid slot range [{start}, {end}] in shard {shard_id}"
                );
                map_consistent = false;
                is_consistent = false;
            }
            i += 2;
        }

        // Extract nodes
        let nodes_array = get_map_entry(&shard_reply, "nodes")
            .and_then(|reply| {
                if let CallReply::Array(arr) = reply {
                    Some(arr)
                } else {
                    None
                }
            })
            .expect("CLUSTER_MAP_ERROR: Shard missing 'nodes' field");

        // Parse nodes
        let mut primary: Option<NodeInfo> = None;
        let mut replicas: Vec<NodeInfo> = Vec::new();

        let mut is_local = shard_id == my_node_id.as_str();

        for node_reply in nodes_array.iter() {
            let node_reply = node_reply.expect("CLUSTER_MAP_ERROR: error getting node reply");

            let Some(node) = parse_node_info(&node_reply, &my_node_id) else {
                is_consistent = false;
                map_consistent = false;
                continue;
            };

            if node.location == NodeLocation::Local {
                is_local = true;
            }

            match node.role {
                NodeRole::Primary => primary = Some(node),
                NodeRole::Replica => {
                    replicas.push(node);
                }
            }
        }

        // Calculate slots fingerprint
        let slots_fingerprint = calculate_slots_fingerprint(&owned_slots);

        // Create ShardInfo
        let shard_info = ShardInfo {
            shard_id,
            primary,
            is_local,
            replicas,
            owned_slots,
            slots_fingerprint,
            is_consistent,
        };

        shard_infos.push(shard_info);
    }

    Ok((shard_infos, map_consistent))
}

// Helper function to parse node info from CLUSTER SHARDS reply
fn parse_node_info(node_reply: &CallReply, my_node_id: &NodeId) -> Option<NodeInfo> {
    // Extract node ID
    let node_id_str = get_map_field_as_string(node_reply, "id")
        .expect("CLUSTER_MAP_ERROR: Node entry missing required 'id' field");

    let node_id = NodeId::from_bytes(node_id_str.as_bytes());

    // Extract role
    let role_str = get_map_field_as_string(node_reply, "role")
        .expect("CLUSTER_MAP_ERROR: Node entry missing required 'role' field");

    let role = match role_str.as_str() {
        "master" | "primary" => NodeRole::Primary,
        "replica" | "slave" => NodeRole::Replica,
        _ => {
            log::warn!("CLUSTER_MAP_ERROR: Unknown role '{role_str}' for node {node_id}, skipping node");
            return None;
        }
    };

    // First pass: determine if this is a local shard by checking ALL nodes
    let is_local = &node_id == my_node_id;

    // extract port info. Either port or tls-port will be present
    let Some(port) = get_map_field_as_integer(node_reply, "port")
        .or_else(|| get_map_field_as_integer(node_reply, "tls-port")) else {
        log::warn!("CLUSTER SHARDS: Node {node_id} entry missing 'port' or 'tls-port' field");
        return None;
    };

    // Try to get an endpoint from the response
    let endpoint = get_map_field_as_string(node_reply, "endpoint");
    let Some(endpoint) = endpoint.as_ref().filter(|&s| !s.is_empty() && s != "?") else {
        log::warn!("Node {node_id} has an invalid node endpoint.");
        return None;
    };

    let Ok(addr) = endpoint.parse::<Ipv6Addr>() else {
        log::warn!("Node {node_id} has an invalid IPv6 address: {endpoint}");
        return None;
    };

    let health = get_map_field_as_string(node_reply, "health")
        .expect("CLUSTER_MAP_ERROR: Node entry missing required 'health' field");
    // valid values are "online", "failed", "loading"
    if health != "online" {
        log::debug!("Node {node_id} is not online (health={health}), skipping ...");
        return None;
    }

    let location = if is_local {
        NodeLocation::Local
    } else {
        NodeLocation::Remote
    };

    let node = NodeInfo::create(node_id, addr, port as u16, role, location);

    Some(node)
}

/// Helper method to calculate slot fingerprint
fn calculate_slots_fingerprint(slots: &SlotRangeSet) -> u64 {
    let mut hasher = DefaultHasher::new();
    slots.hash(&mut hasher);
    hasher.finish()
}

fn get_reply_as_integer(elem: Option<CallResult>, default_value: i64) -> i64 {
    match elem {
        Some(Ok(CallReply::I64(val))) => val.to_i64(),
        _ => default_value,
    }
}

// Helper functions for parsing CLUSTER SHARDS response

/// Get a field from a map reply as a string
fn get_map_field_as_string<'a>(
    map: &CallReply<'a>,
    field_name: &str,
) -> Option<String> {
    let entry = get_map_entry(map, field_name)?;
    if let CallReply::String(key_str) = entry {
        return key_str.to_string();
    }
    None
}

fn get_map_field_as_integer<'a>(map: &CallReply<'a>, field_name: &str) -> Option<i64> {
    let entry = get_map_entry(map, field_name)?;
    match entry {
        CallReply::I64(val) => Some(val.to_i64()),
        _ => None,
    }
}

fn get_map_entry<'a>(map: &'a CallReply<'a>, field_name: &str) -> Option<CallReply<'a>> {
    let CallReply::Map(map) = map else {
        return None;
    };
    let field_bytes = field_name.as_bytes();
    for (key, value) in map.iter() {
        let Ok(CallReply::String(key_str)) = key else {
            continue;
        };
        if key_str.as_bytes() != field_bytes {
            continue;
        }
        return match value {
            Ok(v) => Some(v),
            Err(e) => {
                log::warn!("CLUSTER SHARDS: Failed to parse call result for {field_name} : {e}");
                None
            }
        };
    }
    None
}

pub(super) fn get_node_info(ctx: &Context, node_id: *const c_char) -> Option<RawNodeInfo> {
    let node_info = get_cluster_node_info(ctx, node_id)?;
    if node_info.is_failed() {
        log::debug!(
            "Node {} ({}) is failing, skipping for fanout...",
            node_info.node_id,
            node_info.ip()
        );
        return None;
    }
    Some(node_info)
}

pub fn get_current_node_id() -> Option<NodeId> {
    let node_id = *CURRENT_NODE_ID;
    if node_id.is_empty() {
        None
    } else {
        Some(node_id)
    }
}