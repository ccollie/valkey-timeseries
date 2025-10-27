use super::utils::{copy_node_id_from_str, is_client_read_only};
use crate::fanout::cluster_map::{
    FanoutTargetMode, NUM_SLOTS, NodeIdBuf, NodeInfo, NodeLocation, NodeRole, ShardInfo,
    SocketAddress, VALKEYMODULE_NODE_MASTER,
};
use crate::fanout::get_current_node_id;
use std::collections::{BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv6Addr;
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, VALKEYMODULE_NODE_FAIL,
    VALKEYMODULE_NODE_ID_LEN, VALKEYMODULE_NODE_MYSELF, VALKEYMODULE_NODE_PFAIL, VALKEYMODULE_OK,
    ValkeyError, ValkeyModule_GetClusterNodeInfo, ValkeyModuleCtx, ValkeyResult,
};

pub static FORCE_REPLICAS_READONLY: AtomicBool = AtomicBool::new(false);

// master_id and ip are not null terminated, so we add 1 for null terminator for safety
const MASTER_ID_LEN: usize = (VALKEYMODULE_NODE_ID_LEN + 1) as usize;

/// Maximum length of an IPv6 address string
pub const INET6_ADDR_STR_LEN: usize = 46;

/// Helper function to check if the Valkey server version is considered "legacy" (e.g., < 9).
/// In legacy versions, client read-only status might not be reliably determinable.
fn is_valkey_version_legacy(context: &Context) -> bool {
    context
        .get_server_version()
        .is_ok_and(|version| version.major < 9)
}

pub fn compute_query_fanout_mode(context: &Context) -> FanoutTargetMode {
    if FORCE_REPLICAS_READONLY.load(Ordering::Relaxed) {
        // Testing only
        return FanoutTargetMode::ReplicasOnly;
    }
    // Determine fanout mode based on Valkey version and client read-only status.
    // The following logic is based on the issue https://github.com/valkey-io/valkey-search/issues/139
    if is_valkey_version_legacy(context) {
        // Valkey 8 doesn't provide a way to determine if a client is READONLY,
        // So we choose random distribution.
        FanoutTargetMode::Random
    } else {
        match is_client_read_only(context) {
            Ok(true) => FanoutTargetMode::Random,
            Ok(false) => FanoutTargetMode::Primary,
            Err(_) => {
                // If we can't determine client read-only status, default to Random
                log::warn!(
                    "Could not determine client read-only status, defaulting to Random fanout mode."
                );
                FanoutTargetMode::Random
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RawNodeInfo {
    node_buf: NodeIdBuf,
    ip_buf: [u8; INET6_ADDR_STR_LEN],
    master_buf: NodeIdBuf,
    #[allow(unused_variables)]
    pub flags: u32,
    pub port: u32,
    pub role: NodeRole,
    pub location: NodeLocation,
}

impl Hash for RawNodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_buf.hash(state);
    }
}

impl RawNodeInfo {
    pub fn is_failed(&self) -> bool {
        self.flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL) != 0
    }

    pub fn node_id(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.node_buf[..VALKEYMODULE_NODE_ID_LEN as usize])
        }
    }

    pub fn master_id(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(&self.master_buf[..VALKEYMODULE_NODE_ID_LEN as usize])
        }
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
    let mut master_buf: NodeIdBuf = [0; MASTER_ID_LEN];
    let node_buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
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
    let role = if flags & VALKEYMODULE_NODE_MASTER != 0 {
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
        node_buf,
        ip_buf,
        port,
        master_buf,
        flags,
        role,
        location,
    })
}

fn get_node(ctx: &Context, node_id: &str) -> Option<RawNodeInfo> {
    let mut node_buf: NodeIdBuf = [0; (VALKEYMODULE_NODE_ID_LEN as usize) + 1];
    // copy node_id into node_buf
    let bytes = node_id.as_bytes();
    let len = bytes.len().min(VALKEYMODULE_NODE_ID_LEN as usize);
    node_buf[..len].copy_from_slice(&bytes[..len]);
    let node_id_ptr = node_buf.as_ptr() as *const c_char;
    let Some(node_info) = get_cluster_node_info(ctx, node_id_ptr) else {
        #[cfg(debug_assertions)]
        {
            log::debug!("Failed to get node info for node {node_id}, skipping node...");
        }
        return None;
    };

    if node_info.is_failed() {
        log::debug!(
            "Node {node_id} ({}) is failing, skipping for fanout...",
            node_info.ip()
        );
        return None;
    }
    Some(node_info)
}

/// Fetches the cluster shard information by calling the `CLUSTER SHARDS` command.
///
/// https://valkey.io/commands/cluster-shards/
///
pub fn get_cluster_shards(ctx: &Context) -> ValkeyResult<Vec<ShardInfo>> {
    // Call CLUSTER SHARDS from Valkey Module API
    let call_options = CallOptionsBuilder::new()
        .script_mode()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();

    let res: CallReply = ctx
        .call_ext::<_, CallResult>("CLUSTER", &call_options, &["SHARDS"])
        .map_err(|e| -> ValkeyError { e.into() })?;

    let mut shard_infos: Vec<ShardInfo> = Vec::with_capacity(16);

    // map of socket address to node_id used to detect socket collisions
    let mut socket_addr_to_node_map: HashMap<SocketAddress, String>;

    assert!(
        matches!(res, CallReply::Null(_)),
        "CLUSTER_MAP_ERROR: CLUSTER SHARDS call returns null"
    );

    let shards_array = match res {
        CallReply::Array(arr) => arr,
        _ => {
            log::warn!("CLUSTER_MAP_ERROR: CLUSTER SHARDS call returns non-array reply");
            return Ok(shard_infos);
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
            continue;
        };

        assert!(
            matches!(shard_reply, CallReply::Map(_)),
            "Expected map for CLUSTER SHARDS shard reply"
        );

        // Extract shard ID
        let shard_id = get_map_field_as_string(&shard_reply, "id", true).unwrap();

        // Extract slots
        let slots_array = get_map_entry(&shard_reply, "slots")
            .and_then(|reply| {
                if let CallReply::Array(arr) = reply {
                    Some(arr)
                } else {
                    None
                }
            })
            .expect("CLUSTER_MAP_ERROR: Shard entry missing 'slots' field");

        // Parse slot ranges
        let mut owned_slots = BTreeSet::new();
        let mut i = 0;
        while i < slots_array.len() {
            // Slots are pairs of [start, end]
            let start = get_reply_as_integer(slots_array.get(i), -1);
            let end = get_reply_as_integer(slots_array.get(i + 1), -1);

            if start >= 0 && end >= 0 && start <= end {
                for slot in start..=end {
                    if (slot as usize) < NUM_SLOTS {
                        owned_slots.insert(slot as u16);
                    }
                }
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

        let mut is_local = shard_id == my_node_id;

        for node_reply in nodes_array.iter() {
            let node_reply = node_reply.expect("CLUSTER_MAP_ERROR: error getting node reply");

            let Some(node) = parse_node_info(&node_reply, &my_node_id) else {
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
        };

        shard_infos.push(shard_info);
    }

    Ok(shard_infos)
}

// Helper function to parse node info from CLUSTER SHARDS reply
fn parse_node_info(node_reply: &CallReply, my_node_id: &str) -> Option<NodeInfo> {
    // Extract node ID
    let node_id = get_map_field_as_string(&node_reply, "id", true).unwrap();
    let ip = get_map_field_as_string(&node_reply, "ip", true).unwrap();

    // Extract role
    let role_str = get_map_field_as_string(&node_reply, "role", true).unwrap();
    let role = match role_str.as_str() {
        "master" | "primary" => NodeRole::Primary,
        "replica" | "slave" => NodeRole::Replica,
        _ => {
            panic!("CLUSTER_MAP_ERROR: Unknown role '{role_str}' for node {node_id}");
        }
    };

    // First pass: determine if this is a local shard by checking ALL nodes
    let is_local = node_id == my_node_id;

    // extract port info. Either port or tls-port will be present
    let port = get_map_field_as_integer(&node_reply, "port", false).unwrap_or_else(|| {
        get_map_field_as_integer(&node_reply, "tls-port", false)
            .expect("CLUSTER SHARDS: Node entry missing 'port' or 'tls-port' field")
    }) as u16;

    // Try to get an endpoint from the response
    let endpoint = get_map_field_as_string(&node_reply, "endpoint", false);
    let Some(endpoint) = endpoint.as_ref().filter(|&s| !s.is_empty() && s != "?") else {
        log::debug!("Node {node_id} has an invalid node endpoint.");
        return None
    };

    let health = get_map_field_as_string(&node_reply, "health", true).unwrap();
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

    let primary_endpoint = endpoint.parse::<Ipv6Addr>()
        .expect(format!("Invalid IPv6 address for node {node_id}: {endpoint}").as_str());

    let node = NodeInfo {
        id: copy_node_id_from_str(&node_id),
        socket_address: SocketAddress {
            primary_endpoint,
            port
        },
        role,
        location,
    };

    Some(node)
}

/// Helper method to calculate slot fingerprint
fn calculate_slots_fingerprint(slots: &BTreeSet<u16>) -> u64 {
    let mut hasher = DefaultHasher::new();
    slots.hash(&mut hasher);
    hasher.finish()
}

fn get_reply_as_integer(elem: Option<CallResult>, default_value: i64) -> i64 {
    match elem {
        Some(Ok(CallReply::I64(val))) => val.to_i64(),
        Some(Ok(CallReply::String(s))) => {
            let t = s.to_string().unwrap_or_default();
            t.parse::<i64>().unwrap_or(default_value)
        }
        _ => default_value,
    }
}

// Helper functions for parsing CLUSTER SHARDS response

/// Get a field from a map reply as a string
fn get_map_field_as_string<'a>(
    map: &CallReply<'a>,
    field_name: &str,
    required: bool,
) -> Option<String> {
    let entry = get_map_entry(map, field_name)?;
    if let CallReply::String(key_str) = entry {
        return key_str.to_string();
    }
    assert!(
        required,
        "Missing expected {field_name} in CLUSTER SHARDS response"
    );
    None
}

fn get_map_field_as_integer<'a>(
    map: &CallReply<'a>,
    field_name: &str,
    required: bool,
) -> Option<i64> {
    let entry = get_map_entry(map, field_name)?;
    match entry {
        CallReply::I64(val) => Some(val.to_i64()),
        CallReply::String(s) => {
            let t = s.to_string().unwrap_or_default();
            t.parse::<i64>().ok()
        }
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
                let msg = format!(
                    "CLUSTER SHARDS: Failed to parse call result for {field_name} : {}",
                    e.to_string()
                );
                panic!("{}", msg);
            }
        };
    }
    None
}

pub(super) fn get_node_info(ctx: &Context, node_id: *const c_char) -> Option<RawNodeInfo> {
    let Some(node_info) = get_cluster_node_info(ctx, node_id) else {
        #[cfg(debug_assertions)]
        {
            let id = unsafe {
                let str_slice = std::slice::from_raw_parts(
                    node_id as *const u8,
                    VALKEYMODULE_NODE_ID_LEN as usize,
                );
                String::from_utf8_lossy(str_slice).to_string()
            };
            log::debug!("Failed to get node info for node {id}, skipping node...");
        }
        return None;
    };

    if node_info.is_failed() {
        log::debug!(
            "Node {} ({}) is failing, skipping for fanout...",
            node_info.node_id(),
            node_info.ip()
        );
        return None;
    }
    Some(node_info)
}
