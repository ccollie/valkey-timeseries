use crate::fanout::cluster_map::{NodeId, NodeLocation, NodeRole};
use std::hash::{Hash, Hasher};
use std::net::Ipv6Addr;
use std::os::raw::{c_char, c_int};
use std::sync::LazyLock;
use valkey_module::{
    Context, VALKEYMODULE_NODE_FAIL, VALKEYMODULE_NODE_ID_LEN, VALKEYMODULE_NODE_MYSELF,
    VALKEYMODULE_NODE_PFAIL, VALKEYMODULE_NODE_PRIMARY, VALKEYMODULE_OK,
    ValkeyModule_GetClusterNodeInfo, ValkeyModule_GetMyClusterID, ValkeyModuleCtx,
};

/// Maximum length of an IPv6 address string
pub const INET6_ADDR_STR_LEN: usize = 46;

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
    pub id: NodeId,
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
        self.id.hash(state);
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
        id: NodeId::from_raw(node_id),
        ip_buf,
        port,
        master_id: NodeId::from_raw(master_buf.as_ptr() as *const c_char),
        flags,
        role,
        location,
    })
}

pub(super) fn get_node_info(ctx: &Context, node_id: *const c_char) -> Option<RawNodeInfo> {
    let node_info = get_cluster_node_info(ctx, node_id)?;
    if node_info.is_failed() {
        log::debug!(
            "Node {} ({}) is failing, skipping for fanout...",
            node_info.id,
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
