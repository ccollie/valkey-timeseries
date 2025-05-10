use crate::common::time::valkey_cached_time_millis;
use ahash::AHashMap;
use blart::AsBytes;
use rand::Rng;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::{LazyLock, RwLock, RwLockReadGuard};
use valkey_module::{
    Context, Status, ValkeyModuleCtx, REDISMODULE_NODE_MASTER, VALKEYMODULE_NODE_FAIL,
    VALKEYMODULE_NODE_ID_LEN, VALKEYMODULE_NODE_PFAIL, VALKEYMODULE_OK,
};
use valkey_module::{ContextFlags, ValkeyModule_GetMyClusterID};

// todo: move to config.rs
const CACHE_TIMEOUT: u64 = 5000;

// https://valkey.io/topics/modules-api-ref/#section-modules-cluster-api

type ClusterNodeMap = AHashMap<String, Vec<CString>>;

#[derive(Default)]
struct ClusterMeta {
    last_refresh: i64,
    should_refresh: bool,
    /// map main cluster nodes to nodes in the shard
    nodes: ClusterNodeMap,
}

static CLUSTER_NODES: LazyLock<RwLock<ClusterMeta>> = LazyLock::new(RwLock::default);

/// Sends a message to a specific cluster node.
///
/// # Arguments
///
/// * `target_node_id` - The 40-byte hex ID of the target node.
/// * `msg_type` - The type of the message to send.
/// * `message_body` - The raw byte payload of the message.
///
/// # Returns
///
/// `Status::Ok` on success, `Status::Err` with a message on failure.
pub fn send_cluster_message(
    ctx: &Context,
    target_node_id: *const c_char,
    msg_type: u8,
    message_body: &[u8],
) -> Status {
    unsafe {
        if valkey_module::ValkeyModule_SendClusterMessage
            .expect("ValkeyModule_SendClusterMessage is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            target_node_id,
            msg_type,
            message_body.as_ptr().cast::<c_char>(),
            message_body.len() as u32,
        ) == VALKEYMODULE_OK as c_int
        {
            Status::Ok
        } else {
            Status::Err
        }
    }
}

/// Registers a callback function to handle incoming cluster messages.
/// This should typically be called during module initialization (e.g., ValkeyModule_OnLoad).
///
/// # Arguments
///
/// * `receiver_func` - The function pointer matching the expected signature
///   `ValkeyModuleClusterMessageReceiverFunc`.
///
/// # Safety
///
/// The provided `receiver_func` must be valid for the lifetime of the module
/// and correctly handle the arguments passed by Valkey.
pub fn register_message_receiver(
    ctx: &Context, // Static method as it's usually done once at load time
    type_: u8,
    receiver_func: valkey_module::ValkeyModuleClusterMessageReceiver,
) {
    unsafe {
        valkey_module::ValkeyModule_RegisterClusterMessageReceiver
            .expect("ValkeyModule_RegisterClusterMessageReceiver is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            type_,
            receiver_func,
        );
    }
}

fn acquire_cluster_nodes_lock(ctx: &Context) -> RwLockReadGuard<'static, ClusterMeta> {
    let mut meta = CLUSTER_NODES
        .read()
        .expect("Failed to get cluster info read lock");

    let should_refresh = if !meta.should_refresh {
        let now = valkey_cached_time_millis();
        now - meta.last_refresh > CACHE_TIMEOUT as i64
    } else {
        true
    };

    if should_refresh {
        drop(meta);
        refresh_cluster_nodes(ctx);
        meta = CLUSTER_NODES
            .read()
            .expect("Failed to get cluster info read lock");
    }
    meta
}

pub fn fanout_cluster_message(ctx: &Context, msg_type: u8, payload: &[u8]) -> usize {
    let mut rng = rand::rng();
    let meta = acquire_cluster_nodes_lock(ctx);

    let mut node_count = 0;
    let mut should_refresh = false;
    for (_, targets) in meta.nodes.iter() {
        let index = rng.random_range(0..targets.len());
        let node_id = targets[index].as_ptr();
        if send_cluster_message(ctx, node_id, msg_type, payload) == Status::Err {
            should_refresh = true;
            node_count += 1;
        }
    }

    if should_refresh {
        let mut meta = CLUSTER_NODES
            .write()
            .expect("Failed to get cluster info write lock");
        meta.should_refresh = true;
    }
    node_count
}

// --- Helper methods  ---
pub fn is_multi_or_lua(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::MULTI) || flags.contains(ContextFlags::LUA)
}

pub fn is_clustered(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::CLUSTER)
}

fn refresh_cluster_nodes(ctx: &Context) {
    let mut shard_id_to_target = AHashMap::new();
    unsafe {
        load_targets_for_fanout(ctx, &mut shard_id_to_target);
    }
    let mut meta = CLUSTER_NODES
        .write()
        .expect("Failed to acquire cluster nodes RW lock");
    meta.nodes = shard_id_to_target;
    meta.should_refresh = false;
    meta.last_refresh = valkey_cached_time_millis();
}

/// Retrieves information about the cluster nodes.
unsafe fn load_targets_for_fanout(
    ctx: &Context,
    shard_id_to_target: &mut AHashMap<String, Vec<CString>>,
) {
    let mut num_nodes: usize = 0;
    let nodes_list = unsafe {
        valkey_module::ValkeyModule_GetClusterNodesList
            .expect("ValkeyModule_GetClusterNodesList function is unavailable")(
            ctx.ctx as *mut ValkeyModuleCtx,
            &mut num_nodes,
        )
    };

    if num_nodes == 0 || nodes_list.is_null() {
        return;
    }

    // master_id and ip are not null terminated, so we add 1 for null terminator for safety
    const MASTER_ID_LEN: usize = (VALKEYMODULE_NODE_ID_LEN + 1) as usize;

    let nodes = std::slice::from_raw_parts(nodes_list, num_nodes);
    let mut master_id = String::new();

    for node_id in nodes {
        let mut master_buf: [u8; MASTER_ID_LEN] = [0; MASTER_ID_LEN];
        let mut flags_: c_int = 0;

        let added = unsafe {
            valkey_module::ValkeyModule_GetClusterNodeInfo
                .expect("ValkeyModule_GetClusterNodeInfo function is unavailable")(
                ctx.ctx as *mut ValkeyModuleCtx,
                *node_id as *const c_char,
                ptr::null_mut::<c_char>(),
                master_buf.as_mut_ptr() as *mut c_char,
                ptr::null_mut::<c_int>(),
                &mut flags_,
            ) == VALKEYMODULE_OK as c_int
        };

        if added {
            let flags = flags_ as u32;

            if flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL) != 0 {
                continue;
            }

            if (flags & REDISMODULE_NODE_MASTER != 0) || master_id.is_empty() {
                // SAFETY: the id value obtained is guaranteed to be a hex string exactly VALKEYMODULE_NODE_ID_LEN long.
                // IOW, it is explicitly utf-8.
                let buf = &master_buf[..VALKEYMODULE_NODE_ID_LEN as usize];
                master_id = String::from_utf8_lossy(buf).into_owned();
            };

            if let Ok(address) = raw_to_cstring(*node_id, VALKEYMODULE_NODE_ID_LEN as usize) {
                shard_id_to_target
                    .entry(master_id.clone())
                    .or_default()
                    .push(address);
            } else {
                ctx.log_warning("Failed to convert node ID to CString");
            }
        }
    }

    if !nodes_list.is_null() {
        valkey_module::ValkeyModule_FreeClusterNodesList
            .expect("ValkeyModule_FreeClusterNodesList function does not exist")(nodes_list);
    }
}

pub fn get_current_node() -> CString {
    unsafe {
        // C API: Get current node's cluster ID
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();

        CStr::from_ptr(node_id).to_owned()
    }
}

unsafe fn raw_to_cstring(ptr: *const c_char, len: usize) -> Result<CString, std::ffi::NulError> {
    let bytes = std::slice::from_raw_parts(ptr, len);
    CString::new(bytes.as_bytes())
}
