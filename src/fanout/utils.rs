use super::cluster_api::get_cluster_node_info;
use super::snowflake::SnowflakeIdGenerator;
use crate::fanout::FanoutTargetMode;
use std::hash::{BuildHasher, RandomState};
use std::net::Ipv6Addr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{
    Context, ContextFlags, DetachedContext, RedisModule_Milliseconds, VALKEYMODULE_NODE_ID_LEN,
    ValkeyModule_GetMyClusterID, ValkeyResult,
};

const VALKEYMODULE_CLIENT_INFO_FLAG_READONLY: u64 = 1 << 6; /* Valkey 9 */

pub static FORCE_REPLICAS_READONLY: AtomicBool = AtomicBool::new(false);

pub fn is_client_read_only(ctx: &Context) -> ValkeyResult<bool> {
    let info = ctx.get_client_info()?;
    Ok(info.flags & VALKEYMODULE_CLIENT_INFO_FLAG_READONLY != 0)
}

pub fn is_clustered(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::CLUSTER)
}

pub fn is_multi_or_lua(ctx: &Context) -> bool {
    let flags = ctx.get_flags();
    flags.contains(ContextFlags::MULTI) || flags.contains(ContextFlags::LUA)
}

/// Returns the time duration since UNIX_EPOCH in milliseconds.
#[cfg(test)]
fn system_time_millis() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

fn valkey_current_time_millis() -> i64 {
    unsafe { RedisModule_Milliseconds.unwrap()() }
}

pub(super) fn current_time_millis() -> i64 {
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            system_time_millis()
        } else {
            valkey_current_time_millis()
        }
    }
}

pub fn get_current_node_id() -> Option<String> {
    unsafe {
        let id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();

        if id.is_null() {
            return None;
        }

        let str_slice =
            std::slice::from_raw_parts(id as *const u8, VALKEYMODULE_NODE_ID_LEN as usize);
        Some(String::from_utf8_lossy(str_slice).to_string())
    }
}

pub fn get_current_node_ip(ctx: &Context) -> Option<Ipv6Addr> {
    unsafe {
        // C API: Get current node's cluster ID
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();

        if node_id.is_null() {
            // We're not clustered, so a random string is good enough
            return Some(Ipv6Addr::LOCALHOST);
        }
        let Some(info) = get_cluster_node_info(ctx, node_id) else {
            ctx.log_warning("get_current_node_ip(): error getting cluster node info");
            return None;
        };
        Some(info.addr())
    }
}

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

static ID_GENERATOR: LazyLock<SnowflakeIdGenerator> = LazyLock::new(|| {
    let ctx = DetachedContext::new();
    let addr = get_current_node_ip(&ctx.lock()).unwrap_or(Ipv6Addr::LOCALHOST);
    let hasher = RandomState::new();
    let hash = hasher.hash_one(addr);
    let node_id: u32 = hash as u32;
    let machine_id = (hash >> 32) as u32;
    SnowflakeIdGenerator::new(machine_id, node_id)
});

pub(super) fn generate_id() -> u64 {
    ID_GENERATOR.generate()
}
