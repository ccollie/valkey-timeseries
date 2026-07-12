pub mod acl;
mod blocked_client;
mod cluster_map;
pub(crate) mod cluster_migrations;
mod cluster_rpc;
mod fanout_client_command;
mod fanout_command;
mod fanout_error;
mod fanout_message;
mod registry;
pub mod serialization;
mod utils;

use ahash::HashSet;
use arc_swap::{ArcSwap, Guard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use valkey_module::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;
pub use acl::*;
pub use fanout_client_command::*;
pub use fanout_command::*;
pub use fanout_error::*;
pub use utils::*;

pub use cluster_map::{ClusterMap, FanoutTargetMode, NodeInfo};
pub use registry::register_fanout_operation;

pub(crate) fn init_fanout(ctx: &Context) {
    if !is_clustered(ctx) {
        return;
    }
    register_cluster_message_handlers(ctx);
}

static CLUSTER_MAP: LazyLock<ArcSwap<ClusterMap>> =
    LazyLock::new(|| ArcSwap::from_pointee(ClusterMap::default()));

/// Set when a peer rejects one of our requests with a cluster-map mismatch.
/// The next target-selection or map access treats the local map as needing a
/// refresh, so the following fanout is built from up-to-date topology.
static CLUSTER_MAP_STALE: AtomicBool = AtomicBool::new(false);

/// The set of target nodes for a fanout, together with the cluster-map
/// fingerprint of the *same* snapshot the targets were chosen from. Keeping them
/// together avoids a race where the map is swapped between target selection and
/// sending, which would pair a fresh target list with a stale fingerprint.
#[derive(Clone)]
pub struct FanoutTargets {
    pub nodes: Arc<HashSet<NodeInfo>>,
    pub cluster_fingerprint: u64,
}

pub fn get_cluster_map() -> Guard<Arc<ClusterMap>> {
    CLUSTER_MAP.load()
}

/// Mark the local cluster map as stale so it is rebuilt on next use.
pub fn mark_cluster_map_stale() {
    CLUSTER_MAP_STALE.store(true, Ordering::Release);
}

fn take_cluster_map_stale() -> bool {
    CLUSTER_MAP_STALE.swap(false, Ordering::AcqRel)
}

fn update_cluster_map(map: ClusterMap) {
    CLUSTER_MAP.swap(Arc::new(map));
}

pub fn get_fanout_targets(ctx: &Context, mode: FanoutTargetMode) -> FanoutTargets {
    let current_map = CLUSTER_MAP.load();
    // Check if we need to refresh. A stale flag is set when a peer rejected one
    // of our requests due to a topology mismatch.
    let stale = take_cluster_map_stale();
    let needs_refresh = !current_map.is_consistent || current_map.is_expired() || stale;
    if !needs_refresh {
        return FanoutTargets {
            nodes: current_map.get_targets(mode),
            cluster_fingerprint: current_map.cluster_slots_fingerprint(),
        };
    }
    // Possibly race condition, but only if called concurrently, which is possible but very unlikely.
    // In any case, the worst that can happen is that we refresh more than once.
    refresh_cluster_map(ctx);
    // Load the refreshed map once so targets and fingerprint come from the same snapshot.
    let map = CLUSTER_MAP.load();
    FanoutTargets {
        nodes: map.get_targets(mode),
        cluster_fingerprint: map.cluster_slots_fingerprint(),
    }
}

// Refresh the cluster map by creating a new one from the current cluster state.
// If building the new map fails, the previous map is kept in place and a
// warning is logged so subsequent calls will retry the refresh.
pub fn refresh_cluster_map(ctx: &Context) {
    ctx.log_notice("Refreshing cluster map...");
    match ClusterMap::create(ctx) {
        Some(new_map) => {
            update_cluster_map(new_map);
            ctx.log_notice("Cluster map refreshed");
        }
        None => {
            ctx.log_warning(
                "Failed to build cluster map; keeping the previous map and retrying later",
            );
        }
    }
}

pub fn get_or_refresh_cluster_map(ctx: &Context) -> Arc<ClusterMap> {
    let current_map = get_cluster_map();
    let stale = take_cluster_map_stale();

    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired() || stale;

    if needs_refresh {
        drop(current_map);
        refresh_cluster_map(ctx);
        return get_cluster_map().clone();
    }

    current_map.clone()
}
