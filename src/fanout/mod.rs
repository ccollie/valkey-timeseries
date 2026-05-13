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
use std::sync::{Arc, LazyLock};
use valkey_module::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;
pub use fanout_client_command::*;
pub use fanout_command::*;
pub use fanout_error::*;
pub use utils::*;

use crate::fanout::cluster_migrations::{
    AtomicSlotMigrationEventHandler, register_atomic_slot_migration_event_handler,
    supports_atomic_slot_migration,
};
pub use cluster_map::{ClusterMap, FanoutTargetMode, NodeInfo};

#[allow(unused_imports)]
pub use cluster_migrations::{SlotMigration, get_slot_migrations};
pub use registry::register_fanout_operation;

pub(crate) fn init_fanout(ctx: &Context, cleanup_fn: Option<AtomicSlotMigrationEventHandler>) {
    if !is_clustered(ctx) {
        return;
    }
    register_cluster_message_handlers(ctx);
    if supports_atomic_slot_migration(ctx) {
        register_atomic_slot_migration_event_handler(ctx, cleanup_fn);
    }
}

static CLUSTER_MAP: LazyLock<ArcSwap<ClusterMap>> =
    LazyLock::new(|| ArcSwap::from_pointee(ClusterMap::default()));

pub fn get_cluster_map() -> Guard<Arc<ClusterMap>> {
    CLUSTER_MAP.load()
}

fn update_cluster_map(map: ClusterMap) {
    CLUSTER_MAP.swap(Arc::new(map));
}

pub fn get_fanout_targets(ctx: &Context, mode: FanoutTargetMode) -> Arc<HashSet<NodeInfo>> {
    let current_map = CLUSTER_MAP.load();
    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired();
    if !needs_refresh {
        return current_map.get_targets(mode);
    }
    // Possibly race condition, but only if called concurrently, which is possible but very unlikely.
    // In any case, the worst that can happen is that we refresh more than once.
    refresh_cluster_map(ctx);
    CLUSTER_MAP.load().get_targets(mode)
}

// Refresh the cluster map by creating a new one from the current cluster state
pub fn refresh_cluster_map(ctx: &Context) {
    ctx.log_notice("Refreshing cluster map...");
    let new_map = ClusterMap::create(ctx);
    update_cluster_map(new_map);
    ctx.log_notice("Cluster map refreshed");
}

pub fn get_or_refresh_cluster_map(ctx: &Context) -> Arc<ClusterMap> {
    let current_map = get_cluster_map();

    // Check if we need to refresh
    let needs_refresh = !current_map.is_consistent || current_map.is_expired();

    if needs_refresh {
        drop(current_map);
        refresh_cluster_map(ctx);
        return get_cluster_map().clone();
    }

    current_map.clone()
}
