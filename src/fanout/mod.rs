mod cluster_message;
mod cluster_rpc;
mod fanout_error;
mod fanout_operation;
mod fanout_targets;
mod registry;
pub mod serialization;
mod snowflake;
mod utils;

use valkey_module::Context;

use super::fanout::cluster_rpc::register_cluster_message_handlers;
pub use fanout_error::*;
pub use fanout_operation::*;
pub use fanout_targets::FanoutTarget;
pub use utils::*;

pub use registry::register_fanout_operation;

pub(crate) fn init_fanout(ctx: &Context) {
    register_cluster_message_handlers(ctx)
}
