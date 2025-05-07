mod common;
mod index_query;
mod label_names;
mod matchers;
mod mget;
mod mrange;
mod range;
#[allow(
    dead_code,
    unused_imports,
    unused_lifetimes,
    clippy::derivable_impls,
    clippy::needless_lifetimes,
    clippy::extra_unused_lifetimes
)]
#[path = "./request_generated.rs"]
mod request_generated;

mod cardinality;
mod label_values;
#[allow(
    dead_code,
    unused_imports,
    unused_lifetimes,
    clippy::derivable_impls,
    clippy::needless_lifetimes,
    clippy::extra_unused_lifetimes
)]
#[path = "./response_generated.rs"]
mod response_generated;
pub(crate) mod serialization;
mod stats;

pub use cardinality::*;
pub use index_query::*;
pub use label_names::*;
pub use label_values::*;
pub use mget::*;
pub use mrange::*;
pub use range::*;
pub use stats::*;

use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{ClusterMessageType, TrackerEnum};
pub use common::{
    deserialize_error_response, serialize_error_response, ErrorResponse, MessageHeader,
};
use valkey_module::{Context, ValkeyResult};

pub trait MultiShardCommand {
    type REQ: Serialized + Deserialized;
    type RES: Serialized + Deserialized;
    fn request_type() -> ClusterMessageType;
    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES>;
    fn update_tracker(tracker: &TrackerEnum, res: Self::RES);
}
