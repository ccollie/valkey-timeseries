mod mget;
mod matchers;
mod range;
mod common;
mod index_query;
mod label_names;
mod mrange;
#[allow(dead_code, unused_imports, unused_lifetimes, clippy::derivable_impls, clippy::needless_lifetimes, clippy::extra_unused_lifetimes)]
#[path = "./request_generated.rs"]
mod request_generated;

#[allow(dead_code, unused_imports, unused_lifetimes, clippy::derivable_impls, clippy::needless_lifetimes, clippy::extra_unused_lifetimes)]
#[path = "./response_generated.rs"]
mod response_generated;
mod cardinality;
mod label_values;
pub(crate) mod serialization;

use crate::fanout::types::ClusterMessageType;
pub use cardinality::*;
pub use index_query::*;
pub use label_names::*;
pub use label_values::*;
pub use mget::*;
pub use mrange::*;
pub use range::*;

use super::types::TrackerEnum;
use crate::fanout::request::serialization::{Deserialized, Serialized};
pub use common::{
    deserialize_error_response,
    serialize_error_response,
    ErrorResponse,
    MessageHeader
};
use valkey_module::{Context, ValkeyResult};

pub trait ShardedCommand {
    type REQ: Serialized + Deserialized;
    type RES: Response;
    fn request_type() -> ClusterMessageType;
    fn exec(_ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES>;
}

pub trait Response: Serialized + Deserialized {
    fn update_tracker(tracker: &TrackerEnum, res: Self);
}