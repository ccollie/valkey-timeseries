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

use crate::fanout::types::ClusterMessageType;
pub use cardinality::*;
pub use index_query::*;
pub use mget::*;
pub use mrange::*;
pub use range::*;
pub use label_names::*;
pub use label_values::*;

pub use common::{
    deserialize_error_response,
    serialize_error_response,
    ErrorResponse,
    MessageHeader
};
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult};

use super::types::TrackerEnum;

pub trait Request<R: Response>: Sized {
    fn request_type() -> ClusterMessageType;
    fn serialize(&self, buf: &mut Vec<u8>);
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> where Self: Sized;
    fn create_tracker<F>(ctx: &Context, request_id: u64, expected_results: usize, callback: F) -> TrackerEnum
        where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[R]) + Send + 'static;
    fn exec(&self, _ctx: &Context) -> ValkeyResult<R> {
        unimplemented!("exec not implemented for this request type");
    }
}

pub trait Response {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> where Self: Sized;
    fn update_tracker(tracker: &TrackerEnum, res: Self);
}