pub mod cluster;
mod coordinator;
mod results_tracker;
mod types;
mod error;
pub mod request;

use crate::fanout::coordinator::send_request;
pub use coordinator::register_cluster_message_handlers;
pub use request::*;
pub(super) use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext};
use crate::fanout::cluster::ClusterMessageType;
use crate::series::request_types::MGetRequest;

pub fn perform_mget<F>(
    ctx: &Context,
    request: &MGetRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[MultiGetResponse]) + Send + 'static,
{
    send_request::<MGetRequest, MultiGetResponse, F>(ctx, request, ClusterMessageType::MGetQuery, on_done)
}

pub fn perform_card<F> (
    ctx: &Context,
    request: &CardinalityRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[CardinalityResponse]) + Send + 'static,
{
    send_request::<CardinalityRequest, CardinalityResponse, F>(ctx, request, ClusterMessageType::Cardinality,  on_done)
}

pub fn perform_label_names<F> (
    ctx: &Context,
    request: &LabelNamesRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelNamesResponse]) + Send + 'static,
{
    send_request::<LabelNamesRequest, LabelNamesResponse, F>(ctx, request, ClusterMessageType::LabelNames, on_done)
}

pub fn perform_label_values<F> (
    ctx: &Context,
    request: &LabelValuesRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelValuesResponse]) + Send + 'static,
{
    send_request::<LabelValuesRequest, LabelValuesResponse, F>(ctx, request, ClusterMessageType::LabelValues, on_done)
}

pub fn perform_index_query<F> (
    ctx: &Context,
    request: &IndexQueryRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[IndexQueryResponse]) + Send + 'static,
{
    send_request::<IndexQueryRequest, IndexQueryResponse, F>(ctx, request, ClusterMessageType::IndexQuery, on_done)
}