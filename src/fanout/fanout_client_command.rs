use super::fanout_command::FanoutCommand;
use crate::fanout::FanoutContext;
use crate::fanout::FanoutResult;
use crate::fanout::blocked_client::FanoutBlockedClient;
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue};

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutClientCommand: FanoutCommand {
    /// Execute the fanout operation across cluster nodes.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue> {
        let mut blocked_client = FanoutBlockedClient::<Self>::new(ctx);

        // IMPORTANT: this callback calls into Valkey main-thread reply helpers.
        // It must never be invoked while the GIL is already held on the same
        // thread.
        let handle_response = move |op: Self, result: FanoutResult| {
            blocked_client.unblock(op, result);
        };

        Self::exec_command(self, ctx, handle_response)?;

        // We will reply later, from the callbacks
        Ok(ValkeyValue::NoReply)
    }

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn reply(&mut self, ctx: &FanoutContext) -> Status;
}
