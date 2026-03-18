use super::fanout_command::FanoutCommand;
use crate::common::threads::run_on_main_thread;
use crate::fanout::FanoutResult;
use valkey_module::{
    BlockedClient, Context, Status, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyValue,
};

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutOperation: FanoutCommand {
    /// Execute the fanout operation across cluster nodes.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue> {
        let blocked_client = ctx.block_client();

        // IMPORTANT: this callback calls thread_ctx.lock() (acquires the GIL).
        // It must never be invoked while the GIL is already held on the same
        // thread.
        let handle_response = move |mut op: Self, result: FanoutResult| {
            run_on_main_thread(false, move || {
                let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
                match result {
                    Ok(_) => {
                        op.reply(&thread_ctx);
                    }
                    Err(err) => {
                        let _err: ValkeyError = err.into();
                        thread_ctx.reply(Err(_err));
                    }
                }
            });
        };

        Self::exec_command(self, ctx, handle_response)?;

        // We will reply later, from the callbacks
        Ok(ValkeyValue::NoReply)
    }

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn reply(&mut self, thread_ctx: &ThreadSafeContext<BlockedClient>) -> Status;
}
