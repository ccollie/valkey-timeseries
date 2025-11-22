use crate::fanout::FanoutOperation;
use crate::fanout::fanout_operation::ResponseContext;
use std::ffi::{CString, c_void};
use std::os::raw::c_int;
use valkey_module::{
    Context, ValkeyModule_BlockClient, ValkeyModule_BlockedClientMeasureTimeEnd,
    ValkeyModule_BlockedClientMeasureTimeStart, ValkeyModule_GetBlockedClientPrivateData,
    ValkeyModule_ReplyWithError, ValkeyModule_UnblockClient, ValkeyModuleCtx, ValkeyModuleString,
    raw,
};

const NO_TIMEOUT: i64 = 86400000;

/// High-level wrapper for a blocked client.
pub(super) struct FanoutBlockedClient<T: FanoutOperation> {
    inner: *mut raw::ValkeyModuleBlockedClient,
    data: Option<Box<ResponseContext<T>>>,
    time_measurement_ongoing: bool,
    unblocked: bool,
}

// We need to be able to send the inner pointer to another thread
unsafe impl<T: FanoutOperation> Send for FanoutBlockedClient<T> {}

impl<T> FanoutBlockedClient<T>
where
    T: FanoutOperation,
{
    pub fn new(ctx: &Context) -> Self {
        let bc_ptr = unsafe {
            ValkeyModule_BlockClient.unwrap()(
                ctx.ctx as *mut ValkeyModuleCtx,
                Some(reply_callback::<T>),
                None,
                Some(free_callback::<T>),
                NO_TIMEOUT,
            )
        };
        Self {
            inner: bc_ptr,
            time_measurement_ongoing: false,
            unblocked: false,
            data: None,
        }
    }

    /// Set the private data that will be passed back on unblock.
    pub fn set_reply_private_data(&mut self, private_data: ResponseContext<T>) {
        self.data = Some(Box::new(private_data));
    }

    pub fn unblock_client(&mut self) {
        // If nothing to do, return early.
        if self.unblocked {
            return;
        }

        self.unblocked = true;

        // Ensure any ongoing measurement is ended.
        self.measure_time_end();

        // Take private_data and tracked_client_id for local use.
        let private_data_ptr = self.data.take().map_or(std::ptr::null_mut(), |boxed| {
            Box::into_raw(boxed) as *mut c_void
        });

        if private_data_ptr.is_null() {
            // No private data to pass back.
            // todo: log warning ?
            return;
        }

        // Call out to the C API to actually unblock.
        unsafe {
            ValkeyModule_UnblockClient.unwrap()(self.inner, private_data_ptr);
        }
    }

    /// Start measuring time for a blocked client.
    pub fn measure_time_start(&mut self) {
        if self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeStart.unwrap()(self.inner) };
        self.time_measurement_ongoing = true;
    }

    /// End measuring time for a blocked client.
    pub fn measure_time_end(&mut self) {
        if !self.time_measurement_ongoing {
            return;
        }
        unsafe { ValkeyModule_BlockedClientMeasureTimeEnd.unwrap()(self.inner) };
        self.time_measurement_ongoing = false;
    }
}

impl<T: FanoutOperation> Drop for FanoutBlockedClient<T> {
    fn drop(&mut self) {
        // Ensure we try to unblock when the wrapper is dropped, following RAII.
        // swallow any panics to avoid unwinding across FFI boundaries.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.unblock_client();
        }))
        .ok();
    }
}

unsafe extern "C" fn reply_callback<T: FanoutOperation>(
    ctx: *mut ValkeyModuleCtx,
    _argv: *mut *mut ValkeyModuleString,
    _argc: c_int,
) -> c_int {
    let op_ptr = unsafe { ValkeyModule_GetBlockedClientPrivateData.unwrap()(ctx) };

    if op_ptr.is_null() {
        let err_msg = CString::new("No reply data").unwrap();
        unsafe { ValkeyModule_ReplyWithError.unwrap()(ctx, err_msg.as_ptr()) };
    } else {
        // Cast to the correct type and then dereference once to get &mut ResponseContext<T>
        let op: &mut ResponseContext<T> = unsafe { &mut *(op_ptr as *mut ResponseContext<T>) };

        let ctx = Context::new(ctx as *mut raw::RedisModuleCtx);
        op.reply(&ctx);
    }
    0
}

unsafe extern "C" fn free_callback<T: FanoutOperation>(
    _ctx: *mut ValkeyModuleCtx,
    private_data: *mut c_void,
) {
    if !private_data.is_null() {
        let boxed: Box<ResponseContext<T>> = Box::from_raw(private_data as *mut ResponseContext<T>);
        drop(boxed);
    }
}
