mod blocked;
mod client_reply_context;
pub mod replies;
mod thread_safe;

use std::ffi::CString;
use std::os::raw::c_int;
use valkey_module::{
    Context, ContextFlags, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status, ValkeyError,
    ValkeyModule_GetServerInfo, ValkeyModule_ServerInfoGetFieldSigned, ValkeyModuleCtx,
    ValkeyModuleServerInfoData, ValkeyResult,
};

pub use blocked::*;
pub(crate) use client_reply_context::*;
pub use thread_safe::*;

// Safety: RedisModule_GetSelectedDb is safe to call
pub(crate) fn get_current_db(ctx: &Context) -> i32 {
    unsafe { RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

pub(crate) fn set_current_db(ctx: &Context, db: i32) -> Status {
    // Safety: RedisModule_SelectDb is safe to call. It is a bug in the valkey_module
    // if the function is not available.
    unsafe {
        match RedisModule_SelectDb.unwrap()(ctx.ctx, db) {
            0 => Status::Ok,
            _ => Status::Err,
        }
    }
}

#[inline]
pub(crate) fn is_aof_client(client_id: u64) -> bool {
    client_id == u64::MAX
}

pub(crate) fn is_real_user_client(ctx: &Context) -> bool {
    let client_id = ctx.get_client_id();
    if client_id == 0 || crate::common::context::is_aof_client(client_id) {
        return false;
    }
    if ctx.get_flags().contains(ContextFlags::REPLICATED) {
        return false;
    }
    true
}

pub(crate) fn get_server_info(ctx: &Context, section: &str) -> *mut ValkeyModuleServerInfoData {
    let info_fn = unsafe { ValkeyModule_GetServerInfo.unwrap() };
    let context = ctx.ctx as *mut ValkeyModuleCtx;
    let section_cstr = CString::new(section).expect("Failed to convert section to CString");
    unsafe { info_fn(context, section_cstr.as_ptr()) }
}

fn get_server_info_field_signed(
    info: *mut ValkeyModuleServerInfoData,
    field: &str,
) -> ValkeyResult<i64> {
    let get_signed_field_fn = unsafe {
        ValkeyModule_ServerInfoGetFieldSigned
            .expect("Failed to get ValkeyModule_ServerInfoGetFieldSigned")
    };
    let mut ignored: c_int = 0;
    unsafe {
        let field_value = CString::new(field).expect("Failed to convert field to CString");
        let res = get_signed_field_fn(info, field_value.as_ptr(), &mut ignored);
        if ignored != 0 {
            let msg = format!("Field '{field}' not found in server info");
            return Err(ValkeyError::String(msg));
        }
        Ok(res)
    }
}

pub(crate) fn get_available_memory(ctx: &Context) -> Option<i64> {
    // Fetch INFO MEMORY
    let info = crate::common::context::get_server_info(ctx, "memory");

    let used_memory: i64 = get_server_info_field_signed(info, "used_memory").ok()?;
    let max_memory: i64 = get_server_info_field_signed(info, "maxmemory").ok()?;

    // Compute available = maxm_emory - used_memory (clamped to >= 0)
    if max_memory > 0 {
        let diff = max_memory - used_memory;
        Some(if diff > 0 { diff } else { 0 })
    } else {
        None
    }
}
