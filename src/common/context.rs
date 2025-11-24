use valkey_module::{
    Context, ContextFlags, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status,
};

// Safety: RedisModule_GetSelectedDb is safe to call
pub fn get_current_db(ctx: &Context) -> i32 {
    unsafe { RedisModule_GetSelectedDb.unwrap()(ctx.ctx) }
}

pub fn set_current_db(ctx: &Context, db: i32) -> Status {
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
pub fn is_aof_client(client_id: u64) -> bool {
    client_id == u64::MAX
}

pub fn is_real_user_client(ctx: &Context) -> bool {
    let client_id = ctx.get_client_id();
    if client_id == 0 || is_aof_client(client_id) {
        return false;
    }
    if ctx.get_flags().contains(ContextFlags::REPLICATED) {
        return false;
    }
    true
}
