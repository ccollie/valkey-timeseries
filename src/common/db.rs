use valkey_module::{Context, RedisModule_GetSelectedDb, RedisModule_SelectDb, Status};

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
