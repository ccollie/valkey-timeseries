use valkey_module::{Context, ContextFlags};

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
