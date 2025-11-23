use crate::common::context::is_real_user_client;
use crate::error_consts;
use std::sync::LazyLock;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

#[allow(clippy::declare_interior_mutable_const)]
const ALL_KEYS: LazyLock<ValkeyString> =
    LazyLock::new(|| ValkeyString::create_and_retain("allkeys"));

#[inline]
fn has_key_permissions(ctx: &Context, key: &ValkeyString, permissions: AclPermissions) -> bool {
    let user = ctx.get_current_user();
    ctx.acl_check_key_permission(&user, key, &permissions)
        .is_ok()
}

pub fn check_key_read_permission(ctx: &Context, key: &ValkeyString) -> bool {
    has_key_permissions(ctx, key, AclPermissions::ACCESS)
}

#[inline]
pub fn check_key_permissions(
    ctx: &Context,
    key: &ValkeyString,
    permissions: &AclPermissions,
) -> ValkeyResult<()> {
    if !is_real_user_client(ctx) {
        return Ok(());
    }
    let user = ctx.get_current_user();
    if ctx
        .acl_check_key_permission(&user, key, permissions)
        .is_ok()
    {
        Ok(())
    } else {
        if permissions.contains(AclPermissions::DELETE) {
            return Err(ValkeyError::Str(error_consts::KEY_DELETE_PERMISSION_ERROR));
        }
        if permissions.contains(AclPermissions::UPDATE) {
            return Err(ValkeyError::Str(error_consts::KEY_WRITE_PERMISSION_ERROR));
        }
        Err(ValkeyError::Str(error_consts::PERMISSION_DENIED))
    }
}

// The ALL_KEYS key is a special key used to check permissions for all keys.
// It is never mutated, so we silence the warning.
#[allow(clippy::borrow_interior_mutable_const)]
pub fn check_metadata_permissions(ctx: &Context) -> ValkeyResult<()> {
    let perms = AclPermissions::ACCESS;
    check_key_permissions(ctx, &ALL_KEYS, &perms)
        .map_err(|_| ValkeyError::Str(error_consts::ALL_KEYS_READ_PERMISSION_ERROR))
}
