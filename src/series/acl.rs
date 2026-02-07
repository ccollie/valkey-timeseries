use crate::common::context::is_real_user_client;
use crate::error_consts;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn clone_permissions(permissions: &AclPermissions) -> AclPermissions {
    let mut cloned = AclPermissions::empty();
    if permissions.contains(AclPermissions::ACCESS) {
        cloned |= AclPermissions::ACCESS;
    }
    if permissions.contains(AclPermissions::UPDATE) {
        cloned |= AclPermissions::UPDATE;
    }
    if permissions.contains(AclPermissions::DELETE) {
        cloned |= AclPermissions::DELETE;
    }
    cloned
}

#[inline]
fn has_key_permissions(ctx: &Context, key: &ValkeyString, permissions: AclPermissions) -> bool {
    if !is_real_user_client(ctx) {
        return true;
    }
    let user = ctx.get_current_user();
    ctx.acl_check_key_permission(&user, key, &permissions)
        .is_ok()
}

pub fn has_all_keys_permissions(
    ctx: &Context,
    user: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> bool {
    if !is_real_user_client(ctx) {
        return true;
    }
    let all_keys = ctx.create_string("*");
    match &permissions {
        Some(perms) => ctx.acl_check_key_permission(user, &all_keys, perms).is_ok(),
        None => true,
    }
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

pub fn check_metadata_permissions(ctx: &Context) -> ValkeyResult<()> {
    let perms = AclPermissions::ACCESS;
    let key = ctx.create_string("*");
    check_key_permissions(ctx, &key, &perms)
        .map_err(|_| ValkeyError::Str(error_consts::ALL_KEYS_READ_PERMISSION_ERROR))
}
