use std::cell::RefCell;
use valkey_module::{Context, ValkeyError, ValkeyResult};

thread_local! {
    pub(crate) static FANOUT_ACL_USER: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// RAII marker for fanout ACL checks in detached contexts.
///
/// # Invariant
/// This marker is thread-local and only applies on the current thread. Any ACL-relevant
/// operation must execute on the same thread where this scope is entered.
pub struct FanoutAclScope;

impl FanoutAclScope {
    pub fn enter_with_user(user: &str) -> Self {
        FANOUT_ACL_USER.with(|u| {
            u.replace(Some(user.to_owned()));
        });
        Self
    }
}

impl Drop for FanoutAclScope {
    fn drop(&mut self) {
        FANOUT_ACL_USER.with(|u| {
            u.replace(None);
        });
    }
}

#[inline]
pub fn fanout_acl_scope_active() -> bool {
    FANOUT_ACL_USER.with(|u| {
        let u = u.borrow();
        u.as_ref().is_some_and(|v| !v.is_empty())
    })
}

pub fn with_fanout_user<T, F>(ctx: &Context, user: Option<&str>, f: F) -> ValkeyResult<T>
where
    F: FnOnce(&Context) -> ValkeyResult<T>,
{
    let Some(user) = user.filter(|name| !name.is_empty()) else {
        return f(ctx);
    };

    let user_name = ctx.create_string(user);
    let _user_scope = ctx.authenticate_user(&user_name).map_err(|err| {
        ValkeyError::String(format!(
            "ACL user '{user}' does not exist or is disabled: {err}"
        ))
    })?;
    let _acl_scope = FanoutAclScope::enter_with_user(user);

    f(ctx)
}

pub(super) fn get_fanout_user(ctx: &Context) -> Option<String> {
    let user = ctx.get_current_user().to_string();
    if user.is_empty() {
        return None;
    }
    Some(user)
}

#[cfg(test)]
mod tests {
    use crate::fanout::{FanoutAclScope, fanout_acl_scope_active};
    use std::thread;

    #[test]
    fn test_fanout_acl_scope_is_thread_local() {
        assert!(!fanout_acl_scope_active());
        let _scope = FanoutAclScope::enter_with_user("test_user");
        assert!(fanout_acl_scope_active());

        let child = thread::spawn(fanout_acl_scope_active)
            .join()
            .expect("thread join failed");
        assert!(!child);
    }
}
