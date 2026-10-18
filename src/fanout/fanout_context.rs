use crate::common::acl::AclContextGuard;
use crate::common::context::set_current_db;
use crate::common::threads::LockGil;
use valkey_module::{DetachedContext, MODULE_CONTEXT, Status, ValkeyError, ValkeyResult};

/// The shard-local side of one fan-out request: the ACL user it executes as,
/// the database it targets, and the module's detached context used to take
/// the GIL.
///
/// Built by the two fan-out entry points (the coordinator's own shard and the
/// cluster RPC receiver) and handed to `FanoutCommand::get_local_response`,
/// which runs on a worker thread with the GIL *not* held. [`FanoutContext::lock`]
/// is the only way that code should take it: the detached context is shared by
/// every worker, so the request's database has to be re-selected on each
/// acquisition, and the [`ModuleUser`] handle the ACL checks rely on is only
/// valid while the lock that resolved it is held.
///
/// # Invariant
/// The ACL identity a lock installs is thread-local, so a request must take
/// every GIL it needs on the thread that owns the `FanoutContext`.
pub struct FanoutContext {
    user: Option<String>,
    db: i32,
    ctx: &'static DetachedContext,
}

impl FanoutContext {
    /// `user` is the coordinator-side client's ACL name (`None`/empty = no
    /// enforcement), `db` the database the request runs against.
    pub fn new(user: Option<String>, db: i32) -> Self {
        let user = user.filter(|name| !name.is_empty());
        Self {
            user,
            db,
            ctx: &MODULE_CONTEXT,
        }
    }

    /// The ACL user the request executes as, if enforcement applies.
    #[cfg(test)]
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// The database the request runs against.
    pub fn db(&self) -> i32 {
        self.db
    }

    /// Take the GIL for a step of this request.
    ///
    /// Keep the guard's scope as small as the work allows — decode the request
    /// before locking and encode the response after releasing, so the server
    /// stays responsive while the shard-local reply is materialized.
    ///
    /// Fails when the request's database cannot be selected or its ACL user no
    /// longer exists, so a request cannot silently run against the wrong
    /// database or without enforcement.
    pub fn lock(&self) -> ValkeyResult<AclContextGuard> {
        let ctx = self.ctx.lock_gil();

        if set_current_db(&ctx, self.db) == Status::Err {
            return Err(ValkeyError::String(format!(
                "failed to select database {}",
                self.db
            )));
        }
        // Resolved afresh on every lock: a user deleted or disabled mid-request fails the
        // next step rather than finishing with rights it no longer has.
        AclContextGuard::for_user(self.user.as_deref(), ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fanout_context_keeps_user_and_db() {
        let ctx = FanoutContext::new(Some("alice".to_owned()), 3);
        assert_eq!(ctx.user(), Some("alice"));
        assert_eq!(ctx.db(), 3);
    }

    #[test]
    fn test_fanout_context_collapses_empty_user() {
        let ctx = FanoutContext::new(Some(String::new()), 0);
        assert!(ctx.user().is_none());
        assert_eq!(ctx.db(), 0);
    }
}
