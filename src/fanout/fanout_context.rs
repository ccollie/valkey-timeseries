use crate::common::context::{get_current_db, set_current_db};
use crate::series::index::{TimeSeriesIndexGuard, get_db_index};
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{ContextGuard, DetachedFromClient, ThreadSafeContext};

/// A guard that wraps ContextGuard and manages database switching.
/// Automatically switches to the target database on creation and restores
/// the original database on drop.
pub struct FanoutContextGuard {
    guard: ContextGuard,
    saved_db: i32,
    target_db: i32,
    switched_db: bool,
}

impl FanoutContextGuard {
    /// Creates a new FanoutContextGuard, switching to the target database.
    pub fn new(guard: ContextGuard, target_db: i32) -> Self {
        let saved_db = get_current_db(&guard);
        let mut switched_db = false;
        if saved_db != target_db {
            set_current_db(&guard, target_db);
            switched_db = true;
        }
        Self {
            guard,
            saved_db,
            target_db,
            switched_db,
        }
    }

    /// Returns a reference to the underlying ContextGuard.
    pub fn inner(&self) -> &ContextGuard {
        &self.guard
    }

    /// Returns the current target database index.
    pub fn db(&self) -> i32 {
        self.target_db
    }
}

impl Drop for FanoutContextGuard {
    fn drop(&mut self) {
        if self.switched_db {
            set_current_db(&self.guard, self.saved_db);
        }
    }
}

impl std::ops::Deref for FanoutContextGuard {
    type Target = ContextGuard;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for FanoutContextGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

/// The context for fanout operations. We use this to facilitate fine-grained
/// locking of Valkey when executing fanout operations. For example, the metadata query
/// functions do not access Valkey, so they do not require locking. However, when
/// executing other commands, we want to limit locks to only code that accesses Valkey global state.
///
/// It is a thin wrapper valkey_module::MODULE_CONTEXT, holding only the current database index.
/// We automatically set the current database index when locking for command execution.
/// In the future, we might want to extend this to hold other context information such as the authenticated
/// user.
pub struct FanoutContext {
    save_db: Option<i32>,
    ctx: ThreadSafeContext<DetachedFromClient>,
    pub db: i32,
}

impl FanoutContext {
    pub fn new(db: i32) -> Self {
        Self {
            save_db: None,
            ctx: ThreadSafeContext::new(),
            db,
        }
    }

    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        let c = self.ctx.lock();
        c.log(level, message);
    }

    pub fn log_debug(&self, message: &str) {
        self.log(ValkeyLogLevel::Debug, message);
    }

    pub fn log_notice(&self, message: &str) {
        self.log(ValkeyLogLevel::Notice, message);
    }

    pub fn log_verbose(&self, message: &str) {
        self.log(ValkeyLogLevel::Verbose, message);
    }

    pub fn log_warning(&self, message: &str) {
        self.log(ValkeyLogLevel::Warning, message);
    }

    pub fn lock(&mut self) -> FanoutContextGuard {
        let guard = self.ctx.lock();
        FanoutContextGuard::new(guard, self.db)
    }

    pub fn get_db_index(&self) -> TimeSeriesIndexGuard<'_> {
        get_db_index(self.db)
    }
}

unsafe impl Send for FanoutContext {}
unsafe impl Sync for FanoutContext {}
