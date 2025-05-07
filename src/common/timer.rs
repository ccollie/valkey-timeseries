use crate::common::ids::flake_id;
use papaya::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
use valkey_module::{Context, RedisModuleTimerID};

static TIMERS: LazyLock<HashMap<u64, RedisModuleTimerID>> = LazyLock::new(HashMap::default);

/// Creates a one-shot timer that executes the provided callback function after the specified duration.
///
/// # Arguments
/// * `ctx` - The Context used to create and manage the timer
/// * `duration` - How long to wait before executing the callback
/// * `callback` - Function to execute when the timer expires
/// * `param` - Parameter to pass to the callback function
///
/// # Returns
/// A Timer handle that can be used to cancel the timer if needed
///
/// # Example
/// ```
/// use std::time::Duration;
///
/// fn timer_callback(value: String) {
///     println!("Timer expired with value: {}", value);
/// }
///
/// fn example(ctx: &Context) {
///     let timer = create_one_shot_timer(
///         ctx,
///         Duration::from_secs(5),
///         timer_callback,
///         "Hello from timer".to_string()
///     );
///     
///     // If needed, can cancel the timer before it fires
///     // ctx.stop_timer(timer);
/// }
/// ```
pub fn create_one_shot_timer<F, T>(
    ctx: &Context,
    delay: Duration,
    callback: F,
    arg: T,
)
where
    F: FnOnce(T) + Send + 'static,
    T: Send + 'static,
{
    // Boxing the closure and argument for move into timer
    // We'll use Arc to guarantee the handler outlives the timer
    let id = flake_id();

    // Create a timer; store the handle to be able to stop it inside callback
    let timer_handle = ctx.create_timer(delay,  move |ctx, arg| {
        let (cb, v, id) = arg;
        cb(v);
            // Stop and cleanup this timer handle 
        if let Some(timer_id) = TIMERS.pin().remove(&id) {
            let _ = ctx.stop_timer::<T>(*timer_id); // Safety: Usually provided by Valkey API
        } else {
            ctx.log_debug("OneShotTimer: Timer not found");
        }
    }, (callback, arg, id));

    TIMERS.pin().insert(id, timer_handle);
}