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
/// A function that when called will cancel the timer if it hasn't fired yet
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
///     let cancel_timer = create_one_shot_timer(
///         ctx,
///         Duration::from_secs(5),
///         timer_callback,
///         "Hello from timer".to_string()
///     );
///     
///     // If needed, can cancel the timer before it fires
///     cancel_timer();
/// }
/// ```
pub fn create_one_shot_timer<F, T>(ctx: &Context, delay: Duration, callback: F, arg: T) -> u64
where
    F: FnOnce(T) + Send + 'static,
    T: Send + 'static,
{
    let id = flake_id();
    let ctx = ctx.clone(); // Clone the context to avoid lifetime issues

    // Create a timer; store the handle to be able to stop it inside callback
    let timer_handle = ctx.create_timer(
        delay,
        move |ctx, arg| {
            let (cb, v, id) = arg;
            cb(v);
            // Stop and cleanup this timer handle
            cancel_one_shot_timer(ctx, id)
        },
        (callback, arg, id),
    );

    TIMERS.pin().insert(id, timer_handle);
    id
}

/// Cancels a timer with the given ID.
/// This function is safe to call even if the timer has already fired or was never created.
/// It will not panic if the timer ID is not found in the map.
pub fn cancel_one_shot_timer(ctx: &Context, id: u64) {
    if let Some(timer_id) = TIMERS.pin().remove(&id) {
        ctx.stop_timer(*timer_id).unwrap_or_else(|_| {
            // Handle the error if needed
            ctx.log_warning("Failed to stop one-shot timer");
        });
    }
}
