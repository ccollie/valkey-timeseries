use crate::fanout::cluster::get_current_node;
use ahash::AHasher;
use snowdon::{ClassicLayout, Epoch, Generator, MachineId};
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use std::thread;

fn machine_id() -> String {
    let node_id = get_current_node();
    node_id.to_string_lossy().into_owned()
}

// Specify our custom snowflake
#[derive(Debug)]
pub struct SnowflakeParameters;
impl MachineId for SnowflakeParameters {
    fn machine_id() -> u64 {
        let mid = machine_id();
        let mut hasher = AHasher::default();
        mid.hash(&mut hasher);
        hasher.finish()
    }
}
impl Epoch for SnowflakeParameters {
    fn millis_since_unix() -> u64 {
        // Our epoch starts with at midnight May o1 2025
        1746072000000
    }
}
type SnowflakeGenerator = Generator<ClassicLayout<SnowflakeParameters>, SnowflakeParameters>;

static SNOWFLAKE_GENERATOR: LazyLock<SnowflakeGenerator> =
    LazyLock::new(SnowflakeGenerator::default);
pub fn flake_id() -> u64 {
    let mut count = 0;
    loop {
        match SNOWFLAKE_GENERATOR.generate() {
            Ok(id) => return id.into_inner(),
            Err(e) => {
                // We only get here if the system clock went backwards. However, ids are not
                // system-critical, so we retry after a short pause.
                // This is a bit of a hack, but we don't want to panic in production.
                thread::sleep(std::time::Duration::from_millis(1));
                count += 1;
                if count > 2 {
                    panic!("Failed to generate snowflake ID: {}", e)
                }
            }
        }
    }
}
