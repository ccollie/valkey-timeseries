use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex};
use ahash::AHasher;
use machineid_rs::{IdBuilder, Encryption, HWIDComponent};
use snowdon::{Snowflake as BaseSlowflake, ClassicLayout, Epoch, Generator, MachineId};

static MACHINE_ID_BUILDER: LazyLock<Mutex<IdBuilder>> = LazyLock::new(|| {
    let mut builder = IdBuilder::new(Encryption::SHA256);
    builder
        .add_component(HWIDComponent::SystemID)
        .add_component(HWIDComponent::CPUID)
        .add_component(HWIDComponent::MachineName);
    Mutex::new(builder)
});

pub fn machine_id() -> String {
    MACHINE_ID_BUILDER.lock().unwrap().build("machine_id")
        .expect("Failed to generate machine ID")
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
pub type SnowflakeGenerator =
    Generator<ClassicLayout<SnowflakeParameters>, SnowflakeParameters>;
pub type Snowflake = BaseSlowflake<
    ClassicLayout<SnowflakeParameters>,
    SnowflakeParameters,
>;

static SNOWFLAKE_GENERATOR: LazyLock<SnowflakeGenerator> =
    LazyLock::new(|| {
        SnowflakeGenerator::default()
    });
pub fn flake_id() -> u64 {
    match SNOWFLAKE_GENERATOR.generate() {
        Ok(id) => id.into_inner(),
        Err(e) => {
            // todo:
            panic!("Failed to generate snowflake ID: {}", e)
        },
    }
}