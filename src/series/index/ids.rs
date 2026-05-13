// MIT License
//
// Copyright (c) 2026 Banan Technologies
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
// Original source: https://github.com/banan-tech/banuid
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const CUSTOM_EPOCH: u64 = 1767225600; // 2026-01-01 00:00:00 UTC
const SHARD_ID_BITS: u8 = 13;
const SEQUENCE_BITS: u8 = 10;

const MAX_SHARD_ID: u64 = (1 << SHARD_ID_BITS) - 1;
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;

const SHARD_ID_SHIFT: u8 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u8 = SHARD_ID_BITS + SEQUENCE_BITS;

struct GeneratorState {
    last_timestamp: u64,
    sequence: u64,
}

pub struct IdGenerator {
    shard_id: u16,
    state: Mutex<GeneratorState>,
}

// Module-level generator for convenience API
static DEFAULT_GENERATOR: std::sync::LazyLock<IdGenerator> =
    std::sync::LazyLock::new(IdGenerator::new);

impl IdGenerator {
    pub fn new() -> Self {
        let shard_id = derive_shard_id();
        IdGenerator {
            shard_id,
            state: Mutex::new(GeneratorState {
                last_timestamp: 0,
                sequence: 0,
            }),
        }
    }

    /// Generate an ID using this instance (new ergonomic method)
    pub fn generate(&self) -> u64 {
        self.next_id()
    }

    pub fn with_shard_id(shard_id: u16) -> Self {
        let shard_id = shard_id & (MAX_SHARD_ID as u16);
        IdGenerator {
            shard_id,
            state: Mutex::new(GeneratorState {
                last_timestamp: 0,
                sequence: 0,
            }),
        }
    }

    pub fn next_id(&self) -> u64 {
        loop {
            let mut state = self.state.lock().unwrap();
            let timestamp = current_timestamp();

            return if timestamp == state.last_timestamp {
                if state.sequence >= MAX_SEQUENCE {
                    drop(state);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                state.sequence += 1;
                let sequence = state.sequence;
                ((timestamp - CUSTOM_EPOCH) << TIMESTAMP_SHIFT)
                    | ((self.shard_id as u64) << SHARD_ID_SHIFT)
                    | sequence
            } else {
                state.last_timestamp = timestamp;
                state.sequence = 0;
                ((timestamp - CUSTOM_EPOCH) << TIMESTAMP_SHIFT)
                    | ((self.shard_id as u64) << SHARD_ID_SHIFT)
            };
        }
    }

    pub fn extract_timestamp(id: u64) -> u64 {
        (id >> TIMESTAMP_SHIFT) + CUSTOM_EPOCH
    }

    pub fn extract_shard_id(id: u64) -> u16 {
        ((id >> SHARD_ID_SHIFT) & MAX_SHARD_ID) as u16
    }

    pub fn extract_sequence(id: u64) -> u16 {
        (id & MAX_SEQUENCE) as u16
    }

    /// Parse timestamp from ID (new ergonomic method)
    pub fn parse_timestamp(id: u64) -> u64 {
        Self::extract_timestamp(id)
    }

    /// Parse shard ID from ID (new ergonomic method)
    pub fn parse_shard_id(id: u64) -> u16 {
        Self::extract_shard_id(id)
    }

    /// Parse sequence from ID (new ergonomic method)
    pub fn parse_sequence(id: u64) -> u16 {
        Self::extract_sequence(id)
    }

    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }
}

// Convenient free functions for ergonomic API
/// Generate a unique ID using default generator
pub fn generate() -> u64 {
    DEFAULT_GENERATOR.next_id()
}

/// Parse timestamp from ID using default generator methods
pub fn parse_timestamp(id: u64) -> u64 {
    IdGenerator::extract_timestamp(id)
}

/// Parse shard ID from ID using default generator methods
pub fn parse_shard_id(id: u64) -> u16 {
    IdGenerator::extract_shard_id(id)
}

/// Parse sequence from ID using default generator methods
pub fn parse_sequence(id: u64) -> u16 {
    IdGenerator::extract_sequence(id)
}

fn derive_shard_id() -> u16 {
    let mut hash: u64 = 14695981039346656037; // FNV offset basis
    const FNV_PRIME: u64 = 1099511628211;
    let mut has_identifier = false;

    // Try hostname first (most reliable in containerized environments)
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        has_identifier = true;
        for byte in hostname.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    // Try machine-id (Linux-specific, may fail in containers)
    if let Ok(machine_id) = std::fs::read_to_string("/etc/machine-id") {
        has_identifier = true;
        for byte in machine_id.trim().bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    // Always include process ID for uniqueness within the same host
    let pid = std::process::id();
    for byte in pid.to_string().bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    // If no reliable host identifier found, add randomness with fallback
    if !has_identifier {
        let random_value = get_fallback_random();

        for byte in random_value.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    (hash % (MAX_SHARD_ID + 1)) as u16
}

fn get_fallback_random() -> u32 {
    // Multi-layer fallback for random number generation

    // Layer 1: System time nanoseconds
    if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
        return duration.subsec_nanos();
    }

    // Layer 2: Memory address of a stack variable (non-deterministic)
    let stack_var = 0u64;
    let stack_addr = &stack_var as *const u64 as usize;
    (stack_addr & 0xFFFFFFFF) as u32
}

fn current_timestamp() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => {
            // Fallback: use combination of fallback random and process start time
            let base_time = get_fallback_random() as u64;
            let pid_component = std::process::id() as u64;
            base_time ^ (pid_component << 16)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_generation() {
        let generator = IdGenerator::with_shard_id(42);
        let id1 = generator.next_id();
        let id2 = generator.next_id();

        assert_ne!(id1, id2, "IDs should be unique");
        assert!(id2 > id1, "IDs should be orderable");
    }

    #[test]
    fn test_extract_timestamp() {
        let generator = IdGenerator::with_shard_id(1);
        let id = generator.next_id();
        let extracted = IdGenerator::extract_timestamp(id);
        let now = current_timestamp();

        assert!(
            extracted <= now && extracted >= now - 1000,
            "Extracted timestamp should be recent"
        );
    }

    #[test]
    fn test_extract_shard_id() {
        let shard_id: u16 = 42;
        let generator = IdGenerator::with_shard_id(shard_id);
        let id = generator.next_id();
        let extracted = IdGenerator::extract_shard_id(id);

        assert_eq!(extracted, shard_id, "Shard ID should match");
    }

    #[test]
    fn test_shard_id_bounds() {
        let generator = IdGenerator::with_shard_id(8191); // Max 13-bit value
        assert_eq!(generator.shard_id(), 8191);

        let generator2 = IdGenerator::with_shard_id(10000); // Overflow
        assert_eq!(generator2.shard_id(), 10000 & (MAX_SHARD_ID as u16));
    }

    #[test]
    fn test_auto_derived_shard() {
        let generator = IdGenerator::new();
        let id = generator.next_id();
        let extracted_shard = IdGenerator::extract_shard_id(id);

        assert_eq!(extracted_shard, generator.shard_id());
        assert!(extracted_shard <= 8191);
    }

    #[test]
    fn test_concurrent_generation() {
        use std::sync::Arc;
        use std::thread;

        let generator = Arc::new(IdGenerator::with_shard_id(1));
        let mut handles = vec![];
        let mut ids = std::collections::HashSet::new();

        for _ in 0..10 {
            let gen_ = Arc::clone(&generator);
            handles.push(thread::spawn(move || {
                (0..100).map(|_| gen_.next_id()).collect::<Vec<_>>()
            }));
        }

        for handle in handles {
            let thread_ids = handle.join().unwrap();
            for id in thread_ids {
                assert!(!ids.contains(&id), "Duplicate ID found: {}", id);
                ids.insert(id);
            }
        }

        assert_eq!(ids.len(), 1000, "Should have 1000 unique IDs");
    }

    #[test]
    fn test_fallback_random() {
        let random1 = get_fallback_random();
        let random2 = get_fallback_random();

        // Should produce some variation (not guaranteed, but highly likely)
        assert!(
            random1 != 0 || random2 != 0,
            "At least one value should be non-zero"
        );
    }

    #[test]
    fn test_derive_shard_id_bounds() {
        let shard1 = derive_shard_id();
        let shard2 = derive_shard_id();

        // Both should be within valid bounds
        assert!(shard1 <= MAX_SHARD_ID as u16);
        assert!(shard2 <= MAX_SHARD_ID as u16);
    }

    #[test]
    fn test_ergonomic_api() {
        // Test free functions
        let id1 = generate();
        let id2 = generate();
        assert_ne!(id1, id2, "Generated IDs should be unique");

        // Test parsing functions
        let timestamp = parse_timestamp(id1);
        let shard_id = parse_shard_id(id1);
        let sequence = parse_sequence(id1);

        // Test instance methods
        let generator = IdGenerator::with_shard_id(123);
        let id3 = generator.generate();
        assert_ne!(id3, 0, "Generated ID should be non-zero");

        // Test associated functions (static methods)
        let timestamp2 = IdGenerator::parse_timestamp(id3);
        let shard_id2 = IdGenerator::parse_shard_id(id3);
        let sequence2 = IdGenerator::parse_sequence(id3);

        // Verify consistency between old and new methods
        assert_eq!(timestamp, IdGenerator::extract_timestamp(id1));
        assert_eq!(shard_id, IdGenerator::extract_shard_id(id1));
        assert_eq!(sequence, IdGenerator::extract_sequence(id1));

        assert_eq!(timestamp2, IdGenerator::extract_timestamp(id3));
        assert_eq!(shard_id2, IdGenerator::extract_shard_id(id3));
        assert_eq!(sequence2, IdGenerator::extract_sequence(id3));
    }
}
