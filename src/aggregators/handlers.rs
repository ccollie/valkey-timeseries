// Initial code based on:
// https://github.com/cryptorelay/redis-aggregation/tree/master
// License: Apache License 2.0

use super::{AggregationType, AllAggregator, AnyAggregator, NoneAggregator};
use crate::aggregators::filtered::{CountIfAggregator, ShareAggregator, SumIfAggregator};
use crate::aggregators::kahan::{KahanAvg, KahanSum};
use crate::common::Sample;
use crate::common::hash::hash_f64;
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_optional_f64, rdb_load_u8, rdb_load_usize,
    rdb_save_bool, rdb_save_optional_f64, rdb_save_u8, rdb_save_usize,
};
use enum_dispatch::enum_dispatch;
use get_size2::GetSize;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, ValkeyString, raw};

type Value = f64;
type Timestamp = i64;

#[enum_dispatch]
pub trait AggregationHandler: RdbSerializable + Default + GetSize {
    fn update(&mut self, timestamp: Timestamp, value: Value);
    fn reset(&mut self);
    fn current(&self) -> Option<Value>;
    fn empty_value(&self) -> Value {
        f64::NAN
    }
    fn finalize(&mut self) -> f64 {
        let result = if let Some(v) = self.current() {
            v
        } else {
            self.empty_value()
        };
        self.reset();
        result
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct FirstAggregator(Option<Value>);
impl AggregationHandler for FirstAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        if self.0.is_none() {
            self.0 = Some(value)
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for FirstAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for FirstAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct LastAggregator(Option<Value>);
impl AggregationHandler for LastAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0 = Some(value)
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for LastAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for LastAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct MinAggregator(Option<Value>);
impl AggregationHandler for MinAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0 = Some(match self.0 {
            None => value,
            Some(v) => v.min(value),
        });
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for MinAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0)
    }
    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for MinAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct MaxAggregator(Option<Value>);
impl AggregationHandler for MaxAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0 = Some(match self.0 {
            None => value,
            Some(v) => v.max(value),
        });
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

impl RdbSerializable for MaxAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let value = rdb_load_optional_f64(rdb)?;
        Ok(Self(value))
    }
}

impl Hash for MaxAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct RangeAggregatorState {
    min: Value,
    max: Value,
    _init: bool,
}

impl Hash for RangeAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.min, state);
        hash_f64(self.max, state);
        self._init.hash(state);
    }
}

impl RdbSerializable for RangeAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_bool(rdb, self._init);
        if self._init {
            raw::save_double(rdb, self.min);
            raw::save_double(rdb, self.max);
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let init = rdb_load_bool(rdb)?;
        if init {
            let min = raw::load_double(rdb)?;
            let max = raw::load_double(rdb)?;
            Ok(Self {
                min,
                max,
                _init: init,
            })
        } else {
            Ok(Self::default())
        }
    }
}

impl RangeAggregatorState {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        if !self._init {
            self._init = true;
            self.min = value;
            self.max = value;
        } else {
            self.max = self.max.max(value);
            self.min = self.min.min(value);
        }
    }

    fn reset(&mut self) {
        self.min = 0.;
        self.max = 0.;
        self._init = false;
    }

    fn current(&self) -> Option<Value> {
        if !self._init {
            None
        } else {
            Some(self.max - self.min)
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct RangeAggregator(Box<RangeAggregatorState>);
impl AggregationHandler for RangeAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0.update(_timestamp, value);
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<Value> {
        self.0.current()
    }
}

impl RdbSerializable for RangeAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        RangeAggregatorState::rdb_load(rdb).map(|state| Self(Box::new(state)))
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct AvgAggregator(KahanAvg);

impl AggregationHandler for AvgAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0 += value;
    }
    fn reset(&mut self) {
        self.0.reset();
    }
    fn current(&self) -> Option<Value> {
        self.0.value()
    }
}

impl RdbSerializable for AvgAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = KahanAvg::rdb_load(rdb)?;
        Ok(Self(inner))
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize, Hash)]
pub struct SumAggregator(KahanSum);
impl AggregationHandler for SumAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0 += value;
    }
    fn reset(&mut self) {
        self.0.reset();
    }
    fn current(&self) -> Option<Value> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.value())
        }
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for SumAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = KahanSum::rdb_load(rdb)?;
        Ok(Self(inner))
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct CountAggregator(usize);
impl AggregationHandler for CountAggregator {
    fn update(&mut self, _timestamp: Timestamp, _value: Value) {
        self.0 += 1;
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        if self.0 == 0 {
            return None;
        }
        Some(self.0 as Value)
    }
    fn empty_value(&self) -> Value {
        0.
    }
}

impl RdbSerializable for CountAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_usize(rdb).map(Self)
    }
}

impl Hash for CountAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct AggStd {
    sum: Value,
    sum_2: Value,
    count: usize,
}

impl AggStd {
    fn add(&mut self, value: Value) {
        self.sum += value;
        self.sum_2 += value * value;
        self.count += 1;
    }
    fn reset(&mut self) {
        self.sum = 0.;
        self.sum_2 = 0.;
        self.count = 0;
    }
    fn variance(&self) -> Value {
        if self.count <= 1 {
            0.
        } else {
            let avg = self.sum / self.count as Value;
            self.sum_2 - 2. * self.sum * avg + avg * avg * self.count as Value
        }
    }
}

impl Display for AggStd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sum: {}, sum2: {}, count: {}",
            self.sum, self.sum_2, self.count
        )
    }
}

impl RdbSerializable for AggStd {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.sum);
        raw::save_double(rdb, self.sum_2);
        rdb_save_usize(rdb, self.count);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let sum = raw::load_double(rdb)?;
        let sum_2 = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        Ok(Self { sum, sum_2, count })
    }
}

impl Hash for AggStd {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.sum, state);
        hash_f64(self.sum_2, state);
        self.count.hash(state);
    }
}

// boxed to minimize size of stack Aggregator enum
pub(crate) type OnlineAggregator = Box<AggStd>;
impl RdbSerializable for OnlineAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.as_ref().rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = AggStd::rdb_load(rdb)?;
        Ok(Box::new(inner))
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarPAggregator(OnlineAggregator);
impl AggregationHandler for VarPAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some(self.0.variance() / self.0.count as Value)
        }
    }
}

impl RdbSerializable for VarPAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarSAggregator(OnlineAggregator);
impl AggregationHandler for VarSAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some(self.0.variance() / (self.0.count - 1) as Value)
        }
    }
}

impl RdbSerializable for VarSAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdPAggregator(OnlineAggregator);
impl AggregationHandler for StdPAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else {
            Some((self.0.variance() / self.0.count as Value).sqrt())
        }
    }
}

impl RdbSerializable for StdPAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdSAggregator(OnlineAggregator);
impl AggregationHandler for StdSAggregator {
    fn update(&mut self, _timestamp: Timestamp, value: Value) {
        self.0.add(value)
    }
    fn reset(&mut self) {
        self.0.reset()
    }
    fn current(&self) -> Option<Value> {
        if self.0.count == 0 {
            None
        } else if self.0.count == 1 {
            Some(0.)
        } else {
            Some((self.0.variance() / (self.0.count - 1) as Value).sqrt())
        }
    }
}

impl RdbSerializable for StdSAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        OnlineAggregator::rdb_load(rdb).map(Self)
    }
}

/// State for rate/increase calculations of monotonically increasing counters.
/// It also properly handles resets
#[derive(Debug, Default, Clone, PartialEq, GetSize)]
pub struct CounterAggregatorState {
    /// Accumulated deltas
    sum_deltas: f64,
    // last raw value we saw
    last_value: Option<f64>,
}

impl Hash for CounterAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.sum_deltas, state);
        hash_f64(self.last_value.unwrap_or(f64::NAN), state);
    }
}

impl RdbSerializable for CounterAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.last_value);
        raw::save_double(rdb, self.sum_deltas);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let last_value = rdb_load_optional_f64(rdb)?;
        let sum_deltas = raw::load_double(rdb)?;
        Ok(CounterAggregatorState {
            sum_deltas,
            last_value,
        })
    }
}

impl CounterAggregatorState {
    pub fn update(&mut self, _t: Timestamp, v: f64) {
        // Compute delta with reset handling.
        let delta = match self.last_value {
            None => 0.0, // first point: no prior delta
            Some(last) => {
                let d = v - last;
                if d >= 0.0 {
                    d
                } else {
                    // Counter reset or went backwards: treat as 0 increment.
                    0.0
                }
            }
        };

        self.last_value = Some(v);

        // Add the new delta to the window.
        if delta > 0.0 {
            self.sum_deltas += delta;
        } else {
            // Even if delta == 0, we may want to keep the timestamp to
            // age out the window precisely. For simplicity here, we don't.
        }
    }

    pub fn clear(&mut self) {
        self.sum_deltas = 0.0;
        self.last_value = None;
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct IncreaseAggregator(CounterAggregatorState);

impl AggregationHandler for IncreaseAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) {
        self.0.update(timestamp, value);
    }
    fn reset(&mut self) {
        self.0.clear();
    }
    fn current(&self) -> Option<Value> {
        self.0.last_value?;
        Some(self.0.sum_deltas)
    }
}

impl RdbSerializable for IncreaseAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        CounterAggregatorState::rdb_load(rdb).map(Self)
    }
}

#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct RateAggregatorState {
    window: u64,
    counter: CounterAggregatorState,
}

/// Online rate calculator for a monotonically increasing counter with resets.
///
/// Algorithm:
/// - For each new sample, compute delta = max(v - last_v, 0).
///   This treats negative jumps as resets and never produces negative deltas.
/// - Maintain a sliding time window of these deltas and their timestamps.
/// - Rate ≈ sum(deltas in window) / window_length.
#[derive(Debug, Default, Clone, Hash, PartialEq, GetSize)]
pub struct RateAggregator(Box<RateAggregatorState>);

impl RateAggregator {
    /// Create a new online rate calculator with the given window length.
    pub fn new(window: Duration) -> Self {
        let counter = CounterAggregatorState {
            sum_deltas: 0.0,
            last_value: None,
        };
        let state = Box::new(RateAggregatorState {
            window: window.as_secs(),
            counter,
        });
        Self(state)
    }

    pub fn set_window_ms(&mut self, window: u64) {
        let secs = window / 1000;
        self.0.window = secs;
    }

    fn clear(&mut self) {
        self.0.counter.clear();
    }
}

impl RdbSerializable for RateAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0.window as usize);
        self.0.counter.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let window = rdb_load_usize(rdb)? as u64;
        let counter = CounterAggregatorState::rdb_load(rdb)?;
        let state = Box::new(RateAggregatorState { window, counter });

        Ok(Self(state))
    }
}

impl AggregationHandler for RateAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) {
        self.0.counter.update(timestamp, value);
    }
    fn reset(&mut self) {
        self.clear();
    }
    fn current(&self) -> Option<Value> {
        let state = &self.0;
        state.counter.last_value?;
        if state.window > 0 {
            Some(state.counter.sum_deltas / state.window as f64)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, GetSize)]
pub struct IRateAggregatorState {
    dt: i64,
    dv: f64,
    last_sample: Option<Sample>,
}

impl Hash for IRateAggregatorState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.dt.hash(state);
        hash_f64(self.dv, state);
        if let Some(sample) = &self.last_sample {
            sample.hash(state);
        } else {
            hash_f64(f64::NAN, state);
        }
    }
}

impl IRateAggregatorState {
    fn update_irate(&mut self, timestamp: Timestamp, value: Value) {
        let current = Sample { timestamp, value };

        if let Some(prev) = self.last_sample {
            // Detect reset: counter dropped instead of increased.
            if current.value < prev.value {
                // Treat as reset: keep current as the new baseline, but no rate.
                self.dv = 0.0;
                self.dt = 0;
                self.last_sample = Some(current);
            } else {
                self.dt = current.timestamp - prev.timestamp;
                if self.dt > 0 {
                    self.dv = current.value - prev.value;
                    self.last_sample = Some(current);
                }
            }
        } else {
            // First sample: just store it.
            self.last_sample = Some(current);
        }
    }

    fn reset(&mut self) {
        self.dt = 0;
        self.dv = 0.0;
        self.last_sample = None;
    }
}

impl RdbSerializable for IRateAggregatorState {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        if let Some(x) = self.last_sample {
            rdb_save_optional_f64(rdb, Some(x.value));
            rdb_save_usize(rdb, x.timestamp as usize);
            raw::save_signed(rdb, self.dt);
            raw::save_double(rdb, self.dv);
            return;
        }
        rdb_save_optional_f64(rdb, None);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let Some(value) = rdb_load_optional_f64(rdb)? else {
            return Ok(Self {
                last_sample: None,
                dt: 0,
                dv: 0.0,
            });
        };

        let timestamp = rdb_load_usize(rdb)? as i64;
        let last_sample = Some(Sample { timestamp, value });
        let dt = raw::load_signed(rdb)?;
        let dv = raw::load_double(rdb)?;

        Ok(Self {
            last_sample,
            dt,
            dv,
        })
    }
}

#[derive(Clone, Debug, Default, GetSize, Hash, PartialEq)]
pub struct IRateAggregator(Box<IRateAggregatorState>);

impl RdbSerializable for IRateAggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        self.0.rdb_save(rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let inner = IRateAggregatorState::rdb_load(rdb)?;
        Ok(Self(Box::new(inner)))
    }
}

impl AggregationHandler for IRateAggregator {
    fn update(&mut self, timestamp: Timestamp, value: Value) {
        self.0.update_irate(timestamp, value);
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn current(&self) -> Option<Value> {
        if self.0.last_sample.is_none() || self.0.dt == 0 {
            return None;
        }
        let rate = self.0.dv / (self.0.dt as f64 / 1e3);
        Some(rate)
    }
}

// Deriv implementation for gauge aggregators
#[derive(Clone, Debug, Hash, PartialEq, GetSize)]
pub struct DerivAggregator(IRateAggregatorState);

#[enum_dispatch(AggregationHandler)]
#[derive(Clone, Debug, Hash, PartialEq, GetSize)]
pub enum Aggregator {
    All(AllAggregator),
    Any(AnyAggregator),
    Avg(AvgAggregator),
    Count(CountAggregator),
    CountIf(CountIfAggregator),
    First(FirstAggregator),
    Increase(IncreaseAggregator),
    IRate(IRateAggregator),
    Last(LastAggregator),
    Max(MaxAggregator),
    Min(MinAggregator),
    None(NoneAggregator),
    Range(RangeAggregator),
    Rate(RateAggregator),
    Share(ShareAggregator),
    StdP(StdPAggregator),
    StdS(StdSAggregator),
    Sum(SumAggregator),
    SumIf(SumIfAggregator),
    VarP(VarPAggregator),
    VarS(VarSAggregator),
}

impl Default for Aggregator {
    fn default() -> Self {
        AggregationType::Avg.into()
    }
}

impl TryFrom<&ValkeyString> for Aggregator {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        let aggregation = AggregationType::try_from(str.as_str())?;
        Ok(aggregation.into())
    }
}

impl TryFrom<&str> for Aggregator {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let aggregation = AggregationType::try_from(value)?;
        Ok(aggregation.into())
    }
}

impl From<AggregationType> for Aggregator {
    fn from(agg: AggregationType) -> Self {
        match agg {
            AggregationType::All => Aggregator::All(AllAggregator::default()),
            AggregationType::Any => Aggregator::Any(AnyAggregator::default()),
            AggregationType::Avg => Aggregator::Avg(AvgAggregator::default()),
            AggregationType::Count => Aggregator::Count(CountAggregator::default()),
            AggregationType::CountIf => Aggregator::CountIf(CountIfAggregator::default()),
            AggregationType::First => Aggregator::First(FirstAggregator::default()),
            AggregationType::Increase => Aggregator::Increase(IncreaseAggregator::default()),
            AggregationType::IRate => Aggregator::IRate(IRateAggregator::default()),
            AggregationType::Last => Aggregator::Last(LastAggregator::default()),
            AggregationType::Max => Aggregator::Max(MaxAggregator::default()),
            AggregationType::Min => Aggregator::Min(MinAggregator::default()),
            AggregationType::None => Aggregator::None(NoneAggregator::default()),
            AggregationType::Range => Aggregator::Range(RangeAggregator::default()),
            AggregationType::Rate => Aggregator::Rate(RateAggregator::default()),
            AggregationType::Share => Aggregator::Share(ShareAggregator::default()),
            AggregationType::StdP => Aggregator::StdP(StdPAggregator::default()),
            AggregationType::StdS => Aggregator::StdS(StdSAggregator::default()),
            AggregationType::VarP => Aggregator::VarP(VarPAggregator::default()),
            AggregationType::VarS => Aggregator::VarS(VarSAggregator::default()),
            AggregationType::Sum => Aggregator::Sum(SumAggregator::default()),
            AggregationType::SumIf => Aggregator::SumIf(SumIfAggregator::default()),
        }
    }
}

impl RdbSerializable for Aggregator {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        let agg_type = self.aggregation_type() as u8;
        rdb_save_u8(rdb, agg_type);

        match self {
            Aggregator::All(agg) => agg.rdb_save(rdb),
            Aggregator::Any(agg) => agg.rdb_save(rdb),
            Aggregator::Avg(agg) => agg.rdb_save(rdb),
            Aggregator::Count(agg) => agg.rdb_save(rdb),
            Aggregator::CountIf(agg) => agg.rdb_save(rdb),
            Aggregator::First(agg) => agg.rdb_save(rdb),
            Aggregator::Increase(agg) => agg.rdb_save(rdb),
            Aggregator::IRate(agg) => agg.rdb_save(rdb),
            Aggregator::Last(agg) => agg.rdb_save(rdb),
            Aggregator::Max(agg) => agg.rdb_save(rdb),
            Aggregator::Min(agg) => agg.rdb_save(rdb),
            Aggregator::None(agg) => agg.rdb_save(rdb),
            Aggregator::Rate(agg) => agg.rdb_save(rdb),
            Aggregator::Range(agg) => agg.rdb_save(rdb),
            Aggregator::Share(agg) => agg.rdb_save(rdb),
            Aggregator::Sum(agg) => agg.rdb_save(rdb),
            Aggregator::SumIf(agg) => agg.rdb_save(rdb),
            Aggregator::StdP(agg) => agg.rdb_save(rdb),
            Aggregator::StdS(agg) => agg.rdb_save(rdb),
            Aggregator::VarP(agg) => agg.rdb_save(rdb),
            Aggregator::VarS(agg) => agg.rdb_save(rdb),
        }
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        let agg_type_u8 = rdb_load_u8(rdb)?;
        let agg_type = AggregationType::try_from(agg_type_u8)?;

        match agg_type {
            AggregationType::All => AllAggregator::rdb_load(rdb).map(Aggregator::All),
            AggregationType::Any => AnyAggregator::rdb_load(rdb).map(Aggregator::Any),
            AggregationType::Avg => AvgAggregator::rdb_load(rdb).map(Aggregator::Avg),
            AggregationType::Count => CountAggregator::rdb_load(rdb).map(Aggregator::Count),
            AggregationType::CountIf => CountIfAggregator::rdb_load(rdb).map(Aggregator::CountIf),
            AggregationType::First => FirstAggregator::rdb_load(rdb).map(Aggregator::First),
            AggregationType::Increase => {
                IncreaseAggregator::rdb_load(rdb).map(Aggregator::Increase)
            }
            AggregationType::IRate => IRateAggregator::rdb_load(rdb).map(Aggregator::IRate),
            AggregationType::Last => LastAggregator::rdb_load(rdb).map(Aggregator::Last),
            AggregationType::Max => MaxAggregator::rdb_load(rdb).map(Aggregator::Max),
            AggregationType::Min => MinAggregator::rdb_load(rdb).map(Aggregator::Min),
            AggregationType::None => NoneAggregator::rdb_load(rdb).map(Aggregator::None),
            AggregationType::Range => RangeAggregator::rdb_load(rdb).map(Aggregator::Range),
            AggregationType::Rate => RateAggregator::rdb_load(rdb).map(Aggregator::Rate),
            AggregationType::Share => ShareAggregator::rdb_load(rdb).map(Aggregator::Share),
            AggregationType::StdP => StdPAggregator::rdb_load(rdb).map(Aggregator::StdP),
            AggregationType::StdS => StdSAggregator::rdb_load(rdb).map(Aggregator::StdS),
            AggregationType::VarP => VarPAggregator::rdb_load(rdb).map(Aggregator::VarP),
            AggregationType::VarS => VarSAggregator::rdb_load(rdb).map(Aggregator::VarS),
            AggregationType::Sum => SumAggregator::rdb_load(rdb).map(Aggregator::Sum),
            AggregationType::SumIf => SumIfAggregator::rdb_load(rdb).map(Aggregator::SumIf),
        }
    }
}

impl Aggregator {
    pub fn aggregation_type(&self) -> AggregationType {
        match self {
            Aggregator::All(_) => AggregationType::All,
            Aggregator::Any(_) => AggregationType::Any,
            Aggregator::Avg(_) => AggregationType::Avg,
            Aggregator::Count(_) => AggregationType::Count,
            Aggregator::CountIf(_) => AggregationType::CountIf,
            Aggregator::First(_) => AggregationType::First,
            Aggregator::Increase(_) => AggregationType::Increase,
            Aggregator::IRate(_) => AggregationType::IRate,
            Aggregator::Last(_) => AggregationType::Last,
            Aggregator::Max(_) => AggregationType::Max,
            Aggregator::Min(_) => AggregationType::Min,
            Aggregator::None(_) => AggregationType::None,
            Aggregator::Range(_) => AggregationType::Range,
            Aggregator::Rate(_) => AggregationType::Rate,
            Aggregator::Share(_) => AggregationType::Share,
            Aggregator::StdP(_) => AggregationType::StdP,
            Aggregator::StdS(_) => AggregationType::StdS,
            Aggregator::VarP(_) => AggregationType::VarP,
            Aggregator::VarS(_) => AggregationType::VarS,
            Aggregator::Sum(_) => AggregationType::Sum,
            Aggregator::SumIf(_) => AggregationType::SumIf,
        }
    }
}
