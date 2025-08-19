// The code in this file is copied from
// https://github.com/cryptorelay/redis-aggregation/tree/master
// License: Apache License 2.0

use super::AggregationType;
use crate::common::hash::hash_f64;
use crate::common::rdb::{
    rdb_load_bool, rdb_load_optional_f64, rdb_load_u8, rdb_load_usize, rdb_save_bool,
    rdb_save_optional_f64, rdb_save_u8, rdb_save_usize,
};
use enum_dispatch::enum_dispatch;
use get_size::GetSize;
use std::fmt::Display;
use std::hash::Hash;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, ValkeyString, raw};

type Value = f64;

#[enum_dispatch]
pub trait AggregationHandler {
    fn update(&mut self, value: Value);
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO);
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct FirstAggregator(Option<Value>);
impl AggregationHandler for FirstAggregator {
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.current());
    }
}

impl FirstAggregator {
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
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
    fn update(&mut self, value: Value) {
        self.0 = Some(value)
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.current());
    }
}

impl LastAggregator {
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
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
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.current());
    }
}

impl MinAggregator {
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
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
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_optional_f64(rdb, self.current());
    }
}

impl MaxAggregator {
    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        rdb_load_optional_f64(rdb).map(Self)
    }
}

impl Hash for MaxAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0.unwrap_or(f64::NAN), state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct RangeAggregator {
    min: Value,
    max: Value,
    init: bool,
}
impl AggregationHandler for RangeAggregator {
    fn update(&mut self, value: Value) {
        if !self.init {
            self.init = true;
            self.min = value;
            self.max = value;
        } else {
            self.max = self.max.max(value);
            self.min = self.min.min(value);
        }
    }
    fn reset(&mut self) {
        self.max = 0.;
        self.min = 0.;
        self.init = false;
    }
    fn current(&self) -> Option<Value> {
        if !self.init {
            None
        } else {
            Some(self.max - self.min)
        }
    }
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_bool(rdb, self.init);
        if self.init {
            raw::save_double(rdb, self.min);
            raw::save_double(rdb, self.max);
        }
    }
}

impl RangeAggregator {
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let init = rdb_load_bool(rdb)?;
        if init {
            let min = raw::load_double(rdb)?;
            let max = raw::load_double(rdb)?;
            Ok(Self { min, max, init })
        } else {
            Ok(Self::default())
        }
    }
}

impl Hash for RangeAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.init.hash(state);
        hash_f64(self.min, state);
        hash_f64(self.max, state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct AvgAggregator {
    count: usize,
    sum: Value,
}
impl AggregationHandler for AvgAggregator {
    fn update(&mut self, value: Value) {
        self.sum += value;
        self.count += 1;
    }
    fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.;
    }
    fn current(&self) -> Option<Value> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64)
        }
    }
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.sum);
        rdb_save_usize(rdb, self.count);
    }
}

impl AvgAggregator {
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let sum = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        Ok(Self { count, sum })
    }
}

impl Hash for AvgAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.sum, state);
        self.count.hash(state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct SumAggregator(Value);
impl AggregationHandler for SumAggregator {
    fn update(&mut self, value: Value) {
        self.0 += value;
    }
    fn reset(&mut self) {
        self.0 = 0.;
    }
    fn current(&self) -> Option<Value> {
        Some(self.0)
    }
    fn empty_value(&self) -> Value {
        0.
    }
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.0);
    }
}

impl SumAggregator {
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let value = raw::load_double(rdb)?;
        Ok(Self(value))
    }
}

impl Hash for SumAggregator {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        hash_f64(self.0, state);
    }
}

#[derive(Copy, Clone, Default, Debug, PartialEq, GetSize)]
pub struct CountAggregator(usize);
impl AggregationHandler for CountAggregator {
    fn update(&mut self, _value: Value) {
        self.0 += 1;
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
    fn current(&self) -> Option<Value> {
        Some(self.0 as Value)
    }
    fn empty_value(&self) -> Value {
        0.
    }
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.0);
    }
}

impl CountAggregator {
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let value = rdb_load_usize(rdb)?;
        Ok(Self(value))
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
    fn from_str(buf: &str) -> AggStd {
        let t = serde_json::from_str::<(Value, Value, usize)>(buf).unwrap();
        Self {
            sum: t.0,
            sum_2: t.1,
            count: t.2,
        }
    }
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
        //  var(X) = sum((x_i - E[X])^2)
        //  = sum(x_i^2) - 2 * sum(x_i) * E[X] + E^2[X]
        if self.count <= 1 {
            0.
        } else {
            let avg = self.sum / self.count as Value;
            self.sum_2 - 2. * self.sum * avg + avg * avg * self.count as Value
        }
    }

    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        raw::save_double(rdb, self.sum);
        raw::save_double(rdb, self.sum_2);
        rdb_save_usize(rdb, self.count);
    }

    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let sum = raw::load_double(rdb)?;
        let sum_2 = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        Ok(Self { sum, sum_2, count })
    }
}

impl Display for AggStd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = serde_json::to_string(&(self.sum, self.sum_2, self.count)).unwrap();
        write!(f, "{repr}")
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

fn load_online_aggregator(rdb: *mut RedisModuleIO) -> ValkeyResult<OnlineAggregator> {
    let inner = AggStd::load_from_rdb(rdb)?;
    Ok(Box::new(inner))
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarPAggregator(OnlineAggregator);
impl AggregationHandler for VarPAggregator {
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }
}

impl VarPAggregator {
    fn new() -> Self {
        Self(Box::default())
    }
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let inner = AggStd::load_from_rdb(rdb)?;
        Ok(Self(Box::new(inner)))
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct VarSAggregator(OnlineAggregator);
impl AggregationHandler for VarSAggregator {
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }
}

impl VarSAggregator {
    fn new() -> Self {
        Self(Box::default())
    }
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let inner = load_online_aggregator(rdb)?;
        Ok(Self(inner))
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdPAggregator(OnlineAggregator);
impl AggregationHandler for StdPAggregator {
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }
}

impl StdPAggregator {
    fn new() -> Self {
        Self(Box::default())
    }
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let inner = load_online_aggregator(rdb)?;
        Ok(Self(inner))
    }
}

#[derive(Clone, Default, Debug, Hash, PartialEq, GetSize)]
pub struct StdSAggregator(OnlineAggregator);
impl AggregationHandler for StdSAggregator {
    fn update(&mut self, value: Value) {
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
    fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        self.0.save_to_rdb(rdb);
    }
}

impl StdSAggregator {
    fn new() -> Self {
        Self(Box::default())
    }
    fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let inner = load_online_aggregator(rdb)?;
        Ok(Self(inner))
    }
}

#[enum_dispatch(AggregationHandler)]
#[derive(Clone, Debug, Hash, PartialEq, GetSize)]
pub enum Aggregator {
    First(FirstAggregator),
    Last(LastAggregator),
    Min(MinAggregator),
    Max(MaxAggregator),
    Avg(AvgAggregator),
    Sum(SumAggregator),
    Count(CountAggregator),
    Range(RangeAggregator),
    StdS(StdSAggregator),
    StdP(StdPAggregator),
    VarS(VarSAggregator),
    VarP(VarPAggregator),
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
            AggregationType::Avg => Aggregator::Avg(AvgAggregator::default()),
            AggregationType::Count => Aggregator::Count(CountAggregator::default()),
            AggregationType::First => Aggregator::First(FirstAggregator::default()),
            AggregationType::Last => Aggregator::Last(LastAggregator::default()),
            AggregationType::Max => Aggregator::Max(MaxAggregator::default()),
            AggregationType::Min => Aggregator::Min(MinAggregator::default()),
            AggregationType::Range => Aggregator::Range(RangeAggregator::default()),
            AggregationType::StdP => Aggregator::StdP(StdPAggregator::default()),
            AggregationType::StdS => Aggregator::StdS(StdSAggregator::default()),
            AggregationType::VarP => Aggregator::VarP(VarPAggregator::default()),
            AggregationType::VarS => Aggregator::VarS(VarSAggregator::default()),
            AggregationType::Sum => Aggregator::Sum(SumAggregator::default()),
        }
    }
}

impl Aggregator {
    pub fn new(aggr: AggregationType) -> Self {
        aggr.into()
    }

    pub fn aggregation_type(&self) -> AggregationType {
        match self {
            Aggregator::First(_) => AggregationType::First,
            Aggregator::Last(_) => AggregationType::Last,
            Aggregator::Min(_) => AggregationType::Min,
            Aggregator::Max(_) => AggregationType::Max,
            Aggregator::Avg(_) => AggregationType::Avg,
            Aggregator::Sum(_) => AggregationType::Sum,
            Aggregator::Count(_) => AggregationType::Count,
            Aggregator::Range(_) => AggregationType::Range,
            Aggregator::StdP(_) => AggregationType::StdP,
            Aggregator::StdS(_) => AggregationType::StdS,
            Aggregator::VarP(_) => AggregationType::VarP,
            Aggregator::VarS(_) => AggregationType::VarS,
        }
    }

    pub fn save(&self, rdb: *mut RedisModuleIO) {
        // Save the aggregation type first
        let agg_type = self.aggregation_type();
        save_aggregation_type(rdb, agg_type);

        match self {
            Aggregator::First(agg) => agg.save_to_rdb(rdb),
            Aggregator::Last(agg) => agg.save_to_rdb(rdb),
            Aggregator::Min(agg) => agg.save_to_rdb(rdb),
            Aggregator::Max(agg) => agg.save_to_rdb(rdb),
            Aggregator::Avg(agg) => agg.save_to_rdb(rdb),
            Aggregator::Sum(agg) => agg.save_to_rdb(rdb),
            Aggregator::Count(agg) => agg.save_to_rdb(rdb),
            Aggregator::Range(agg) => agg.save_to_rdb(rdb),
            Aggregator::StdP(agg) => agg.save_to_rdb(rdb),
            Aggregator::StdS(agg) => agg.save_to_rdb(rdb),
            Aggregator::VarP(agg) => agg.save_to_rdb(rdb),
            Aggregator::VarS(agg) => agg.save_to_rdb(rdb),
        }
    }

    pub fn load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        // Load the aggregation type
        let agg_type = load_aggregation_type(rdb)?;
        let agg: Aggregator = match agg_type {
            AggregationType::Avg => AvgAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Count => CountAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::First => FirstAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Last => LastAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Max => MaxAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Min => MinAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Range => RangeAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::StdP => StdPAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::StdS => StdSAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::VarP => VarPAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::VarS => VarSAggregator::load_from_rdb(rdb)?.into(),
            AggregationType::Sum => SumAggregator::load_from_rdb(rdb)?.into(),
        };
        Ok(agg)
    }
}

fn save_aggregation_type(rdb: *mut RedisModuleIO, agg_type: AggregationType) {
    let val: u8 = agg_type.into();
    rdb_save_u8(rdb, val);
}

fn load_aggregation_type(rdb: *mut RedisModuleIO) -> ValkeyResult<AggregationType> {
    let val = rdb_load_u8(rdb)?;
    AggregationType::try_from(val)
}

#[cfg(test)]
mod tests {}
