// The code in this file is copied from
// https://github.com/cryptorelay/redis-aggregation/tree/master
// License: Apache License 2.0

use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyString};

type Value = f64;

pub trait AggOp {
    fn save(&self) -> (&str, String);
    fn load(&mut self, buf: &str);
    fn update(&mut self, value: Value);
    fn reset(&mut self);
    fn current(&self) -> Option<Value>;
    fn empty_value(&self) -> Value {
        f64::NAN
    }
}

#[derive(Clone, Default, Debug)]
pub struct AggFirst(Option<Value>);
impl AggOp for AggFirst {
    fn save(&self) -> (&str, String) {
        ("first", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggLast(Option<Value>);
impl AggOp for AggLast {
    fn save(&self) -> (&str, String) {
        ("last", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
    fn update(&mut self, value: Value) {
        self.0 = Some(value)
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

#[derive(Clone, Default, Debug)]
pub struct AggMin(Option<Value>);
impl AggOp for AggMin {
    fn save(&self) -> (&str, String) {
        ("min", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
    fn update(&mut self, value: Value) {
        match self.0 {
            None => self.0 = Some(value),
            Some(v) if v > value => self.0 = Some(value),
            _ => {}
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

#[derive(Clone, Default, Debug)]
pub struct AggMax(Option<Value>);
impl AggOp for AggMax {
    fn save(&self) -> (&str, String) {
        ("max", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str::<Option<Value>>(buf).unwrap();
    }
    fn update(&mut self, value: Value) {
        match self.0 {
            None => self.0 = Some(value),
            Some(v) if v < value => self.0 = Some(value),
            _ => {}
        }
    }
    fn reset(&mut self) {
        self.0 = None;
    }
    fn current(&self) -> Option<Value> {
        self.0
    }
}

#[derive(Clone, Default, Debug)]
pub struct AggRange {
    min: Value,
    max: Value,
    init: bool,
}
impl AggOp for AggRange {
    fn save(&self) -> (&str, String) {
        (
            "range",
            serde_json::to_string(&(self.init, self.min, self.max)).unwrap(),
        )
    }
    fn load(&mut self, buf: &str) {
        let t = serde_json::from_str::<(bool, Value, Value)>(buf).unwrap();
        self.init = t.0;
        self.min = t.1;
        self.max = t.2;
    }
    fn update(&mut self, value: Value) {
        self.max = self.max.max(value);
        self.min = self.min.min(value);
        self.init = true;
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
}

#[derive(Clone, Default, Debug)]
pub struct AggAvg {
    count: usize,
    sum: Value,
}
impl AggOp for AggAvg {
    fn save(&self) -> (&str, String) {
        (
            "avg",
            serde_json::to_string(&(self.count, self.sum)).unwrap(),
        )
    }
    fn load(&mut self, buf: &str) {
        let t = serde_json::from_str::<(usize, Value)>(buf).unwrap();
        self.count = t.0;
        self.sum = t.1;
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggSum(Value);
impl AggOp for AggSum {
    fn save(&self) -> (&str, String) {
        ("sum", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str(buf).unwrap();
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggCount(usize);
impl AggOp for AggCount {
    fn save(&self) -> (&str, String) {
        ("count", serde_json::to_string(&self.0).unwrap())
    }
    fn load(&mut self, buf: &str) {
        self.0 = serde_json::from_str(buf).unwrap();
    }
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
}

#[derive(Clone, Default, Debug)]
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
}

impl Display for AggStd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = serde_json::to_string(&(self.sum, self.sum_2, self.count)).unwrap();
        write!(f, "{}", repr)
    }
}

#[derive(Clone, Default, Debug)]
pub struct AggVarP(AggStd);
impl AggOp for AggVarP {
    fn save(&self) -> (&str, String) {
        ("varp", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggVarS(AggStd);
impl AggOp for AggVarS {
    fn save(&self) -> (&str, String) {
        ("vars", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggStdP(AggStd);
impl AggOp for AggStdP {
    fn save(&self) -> (&str, String) {
        ("stdp", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
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
}

#[derive(Clone, Default, Debug)]
pub struct AggStdS(AggStd);
impl AggOp for AggStdS {
    fn save(&self) -> (&str, String) {
        ("stds", self.0.to_string())
    }
    fn load(&mut self, buf: &str) {
        self.0 = AggStd::from_str(buf);
    }
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
}

#[derive(Clone, Debug)]
pub enum Aggregator {
    First(AggFirst),
    Last(AggLast),
    Min(AggMin),
    Max(AggMax),
    Avg(AggAvg),
    Sum(AggSum),
    Count(AggCount),
    Range(AggRange),
    StdS(AggStdS),
    StdP(AggStdP),
    VarS(AggVarS),
    VarP(AggVarP),
}

impl TryFrom<&ValkeyString> for Aggregator {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        str.as_str().try_into()
    }
}

impl TryFrom<&str> for Aggregator {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Some(agg) = Self::new(value) {
            return Ok(agg);
        }
        Err(ValkeyError::Str("TSDB: unknown AGGREGATION type"))
    }
}

impl Aggregator {
    pub fn new(name: &str) -> Option<Self> {
        hashify::tiny_map_ignore_case! {
            name.as_bytes(),
            "first" => Aggregator::First(AggFirst::default()),
            "last" => Aggregator::Last(AggLast::default()),
            "min" => Aggregator::Min(AggMin::default()),
            "max" => Aggregator::Max(AggMax::default()),
            "avg" => Aggregator::Avg(AggAvg::default()),
            "sum" => Aggregator::Sum(AggSum::default()),
            "count" => Aggregator::Count(AggCount::default()),
            "range" => Aggregator::Range(AggRange::default()),
            "std.s" => Aggregator::StdS(AggStdS::default()),
            "std.p" => Aggregator::StdP(AggStdP::default()),
            "var.s" => Aggregator::VarS(AggVarS::default()),
            "var.p" => Aggregator::VarP(AggVarP::default()),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Aggregator::First(_) => "first",
            Aggregator::Last(_) => "last",
            Aggregator::Min(_) => "min",
            Aggregator::Max(_) => "max",
            Aggregator::Avg(_) => "avg",
            Aggregator::Sum(_) => "sum",
            Aggregator::Count(_) => "count",
            Aggregator::StdS(_) => "std.s",
            Aggregator::StdP(_) => "std.p",
            Aggregator::VarS(_) => "var.s",
            Aggregator::VarP(_) => "var.p",
            Aggregator::Range(_) => "range",
        }
    }

    pub fn finalize(&self) -> f64 {
        if let Some(v) = self.current() {
            v
        } else {
            self.empty_value()
        }
    }
}

impl AggOp for Aggregator {
    fn save(&self) -> (&str, String) {
        match self {
            Aggregator::First(agg) => agg.save(),
            Aggregator::Last(agg) => agg.save(),
            Aggregator::Min(agg) => agg.save(),
            Aggregator::Max(agg) => agg.save(),
            Aggregator::Avg(agg) => agg.save(),
            Aggregator::Sum(agg) => agg.save(),
            Aggregator::Count(agg) => agg.save(),
            Aggregator::StdS(agg) => agg.save(),
            Aggregator::StdP(agg) => agg.save(),
            Aggregator::VarS(agg) => agg.save(),
            Aggregator::VarP(agg) => agg.save(),
            Aggregator::Range(agg) => agg.save(),
        }
    }

    fn load(&mut self, buf: &str) {
        match self {
            Aggregator::First(agg) => agg.load(buf),
            Aggregator::Last(agg) => agg.load(buf),
            Aggregator::Min(agg) => agg.load(buf),
            Aggregator::Max(agg) => agg.load(buf),
            Aggregator::Avg(agg) => agg.load(buf),
            Aggregator::Sum(agg) => agg.load(buf),
            Aggregator::Count(agg) => agg.load(buf),
            Aggregator::StdS(agg) => agg.load(buf),
            Aggregator::StdP(agg) => agg.load(buf),
            Aggregator::VarS(agg) => agg.load(buf),
            Aggregator::VarP(agg) => agg.load(buf),
            Aggregator::Range(agg) => agg.load(buf),
        }
    }

    fn update(&mut self, value: Value) {
        match self {
            Aggregator::First(agg) => agg.update(value),
            Aggregator::Last(agg) => agg.update(value),
            Aggregator::Min(agg) => agg.update(value),
            Aggregator::Max(agg) => agg.update(value),
            Aggregator::Avg(agg) => agg.update(value),
            Aggregator::Sum(agg) => agg.update(value),
            Aggregator::Count(agg) => agg.update(value),
            Aggregator::StdS(agg) => agg.update(value),
            Aggregator::StdP(agg) => agg.update(value),
            Aggregator::VarS(agg) => agg.update(value),
            Aggregator::VarP(agg) => agg.update(value),
            Aggregator::Range(agg) => agg.update(value),
        }
    }

    fn reset(&mut self) {
        match self {
            Aggregator::First(agg) => agg.reset(),
            Aggregator::Last(agg) => agg.reset(),
            Aggregator::Min(agg) => agg.reset(),
            Aggregator::Max(agg) => agg.reset(),
            Aggregator::Avg(agg) => agg.reset(),
            Aggregator::Sum(agg) => agg.reset(),
            Aggregator::Count(agg) => agg.reset(),
            Aggregator::StdS(agg) => agg.reset(),
            Aggregator::StdP(agg) => agg.reset(),
            Aggregator::VarS(agg) => agg.reset(),
            Aggregator::VarP(agg) => agg.reset(),
            Aggregator::Range(agg) => agg.reset(),
        }
    }

    fn current(&self) -> Option<Value> {
        match self {
            Aggregator::First(agg) => agg.current(),
            Aggregator::Last(agg) => agg.current(),
            Aggregator::Min(agg) => agg.current(),
            Aggregator::Max(agg) => agg.current(),
            Aggregator::Avg(agg) => agg.current(),
            Aggregator::Sum(agg) => agg.current(),
            Aggregator::Count(agg) => agg.current(),
            Aggregator::StdS(agg) => agg.current(),
            Aggregator::StdP(agg) => agg.current(),
            Aggregator::VarS(agg) => agg.current(),
            Aggregator::VarP(agg) => agg.current(),
            Aggregator::Range(agg) => agg.current(),
        }
    }

    fn empty_value(&self) -> Value {
        match self {
            Aggregator::First(agg) => agg.empty_value(),
            Aggregator::Last(agg) => agg.empty_value(),
            Aggregator::Min(agg) => agg.empty_value(),
            Aggregator::Max(agg) => agg.empty_value(),
            Aggregator::Avg(agg) => agg.empty_value(),
            Aggregator::Sum(agg) => agg.empty_value(),
            Aggregator::Count(agg) => agg.empty_value(),
            Aggregator::Range(agg) => agg.empty_value(),
            Aggregator::StdS(agg) => agg.empty_value(),
            Aggregator::StdP(agg) => agg.empty_value(),
            Aggregator::VarS(agg) => agg.empty_value(),
            Aggregator::VarP(agg) => agg.empty_value(),
        }
    }
}
