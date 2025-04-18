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
        ("var.p", self.0.to_string())
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
        ("var.s", self.0.to_string())
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
        ("std.p", self.0.to_string())
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
        ("std.s", self.0.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregator_first_save_load() {
        let agg = Aggregator::First(AggFirst(Some(42.0)));
        let (name, serialized) = agg.save();

        assert_eq!(name, "first");

        let mut new_agg = Aggregator::First(AggFirst::default());
        new_agg.load(&serialized);

        if let Aggregator::First(first) = new_agg {
            assert_eq!(first.0, Some(42.0));
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_last_save_load() {
        let agg = Aggregator::Last(AggLast(Some(123.5)));
        let (name, serialized) = agg.save();

        assert_eq!(name, "last");

        let mut new_agg = Aggregator::Last(AggLast::default());
        new_agg.load(&serialized);

        if let Aggregator::Last(last) = new_agg {
            assert_eq!(last.0, Some(123.5));
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_min_save_load() {
        let agg = Aggregator::Min(AggMin(Some(-1.5)));
        let (name, serialized) = agg.save();

        assert_eq!(name, "min");

        let mut new_agg = Aggregator::Min(AggMin::default());
        new_agg.load(&serialized);

        if let Aggregator::Min(min) = new_agg {
            assert_eq!(min.0, Some(-1.5));
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_max_save_load() {
        let agg = Aggregator::Max(AggMax(Some(999.9)));
        let (name, serialized) = agg.save();

        assert_eq!(name, "max");

        let mut new_agg = Aggregator::Max(AggMax::default());
        new_agg.load(&serialized);

        if let Aggregator::Max(max) = new_agg {
            assert_eq!(max.0, Some(999.9));
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_sum_save_load() {
        let agg = Aggregator::Sum(AggSum(123.45));
        let (name, serialized) = agg.save();

        assert_eq!(name, "sum");

        let mut new_agg = Aggregator::Sum(AggSum::default());
        new_agg.load(&serialized);

        if let Aggregator::Sum(sum) = new_agg {
            assert_eq!(sum.0, 123.45);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_count_save_load() {
        let agg = Aggregator::Count(AggCount(42));
        let (name, serialized) = agg.save();

        assert_eq!(name, "count");

        let mut new_agg = Aggregator::Count(AggCount::default());
        new_agg.load(&serialized);

        if let Aggregator::Count(count) = new_agg {
            assert_eq!(count.0, 42);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_avg_save_load() {
        let agg = Aggregator::Avg(AggAvg {
            count: 5,
            sum: 50.0,
        });
        let (name, serialized) = agg.save();

        assert_eq!(name, "avg");

        let mut new_agg = Aggregator::Avg(AggAvg::default());
        new_agg.load(&serialized);

        if let Aggregator::Avg(avg) = new_agg {
            assert_eq!(avg.count, 5);
            assert_eq!(avg.sum, 50.0);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_range_save_load() {
        let agg = Aggregator::Range(AggRange {
            min: 10.0,
            max: 20.0,
            init: true,
        });
        let (name, serialized) = agg.save();

        assert_eq!(name, "range");

        let mut new_agg = Aggregator::Range(AggRange::default());
        new_agg.load(&serialized);

        if let Aggregator::Range(range) = new_agg {
            assert_eq!(range.min, 10.0);
            assert_eq!(range.max, 20.0);
            assert!(range.init);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_stdp_save_load() {
        let agg = Aggregator::StdP(AggStdP(AggStd {
            sum: 100.0,
            sum_2: 1050.0,
            count: 10,
        }));
        let (name, serialized) = agg.save();

        assert_eq!(name, "std.p");

        let mut new_agg = Aggregator::StdP(AggStdP::default());
        new_agg.load(&serialized);

        if let Aggregator::StdP(stdp) = new_agg {
            assert_eq!(stdp.0.sum, 100.0);
            assert_eq!(stdp.0.sum_2, 1050.0);
            assert_eq!(stdp.0.count, 10);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_stds_save_load() {
        let agg = Aggregator::StdS(AggStdS(AggStd {
            sum: 200.0,
            sum_2: 4100.0,
            count: 20,
        }));
        let (name, serialized) = agg.save();

        assert_eq!(name, "std.s");

        let mut new_agg = Aggregator::StdS(AggStdS::default());
        new_agg.load(&serialized);

        if let Aggregator::StdS(stds) = new_agg {
            assert_eq!(stds.0.sum, 200.0);
            assert_eq!(stds.0.sum_2, 4100.0);
            assert_eq!(stds.0.count, 20);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_varp_save_load() {
        let agg = Aggregator::VarP(AggVarP(AggStd {
            sum: 150.0,
            sum_2: 2350.0,
            count: 15,
        }));
        let (name, serialized) = agg.save();

        assert_eq!(name, "var.p");

        let mut new_agg = Aggregator::VarP(AggVarP::default());
        new_agg.load(&serialized);

        if let Aggregator::VarP(varp) = new_agg {
            assert_eq!(varp.0.sum, 150.0);
            assert_eq!(varp.0.sum_2, 2350.0);
            assert_eq!(varp.0.count, 15);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_vars_save_load() {
        let agg = Aggregator::VarS(AggVarS(AggStd {
            sum: 250.0,
            sum_2: 6350.0,
            count: 25,
        }));
        let (name, serialized) = agg.save();

        assert_eq!(name, "var.s");

        let mut new_agg = Aggregator::VarS(AggVarS::default());
        new_agg.load(&serialized);

        if let Aggregator::VarS(vars) = new_agg {
            assert_eq!(vars.0.sum, 250.0);
            assert_eq!(vars.0.sum_2, 6350.0);
            assert_eq!(vars.0.count, 25);
        } else {
            panic!("Wrong aggregator type after loading");
        }
    }

    #[test]
    fn test_aggregator_empty_save_load() {
        // Test with default values
        let aggregator_types = vec![
            Aggregator::First(AggFirst::default()),
            Aggregator::Last(AggLast::default()),
            Aggregator::Min(AggMin::default()),
            Aggregator::Max(AggMax::default()),
            Aggregator::Avg(AggAvg::default()),
            Aggregator::Sum(AggSum::default()),
            Aggregator::Count(AggCount::default()),
            Aggregator::Range(AggRange::default()),
            Aggregator::StdS(AggStdS::default()),
            Aggregator::StdP(AggStdP::default()),
            Aggregator::VarS(AggVarS::default()),
            Aggregator::VarP(AggVarP::default()),
        ];

        for agg in aggregator_types {
            let (name, serialized) = agg.save();
            assert!(!name.is_empty());
            assert!(!serialized.is_empty());

            let mut new_agg = match &agg {
                Aggregator::First(_) => Aggregator::First(AggFirst::default()),
                Aggregator::Last(_) => Aggregator::Last(AggLast::default()),
                Aggregator::Min(_) => Aggregator::Min(AggMin::default()),
                Aggregator::Max(_) => Aggregator::Max(AggMax::default()),
                Aggregator::Avg(_) => Aggregator::Avg(AggAvg::default()),
                Aggregator::Sum(_) => Aggregator::Sum(AggSum::default()),
                Aggregator::Count(_) => Aggregator::Count(AggCount::default()),
                Aggregator::Range(_) => Aggregator::Range(AggRange::default()),
                Aggregator::StdS(_) => Aggregator::StdS(AggStdS::default()),
                Aggregator::StdP(_) => Aggregator::StdP(AggStdP::default()),
                Aggregator::VarS(_) => Aggregator::VarS(AggVarS::default()),
                Aggregator::VarP(_) => Aggregator::VarP(AggVarP::default()),
            };

            new_agg.load(&serialized);

            // Check that they're the same type after loading
            assert_eq!(new_agg.name(), agg.name());
        }
    }
}
