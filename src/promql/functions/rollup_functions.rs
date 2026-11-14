use crate::common::math::{kahan_avg, kahan_std_dev, kahan_sum, kahan_variance, quantile};
use crate::common::{Sample, Timestamp};
use crate::promql::common::math::{max_ignore_nan, min_ignore_nan};
use crate::promql::functions::PromQLArg;
use crate::promql::functions::PromQLFunction;
use crate::promql::functions::PromqlFunctionKind;
use crate::promql::functions::deriv::rollup_deriv;
use crate::promql::functions::idelta::rollup_idelta;
use crate::promql::functions::integral::{IntegralStrategy, rollup_integral};
use crate::promql::functions::irate::rollup_irate;
use crate::promql::functions::range_vector_functions::{rollup_changes, rollup_resets};
use crate::promql::functions::rate::{RateKind, extrapolated_rate_window};
use crate::promql::functions::rollups::{
    eval_series, rollup_series_over_grid, rollup_window_value, window_samples,
};
use crate::promql::functions::state::{rollup_state_count, state_duration_window};
use crate::promql::functions::types::FunctionCallContext;
use crate::promql::functions::types::RollupWindow;
use crate::promql::functions::utils::{
    exact_arity_error, expect_range_vector, expect_scalar, labels_for_absent, two_args,
};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvalSamples, ExprResult};
use promql_parser::parser::Expr;
use promql_parser::parser::Function;
use promql_parser::parser::value::ValueType;
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/promql/rollup.go

/// Generates [`RollupKind`]. Each variant is named for the
/// [`PromqlFunctionKind`] it stands for, so one list makes the enum, both
/// conversions and [`RollupKind::all`].
macro_rules! rollup_kinds {
    ( $( #[$meta: meta] )* $( $variant: ident ),* $(,)? ) => {
        $( #[$meta] )*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[allow(clippy::enum_variant_names)]
        pub enum RollupKind {
            $( $variant, )*
        }

        impl RollupKind {
            /// The kind for a PromQL function, or `None` when that function
            /// cannot be pushed down.
            fn from_function_kind(kind: PromqlFunctionKind) -> Option<Self> {
                match kind {
                    $( PromqlFunctionKind::$variant => Some(Self::$variant), )*
                    _ => None,
                }
            }

            /// The PromQL function this kind stands for.
            #[cfg(test)]
            pub(in crate::promql) fn function_kind(self) -> PromqlFunctionKind {
                match self {
                    $( Self::$variant => PromqlFunctionKind::$variant, )*
                }
            }

            /// Every kind, for exhaustive testing.
            #[cfg(test)]
            pub(in crate::promql) fn all() -> Vec<RollupKind> {
                vec![$( Self::$variant, )*]
            }
        }
    };
}

rollup_kinds! {
    /// A range-vector function that a shard can evaluate on its own.
    ///
    /// A timeseries lives entirely on one shard, so a rollup over that series'
    /// own window needs no cross-shard merge algebra: the shard computes the final
    /// value and the coordinator concatenates. That locality — not decomposability —
    /// is the membership rule for this enum. A function belongs here when its output
    /// depends on nothing but one series' samples inside the window, which is why
    /// `absent_over_time` can never join: its answer depends on series being absent
    /// across the whole cluster, something no single shard can observe.
    ///
    /// The variants deliberately do not cover every eligible function yet; the list
    /// grows once the conformance suite covers each addition.
    SumOverTime,
    CountOverTime,
    LastOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    StddevOverTime,
    StdvarOverTime,
    MadOverTime,
    PresentOverTime,
    FirstOverTime,
    QuantileOverTime,
    TsOfFirstOverTime,
    TsOfLastOverTime,
    TsOfMinOverTime,
    TsOfMaxOverTime,
    Rate,
    Increase,
    Delta,
    IRate,
    IDelta,
    Deriv,
    Resets,
    Changes,
    StateDurationOverTime,
    StateCountOverTime,
    RangeOverTime,
    Integral,
}

/// How a rollup reduces one window, mirroring the shapes the local evaluation
/// paths take.
enum RollupImpl {
    /// Reads the window through a [`RollupWindow`] view over de-interleaved
    /// value/timestamp arrays, which the compiler can vectorize over.
    Windowed(fn(&RollupWindow, Option<f64>) -> f64),
    /// Reads whole `Sample`s.
    WholeWindow(fn(&[Sample]) -> f64),
    /// Reads whole `Sample`s and may decline to produce a value.
    /// `irate` over a one-sample window has no answer, and declining is not the
    /// same as answering NaN.
    WholeWindowOptional(fn(&[Sample]) -> Option<f64>),
    /// Needs the window's own bounds as well as its samples: `rate` and friends
    /// extrapolate to the window edges, so where those edges are changes the
    /// answer even when the samples do not.
    WindowAware(fn(&[Sample], i64, Timestamp) -> Option<f64>),
    /// Reads whole `Sample`s with the scalar parameter and an output unit, and
    /// may decline — `state_duration_over_time`'s shape: its `state` is the
    /// parameter, and a window whose last sample is not in it has no answer.
    WholeWindowWithUnit(UnitRollupFn),
}

/// A whole-window reduction taking the scalar parameter and a unit in
/// milliseconds, which may decline to answer.
type UnitRollupFn = fn(&[Sample], Option<f64>, Option<i64>) -> Option<f64>;

impl RollupKind {
    /// The kind for a PromQL function name, or `None` when that function cannot
    /// be pushed down.
    ///
    /// Deliberately absent, and why:
    ///
    /// * `absent_over_time` — permanently. Its answer depends on a series being
    ///   absent across the *whole cluster*, which no single shard can observe.
    /// * `predict_linear` — it predicts relative to the query's evaluation
    ///   timestamp, which `@`/`offset` divorce from the window end. A shard is
    ///   told window ends only, so it cannot compute the right origin.
    /// * `double_exponential_smoothing` / `holt_winters` — takes two scalar
    ///   parameters and the request carries one.
    pub(in crate::promql) fn from_function_name(name: &str) -> Option<Self> {
        PromqlFunctionKind::try_from(name)
            .ok()
            .and_then(Self::from_function_kind)
    }

    /// The PromQL function this kind stands for. Tests use it to build a query
    /// per kind, so the conformance suite covers whatever `all()` returns
    /// without a second list to keep in step.
    #[cfg(test)]
    pub(in crate::promql) fn function_name(self) -> &'static str {
        self.function_kind().name()
    }

    /// A valid `(param, unit_ms)` for this kind, for tests that build one
    /// request or query per kind.
    #[cfg(test)]
    pub(in crate::promql) fn test_params(self) -> (Option<f64>, Option<i64>) {
        match self {
            RollupKind::QuantileOverTime => (Some(0.9), None),
            RollupKind::StateDurationOverTime => (Some(1.0), Some(60_000)),
            RollupKind::StateCountOverTime => (Some(1.0), None),
            _ => (None, None),
        }
    }

    /// Whether a request carrying `param` and `unit_ms` for this kind is one
    /// that reproduces the local answer.
    ///
    /// The coordinator keeps anything this refuses local, so the local path
    /// reports the error the user should see (a `NaN` state, say); a shard
    /// refuses the request outright, since no coordinator of this build sends
    /// one. Only `state_duration_over_time` takes a unit; it and
    /// `state_count_over_time` need a state.
    pub(in crate::promql) fn check_params(
        self,
        param: Option<f64>,
        unit_ms: Option<i64>,
    ) -> Result<(), String> {
        let takes_unit = self == RollupKind::StateDurationOverTime;
        match unit_ms {
            Some(_) if !takes_unit => return Err(format!("{self:?} takes no unit")),
            Some(unit_ms) if unit_ms <= 0 => {
                return Err(format!("{self:?} unit must be positive, got {unit_ms} ms"));
            }
            _ => {}
        }
        let takes_state = matches!(
            self,
            RollupKind::StateDurationOverTime | RollupKind::StateCountOverTime
        );
        if takes_state && param.is_none_or(f64::is_nan) {
            return Err(format!("{self:?} needs a state that is not NaN"));
        }
        if self == RollupKind::Integral && IntegralStrategy::from_param(param).is_none() {
            return Err(format!("{self:?} needs a strategy of 0, 1 or 2"));
        }
        Ok(())
    }

    /// `param` in its canonical spelling, so two calls asking for the same
    /// reduction ask for the same grid.
    pub(in crate::promql) fn canonical_param(self, param: Option<f64>) -> Option<f64> {
        match (self, param) {
            (RollupKind::Integral, Some(2.0)) => None,
            _ => param,
        }
    }

    /// The reduction this kind applies — the *same* function pointer the local
    /// evaluation path uses, so a pushed-down result cannot drift from a local
    /// one by implementation.
    fn implementation(self) -> RollupImpl {
        match self {
            RollupKind::SumOverTime => RollupImpl::Windowed(rollup_sum),
            RollupKind::CountOverTime => RollupImpl::WholeWindow(rollup_count),
            RollupKind::LastOverTime => RollupImpl::WholeWindow(rollup_last),
            RollupKind::AvgOverTime => RollupImpl::Windowed(rollup_avg),
            RollupKind::MinOverTime => RollupImpl::Windowed(rollup_min),
            RollupKind::MaxOverTime => RollupImpl::Windowed(rollup_max),
            RollupKind::StddevOverTime => RollupImpl::Windowed(rollup_stddev),
            RollupKind::StdvarOverTime => RollupImpl::Windowed(rollup_stdvar),
            RollupKind::MadOverTime => RollupImpl::Windowed(rollup_mad),
            RollupKind::PresentOverTime => RollupImpl::WholeWindow(rollup_present),
            RollupKind::FirstOverTime => RollupImpl::WholeWindow(rollup_first),
            RollupKind::QuantileOverTime => RollupImpl::Windowed(rollup_quantile),
            RollupKind::TsOfFirstOverTime => RollupImpl::WholeWindow(rollup_tfirst),
            RollupKind::TsOfLastOverTime => RollupImpl::WholeWindow(rollup_tlast),
            RollupKind::TsOfMinOverTime => RollupImpl::Windowed(rollup_ts_of_min),
            RollupKind::TsOfMaxOverTime => RollupImpl::Windowed(rollup_ts_of_max),
            RollupKind::Rate => RollupImpl::WindowAware(rate_window),
            RollupKind::Increase => RollupImpl::WindowAware(increase_window),
            RollupKind::Delta => RollupImpl::WindowAware(delta_window),
            RollupKind::IRate => RollupImpl::WholeWindowOptional(rollup_irate),
            RollupKind::IDelta => RollupImpl::WholeWindowOptional(rollup_idelta),
            RollupKind::Deriv => RollupImpl::WholeWindowOptional(rollup_deriv),
            RollupKind::Resets => RollupImpl::WholeWindowOptional(rollup_resets),
            RollupKind::Changes => RollupImpl::WholeWindowOptional(rollup_changes),
            RollupKind::StateDurationOverTime => {
                RollupImpl::WholeWindowWithUnit(state_duration_window)
            }
            RollupKind::StateCountOverTime => RollupImpl::Windowed(rollup_state_count),
            RollupKind::RangeOverTime => RollupImpl::Windowed(rollup_range),
            RollupKind::Integral => RollupImpl::Windowed(rollup_integral),
        }
    }

    /// Reduce `samples` over the windows ending at each of `window_ends`.
    ///
    /// The result is sparse: a window holding no samples contributes nothing, so
    /// the output is shorter than `window_ends` whenever the series has gaps.
    /// `samples` must be sorted ascending by timestamp. `unit_ms` is read only
    /// by the kinds that take a unit; see [`Self::check_params`].
    #[allow(clippy::too_many_arguments)]
    pub(in crate::promql) fn eval_windows(
        self,
        samples: &[Sample],
        window_ms: i64,
        window_ends: impl IntoIterator<Item = Timestamp>,
        param: Option<f64>,
        unit_ms: Option<i64>,
    ) -> Vec<Sample> {
        match self.implementation() {
            RollupImpl::Windowed(f) => {
                rollup_series_over_grid(samples, window_ms, window_ends, param, f)
            }
            RollupImpl::WholeWindow(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, _| Some(f(window)))
            }
            RollupImpl::WholeWindowOptional(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, _| f(window))
            }
            RollupImpl::WindowAware(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, end| {
                    f(window, window_ms, end)
                })
            }
            RollupImpl::WholeWindowWithUnit(f) => {
                Self::over_windows(samples, window_ms, window_ends, |window, _| {
                    f(window, param, unit_ms)
                })
            }
        }
    }

    /// Reduce the one window of `samples` ending at `window_end` — the local
    /// per-step path's form of [`Self::eval_windows`], through the same
    /// implementation — or `None` when the window is empty or this kind
    /// declines to answer for it.
    pub(in crate::promql) fn eval_window(
        self,
        samples: &[Sample],
        window_end: Timestamp,
        window_ms: i64,
        param: Option<f64>,
        unit_ms: Option<i64>,
    ) -> Option<f64> {
        let window = || window_samples(samples, window_end, window_ms);
        match self.implementation() {
            RollupImpl::Windowed(f) => {
                rollup_window_value(samples, window_end, window_ms, param, f)
            }
            RollupImpl::WholeWindow(f) => window().map(f),
            RollupImpl::WholeWindowOptional(f) => f(window()?),
            RollupImpl::WindowAware(f) => f(window()?, window_ms, window_end),
            RollupImpl::WholeWindowWithUnit(f) => f(window()?, param, unit_ms),
        }
    }

    /// Slice each window out of `samples` and reduce it with `f`, dropping the
    /// windows that hold no samples and the ones `f` declines to answer for.
    fn over_windows(
        samples: &[Sample],
        window_ms: i64,
        window_ends: impl IntoIterator<Item = Timestamp>,
        f: impl Fn(&[Sample], Timestamp) -> Option<f64>,
    ) -> Vec<Sample> {
        window_ends
            .into_iter()
            .filter_map(|window_end| {
                let window = window_samples(samples, window_end, window_ms)?;
                Some(Sample {
                    value: f(window, window_end)?,
                    timestamp: window_end,
                })
            })
            .collect()
    }
}

/// `rate` over one window, in the shape [`RollupImpl::WindowAware`] needs.
fn rate_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Rate)
}

fn increase_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Increase)
}

fn delta_window(samples: &[Sample], window_ms: i64, window_end: Timestamp) -> Option<f64> {
    extrapolated_rate_window(samples, window_ms, window_end, RateKind::Delta)
}

/// Evaluate `kind` over each series' window, one value per series.
///
/// The window is the one the matrix selector loaded: it ends at
/// [`EvalSamples::range_end_ms`], which already carries any `@`/`offset`
/// adjustment. Series whose window holds no samples, or that `kind` declines,
/// are dropped rather than emitted as NaN.
///
/// [`EvalSamples::range_end_ms`]: crate::promql::EvalSamples::range_end_ms
pub(super) fn eval_rollup(
    ctx: &EvalContext,
    kind: RollupKind,
    series: Vec<EvalSamples>,
    param: Option<f64>,
    unit_ms: Option<i64>,
) -> EvalResult<ExprResult> {
    let samples = eval_series(ctx, series, |series| {
        kind.eval_window(
            &series.values,
            series.range_end_ms,
            series.range_ms,
            param,
            unit_ms,
        )
    })?;
    Ok(ExprResult::InstantVector(samples))
}

/// A rollup over a range vector alone, evaluated as its [`RollupKind`] is, so
/// the local and pushed-down paths reduce a window with one implementation.
macro_rules! rollup_function {
    ( $type_name: ident, $kind: ident ) => {
        #[derive(Copy, Clone, Default)]
        pub(in crate::promql) struct $type_name;

        impl PromQLFunction for $type_name {
            fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
                eval_rollup(ctx, RollupKind::$kind, arg.into_range_vector()?, None, None)
            }
        }
    };
}

rollup_function!(AvgOverTimeFunction, AvgOverTime);
rollup_function!(ChangesFunction, Changes);
rollup_function!(CountOverTimeFunction, CountOverTime);
rollup_function!(DeltaFunction, Delta);
rollup_function!(DerivFunction, Deriv);
rollup_function!(FirstOverTimeFunction, FirstOverTime);
rollup_function!(IDeltaFunction, IDelta);
rollup_function!(IRateFunction, IRate);
rollup_function!(IncreaseFunction, Increase);
rollup_function!(LastOverTimeFunction, LastOverTime);
rollup_function!(MadOverTimeFunction, MadOverTime);
rollup_function!(MaxOverTimeFunction, MaxOverTime);
rollup_function!(MinOverTimeFunction, MinOverTime);
rollup_function!(PresentOverTimeFunction, PresentOverTime);
rollup_function!(RangeOverTimeFunction, RangeOverTime);
rollup_function!(RateFunction, Rate);
rollup_function!(ResetsFunction, Resets);
rollup_function!(StddevOverTimeFunction, StddevOverTime);
rollup_function!(StdvarOverTimeFunction, StdvarOverTime);
rollup_function!(SumOverTimeFunction, SumOverTime);
rollup_function!(TsOfFirstOverTimeFunction, TsOfFirstOverTime);
rollup_function!(TsOfLastOverTimeFunction, TsOfLastOverTime);
rollup_function!(TsOfMaxOverTimeFunction, TsOfMaxOverTime);
rollup_function!(TsOfMinOverTimeFunction, TsOfMinOverTime);

fn rollup_avg(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_avg(rfa.values)
}

fn rollup_quantile(rfa: &RollupWindow, phi: Option<f64>) -> f64 {
    let mut values = rfa.values.to_vec();
    quantile(&mut values, phi.unwrap_or(0.5))
}

/// Min over time: [`min_ignore_nan`] folded from the first sample, so a NaN
/// is replaced by any later real value and an all-NaN window stays NaN (a fold
/// seeded with `+Inf` would answer `+Inf` there). Of equal values the first is
/// kept, so `-0` and `+0` resolve as the `min` aggregator does.
fn rollup_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let (&first, rest) = rfa
        .values
        .split_first()
        .expect("a rollup window is never empty");
    rest.iter()
        .fold(first, |min, &cur| min_ignore_nan(min, cur))
}

fn rollup_mad(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let mut values = rfa.values.to_vec();

    let median = quantile(&mut values, 0.5);

    // reuse values vec for deviations to avoid extra allocation
    for value in values.iter_mut() {
        *value = (*value - median).abs();
    }

    quantile(&mut values, 0.5)
}

/// Max over time: [`max_ignore_nan`] folded from the first sample; see
/// [`rollup_min`].
fn rollup_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let (&first, rest) = rfa
        .values
        .split_first()
        .expect("a rollup window is never empty");
    rest.iter()
        .fold(first, |max, &cur| max_ignore_nan(max, cur))
}

/// `range_over_time`: `max_over_time - min_over_time`, in one pass.
///
/// Both ends follow [`rollup_min`] and [`rollup_max`] exactly — a NaN is
/// replaced by any later real value — so the answer is the difference of the
/// two functions' answers, including NaN for an all-NaN window. Infinities
/// subtract as IEEE says: `+Inf` and `-Inf` give `+Inf`, an all-`+Inf` window
/// gives NaN.
fn rollup_range(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    let (&first, rest) = rfa
        .values
        .split_first()
        .expect("a rollup window is never empty");
    let (min, max) = rest.iter().fold((first, first), |(min, max), &cur| {
        (min_ignore_nan(min, cur), max_ignore_nan(max, cur))
    });
    max - min
}

fn rollup_ts_of_min(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    ts_of_extreme(rfa, |cur, min| cur <= min)
}

fn rollup_ts_of_max(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    ts_of_extreme(rfa, |cur, max| cur >= max)
}

/// The timestamp, in seconds, of the window's extreme value, where
/// `replaces(cur, best)` says whether `cur` takes over from the best so far.
/// The comparisons are inclusive, so of equal values the last is kept, as most
/// users expect. Unlike [`rollup_min`], a NaN best is replaced by every later
/// sample, NaN or not: an all-NaN window answers its last timestamp.
fn ts_of_extreme(rfa: &RollupWindow, replaces: impl Fn(f64, f64) -> bool) -> f64 {
    let mut best = rfa.values[0];
    let mut best_ts = rfa.timestamps[0];
    for (&v, &ts) in rfa.values.iter().zip(rfa.timestamps).skip(1) {
        if replaces(v, best) || best.is_nan() {
            best = v;
            best_ts = ts;
        }
    }
    best_ts as f64 / 1e3_f64
}

fn rollup_sum(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_sum(rfa.values)
}

fn rollup_stddev(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_std_dev(rfa.values)
}

fn rollup_stdvar(rfa: &RollupWindow, _param: Option<f64>) -> f64 {
    kahan_variance(rfa.values)
}

fn rollup_present(samples: &[Sample]) -> f64 {
    if !samples.is_empty() { 1.0 } else { 0.0 }
}

fn rollup_count(samples: &[Sample]) -> f64 {
    samples.len() as f64
}

fn rollup_tfirst(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].timestamp as f64 / 1e3_f64
}

fn rollup_tlast(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len() - 1].timestamp as f64 / 1e3_f64
}

fn rollup_first(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[0].value
}

fn rollup_last(samples: &[Sample]) -> f64 {
    // Safety: the caller ensures !samples.is_empty()
    samples[samples.len() - 1].value
}

/// `absent_over_time(range-vector)`
///
/// Returns an empty vector if the range vector has any elements (i.e., at least
/// one series with at least one sample in the look-back window), or a
/// single-element instant vector with value `1` otherwise.
///
/// The labels on that sample come from the argument selector's matchers, not
/// from any series — see [`labels_for_absent`]. Reaching them requires the
/// unevaluated argument, which is why this is one of the few functions to
/// override `apply_call` rather than implement `apply`.
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentOverTimeFunction;

impl AbsentOverTimeFunction {
    fn evaluate(
        arg: PromQLArg,
        ctx: &EvalContext,
        raw_arg: Option<&Expr>,
    ) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;
        let has_samples = series.iter().any(|s| !s.values.is_empty());
        if has_samples {
            Ok(ExprResult::InstantVector(vec![]))
        } else {
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: ctx.evaluation_ts,
                value: 1.0,
                labels: labels_for_absent(raw_arg),
                drop_name: false,
            }]))
        }
    }
}

impl PromQLFunction for AbsentOverTimeFunction {
    /// Reached only by callers that have no call site to speak of — the AST-less
    /// path cannot know the matchers, so it answers with no labels.
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Self::evaluate(arg, ctx, None)
    }

    fn apply_call(
        &self,
        args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        let mut args = args;
        if args.len() != 1 {
            return Err(exact_arity_error("absent_over_time", 1, args.len()));
        }
        Self::evaluate(args.swap_remove(0), ctx, ctx.raw_arg(0))
    }
}

/// `quantile_over_time(scalar, range-vector)`
///
/// the φ-quantile (0 ≤ φ ≤ 1) of all float samples in the specified interval.
#[derive(Copy, Clone)]
pub(in crate::promql) struct QuantileOverTimeFunction;

impl PromQLFunction for QuantileOverTimeFunction {
    fn apply_call(
        &self,
        args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        let (phi_arg, range_arg) = two_args("quantile_over_time", args)?;
        let phi = expect_scalar(phi_arg, "quantile_over_time", "phi")?;
        let range = expect_range_vector(range_arg, "quantile_over_time")?;

        eval_rollup(ctx, RollupKind::QuantileOverTime, range, Some(phi), None)
    }
}

/// Parser-side declarations for the rollups that are not PromQL built-ins.
/// Each is experimental, so a request must enable experimental functions to
/// call it.
pub(in crate::promql) fn get_extended_rollup_functions() -> Vec<Function> {
    vec![Function::new(
        "range_over_time",
        vec![ValueType::Matrix],
        0,
        ValueType::Vector,
        true,
    )]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::functions::rollups::window_samples;

    /// A NaN first sample must not pin the timestamp: `ts_of_min_over_time` and
    /// `ts_of_max_over_time` replace a NaN seed with the first real value, as
    /// `min_over_time`/`max_over_time` (and Prometheus) do. Ties still report
    /// the last timestamp, the documented `<=`/`>=` rule.
    #[test]
    fn ts_of_extrema_replace_a_nan_seed() {
        let values = [f64::NAN, 3.0, 1.0, 1.0];
        let timestamps = [10_i64, 20, 30, 40];
        let window = RollupWindow {
            values: &values,
            timestamps: &timestamps,
        };
        assert_eq!(rollup_ts_of_min(&window, None), 0.04);
        assert_eq!(rollup_ts_of_max(&window, None), 0.02);

        // All NaN: the seed is replaced at every sample, so the last one wins.
        let nans = [f64::NAN, f64::NAN];
        let window = RollupWindow {
            values: &nans,
            timestamps: &[10, 20],
        };
        assert_eq!(rollup_ts_of_min(&window, None), 0.02);
        assert_eq!(rollup_ts_of_max(&window, None), 0.02);
    }

    fn window(values: &[f64]) -> (Vec<f64>, Vec<i64>) {
        let timestamps = (0..values.len() as i64).map(|i| i * 1_000).collect();
        (values.to_vec(), timestamps)
    }

    /// The φ-quantile interpolates linearly at rank φ·(n−1) of the sorted
    /// values, as Prometheus' `quantile_over_time` does; φ outside [0, 1] is
    /// −Inf/+Inf, and the input order does not matter.
    #[test]
    fn quantile_over_time_interpolates_between_ranks() {
        let (values, timestamps) = window(&[40.0, 10.0, 30.0, 20.0]);
        let w = RollupWindow {
            values: &values,
            timestamps: &timestamps,
        };
        assert_eq!(rollup_quantile(&w, Some(0.0)), 10.0);
        assert_eq!(rollup_quantile(&w, Some(0.25)), 17.5);
        assert_eq!(rollup_quantile(&w, Some(0.5)), 25.0);
        assert_eq!(rollup_quantile(&w, Some(1.0)), 40.0);
        assert_eq!(rollup_quantile(&w, Some(-0.5)), f64::NEG_INFINITY);
        assert_eq!(rollup_quantile(&w, Some(1.5)), f64::INFINITY);
        assert!(rollup_quantile(&w, Some(f64::NAN)).is_nan());
    }

    /// The median of the absolute deviations from the median.
    #[test]
    fn mad_over_time_is_the_median_absolute_deviation() {
        // Median 3; deviations 2, 1, 0, 1, 97; their median 1.
        let (values, timestamps) = window(&[1.0, 2.0, 3.0, 4.0, 100.0]);
        let w = RollupWindow {
            values: &values,
            timestamps: &timestamps,
        };
        assert_eq!(rollup_mad(&w, None), 1.0);

        // Even count: median 2.5; deviations 1.5, 0.5, 0.5, 1.5; their median 1.
        let (values, timestamps) = window(&[4.0, 1.0, 3.0, 2.0]);
        let w = RollupWindow {
            values: &values,
            timestamps: &timestamps,
        };
        assert_eq!(rollup_mad(&w, None), 1.0);

        // A constant window has no deviation.
        let (values, timestamps) = window(&[7.0, 7.0, 7.0]);
        let w = RollupWindow {
            values: &values,
            timestamps: &timestamps,
        };
        assert_eq!(rollup_mad(&w, None), 0.0);
    }

    #[test]
    fn only_integrals_explicit_default_strategy_is_rewritten() {
        assert_eq!(RollupKind::Integral.canonical_param(Some(2.0)), None);
        assert_eq!(RollupKind::Integral.canonical_param(Some(0.0)), Some(0.0));
        assert_eq!(RollupKind::Integral.canonical_param(None), None);
        assert_eq!(
            RollupKind::QuantileOverTime.canonical_param(Some(2.0)),
            Some(2.0)
        );
    }

    /// A strategy naming no rule stays local, which also keeps a NaN out of the
    /// grid request and its preload key. A shard refuses one outright.
    #[test]
    fn integral_params_accept_only_the_three_rules() {
        let kind = RollupKind::Integral;
        for strategy in [None, Some(0.0), Some(1.0), Some(2.0)] {
            assert!(kind.check_params(strategy, None).is_ok(), "{strategy:?}");
        }
        for strategy in [3.0, 0.5, f64::NAN] {
            assert!(
                kind.check_params(Some(strategy), None).is_err(),
                "{strategy}"
            );
        }
        assert!(
            kind.check_params(None, Some(60_000)).is_err(),
            "takes no unit"
        );
    }

    /// A kind's name must round-trip: with two dozen hand-written entries in
    /// `from_function_name`, a typo would silently make one function
    /// unpushable — or, worse, map it to the wrong reduction.
    #[test]
    fn every_kind_round_trips_through_its_function_name() {
        for kind in RollupKind::all() {
            assert_eq!(
                RollupKind::from_function_name(kind.function_name()),
                Some(kind),
                "{kind:?} does not round-trip via {:?}",
                kind.function_name()
            );
        }
    }

    /// `resolve_function` must know every pushable name, or the local fallback
    /// for that kind would fail to evaluate at all.
    #[test]
    fn every_kind_names_a_real_function() {
        for kind in RollupKind::all() {
            assert!(
                crate::promql::functions::resolve_function(kind.function_name()).is_some(),
                "{kind:?} names an unknown function {:?}",
                kind.function_name()
            );
        }
    }

    /// Seeing samples outside its window must not change a rollup's answer.
    ///
    /// The local path hands `eval_windows` exactly one window's samples, because
    /// that is all the matrix selector loaded. The pushed-down path hands it the
    /// union of the grid's windows and lets it slice, because loading each
    /// window separately is the transfer the push-down exists to avoid. A rollup
    /// that read the surrounding samples would answer differently on the two
    /// paths for the same query.
    ///
    /// So every [`RollupKind`] must reduce a window identically with and without
    /// its neighbours in view. A function that cannot is not eligible for
    /// push-down, and this is where that is caught.
    #[test]
    fn every_kind_ignores_samples_outside_the_window() {
        let series: Vec<Sample> = (0..=30)
            .map(|i| Sample {
                timestamp: i * 10_000,
                value: i as f64,
            })
            .collect();
        let window_ms = 60_000;
        let ends: Vec<Timestamp> = (0..=300_000).step_by(30_000).collect();

        for kind in RollupKind::all() {
            let (param, unit_ms) = kind.test_params();

            // The whole series in view, sliced per window — the grid path.
            let with_context =
                kind.eval_windows(&series, window_ms, ends.iter().copied(), param, unit_ms);

            // One window at a time, nothing else in view — the local path.
            let isolated: Vec<Sample> = ends
                .iter()
                .filter_map(|&end| {
                    let window = window_samples(&series, end, window_ms)?;
                    kind.eval_windows(window, window_ms, [end], param, unit_ms)
                        .pop()
                })
                .collect();

            assert_eq!(
                with_context, isolated,
                "{kind:?} must not depend on samples outside its window"
            );
        }
    }

    /// The generic check above uses a strictly increasing series, where every
    /// run of one value is a single sample. A run that crosses a window's start
    /// is the case that matters here: with the neighbours in view it must still
    /// be measured from the window's first sample.
    #[test]
    fn state_duration_ignores_runs_before_the_window() {
        // Runs of four samples in and four out of state, 10 s apart.
        let series: Vec<Sample> = (0..=40)
            .map(|i| Sample {
                timestamp: i * 10_000,
                value: ((i / 4) % 2) as f64,
            })
            .collect();
        let window_ms = 25_000;
        let ends: Vec<Timestamp> = (0..=400_000).step_by(5_000).collect();
        let kind = RollupKind::StateDurationOverTime;
        for unit_ms in [None, Some(60_000)] {
            let with_context =
                kind.eval_windows(&series, window_ms, ends.iter().copied(), Some(1.0), unit_ms);
            let isolated: Vec<Sample> = ends
                .iter()
                .filter_map(|&end| {
                    let window = window_samples(&series, end, window_ms)?;
                    kind.eval_windows(window, window_ms, [end], Some(1.0), unit_ms)
                        .pop()
                })
                .collect();
            // A 25 s window holds at most three samples 10 s apart, so no answer
            // can exceed 20 s, though the runs themselves last 30 s.
            let longest = 20_000.0 / unit_ms.unwrap_or(1_000) as f64;
            assert!(!with_context.is_empty());
            assert!(with_context.iter().all(|s| s.value <= longest));
            assert_eq!(with_context, isolated, "unit {unit_ms:?}");
        }
    }

    /// The count's version of the check above: a run that started before the
    /// window is counted from the window's first sample, and windows out of
    /// state answer `0` rather than dropping out.
    #[test]
    fn state_count_ignores_runs_before_the_window() {
        // Runs of four samples in and four out of state, 10 s apart.
        let series: Vec<Sample> = (0..=40)
            .map(|i| Sample {
                timestamp: i * 10_000,
                value: ((i / 4) % 2) as f64,
            })
            .collect();
        let window_ms = 25_000;
        let ends: Vec<Timestamp> = (0..=400_000).step_by(5_000).collect();
        let kind = RollupKind::StateCountOverTime;
        let with_context =
            kind.eval_windows(&series, window_ms, ends.iter().copied(), Some(1.0), None);
        let isolated: Vec<Sample> = ends
            .iter()
            .filter_map(|&end| {
                let window = window_samples(&series, end, window_ms)?;
                kind.eval_windows(window, window_ms, [end], Some(1.0), None)
                    .pop()
            })
            .collect();
        // A 25 s window holds at most three samples, though the runs hold four.
        assert!(with_context.iter().all(|s| s.value <= 3.0));
        assert!(with_context.iter().any(|s| s.value == 3.0));
        assert!(with_context.iter().any(|s| s.value == 0.0));
        assert_eq!(with_context, isolated);
    }

    /// `range_over_time` folds `max_over_time - min_over_time` into one pass;
    /// the two must agree bit for bit, including where NaN and the infinities
    /// make a naive fold go wrong.
    #[test]
    fn rollup_range_is_max_minus_min() {
        let nan = f64::NAN;
        let inf = f64::INFINITY;
        let cases: &[&[f64]] = &[
            &[1.0],
            &[3.0, 1.0, 2.0],
            &[nan, 2.0, 5.0],
            &[2.0, nan, 5.0],
            &[5.0, 2.0, nan],
            &[nan],
            &[nan, nan, nan],
            &[-inf, 3.0, inf],
            &[inf, inf],
            &[-inf, -inf],
            &[nan, -inf, nan],
            &[-0.0, 0.0],
        ];
        for values in cases {
            let timestamps: Vec<Timestamp> = (0..values.len() as i64).collect();
            let window = RollupWindow {
                values,
                timestamps: &timestamps,
            };
            let want = rollup_max(&window, None) - rollup_min(&window, None);
            let got = rollup_range(&window, None);
            assert!(
                got.to_bits() == want.to_bits() || (got.is_nan() && want.is_nan()),
                "{values:?}: want {want}, got {got}"
            );
        }
    }

    #[test]
    fn range_over_time_takes_no_params() {
        assert!(RollupKind::RangeOverTime.check_params(None, None).is_ok());
        assert!(
            RollupKind::RangeOverTime
                .check_params(None, Some(1_000))
                .is_err()
        );
    }

    #[test]
    fn check_params_keeps_units_and_states_to_state_duration() {
        use RollupKind::*;
        assert!(StateDurationOverTime.check_params(Some(1.0), None).is_ok());
        assert!(
            StateDurationOverTime
                .check_params(Some(1.0), Some(60_000))
                .is_ok()
        );
        assert!(StateDurationOverTime.check_params(None, None).is_err());
        assert!(
            StateDurationOverTime
                .check_params(Some(f64::NAN), None)
                .is_err()
        );
        assert!(
            StateDurationOverTime
                .check_params(Some(1.0), Some(0))
                .is_err()
        );
        assert!(
            StateDurationOverTime
                .check_params(Some(1.0), Some(-1))
                .is_err()
        );
        assert!(StateCountOverTime.check_params(Some(1.0), None).is_ok());
        assert!(StateCountOverTime.check_params(None, None).is_err());
        assert!(
            StateCountOverTime
                .check_params(Some(f64::NAN), None)
                .is_err()
        );
        assert!(
            StateCountOverTime
                .check_params(Some(1.0), Some(1_000))
                .is_err()
        );
        assert!(SumOverTime.check_params(None, Some(1_000)).is_err());
        assert!(QuantileOverTime.check_params(Some(0.9), None).is_ok());
        assert!(Rate.check_params(None, None).is_ok());
    }
}
