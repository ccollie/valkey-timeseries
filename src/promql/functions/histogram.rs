use std::cell::RefCell;
use std::rc::Rc;

use crate::labels::HasFingerprint;
use crate::promql::functions::utils::{
    exact_arity_error, expect_instant_vector, expect_min_arg_count, expect_scalar, expect_string,
    is_inf, is_valid_label_name,
};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::hashers::FingerprintHashMap;
use crate::promql::model::is_stale_nan;
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
use ahash::AHashMap;
use promql_parser::label::METRIC_NAME;

static ELLIPSIS: &str = "...";
static LE: &str = "le";

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramFractionFunctions;

impl PromQLFunction for HistogramFractionFunctions {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        histogram_fraction(args)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramQuantileFunction;

impl PromQLFunction for HistogramQuantileFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        histogram_quantile(args)
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct HistogramQuantilesFunction;

impl PromQLFunction for HistogramQuantilesFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        histogram_quantiles(args)
    }
}

/// Declare a one-argument function reducing each classic histogram to a float.
macro_rules! histogram_statistic {
    ($Ty:ident, $name:literal, $statistic:path) => {
        #[derive(Copy, Clone)]
        pub(in crate::promql) struct $Ty;

        impl PromQLFunction for $Ty {
            fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
                histogram_statistic(arg, $name, $statistic)
            }
        }
    };
}

histogram_statistic!(HistogramAvgFunction, "histogram_avg", bucket_avg);
histogram_statistic!(HistogramStddevFunction, "histogram_stddev", bucket_stddev);
histogram_statistic!(HistogramStdvarFunction, "histogram_stdvar", bucket_stdvar);

type SharedTimeseries = Rc<RefCell<EvalSample>>;

/// Group timeseries by MetricGroup+tags excluding `vmrange` tag.
#[derive(Clone, Default)]
struct Bucket {
    start_str: String,
    end_str: String,
    start: f64,
    end: f64,
    ts: SharedTimeseries,
}

impl Bucket {
    fn is_zero_ts(&self) -> bool {
        let ts = self.ts.borrow();
        ts.value <= 0.0
    }

    /// Convert the ` vmrange ` label in each group of time series to the ` le ` label.
    fn copy_ts(&self, le_str: &str) -> SharedTimeseries {
        let src = self.ts.borrow();
        let mut ts = src.clone();
        ts.value = 0.0;
        ts.labels.set(LE, le_str.to_string());
        Rc::new(RefCell::new(ts))
    }

    fn set_le(&mut self, end_str: &str) {
        let mut ts = self.ts.borrow_mut();
        ts.labels.set(LE, end_str.to_string());
    }
}

fn vmrange_buckets_to_le(tss: Vec<EvalSample>) -> Vec<EvalSample> {
    let mut rvs: Vec<EvalSample> = Vec::with_capacity(tss.len());

    let mut buckets: FingerprintHashMap<Vec<Bucket>> = FingerprintHashMap::default();

    let empty_str = "".to_string();

    for ts in tss.into_iter() {
        let vm_range = ts.labels.get("vmrange").unwrap_or(&empty_str);

        if vm_range.is_empty() {
            if let Some(le) = ts.labels.get(LE)
                && !le.is_empty()
            {
                // Keep Prometheus-compatible buckets.
                rvs.push(ts);
            }
            continue;
        }

        let n = match vm_range.find(ELLIPSIS) {
            Some(pos) => pos,
            None => continue,
        };

        let start_str = &vm_range[0..n];
        let start = match start_str.parse::<f64>() {
            Err(_) => continue,
            Ok(n) => n,
        };

        let end_str = &vm_range[(n + ELLIPSIS.len())..vm_range.len()];
        let end = match end_str.parse::<f64>() {
            Err(_) => continue,
            Ok(n) => n,
        };

        // prevent borrowing of value from ts.metric_name
        let start_string = start_str.to_string();
        let end_string = end_str.to_string();

        let mut _ts = ts;
        _ts.labels.remove(LE);
        _ts.labels.remove("vmrange");

        let key = _ts.labels.fingerprint();
        // series.push(_ts);
        let shared_ts = Rc::new(RefCell::new(_ts));

        buckets.entry(key).or_default().push(Bucket {
            start_str: start_string,
            end_str: end_string,
            start,
            end,
            ts: shared_ts,
        });
    }

    let mut uniq_ts: AHashMap<String, SharedTimeseries> = AHashMap::with_capacity(8);

    for xss in buckets.values_mut() {
        xss.sort_by(|a, b| a.end.total_cmp(&b.end));
        let mut xss_new: Vec<Bucket> = Vec::with_capacity(xss.len() + 2);
        // The last non-empty bucket kept, if any: the histogram's lower edge
        // is 0 until there is one.
        let mut xs_prev: Option<&Bucket> = None;

        uniq_ts.clear();

        for xs in xss.iter_mut() {
            if xs.is_zero_ts() {
                // Skip time series with zeros. They are substituted by xss_new below.
                // Skip buckets with zero values - they will be merged into a single bucket
                // when the next non-zero bucket appears.

                // Do not store xs in xsPrev to properly create `le` time series
                // for zero buckets.
                // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/4021
                continue;
            }

            if xs.start != xs_prev.map_or(0.0, |prev| prev.end) {
                // There is a gap between the previous bucket and the current bucket,
                // or the previous bucket is skipped because it was zero.
                // Fill it with a time series with le=xs.start.
                if !uniq_ts.contains_key(&xs.start_str) {
                    let copy = xs.copy_ts(&xs.start_str);

                    uniq_ts.insert(xs.start_str.to_string(), xs.ts.clone());
                    xss_new.push(Bucket {
                        start_str: "".to_string(),
                        start: 0.0,
                        end_str: xs.start_str.clone(),
                        end: xs.start,
                        ts: copy,
                    });
                }
            }

            // ugly, but otherwise we get a borrow error if we do xs.set_le(&xs.end_str);
            let end_str = xs.end_str.clone();
            // Convert the current time series to a time series with le=xs.end
            xs.set_le(&end_str);

            if let Some(_shared_ts) = uniq_ts.get(&end_str) {
                // Cannot merge EvalSample with EvalSamples - skip merging here
                // The current implementation doesn't support merging at this stage
            } else {
                uniq_ts.insert(end_str, xs.ts.clone());
                xss_new.push(xs.clone());
            }

            xs_prev = Some(xs);
        }

        if let Some(prev) = xs_prev
            && !is_inf(prev.end, 1)
            && !prev.is_zero_ts()
        {
            let ts = prev.copy_ts("+Inf");

            xss_new.push(Bucket {
                start_str: "".to_string(),
                end_str: "+Inf".to_string(),
                start: 0.0,
                end: f64::INFINITY,
                ts,
            })
        }

        *xss = xss_new;
        if xss.is_empty() {
            continue;
        }

        let mut count: f64 = 0.0;
        for xs in xss.iter_mut() {
            let mut ts = xs.ts.borrow_mut();
            let v = ts.value;
            if v > 0.0 {
                count += v
            }
            ts.value = count
        }

        // The group's other handles on these series are done with, so each
        // series is moved out of its cell rather than cloned.
        uniq_ts.clear();
        for xs in xss.drain(..) {
            match Rc::try_unwrap(xs.ts) {
                Ok(ts) => rvs.push(ts.into_inner()),
                Err(_) => debug_assert!(false, "a vmrange bucket is still shared"),
            }
        }
    }

    rvs
}

/// `histogram_fraction(lower, upper, buckets)`: the estimated fraction of each
/// classic histogram's observations between `lower` and `upper`, as
/// Prometheus' `funcHistogramFraction` computes it (see [`bucket_fraction`]).
fn histogram_fraction(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    if args.len() != 3 {
        return Err(exact_arity_error("histogram_fraction", 3, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let lower = expect_scalar(arg_iter.next().unwrap(), "histogram_fraction", "lower")?;
    let upper = expect_scalar(arg_iter.next().unwrap(), "histogram_fraction", "upper")?;
    let series = expect_instant_vector(arg_iter.next().unwrap(), "histogram_fraction")?;

    let rvs = classic_histograms(series, HistogramGrouping::WithName)
        .map(|(mut sample, mut buckets)| {
            sample.value = bucket_fraction(lower, upper, &mut buckets);
            sample.drop_name = true;
            sample
        })
        .collect();

    Ok(ExprResult::InstantVector(rvs))
}

pub(super) fn histogram_quantile(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    if args.len() != 2 {
        return Err(exact_arity_error("histogram_quantile", 2, args.len()));
    }

    let mut arg_iter = args.into_iter();
    let phi = expect_scalar(arg_iter.next().unwrap(), "histogram_quantile", "phi")?;
    let series = expect_instant_vector(arg_iter.next().unwrap(), "histogram_quantile")?;

    let rvs = classic_histograms(series, HistogramGrouping::WithName)
        .map(|(mut sample, mut buckets)| {
            sample.value = bucket_quantile(phi, &mut buckets);
            sample.drop_name = true;
            sample
        })
        .collect();

    Ok(ExprResult::InstantVector(rvs))
}

/// `histogram_quantiles(buckets, label, phi...)`, as in Prometheus: one series
/// per phi and histogram, its `label` holding the phi as an OpenMetrics float
/// and replacing any label of that name. The same phi twice, or a `label` that
/// was all that told two histograms apart, is left for the evaluator to reject
/// as a duplicate label set.
fn histogram_quantiles(args: Vec<PromQLArg>) -> EvalResult<ExprResult> {
    expect_min_arg_count("histogram_quantiles", 3, args.len())?;

    let mut arg_iter = args.into_iter();
    let series = expect_instant_vector(arg_iter.next().unwrap(), "histogram_quantiles")?;
    let label = expect_string(arg_iter.next().unwrap(), "histogram_quantiles", "label")?;
    if !is_valid_label_name(&label) {
        return Err(EvaluationError::InternalError(format!(
            "invalid label name {label:?}"
        )));
    }
    let phis = arg_iter
        .map(|arg| expect_scalar(arg, "histogram_quantiles", "phi"))
        .collect::<EvalResult<Vec<f64>>>()?;

    let mut histograms: Vec<_> = classic_histograms(series, HistogramGrouping::WithName).collect();
    let mut rvs = Vec::with_capacity(phis.len() * histograms.len());
    for &phi in &phis {
        let phi_label = format_open_metrics_float(phi);
        for (sample, buckets) in histograms.iter_mut() {
            let mut out = sample.clone();
            out.value = bucket_quantile(phi, buckets);
            out.drop_name = true;
            out.labels.set(&label, phi_label.clone());
            rvs.push(out);
        }
    }

    Ok(ExprResult::InstantVector(rvs))
}

/// Prometheus' `labels.FormatOpenMetricsFloat`: Go's shortest `%g`-style float,
/// with `.0` added when that has neither a `.` nor an exponent.
fn format_open_metrics_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if f == 0.0 {
        return "0.0".to_string();
    }
    // `{:e}` has the same shortest digits as Go, only its exponent is unpadded.
    let scientific = format!("{f:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("LowerExp always writes an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("LowerExp writes a decimal exponent");
    if !(-4..6).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        return format!("{mantissa}e{sign}{:02}", exponent.abs());
    }
    let plain = f.to_string();
    if plain.contains('.') {
        plain
    } else {
        plain + ".0"
    }
}

/// One sample per classic histogram in `arg`, holding `statistic` of its buckets.
fn histogram_statistic(
    arg: PromQLArg,
    name: &str,
    statistic: fn(&mut Vec<ClassicBucket>) -> f64,
) -> EvalResult<ExprResult> {
    let series = expect_instant_vector(arg, name)?;
    let rvs = classic_histograms(series, HistogramGrouping::WithoutName)
        .map(|(mut sample, mut buckets)| {
            sample.value = statistic(&mut buckets);
            sample.drop_name = true;
            sample
        })
        .collect();
    Ok(ExprResult::InstantVector(rvs))
}

/// Which labels tell one classic histogram from another, `le` aside.
#[derive(Clone, Copy, PartialEq)]
enum HistogramGrouping {
    /// Prometheus: the name's drop is pending until the result is rendered, so
    /// `a_bucket` and `b_bucket` stay separate histograms, and a duplicate label
    /// set error if nothing else tells them apart.
    WithName,
    /// VictoriaMetrics: the name is removed first, so `a_bucket` and `b_bucket`
    /// with otherwise equal labels are buckets of one histogram.
    WithoutName,
}

/// The classic histograms in `series`, each as its output sample (the first
/// bucket's, without `le`) and its buckets, grouped as `grouping` says.
///
/// A series whose `le` does not parse as a float is not a bucket. VictoriaMetrics
/// `vmrange` buckets are converted to `le` buckets first; Prometheus-format
/// buckets pass through unchanged.
fn classic_histograms(
    series: Vec<EvalSample>,
    grouping: HistogramGrouping,
) -> impl Iterator<Item = (EvalSample, Vec<ClassicBucket>)> {
    let mut histograms: FingerprintHashMap<(EvalSample, Vec<ClassicBucket>)> =
        FingerprintHashMap::default();
    // Converting re-collects every series; Prometheus-format input, which is
    // almost all of it, passes through unchanged, so only convert when some
    // series is a VictoriaMetrics bucket.
    let series = if series.iter().any(|s| s.labels.get("vmrange").is_some()) {
        vmrange_buckets_to_le(series)
    } else {
        series
    };
    for mut ts in series {
        let Some(upper_bound) = ts.labels.get(LE).and_then(|le| le.parse::<f64>().ok()) else {
            continue;
        };
        ts.labels.remove(LE);
        if grouping == HistogramGrouping::WithoutName {
            ts.labels.remove(METRIC_NAME);
        }
        let bucket = ClassicBucket {
            upper_bound,
            count: ts.value,
        };
        histograms
            .entry(ts.labels.fingerprint())
            .or_insert_with(|| (ts, Vec::new()))
            .1
            .push(bucket);
    }
    histograms.into_iter().map(|(_, histogram)| histogram)
}

/// One bucket of a classic histogram: its `le` and its cumulative count.
#[derive(Clone, Copy, Debug)]
pub(super) struct ClassicBucket {
    pub upper_bound: f64,
    pub count: f64,
}

/// Relative differences between adjacent bucket counts below this are taken
/// for floating-point noise and ignored (Prometheus' `smallDeltaTolerance`).
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// The `q` quantile of a classic histogram, as Prometheus' `BucketQuantile`
/// (promql/quantile.go) computes it:
///
/// - `q` NaN is NaN, below 0 is -Inf, above 1 is +Inf — before the buckets are
///   looked at, so even an empty or incomplete histogram answers.
/// - Without a `+Inf` bucket, with fewer than two buckets or with no
///   observations, the answer is NaN.
/// - Buckets with the same bound are merged, and counts are made monotonic
///   (see [`ensure_monotonic_and_ignore_small_deltas`]).
/// - A rank in the `+Inf` bucket answers the highest finite bound. A rank in
///   a lowest bucket whose bound is at most 0 answers that bound. Otherwise
///   the value is interpolated linearly within the bucket, from the previous
///   bound — or from 0 for a lowest bucket above 0.
pub(super) fn bucket_quantile(q: f64, buckets: &mut Vec<ClassicBucket>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    if !buckets
        .last()
        .is_some_and(|b| b.upper_bound == f64::INFINITY)
    {
        return f64::NAN;
    }

    coalesce_buckets(buckets);
    ensure_monotonic_and_ignore_small_deltas(buckets, SMALL_DELTA_TOLERANCE);

    let n = buckets.len();
    if n < 2 {
        return f64::NAN;
    }
    let observations = buckets[n - 1].count;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let b = go_sort_search(n - 1, |i| buckets[i].count >= rank);

    if b == n - 1 {
        return buckets[n - 2].upper_bound;
    }
    if b == 0 && buckets[0].upper_bound <= 0.0 {
        return buckets[0].upper_bound;
    }
    let bucket_end = buckets[b].upper_bound;
    let mut bucket_start = 0.0;
    let mut count = buckets[b].count;
    if b > 0 {
        bucket_start = buckets[b - 1].upper_bound;
        count -= buckets[b - 1].count;
        rank -= buckets[b - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// What the statistics read from a classic histogram's finite buckets, every
/// observation taken to sit at its bucket's midpoint, as VictoriaMetrics'
/// `histogram_avg`/`_stddev`/`_stdvar` take it. The lowest bucket starts at 0,
/// whatever its sign, and observations in the `+Inf` bucket have no midpoint so
/// are left out. Same-bound buckets are merged and decreases lifted first (see
/// [`lift_count_decreases`]). A `+Inf` bucket is not required.
struct MidpointMoments {
    /// The observations in the finite buckets.
    weight: f64,
    /// The sum of those observations.
    sum: f64,
    /// The sum of their squares.
    sum_squares: f64,
}

/// Lift each count that falls below the highest before it, so a dip weighs 0
/// rather than negatively. Every increase stands, however small: unlike
/// [`ensure_monotonic_and_ignore_small_deltas`], which keeps float noise from
/// moving [`bucket_quantile`]'s rank search, there is no tolerance, as a tiny
/// increment is a tiny weight here, and on a large count may be real observations.
fn lift_count_decreases(buckets: &mut [ClassicBucket]) {
    let mut highest = f64::NEG_INFINITY;
    for bucket in buckets {
        if bucket.count < highest {
            bucket.count = highest;
        } else {
            highest = bucket.count;
        }
    }
}

fn midpoint_moments(buckets: &mut Vec<ClassicBucket>) -> MidpointMoments {
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    coalesce_buckets(buckets);
    lift_count_decreases(buckets);
    let mut moments = MidpointMoments {
        weight: 0.0,
        sum: 0.0,
        sum_squares: 0.0,
    };
    let (mut prev_bound, mut prev_count) = (0.0, 0.0);
    for bucket in buckets.iter().filter(|b| b.upper_bound.is_finite()) {
        let midpoint = (bucket.upper_bound + prev_bound) / 2.0;
        let weight = bucket.count - prev_count;
        moments.weight += weight;
        moments.sum += midpoint * weight;
        moments.sum_squares += midpoint * midpoint * weight;
        prev_bound = bucket.upper_bound;
        prev_count = bucket.count;
    }
    moments
}

/// The mean of the finite buckets' observations; NaN when they have none.
fn bucket_avg(buckets: &mut Vec<ClassicBucket>) -> f64 {
    let moments = midpoint_moments(buckets);
    if moments.weight == 0.0 {
        return f64::NAN;
    }
    moments.sum / moments.weight
}

/// The variance of the finite buckets' observations; NaN when they have none.
fn bucket_stdvar(buckets: &mut Vec<ClassicBucket>) -> f64 {
    let moments = midpoint_moments(buckets);
    if moments.weight == 0.0 {
        return f64::NAN;
    }
    let mean = moments.sum / moments.weight;
    // Rounding can take a variance just below zero. `max` would turn NaN into
    // 0, so only a negative value is corrected.
    let variance = moments.sum_squares / moments.weight - mean * mean;
    if variance < 0.0 { 0.0 } else { variance }
}

fn bucket_stddev(buckets: &mut Vec<ClassicBucket>) -> f64 {
    bucket_stdvar(buckets).sqrt()
}

/// Go's `sort.Search`: the smallest `i` in `[0, n)` for which `f(i)` holds,
/// or `n`, found by the same bisection. Kept exact rather than replaced with
/// a linear scan so a NaN count — which makes `f` non-monotonic — lands on
/// the same bucket as in Prometheus.
fn go_sort_search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    let (mut i, mut j) = (0, n);
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

/// Merge adjacent buckets with the same upper bound, summing their counts.
/// `buckets` must be sorted by bound.
fn coalesce_buckets(buckets: &mut Vec<ClassicBucket>) {
    buckets.dedup_by(|next, kept| {
        if next.upper_bound == kept.upper_bound {
            kept.count += next.count;
            true
        } else {
            false
        }
    });
}

/// Make counts non-decreasing with the bound, as a cumulative histogram's
/// must be. A difference within `tolerance` (relative) is floating-point noise
/// and is flattened in either direction; any other decrease is lifted to the
/// previous count. Neither moves the running reference count.
fn ensure_monotonic_and_ignore_small_deltas(buckets: &mut [ClassicBucket], tolerance: f64) {
    let Some(first) = buckets.first() else {
        return;
    };
    let mut prev = first.count;
    for bucket in buckets.iter_mut().skip(1) {
        let curr = bucket.count;
        if curr == prev {
            continue;
        }
        if almost_equal(prev, curr, tolerance) || curr < prev {
            bucket.count = prev;
            continue;
        }
        prev = curr;
    }
}

/// Prometheus' `almost.Equal`: equality within a relative `epsilon`, with two
/// NaNs equal and the stale marker equal only to itself.
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < epsilon * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// The estimated fraction of a classic histogram's observations in
/// `[lower, upper]`, as Prometheus' `BucketFraction` (promql/quantile.go)
/// computes it:
///
/// - Without a `+Inf` bucket, with no observations, or with a NaN bound, NaN.
///   `lower >= upper` is 0, not an error.
/// - Buckets with the same bound are merged; counts are *not* made monotonic.
/// - Observations are assumed spread linearly within a bucket. The lowest
///   bucket starts at 0 when its bound is above 0 and at -Inf otherwise, and
///   an infinite-width bucket (the first when it starts at -Inf, and `+Inf`)
///   contributes nothing by interpolation: a bound inside it counts the whole
///   bucket, or none of it.
pub(super) fn bucket_fraction(lower: f64, upper: f64, buckets: &mut Vec<ClassicBucket>) -> f64 {
    buckets.sort_by(|a, b| a.upper_bound.total_cmp(&b.upper_bound));
    if !buckets
        .last()
        .is_some_and(|b| b.upper_bound == f64::INFINITY)
    {
        return f64::NAN;
    }
    coalesce_buckets(buckets);

    let count = buckets[buckets.len() - 1].count;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let mut rank = 0.0;
    let (mut lower_rank, mut upper_rank) = (0.0, 0.0);
    let (mut lower_set, mut upper_set) = (false, false);

    let mut lower_bound = if buckets[0].upper_bound <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };

    for (i, b) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].upper_bound;
        }
        let upper_bound = b.upper_bound;

        // `v` is finite. An infinite-width bucket is not interpolated: for a
        // +Inf upper bound the second term is 0 and the rank stays at the
        // previous bucket's, and a -Inf lower bound takes the whole bucket.
        let interpolate_linearly = |v: f64| -> f64 {
            if lower_bound == f64::NEG_INFINITY {
                return b.count;
            }
            rank + (b.count - rank) * (v - lower_bound) / (upper_bound - lower_bound)
        };

        if !lower_set && lower_bound >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && lower_bound >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && lower_bound < lower && upper_bound > lower {
            lower_rank = interpolate_linearly(lower);
            lower_set = true;
        }
        if !upper_set && lower_bound < upper && upper_bound > upper {
            upper_rank = interpolate_linearly(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank = b.count;
    }
    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }

    (upper_rank - lower_rank) / count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;
    use crate::promql::EvalLabels;

    fn buckets(pairs: &[(f64, f64)]) -> Vec<ClassicBucket> {
        pairs
            .iter()
            .map(|&(upper_bound, count)| ClassicBucket { upper_bound, count })
            .collect()
    }

    const INF: f64 = f64::INFINITY;

    fn sample(labels: &[(&str, &str)], value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 0,
            value,
            labels: EvalLabels::from_pairs(labels),
            drop_name: false,
        }
    }

    /// `(le, value)` of every output series, ordered by `le`.
    fn le_values(series: Vec<EvalSample>) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = series
            .into_iter()
            .map(|s| (s.labels.get(LE).unwrap_or_default().to_string(), s.value))
            .collect();
        out.sort_by(|a, b| {
            let bound = |le: &str| le.parse::<f64>().unwrap_or(f64::NAN);
            bound(&a.0).total_cmp(&bound(&b.0))
        });
        out
    }

    /// VictoriaMetrics `vmrange` buckets become cumulative `le` buckets, with
    /// a `+Inf` bucket closing the histogram.
    #[test]
    fn vmrange_buckets_become_cumulative_le_buckets() {
        let out = vmrange_buckets_to_le(vec![
            sample(&[("__name__", "h"), ("vmrange", "1...2")], 3.0),
            sample(&[("__name__", "h"), ("vmrange", "0...1")], 2.0),
        ]);
        assert!(out.iter().all(|s| s.labels.get("vmrange").is_none()));
        assert_eq!(
            le_values(out),
            vec![
                ("1".to_string(), 2.0),
                ("2".to_string(), 5.0),
                ("+Inf".to_string(), 5.0),
            ]
        );
    }

    /// A missing range becomes an empty `le` bucket at its lower edge, so the
    /// cumulative counts stay right across it, as VictoriaMetrics converts.
    #[test]
    fn a_gap_between_vmrange_buckets_becomes_an_empty_le_bucket() {
        let out = vmrange_buckets_to_le(vec![
            sample(&[("__name__", "h"), ("vmrange", "0...1")], 2.0),
            sample(&[("__name__", "h"), ("vmrange", "2...4")], 3.0),
        ]);
        assert_eq!(
            le_values(out),
            vec![
                ("1".to_string(), 2.0),
                ("2".to_string(), 2.0),
                ("4".to_string(), 5.0),
                ("+Inf".to_string(), 5.0),
            ]
        );
    }

    /// The first range not starting at 0 gets an empty bucket at its start.
    #[test]
    fn a_histogram_not_starting_at_zero_gets_an_empty_first_bucket() {
        let out = vmrange_buckets_to_le(vec![sample(
            &[("__name__", "h"), ("vmrange", "1...2")],
            3.0,
        )]);
        assert_eq!(
            le_values(out),
            vec![
                ("1".to_string(), 0.0),
                ("2".to_string(), 3.0),
                ("+Inf".to_string(), 3.0),
            ]
        );
    }

    /// No `+Inf` bucket is added when the last range already ends there, and
    /// a histogram with no observations converts to nothing.
    #[test]
    fn closing_and_empty_vmrange_histograms() {
        let out = vmrange_buckets_to_le(vec![
            sample(&[("__name__", "h"), ("vmrange", "0...1")], 1.0),
            sample(&[("__name__", "h"), ("vmrange", "1...+Inf")], 2.0),
        ]);
        assert_eq!(
            le_values(out),
            vec![("1".to_string(), 1.0), ("+Inf".to_string(), 3.0)]
        );

        let empty = vmrange_buckets_to_le(vec![
            sample(&[("__name__", "h"), ("vmrange", "0...1")], 0.0),
            sample(&[("__name__", "h"), ("vmrange", "1...2")], 0.0),
        ]);
        assert!(empty.is_empty(), "{:?}", le_values(empty));
    }

    /// Prometheus-format buckets pass through the conversion unchanged.
    #[test]
    fn prometheus_buckets_pass_through() {
        let input = vec![
            sample(&[("__name__", "h"), ("le", "1")], 2.0),
            sample(&[("__name__", "h"), ("le", "+Inf")], 5.0),
        ];
        assert_eq!(
            le_values(vmrange_buckets_to_le(input.clone())),
            le_values(input)
        );
    }

    #[test]
    fn out_of_range_phi_answers_before_the_buckets_are_read() {
        // Even a histogram with no observations, or no buckets at all.
        let mut empty = buckets(&[(1.0, 0.0), (INF, 0.0)]);
        assert_eq!(bucket_quantile(1.5, &mut empty), INF);
        assert_eq!(bucket_quantile(-0.5, &mut Vec::new()), f64::NEG_INFINITY);
        assert!(bucket_quantile(f64::NAN, &mut empty).is_nan());
    }

    #[test]
    fn incomplete_or_empty_histograms_are_nan() {
        // No +Inf bucket.
        assert!(bucket_quantile(0.5, &mut buckets(&[(1.0, 5.0), (2.0, 10.0)])).is_nan());
        // Only the +Inf bucket.
        assert!(bucket_quantile(0.5, &mut buckets(&[(INF, 10.0)])).is_nan());
        // No observations.
        assert!(bucket_quantile(0.5, &mut buckets(&[(1.0, 0.0), (INF, 0.0)])).is_nan());
    }

    #[test]
    fn interpolates_within_the_bucket_holding_the_rank() {
        let mut h = buckets(&[(INF, 10.0), (1.0, 2.0), (2.0, 6.0)]); // unsorted on purpose
        // rank 4 falls in (1, 2], two of its four observations in.
        assert_eq!(bucket_quantile(0.4, &mut h), 1.5);
        // The lowest bucket interpolates from 0 when its bound is above 0.
        assert_eq!(bucket_quantile(0.1, &mut h), 0.5);
        // A rank in the +Inf bucket answers the highest finite bound.
        assert_eq!(bucket_quantile(0.9, &mut h), 2.0);
    }

    #[test]
    fn a_lowest_bucket_at_or_below_zero_answers_its_bound() {
        let mut h = buckets(&[(-1.0, 5.0), (INF, 10.0)]);
        assert_eq!(bucket_quantile(0.25, &mut h), -1.0);
    }

    #[test]
    fn same_bound_buckets_merge_and_counts_are_made_monotonic() {
        // Two `le="1"` series sum to 4; the dip at le=2 is lifted to 4.
        let mut h = buckets(&[(1.0, 1.0), (1.0, 3.0), (2.0, 3.0), (INF, 8.0)]);
        // rank 4 is reached at le=1 exactly.
        assert_eq!(bucket_quantile(0.5, &mut h), 1.0);
    }

    #[test]
    fn fraction_edge_cases_follow_prometheus() {
        let h = || buckets(&[(1.0, 2.0), (2.0, 6.0), (INF, 10.0)]);
        // lower >= upper is an empty range, not an error.
        assert_eq!(bucket_fraction(2.0, 2.0, &mut h()), 0.0);
        assert_eq!(bucket_fraction(3.0, 2.0, &mut h()), 0.0);
        // NaN bounds, no +Inf bucket, or no observations are NaN.
        assert!(bucket_fraction(f64::NAN, 1.0, &mut h()).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut buckets(&[(1.0, 2.0), (2.0, 6.0)])).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut buckets(&[(1.0, 0.0), (INF, 0.0)])).is_nan());
        // Linear within a bucket: (1, 2] holds 4 of 10; half of it is 0.2.
        assert_eq!(bucket_fraction(1.0, 1.5, &mut h()), 0.2);
        // The lowest bucket starts at 0 when its bound is above 0.
        assert_eq!(bucket_fraction(0.0, 0.5, &mut h()), 0.1);
        // The +Inf bucket is not interpolated: nothing above 2 is counted
        // for an upper bound inside it, all of it for +Inf itself.
        assert_eq!(bucket_fraction(2.0, 100.0, &mut h()), 0.0);
        assert_eq!(bucket_fraction(2.0, INF, &mut h()), 0.4);
    }

    #[test]
    fn a_lowest_bucket_at_or_below_zero_starts_at_minus_infinity() {
        // (-Inf, -1] holds 5 of 10 and is not interpolated: any bound inside
        // it takes the whole bucket.
        let h = || buckets(&[(-1.0, 5.0), (1.0, 8.0), (INF, 10.0)]);
        assert_eq!(bucket_fraction(f64::NEG_INFINITY, -2.0, &mut h()), 0.5);
        // (-1, 1] holds 3; from 0 to 1 is half of it.
        assert_eq!(bucket_fraction(0.0, 1.0, &mut h()), 0.15);
    }

    /// Reference values: midpoints 0.5, 1.5, 3 with weights 2, 4, 2.
    #[test]
    fn statistics_take_observations_at_bucket_midpoints() {
        let h = || buckets(&[(INF, 8.0), (4.0, 8.0), (1.0, 2.0), (2.0, 6.0)]); // unsorted on purpose
        assert_eq!(bucket_avg(&mut h()), 1.625);
        assert_eq!(bucket_stdvar(&mut h()), 0.796875);
        assert_eq!(bucket_stddev(&mut h()), 0.796875_f64.sqrt());
    }

    /// As in VictoriaMetrics, a negative lowest bucket also starts at 0: its
    /// midpoint is halfway to 0, not somewhere below its bound.
    #[test]
    fn a_negative_lowest_bucket_starts_at_zero() {
        // Midpoints -1 (0..-2) and -1.5 (-2..-1), each with weight 2.
        let mut h = buckets(&[(-2.0, 2.0), (-1.0, 4.0), (INF, 4.0)]);
        assert_eq!(bucket_avg(&mut h), -1.25);
    }

    #[test]
    fn a_negative_rounding_variance_is_zero_and_nan_stays_nan() {
        // All observations in one bucket: variance is exactly the rounding error.
        let mut one = buckets(&[(0.1, 3.0), (INF, 3.0)]);
        assert!(bucket_stdvar(&mut one) >= 0.0);
        assert!(bucket_stdvar(&mut buckets(&[(1.0, f64::NAN), (INF, 1.0)])).is_nan());
    }

    #[test]
    fn a_nan_count_makes_the_statistics_nan() {
        let h = || buckets(&[(1.0, 2.0), (2.0, f64::NAN), (INF, 3.0)]);
        assert!(bucket_avg(&mut h()).is_nan());
    }

    #[test]
    fn same_bound_buckets_are_merged_for_the_statistics() {
        // le=1 is 1 + 3 = 4, then 4 more in (1, 2]: (0.5*4 + 1.5*4) / 8.
        let mut h = buckets(&[(1.0, 1.0), (1.0, 3.0), (2.0, 8.0), (INF, 8.0)]);
        assert_eq!(bucket_avg(&mut h), 1.0);
    }

    fn eval_one(f: &dyn PromQLFunction, series: Vec<EvalSample>) -> Vec<EvalSample> {
        let result = f
            .apply_args(
                vec![PromQLArg::InstantVector(series)],
                &EvalContext::default(),
            )
            .unwrap();
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected an instant vector");
        };
        samples
    }

    /// Equal to one part in a billion, for values that do not compare exactly.
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() <= 1e-9 * b.abs(), "{a} is not close to {b}");
    }

    /// A small increment on a large count is an observation, here one at about
    /// 5e11, however tiny relative to the count: the quantile's noise tolerance
    /// would erase it, taking the average to 0.5 and the variance to 0.
    #[test]
    fn statistics_keep_a_small_increment_on_a_large_count() {
        let h = || buckets(&[(1.0, 1e12), (1e12, 1e12 + 1.0), (INF, 1e12 + 1.0)]);
        assert_close(bucket_avg(&mut h()), 1.0);
        assert_close(bucket_stdvar(&mut h()), 2.5e11);
        assert_close(bucket_stddev(&mut h()), 2.5e11_f64.sqrt());
    }

    /// Steps of 1 on 1e12 straddle the quantile's tolerance, which would drop
    /// some of these 1000 observations.
    #[test]
    fn statistics_keep_many_small_increments() {
        let mut pairs = vec![(1.0, 1e12)];
        pairs.extend((1..=1000).map(|i| (1.0 + f64::from(i), 1e12 + f64::from(i))));
        let moments = midpoint_moments(&mut buckets(&pairs));
        assert_eq!(moments.weight, 1e12 + 1000.0);
    }

    /// Only a real decrease is repaired, however small it is.
    #[test]
    fn statistics_lift_a_small_dip_and_weigh_it_zero() {
        // The dip at le=2 is lifted to 8, so its weight is 0, not -1e-3.
        let mut h = buckets(&[(1.0, 8.0), (2.0, 8.0 - 1e-3), (4.0, 10.0), (INF, 10.0)]);
        // (0.5*8 + 1.5*0 + 3*2) / 10.
        assert_eq!(bucket_avg(&mut h), 1.0);
        assert_eq!(h[1].count, 8.0);
    }

    /// VictoriaMetrics' grouping: the name does not separate histograms, any
    /// other label does.
    #[test]
    fn statistics_group_without_the_name() {
        let series = vec![
            sample(&[("__name__", "a_bucket"), ("job", "x"), ("le", "1")], 2.0),
            sample(&[("__name__", "b_bucket"), ("job", "x"), ("le", "2")], 6.0),
            sample(&[("__name__", "a_bucket"), ("job", "y"), ("le", "1")], 3.0),
            // Not a float: not a bucket.
            sample(
                &[("__name__", "a_bucket"), ("job", "x"), ("le", "1kb")],
                100.0,
            ),
        ];
        let mut samples = eval_one(&HistogramAvgFunction, series);
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(samples.len(), 2, "one result per job");
        // x: 2 at 0.5 and 4 at 1.5; y: 3 at 0.5.
        assert_eq!(samples[0].labels.get("job"), Some("x"));
        assert_eq!(samples[0].value, 7.0 / 6.0);
        assert_eq!(samples[1].labels.get("job"), Some("y"));
        assert_eq!(samples[1].value, 0.5);
        for s in &samples {
            assert_eq!(s.labels.get("__name__"), None);
            assert_eq!(s.labels.get("le"), None);
        }
    }

    /// Unlike VictoriaMetrics' statistics, which take a difference between them
    /// in whichever order the sort left them, same-bound buckets of different
    /// names are merged, so the result does not depend on input order.
    #[test]
    fn same_bound_buckets_of_different_names_merge_in_any_order() {
        let a = || sample(&[("__name__", "a_bucket"), ("le", "2")], 2.0);
        let b = || sample(&[("__name__", "b_bucket"), ("le", "2")], 6.0);
        for series in [vec![a(), b()], vec![b(), a()]] {
            let samples = eval_one(&HistogramAvgFunction, series);
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 1.0);
        }
    }

    /// Pinned against Go: `labels.FormatOpenMetricsFloat`, itself
    /// `strconv.FormatFloat(f, 'g', -1, 64)` plus `.0` when there is no `.` or
    /// exponent. `%e` starts at exponents below -4 or from 6 up, with at least
    /// two exponent digits.
    #[test]
    fn phi_labels_are_open_metrics_floats() {
        for (f, want) in [
            (0.0, "0.0"),
            (-0.0, "0.0"),
            (1.0, "1.0"),
            (-1.0, "-1.0"),
            (2.0, "2.0"),
            (0.5, "0.5"),
            (0.99, "0.99"),
            (-0.1, "-0.1"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (100000.0, "100000.0"),
            (1000000.0, "1e+06"),
            (1234567.0, "1.234567e+06"),
            (1e100, "1e+100"),
            (f64::NAN, "NaN"),
            (INF, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
        ] {
            assert_eq!(format_open_metrics_float(f), want, "{f}");
        }
    }

    fn quantiles_args(label: &str, phis: &[f64]) -> Vec<PromQLArg> {
        let mut args = vec![
            PromQLArg::InstantVector(vec![
                bucket("a_bucket", "1", 2.0),
                bucket("a_bucket", "+Inf", 4.0),
            ]),
            PromQLArg::String(label.to_string()),
        ];
        args.extend(phis.iter().map(|&phi| PromQLArg::Scalar(phi)));
        args
    }

    #[test]
    fn quantiles_write_one_labelled_series_per_phi() {
        let ExprResult::InstantVector(mut samples) =
            histogram_quantiles(quantiles_args("q", &[0.25, 0.5])).unwrap()
        else {
            panic!("expected an instant vector");
        };
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("q"), Some("0.25"));
        assert_eq!(samples[0].value, 0.5);
        assert_eq!(samples[1].labels.get("q"), Some("0.5"));
        assert_eq!(samples[1].value, 1.0);
        for s in &samples {
            assert!(s.drop_name);
            assert_eq!(s.labels.get("le"), None);
        }
    }

    #[test]
    fn quantiles_argument_errors() {
        assert!(histogram_quantiles(quantiles_args("", &[0.5])).is_err());
        assert!(histogram_quantiles(quantiles_args("q", &[])).is_err());
    }

    fn bucket(name: &str, le: &str, value: f64) -> EvalSample {
        EvalSample {
            timestamp_ms: 1000,
            value,
            labels: Labels::from_pairs(&[("__name__", name), ("job", "x"), ("le", le)]).into(),
            drop_name: false,
        }
    }

    #[test]
    fn histograms_are_grouped_by_name_too_and_bad_le_is_skipped() {
        let series = vec![
            bucket("a_bucket", "1", 2.0),
            bucket("a_bucket", "+Inf", 4.0),
            bucket("b_bucket", "1", 1.0),
            bucket("b_bucket", "+Inf", 4.0),
            // Not a float: not a bucket.
            bucket("a_bucket", "1kb", 100.0),
        ];
        let result = histogram_quantile(vec![
            PromQLArg::Scalar(0.25),
            PromQLArg::InstantVector(series),
        ])
        .unwrap();
        let ExprResult::InstantVector(mut samples) = result else {
            panic!("expected an instant vector");
        };
        samples.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(samples.len(), 2, "one result per histogram");
        // a: rank 1 of (0, 1] holding 2 → 0.5; b: rank 1 of (0, 1] holding 1 → 1.
        assert_eq!(samples[0].labels.get("__name__"), Some("a_bucket"));
        assert_eq!(samples[0].value, 0.5);
        assert_eq!(samples[1].labels.get("__name__"), Some("b_bucket"));
        assert_eq!(samples[1].value, 1.0);
        for s in &samples {
            assert!(
                s.drop_name,
                "the name is dropped when the result is rendered"
            );
            assert_eq!(s.labels.get("le"), None);
        }
    }
}
