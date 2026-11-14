use crate::common::threads::IntoParRayon;
use crate::common::{Sample, Timestamp};
use crate::labels::{HasFingerprint, SeriesFingerprint, fingerprint_labels};
use crate::promql::exec::types::{EvalLabels, EvalSample, SeriesMap, SubquerySeriesMap};
use crate::promql::hashers::FingerprintHashSet;
use crate::promql::{EvalResult, EvaluationError};
use orx_parallel::{Par, ParResult};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::Expr;

/// Append one evaluation step's instant-vector samples into a per-series map,
/// stamping each sample with the step timestamp.
///
/// For the range-query step merge (`evaluate_range`), whose steps have already
/// had their pending `__name__` drops applied. Callers must feed steps in
/// ascending timestamp order so the per-series sample vectors stay
/// chronologically sorted.
pub(in crate::promql) fn merge_step_into_series_map(
    series_map: &mut SeriesMap,
    step_ts: Timestamp,
    samples: Vec<EvalSample>,
) {
    for sample in samples {
        series_map
            .entry(sample.labels)
            .push(Sample::new(step_ts, sample.value));
    }
    series_map.end_step();
}

/// [`merge_step_into_series_map`] for a subquery's inner steps, whose samples
/// may still carry a pending `__name__` drop: `abs(m)` keeps `m`'s name until
/// the query's result is rendered. The drop is kept per series (set if any
/// step's sample carries it) so the subquery's output series still drop the
/// name, as Prometheus's do: `last_over_time(abs(m)[10m:])` has no `__name__`.
pub(in crate::promql) fn merge_step_into_subquery_map(
    series_map: &mut SubquerySeriesMap,
    step_ts: Timestamp,
    samples: Vec<EvalSample>,
) {
    for sample in samples {
        let (values, drop_name) = series_map.entry(sample.labels);
        values.push(Sample::new(step_ts, sample.value));
        *drop_name |= sample.drop_name;
    }
    series_map.end_step();
}

/// Look through parentheses: `sum((metric))` aggregates a selector just as
/// `sum(metric)` does.
pub(in crate::promql) fn strip_parens(expr: &Expr) -> &Expr {
    let mut current = expr;
    while let Expr::Paren(paren) = current {
        current = &paren.expr;
    }
    current
}

/// Fewer evaluation steps than this — a range query's or a subquery's — are
/// evaluated on the calling thread: handing them to the pool costs more than
/// it saves.
pub(in crate::promql) const PARALLEL_STEP_THRESHOLD: usize = 4;

/// Up to `size` more of `steps`, allocated once: `steps` cannot say how many
/// are left, so a plain `collect` would grow the batch from empty.
pub(in crate::promql) fn next_step_batch(
    steps: &mut impl Iterator<Item = Timestamp>,
    size: usize,
) -> Vec<Timestamp> {
    let mut batch = Vec::with_capacity(size);
    batch.extend(steps.take(size));
    batch
}

/// `eval` at each of `batch`'s steps, the results in step order: on this
/// thread below [`PARALLEL_STEP_THRESHOLD`] steps, across the request pool
/// otherwise. The first error ends the batch.
pub(in crate::promql) fn evaluate_step_batch<T: Send, E: Send>(
    batch: Vec<Timestamp>,
    eval: impl Fn(Timestamp) -> Result<T, E> + Sync,
) -> Result<Vec<T>, E> {
    if batch.len() < PARALLEL_STEP_THRESHOLD {
        batch.into_iter().map(eval).collect()
    } else {
        // A reference is `Copy` and `Send`, as the pool's adapters need.
        let eval = &eval;
        batch.into_par_rayon().map(eval).into_fallible().collect()
    }
}

/// Fingerprint of a sample's *effective* label set: the labels as they will
/// stand once a pending `__name__` drop is applied.
pub(crate) fn effective_fingerprint(labels: &EvalLabels, drop_name: bool) -> SeriesFingerprint {
    if !drop_name {
        return labels.fingerprint();
    }
    fingerprint_labels(labels.iter().filter(|l| l.name != METRIC_NAME))
}

/// Refuse a vector holding two samples with one label set, as they will stand
/// once pending `__name__` drops are applied: what such a vector means is
/// undefined (prometheus/prometheus#4562).
pub(in crate::promql) fn ensure_unique_labelsets(samples: &[EvalSample]) -> EvalResult<()> {
    let mut seen_label_sets = FingerprintHashSet::default();
    for sample in samples {
        let key = effective_fingerprint(&sample.labels, sample.drop_name);
        if !seen_label_sets.insert(key) {
            return Err(EvaluationError::DuplicateLabelSet);
        }
    }

    Ok(())
}

#[cfg(test)]
mod unique_labelset_tests {
    use super::*;

    /// Two samples whose labels are identical once pending `__name__` drops
    /// are applied are duplicates, whatever their `drop_name` flags say.
    #[test]
    fn unique_labelsets_compares_effective_labels_across_drop_name_flags() {
        let sample = |labels: &[(&str, &str)], drop_name: bool| EvalSample {
            timestamp_ms: 0,
            value: 1.0,
            labels: EvalLabels::from_pairs(labels),
            drop_name,
        };

        // Materialized state: the dropping sample has already lost its name.
        let materialized = [
            sample(&[("env", "1")], false),
            sample(&[("env", "1")], true),
        ];
        assert!(ensure_unique_labelsets(&materialized).is_err());

        // Pending state: the name is still there but owed.
        let pending = [
            sample(&[("env", "1")], false),
            sample(&[("__name__", "m"), ("env", "1")], true),
        ];
        assert!(ensure_unique_labelsets(&pending).is_err());

        // Genuinely distinct.
        let distinct = [
            sample(&[("env", "1")], false),
            sample(&[("__name__", "m"), ("env", "1")], false),
        ];
        assert!(ensure_unique_labelsets(&distinct).is_ok());
    }
}
