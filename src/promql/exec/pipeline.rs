//! The evaluator's unpreloaded selector reads: an instant vector selector, a
//! matrix selector, and the subquery vector-selector fast path. Each reads its
//! series over concrete bounds and shapes them into the evaluator's
//! per-series form.

use crate::common::threads::{IntoParRayon, request_par_threads};
use crate::common::{Sample, Timestamp};
use crate::promql::engine::QueryReader;
use crate::promql::exec::types::EvalLabels;
use crate::promql::time::{grid_step_count, step_times};
use crate::promql::{EvalResult, EvalSample, EvalSamples, ExprResult, QueryOptions, RangeSample};
use orx_parallel::Par;
use promql_parser::parser::VectorSelector;

/// Read an instant vector selector: each series' latest sample within the
/// lookback window ending at `eval_ts` (modifiers already applied).
///
/// The reader applies the lookback itself, so a cluster picks each series'
/// latest sample on the shards rather than shipping the window here.
pub(crate) fn read_instant_vector<R: QueryReader + ?Sized>(
    reader: &R,
    selector: &VectorSelector,
    eval_ts: Timestamp,
    lookback_delta_ms: Timestamp,
    options: QueryOptions,
) -> EvalResult<ExprResult> {
    let samples = reader
        .query(
            selector,
            eval_ts,
            options.with_lookback_ms(lookback_delta_ms),
        )?
        .into_iter()
        .map(EvalSample::from)
        .collect();
    Ok(ExprResult::InstantVector(samples))
}

/// Read a matrix selector: each series' samples in `(end - range, end]`,
/// carrying the window's range and end for the rollups that read them.
pub(crate) fn read_matrix<R: QueryReader + ?Sized>(
    reader: &R,
    selector: &VectorSelector,
    end_ms: Timestamp,
    range_ms: Timestamp,
    options: QueryOptions,
) -> EvalResult<ExprResult> {
    let start_ms = end_ms.saturating_sub(range_ms);
    let series = read_left_open(reader, selector, start_ms, end_ms, options)?
        .into_iter()
        .map(|s| EvalSamples {
            labels: s.labels,
            drop_name: false,
            range_ms: end_ms - start_ms,
            values: s.samples.into(),
            range_end_ms: end_ms,
        })
        .collect();
    Ok(ExprResult::RangeVector(series))
}

/// The subquery vector-selector fast path: `selector` over the subquery
/// window `(start_ms, end_ms]` stepped at `step_ms`, read once and reduced to
/// each series' latest sample at every step, within the lookback.
///
/// The read reaches back by `lookback_delta_ms` before the first step so that
/// step has data; each series still carries the subquery's own range, not
/// the extended read's.
pub(crate) fn read_subquery_vector_selector<R: QueryReader + ?Sized>(
    reader: &R,
    selector: &VectorSelector,
    start_ms: Timestamp,
    end_ms: Timestamp,
    step_ms: Timestamp,
    lookback_delta_ms: Timestamp,
    options: QueryOptions,
) -> EvalResult<ExprResult> {
    let steps = SubquerySteps {
        aligned_start_ms: align_subquery_start(start_ms, step_ms),
        end_ms,
        step_ms,
        lookback_delta_ms,
        range_ms: end_ms.saturating_sub(start_ms),
    };
    let read_start_ms = steps.aligned_start_ms.saturating_sub(lookback_delta_ms);
    let series = read_left_open(reader, selector, read_start_ms, end_ms, options)?;
    Ok(ExprResult::RangeVector(shape_subquery_results(
        series, &steps,
    )))
}

/// Each series' samples in `(start_ms, end_ms]`. The reader's start bound is
/// inclusive, so it is asked from one past `start_ms`.
fn read_left_open<R: QueryReader + ?Sized>(
    reader: &R,
    selector: &VectorSelector,
    start_ms: Timestamp,
    end_ms: Timestamp,
    options: QueryOptions,
) -> EvalResult<Vec<RangeSample<EvalLabels>>> {
    Ok(reader.query_range(selector, start_ms.saturating_add(1), end_ms, options)?)
}

/// The first multiple of `step_ms` strictly after `start_ms`: where the steps
/// of a subquery whose window starts at `start_ms` begin.
///
/// Floors with `rem_euclid`, so a negative timestamp aligns down, not toward
/// zero (`-41` at step 10 floors to `-50`, so the steps begin at `-40`).
/// Saturating: near the ends of the range the multiple may not be
/// representable.
pub(crate) fn align_subquery_start(start_ms: Timestamp, step_ms: Timestamp) -> Timestamp {
    let floor = start_ms.saturating_sub(start_ms.rem_euclid(step_ms));
    if floor <= start_ms {
        floor.saturating_add(step_ms)
    } else {
        floor
    }
}

/// The step grid of the subquery fast path.
struct SubquerySteps {
    aligned_start_ms: Timestamp,
    end_ms: Timestamp,
    step_ms: Timestamp,
    lookback_delta_ms: Timestamp,
    /// The subquery's own range, which each output series carries.
    range_ms: Timestamp,
}

/// Walk `steps` (non-decreasing timestamps) over `samples` (ascending by
/// timestamp), invoking `on_step` for each step with the most recent sample at
/// or before the step timestamp, or `None` if that sample falls outside the
/// lookback window (`sample.timestamp <= step_ts - lookback_delta_ms`).
///
/// Shared by the range-query preload path and the subquery vector-selector
/// fast path so the lookback boundary semantics live in one place.
pub(crate) fn for_each_step_sample<I, F>(
    samples: &[Sample],
    steps: I,
    lookback_delta_ms: i64,
    mut on_step: F,
) where
    I: Iterator<Item = i64>,
    F: FnMut(i64, Option<&Sample>),
{
    let mut i = 0usize;
    let mut last_valid: Option<&Sample> = None;

    for step_ts in steps {
        let lookback_start = step_ts.saturating_sub(lookback_delta_ms);

        while i < samples.len() && samples[i].timestamp <= step_ts {
            last_valid = Some(&samples[i]);
            i += 1;
        }

        let within_lookback = last_valid.filter(|s| s.timestamp > lookback_start);
        on_step(step_ts, within_lookback);
    }
}

/// Reduce each series to its latest sample at every step of `steps`, within
/// the lookback window, dropping series left with no step.
///
/// Series are independent; a few short ones are shaped on the caller's
/// thread, since a pool round trip would cost more than the work.
fn shape_subquery_results(
    series_data: Vec<RangeSample<EvalLabels>>,
    steps: &SubquerySteps,
) -> Vec<EvalSamples> {
    let expected_steps =
        grid_step_count(steps.aligned_start_ms, steps.end_ms, steps.step_ms) as usize;
    let work = series_data.iter().map(|s| s.samples.len()).sum::<usize>()
        + expected_steps * series_data.len();
    let threads = request_par_threads(series_data.len(), work);
    series_data
        .into_par_rayon()
        .num_threads(threads)
        .filter_map(|series| {
            if series.samples.is_empty() {
                return None;
            }
            let mut step_samples = Vec::with_capacity(expected_steps);
            for_each_step_sample(
                &series.samples,
                step_times(steps.aligned_start_ms, steps.end_ms, steps.step_ms),
                steps.lookback_delta_ms,
                |step_ts, latest| {
                    if let Some(latest) = latest {
                        step_samples.push(Sample {
                            timestamp: step_ts,
                            value: latest.value,
                        });
                    }
                },
            );
            (!step_samples.is_empty()).then(|| EvalSamples {
                values: step_samples.into(),
                labels: series.labels,
                range_ms: steps.range_ms,
                range_end_ms: steps.end_ms,
                drop_name: false,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Label;

    fn label(name: &str, value: &str) -> Label {
        Label::new(name, value)
    }

    fn sample(ts: i64, val: f64) -> Sample {
        Sample::new(ts, val)
    }

    fn make_loaded(labels: Vec<Label>, samples: Vec<Sample>) -> RangeSample<EvalLabels> {
        RangeSample {
            labels: EvalLabels::from(crate::labels::Labels::new(labels)),
            samples,
        }
    }

    // -----------------------------------------------------------------------
    // Alignment and shaping
    // -----------------------------------------------------------------------

    #[test]
    fn subquery_steps_begin_strictly_after_the_window_start() {
        // ceil to the next step: 15 -> 20; on a multiple, the next one.
        assert_eq!(align_subquery_start(15, 10), 20);
        assert_eq!(align_subquery_start(20, 10), 30);
        // rem_euclid floors -41 to -50, so the steps begin at -40.
        assert_eq!(align_subquery_start(-41, 10), -40);
        assert_eq!(align_subquery_start(i64::MAX - 1, 10), i64::MAX);
    }

    #[test]
    fn shape_subquery_step_bucketing() {
        let labels_a = vec![label("__name__", "m")];

        // Samples at t=5, t=15, t=25
        let bucket_data = vec![make_loaded(
            labels_a,
            vec![sample(5, 1.0), sample(15, 2.0), sample(25, 3.0)],
        )];

        // Steps at 10, 20, 30 with lookback=10
        let steps = SubquerySteps {
            aligned_start_ms: 10,
            end_ms: 30,
            step_ms: 10,
            lookback_delta_ms: 10,
            range_ms: 30,
        };

        let results = shape_subquery_results(bucket_data, &steps);
        assert_eq!(results.len(), 1);
        let values = &results[0].values;
        assert_eq!(values.len(), 3);
        // Step 10: latest sample <= 10 with ts > 0 -> sample at t=5, value=1.0
        assert_eq!(values[0].timestamp, 10);
        assert_eq!(values[0].value, 1.0);
        // Step 20: latest sample <= 20 with ts > 10 -> sample at t=15, value=2.0
        assert_eq!(values[1].timestamp, 20);
        assert_eq!(values[1].value, 2.0);
        // Step 30: latest sample <= 30 with ts > 20 -> sample at t=25, value=3.0
        assert_eq!(values[2].timestamp, 30);
        assert_eq!(values[2].value, 3.0);
    }

    // -----------------------------------------------------------------------
    // The pipeline over a reader
    // -----------------------------------------------------------------------

    use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
    use promql_parser::parser::{Expr, parse};

    fn selector(q: &str) -> VectorSelector {
        match parse(q).unwrap() {
            Expr::VectorSelector(vs) => vs,
            other => panic!("not a selector: {other}"),
        }
    }

    /// `m{s="a"}` every 10 ms from 0 to 100, value = timestamp; `m{s="b"}` only at 5.
    fn reader() -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        let a = crate::labels::Labels::from_pairs(&[("__name__", "m"), ("s", "a")]);
        let b = crate::labels::Labels::from_pairs(&[("__name__", "m"), ("s", "b")]);
        for t in (0..=100).step_by(10) {
            querier.add_sample(&a, sample(t, t as f64));
        }
        querier.add_sample(&b, sample(5, -1.0));
        querier
    }

    fn by_series(result: ExprResult) -> Vec<(String, Vec<(i64, f64)>)> {
        let mut out: Vec<_> = match result {
            ExprResult::InstantVector(samples) => samples
                .into_iter()
                .map(|s| {
                    (
                        s.labels.get("s").unwrap().to_string(),
                        vec![(s.timestamp_ms, s.value)],
                    )
                })
                .collect(),
            ExprResult::RangeVector(series) => series
                .into_iter()
                .map(|s| {
                    let points = s.values.iter().map(|p| (p.timestamp, p.value)).collect();
                    (s.labels.get("s").unwrap().to_string(), points)
                })
                .collect(),
            other => panic!("unexpected result {other:?}"),
        };
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn an_instant_read_takes_each_series_latest_sample_in_its_lookback() {
        let got = read_instant_vector(&reader(), &selector("m"), 55, 20, QueryOptions::default())
            .unwrap();
        // `b`'s only sample, at 5, is outside (35, 55].
        assert_eq!(by_series(got), vec![("a".to_string(), vec![(50, 50.0)])]);
    }

    /// The window excludes its start and includes its end, and each series
    /// carries the window's range and end for the rollups that read them.
    #[test]
    fn a_matrix_read_spans_the_left_open_window() {
        let got = read_matrix(&reader(), &selector("m"), 50, 30, QueryOptions::default()).unwrap();
        let ExprResult::RangeVector(series) = &got else {
            panic!("expected a range vector")
        };
        assert!(
            series
                .iter()
                .all(|s| s.range_ms == 30 && s.range_end_ms == 50)
        );
        assert_eq!(
            by_series(got),
            vec![("a".to_string(), vec![(30, 30.0), (40, 40.0), (50, 50.0)])]
        );
    }

    /// The subquery fast path: one sample per step, the latest within the
    /// lookback, with the subquery's own range rather than the extended read.
    #[test]
    fn a_subquery_read_reduces_each_series_to_its_steps() {
        let got = read_subquery_vector_selector(
            &reader(),
            &selector("m"),
            40,
            100,
            20,
            15,
            QueryOptions::default(),
        )
        .unwrap();
        let ExprResult::RangeVector(series) = &got else {
            panic!("expected a range vector")
        };
        assert!(
            series.iter().all(|s| s.range_ms == 60),
            "{:?}",
            series.iter().map(|s| s.range_ms).collect::<Vec<_>>()
        );
        assert_eq!(
            by_series(got),
            vec![("a".to_string(), vec![(60, 60.0), (80, 80.0), (100, 100.0)])]
        );
    }
}
