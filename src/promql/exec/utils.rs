use crate::common::{Sample, Timestamp};
use crate::promql::exec::types::{EvalSample, SeriesMap, SubquerySeriesMap};
use promql_parser::parser::Expr;
#[cfg(test)]
use promql_parser::parser::SubqueryExpr;

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

/// Every subquery evaluated at this grid, outermost first. Stops at a subquery:
/// what is nested inside one runs at *its* grid, and its own preload covers it.
#[cfg(test)]
pub(in crate::promql) fn collect_subqueries(expr: &Expr) -> Vec<&SubqueryExpr> {
    let mut out = Vec::new();
    collect_subqueries_inner(expr, &mut out);
    out
}

#[cfg(test)]
fn collect_subqueries_inner<'a>(expr: &'a Expr, out: &mut Vec<&'a SubqueryExpr>) {
    match expr {
        Expr::Subquery(sq) => out.push(sq),
        Expr::Aggregate(agg) => {
            collect_subqueries_inner(&agg.expr, out);
            if let Some(ref param) = agg.param {
                collect_subqueries_inner(param, out);
            }
        }
        Expr::Binary(b) => {
            collect_subqueries_inner(&b.lhs, out);
            collect_subqueries_inner(&b.rhs, out);
        }
        Expr::Paren(p) => collect_subqueries_inner(&p.expr, out),
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_subqueries_inner(arg, out);
            }
        }
        Expr::Unary(u) => collect_subqueries_inner(&u.expr, out),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
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

/// Threads for evaluating `steps` steps at once: see [`PARALLEL_STEP_THRESHOLD`].
pub(in crate::promql) fn step_threads(steps: usize) -> usize {
    if steps < PARALLEL_STEP_THRESHOLD {
        1
    } else {
        0
    }
}
