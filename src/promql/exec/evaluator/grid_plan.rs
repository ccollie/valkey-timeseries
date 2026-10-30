//! One analysis of an expression for the step grid it is preloaded over: every
//! read the preload phase may make, and every lookup key the step loop needs,
//! from a single walk of the tree.
//!
//! The preload phase used to walk the expression once per question — whether
//! it calls `timestamp()`, its stepped aggregations, its selectors, its rollups
//! (twice), its subqueries, and its node keys — and to rebuild a rollup's
//! push-down form and key in three of them. [`Evaluator::plan_grid`] answers
//! them together; the passes in `preload.rs` then decide, from what earlier
//! passes cached, which of the planned reads still need making.
//!
//! Scope is the walks' own: the reads and subqueries are those evaluated *at
//! this grid*, so the walk stops collecting them at a subquery, whose inner
//! expression runs at its own grid and gets its own plan. Node keys cover
//! every node, nested subqueries included, as the step loop looks them up
//! wherever it evaluates. The `timestamp()` question covers the whole
//! expression too: a selection under a subquery can surface a sample's time.
//! Twin subqueries' inner expressions add their own subqueries and node keys,
//! never reads: those are what the first twin's are.

use super::subquery::SubqueryKey;
use super::*;

/// One rollup to preload: its grid key, the rollup, its matrix selector and
/// the aggregation fused onto it, if any.
pub(super) type RollupPreload<'e> = (
    GridPreloadKey,
    GridRollup,
    &'e MatrixSelector,
    Option<GridAggregation>,
);

/// What the preload phase may read for one expression over one grid.
pub(super) struct GridPlan<'e> {
    /// Whether the expression calls `timestamp()` anywhere, subqueries included.
    pub(super) sample_timestamps: bool,
    /// Every fusable aggregation directly over a bare vector selector, in the
    /// order met: its stepped grid key, the selector and the aggregation.
    pub(super) stepped: Vec<(GridPreloadKey, &'e VectorSelector, GridAggregation)>,
    /// Every vector selector evaluated at this grid, in the order met, with
    /// the stepped aggregation key that covers it when it is the operand of
    /// one: if that aggregation's grid is cached, the selector is not read.
    pub(super) selectors: Vec<(&'e VectorSelector, Option<GridPreloadKey>)>,
    /// Every call that could be a rollup, in the order met.
    pub(super) rollups: Vec<PlannedRollup<'e>>,
    /// Every subquery evaluated at this grid — of the expression, then of its
    /// twins — outermost first, with its key.
    pub(super) subqueries: Vec<(SubqueryKey, &'e SubqueryExpr)>,
    /// Lookup keys for every node; subquery keys are swapped for the union
    /// map's own copies when preparation finishes.
    pub(super) node_keys: NodeKeys,
}

/// A call that could be pushed down as a rollup, alone or fused with the
/// aggregation directly over it.
pub(super) struct PlannedRollup<'e> {
    pub(super) call: &'e Call,
    /// Its push-down form, when it has one: the grid key it is preloaded
    /// under, the rollup, its matrix selector and the fused aggregation.
    pub(super) pushed: Option<RollupPreload<'e>>,
}

/// Where the walk is: collecting this grid's reads, collecting only a twin's
/// subqueries, or inside a subquery, where only node keys are collected.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Grid,
    Twin,
    Nested,
}

struct Planner<'p, 'e, 'r, R: QueryReader + ?Sized> {
    evaluator: &'p Evaluator<'r, R>,
    plan: GridPlan<'e>,
    /// The call that the enclosing aggregation already offered as a fused
    /// candidate, so it is not offered again on its own.
    fused_call: Option<usize>,
}

impl<'reader, R: QueryReader + ?Sized> Evaluator<'reader, R> {
    /// Plan the preload of `expr` and its `twins` over one grid; see the
    /// module docs.
    pub(super) fn plan_grid<'e>(&self, expr: &'e Expr, twins: &[&'e Expr]) -> GridPlan<'e> {
        let mut planner = Planner {
            evaluator: self,
            plan: GridPlan {
                sample_timestamps: false,
                stepped: Vec::new(),
                selectors: Vec::new(),
                rollups: Vec::new(),
                subqueries: Vec::new(),
                node_keys: NodeKeys::default(),
            },
            fused_call: None,
        };
        planner.visit(expr, Scope::Grid, true);
        for &twin in twins {
            planner.visit(twin, Scope::Twin, false);
        }
        planner.plan
    }
}

impl<'e, R: QueryReader + ?Sized> Planner<'_, 'e, '_, R> {
    /// `in_expr`: the node is part of the planned expression rather than a
    /// twin, so a `timestamp()` call in it counts.
    fn visit(&mut self, expr: &'e Expr, scope: Scope, in_expr: bool) {
        match expr {
            Expr::VectorSelector(vs) => {
                self.plan
                    .node_keys
                    .selectors
                    .insert(node_addr(vs), PreloadKey::from_selector(vs));
                if scope == Scope::Grid {
                    self.plan.selectors.push((vs, None));
                }
            }
            Expr::Call(call) => {
                if in_expr && call.func.name == "timestamp" {
                    self.plan.sample_timestamps = true;
                }
                let pushed = self.evaluator.pushable_rollup(call);
                let rollup_key = pushed
                    .as_ref()
                    .map(|(matrix, rollup)| GridPreloadKey::rollup(&matrix.vs, rollup, None));
                self.plan
                    .node_keys
                    .rollups
                    .insert(node_addr(call), rollup_key.clone());
                let offered = self.fused_call.take() == Some(node_addr(call));
                if scope == Scope::Grid && !offered {
                    self.plan.rollups.push(PlannedRollup {
                        call,
                        pushed: pushed
                            .zip(rollup_key)
                            .map(|((matrix, rollup), key)| (key, rollup, matrix, None)),
                    });
                }
                for arg in &call.args.args {
                    self.visit(arg, scope, in_expr);
                }
            }
            Expr::Aggregate(aggregate) => self.visit_aggregate(aggregate, scope, in_expr),
            Expr::Subquery(subquery) => {
                let key = SubqueryKey::of(subquery);
                self.plan
                    .node_keys
                    .subqueries
                    .insert(node_addr(subquery), key.clone());
                if scope != Scope::Nested {
                    self.plan.subqueries.push((key, subquery));
                }
                self.visit(&subquery.expr, Scope::Nested, in_expr);
            }
            Expr::Binary(binary) => {
                self.visit(&binary.lhs, scope, in_expr);
                self.visit(&binary.rhs, scope, in_expr);
            }
            Expr::Paren(paren) => self.visit(&paren.expr, scope, in_expr),
            Expr::Unary(unary) => self.visit(&unary.expr, scope, in_expr),
            Expr::MatrixSelector(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::Extension(_) => {}
        }
    }

    fn visit_aggregate(&mut self, aggregate: &'e AggregateExpr, scope: Scope, in_expr: bool) {
        let addr = node_addr(aggregate);
        let fusable = fusable_aggregation(aggregate);
        let operand = strip_parens(&aggregate.expr);

        let stepped_key = match (operand, &fusable) {
            (Expr::VectorSelector(vs), Some(aggregation)) => {
                Some(GridPreloadKey::stepped(vs, AggregationKey::of(aggregation)))
            }
            _ => None,
        };
        self.plan
            .node_keys
            .stepped
            .insert(addr, stepped_key.clone());

        let fused = match operand {
            Expr::Call(call) => self
                .evaluator
                .pushable_rollup(call)
                .map(|pushed| (call, pushed)),
            _ => None,
        };
        let fused_key = match (&fused, &fusable) {
            (Some((_, (matrix, rollup))), Some(aggregation)) => Some(GridPreloadKey::rollup(
                &matrix.vs,
                rollup,
                Some(AggregationKey::of(aggregation)),
            )),
            _ => None,
        };
        self.plan.node_keys.fused.insert(addr, fused_key);

        if scope == Scope::Grid {
            match operand {
                // A fusable aggregation over a bare selector is one stepped
                // request; the selector itself is read only if it is not.
                Expr::VectorSelector(vs) => {
                    if let (Some(key), Some(aggregation)) = (&stepped_key, &fusable) {
                        self.plan
                            .stepped
                            .push((key.clone(), vs, aggregation.clone()));
                    }
                }
                // A rollup directly under an aggregation is offered once, as
                // a pair: fused when the operator admits it, else as the
                // rollup alone, with the aggregation run here per step.
                Expr::Call(call) => {
                    let aggregation = fusable.clone();
                    let pushed = fused.map(|(_, (matrix, rollup))| {
                        let key = GridPreloadKey::rollup(
                            &matrix.vs,
                            &rollup,
                            aggregation.as_ref().map(AggregationKey::of),
                        );
                        (key, rollup, matrix, aggregation)
                    });
                    self.plan.rollups.push(PlannedRollup { call, pushed });
                    self.fused_call = Some(node_addr(call));
                }
                _ => {}
            }
        }

        let selectors_before = self.plan.selectors.len();
        self.visit(&aggregate.expr, scope, in_expr);
        // The operand's selector, when the aggregation is a stepped one: read
        // only if that stepped grid is not cached.
        if scope == Scope::Grid
            && stepped_key.is_some()
            && let Some(entry) = self.plan.selectors.get_mut(selectors_before)
        {
            entry.1 = stepped_key;
        }
        self.fused_call = None;
        if let Some(param) = &aggregate.param {
            self.visit(param, scope, in_expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;

    fn plan_names(query: &str) -> (Vec<String>, Vec<String>, Vec<String>, bool) {
        let reader = MemorySeriesQuerier::new();
        let evaluator = Evaluator::new(&reader, QueryOptions::default());
        let expr = promql_parser::parser::parse(query).unwrap();
        let plan = evaluator.plan_grid(&expr, &[]);
        let rollups = plan
            .rollups
            .iter()
            .map(|r| match &r.pushed {
                Some((_, _, _, Some(_))) => format!("fused/{}", r.call.func.name),
                _ => r.call.func.name.to_string(),
            })
            .collect();
        let selectors = plan
            .selectors
            .iter()
            .map(|(vs, covered)| {
                let name = vs.name.clone().unwrap_or_default();
                if covered.is_some() {
                    format!("{name}*")
                } else {
                    name
                }
            })
            .collect();
        let subqueries = plan
            .subqueries
            .iter()
            .map(|(_, sq)| format!("{}", sq.expr))
            .collect();
        (rollups, selectors, subqueries, plan.sample_timestamps)
    }

    /// Calls outside subqueries are this grid's candidates, in the order met;
    /// a rollup under an aggregation is offered once, as a pair.
    #[test]
    fn rollup_candidates_stop_at_subqueries() {
        let rollups = |q: &str| plan_names(q).0;
        assert_eq!(rollups("sum_over_time(m[5m])"), vec!["sum_over_time"]);
        assert_eq!(rollups("sum(rate(m[5m]))"), vec!["fused/rate"]);
        assert_eq!(
            rollups("abs(sum_over_time(m[5m]))"),
            vec!["abs", "sum_over_time"]
        );
        assert_eq!(
            rollups("rate(m[5m]) + count_over_time(n[1m])"),
            vec!["rate", "count_over_time"]
        );
        assert_eq!(rollups("-sum_over_time(m[5m])"), vec!["sum_over_time"]);
        assert_eq!(rollups("m"), Vec::<String>::new());
        assert_eq!(
            rollups("max_over_time(sum_over_time(m[5m])[1h:1m])"),
            vec!["max_over_time"],
            "only the outer call belongs to the outer grid"
        );
        assert_eq!(rollups("(rate(m[5m]))[1h:1m]"), Vec::<String>::new());
        // `quantile` cannot fuse: the rollup is offered on its own.
        assert_eq!(rollups("quantile(0.5, rate(m[5m]))"), vec!["rate"]);
    }

    /// Selectors outside subqueries, marked (`*`) when a stepped aggregation
    /// over them would cover them.
    #[test]
    fn selectors_are_marked_by_the_stepped_aggregation_over_them() {
        let selectors = |q: &str| plan_names(q).1;
        assert_eq!(selectors("a + b"), vec!["a", "b"]);
        assert_eq!(selectors("sum(a) + avg((b))"), vec!["a*", "b*"]);
        assert_eq!(selectors("quantile(0.5, a)"), vec!["a"]);
        assert_eq!(selectors("topk(scalar(k), a)"), vec!["a", "k"]);
        assert_eq!(selectors("max_over_time(a[5m:1m]) + b"), vec!["b"]);
    }

    /// Subqueries at this grid, outermost first; `timestamp()` anywhere counts.
    #[test]
    fn subqueries_and_timestamp_calls() {
        let (_, _, subqueries, timestamps) =
            plan_names("max_over_time(a[5m:1m]) + min_over_time(max_over_time(b[1m:10s])[5m:1m])");
        assert_eq!(subqueries, vec!["a", "max_over_time(b[1m:10s])"]);
        assert!(!timestamps);
        assert!(plan_names("max_over_time(timestamp(a)[5m:1m])").3);
    }
}
