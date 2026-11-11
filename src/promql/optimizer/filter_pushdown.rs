use crate::common::logging::log_debug;
use crate::promql::binops::can_push_down_common_filters;
use crate::promql::functions::{PromqlFunctionKind, resolve_function};
use ahash::AHashSet;
use promql_parser::label::Labels;
use promql_parser::label::{METRIC_NAME, Matcher, Matchers};
use promql_parser::parser::token::{T_COUNT_VALUES, T_LOR, T_LUNLESS};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr, LabelModifier, VectorMatchCardinality, VectorSelector,
};
use std::borrow::Cow;
use std::cell::Cell;
use std::vec::Vec;

/// What the filter push-down knows about a selector's series.
///
/// Every structural rule of the push-down — which labels survive an
/// aggregation, a `label_replace`, an `on()`/`ignoring()` — is the same
/// whatever the source of the leaf facts. The leaf rule is what varies: the
/// static optimizer knows only the matchers written on a selector, while a
/// pass with access to the series index knows the label values the selector's
/// series actually carry (see `engine::derived_filters`).
pub trait LeafFilters {
    /// The label filters every series `vs` yields is known to satisfy, never
    /// including `__name__`.
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher>;

    /// Drop from `filters` those that would exclude no series `vs` reads.
    /// The default keeps every filter: a resolver that cannot tell pushes
    /// them all, as the static optimizer always has.
    fn retain_pruning(&self, _vs: &VectorSelector, _filters: &mut Vec<Matcher>) {}

    /// Whether the operands of `be` may be narrowed by each other's filters
    /// at all. The default says yes for every operation.
    fn narrows(&self, _be: &BinaryExpr) -> bool {
        true
    }
}

/// The static optimizer's leaf rule: the matchers written on the selector.
pub struct WrittenFilters;

impl LeafFilters for WrittenFilters {
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        common_filters_without_metric_name(&vs.matchers)
    }

    /// The same guard as the runtime push-down: no `fill()` (it keeps
    /// unmatched series a filter would drop), no `or`, and both operands
    /// label-carrying vectors.
    fn narrows(&self, be: &BinaryExpr) -> bool {
        can_push_down_common_filters(be)
    }
}

/// How much work the push-down may do on one expression: one unit per node
/// visited and per matcher copied, compared or hashed.
///
/// Each binary operation pushes its filters through its whole subtree, so the
/// work grows with the subtree times the filter count, at every level. A
/// left-deep chain whose terms each bring a new label costs the cube of its
/// length: 400 terms, which fit the default query length, took over five
/// seconds. Typical queries spend a few hundred units. The pass is an
/// optimization, and every filter it pushes is sound on its own, so a spent
/// budget leaves the pushes made so far and skips the rest.
pub(super) const PUSHDOWN_WORK_BUDGET: usize = 200_000;

/// One run of the push-down: the leaf rule and the budget left.
struct Pass<'a> {
    leaves: &'a dyn LeafFilters,
    remaining: Cell<usize>,
}

impl<'a> Pass<'a> {
    fn new(leaves: &'a dyn LeafFilters, budget: usize) -> Self {
        Self {
            leaves,
            remaining: Cell::new(budget),
        }
    }

    /// Take `units` from the budget. False once it is spent, and from then
    /// on: every caller then derives no filters, which pushes nothing more.
    fn spend(&self, units: usize) -> bool {
        let remaining = self.remaining.get();
        if remaining == 0 || remaining < units {
            self.remaining.set(0);
            return false;
        }
        self.remaining.set(remaining - units);
        true
    }

    /// The leaf rule's filters for `vs`; none once the budget is spent.
    fn common_filters(&self, vs: &VectorSelector) -> Vec<Matcher> {
        if !self.spend(1) {
            return vec![];
        }
        let filters = self.leaves.common_filters(vs);
        if !self.spend(filters.len()) {
            return vec![];
        }
        filters
    }

    /// [`binary_filters`], charged for the operand lists it combines.
    fn binary_filters(
        &self,
        be: &BinaryExpr,
        lhs: Vec<Matcher>,
        rhs: Vec<Matcher>,
    ) -> Vec<Matcher> {
        if !self.spend(1 + lhs.len() + rhs.len()) {
            return vec![];
        }
        binary_filters(be, lhs, rhs)
    }
}

/// Whether a call's value carries series labels. A scalar or string — a
/// literal, `time()`, `scalar(x)` — has none: it matches every series on the
/// other side of a binary operation, so it neither contributes filters nor can
/// be narrowed by them. Pushing a filter into the selector under `scalar(x)`
/// would change the value it yields, not prune series.
///
/// Only calls need asking: every other node's value type is fixed by its kind
/// (an aggregation is a vector, a literal is not) or is its operands' (a
/// binary operation of two scalars is a scalar), and the walks below reach
/// those operands anyway — asking `Expr::value_type` at each node instead
/// re-walks every binary chain beneath it.
fn call_carries_labels(fe: &Call) -> bool {
    matches!(fe.func.return_type, ValueType::Vector | ValueType::Matrix)
}

/// The argument of `fe` that its result's series come from: the first one
/// that is neither a scalar nor a string.
fn series_arg_index(fe: &Call) -> Option<usize> {
    fe.func
        .arg_types
        .iter()
        .position(|&arg| arg != ValueType::Scalar && arg != ValueType::String)
        .filter(|&i| i < fe.args.args.len())
}

/// The labels a histogram function's result does not share with its buckets,
/// so whose filters must not cross the call in either direction: the bucket
/// labels it consumes (narrowing `le` would drop buckets and change the value)
/// and the label `histogram_quantiles` writes. `None` for any other function.
fn labels_rewritten_by_call(fe: &Call) -> Option<[&str; 3]> {
    const LE: &str = "le";
    const VMRANGE: &str = "vmrange";
    match fe.func.name {
        "histogram_avg" | "histogram_fraction" | "histogram_quantile" | "histogram_stddev"
        | "histogram_stdvar" => Some([LE, VMRANGE, LE]),
        "histogram_quantiles" => {
            // The parser's signature makes the second argument a string.
            let mut label = fe.args.args.get(1).map(|arg| &**arg);
            while let Some(Expr::Paren(p)) = label {
                label = Some(&*p.expr);
            }
            match label {
                Some(Expr::StringLiteral(s)) => Some([LE, VMRANGE, s.val.as_str()]),
                _ => Some([LE, VMRANGE, LE]),
            }
        }
        _ => None,
    }
}

pub fn pushdown_filters_in_place(expr: &mut Expr) {
    pushdown_filters_in_place_with(expr, &WrittenFilters)
}

/// [`pushdown_filters_in_place`] with the leaf rule supplied by `leaves`.
pub fn pushdown_filters_in_place_with(expr: &mut Expr, leaves: &dyn LeafFilters) {
    let spent = pushdown_filters_within(expr, leaves, PUSHDOWN_WORK_BUDGET);
    if spent >= PUSHDOWN_WORK_BUDGET {
        log_debug(format!(
            "promql filter push-down stopped after its work budget of {PUSHDOWN_WORK_BUDGET} units"
        ));
    }
}

/// [`pushdown_filters_in_place_with`] with `budget` units of work, returning
/// how many it spent (all of them when it stopped short).
pub(super) fn pushdown_filters_within(
    expr: &mut Expr,
    leaves: &dyn LeafFilters,
    budget: usize,
) -> usize {
    let cx = Pass::new(leaves, budget);
    rewrite(expr, &cx);
    budget - cx.remaining.get()
}

/// Narrow every binary operation in `expr`, innermost first, and return the
/// common filters of the rewritten `expr` — what [`get_common_label_filters_with`]
/// would compute on it, without walking it again.
///
/// A binary operation's filters are derived from its operands *after* their
/// own operations were narrowed: a matcher an inner push added is a fact about
/// that operand's series too. So each step hands its parent the filters of
/// what it left behind, and the parent combines them instead of re-deriving
/// them from the leaves.
fn rewrite(expr: &mut Expr, cx: &Pass) -> Vec<Matcher> {
    match expr {
        Expr::VectorSelector(vs) => {
            if vs.name.is_none()
                && let Some(pos) = vs.matchers.matchers.iter().position(|m| {
                    m.name == METRIC_NAME && m.op == promql_parser::label::MatchOp::Equal
                })
            {
                let m = vs.matchers.matchers.remove(pos);
                vs.name = Some(m.value);
            }
            cx.common_filters(vs)
        }
        Expr::MatrixSelector(ms) => cx.common_filters(&ms.vs),
        Expr::Call(fe) => {
            let mut arg_filters: Vec<Vec<Matcher>> = fe
                .args
                .args
                .iter_mut()
                .map(|arg| rewrite(arg, cx))
                .collect();
            call_filters(fe, |i| std::mem::take(&mut arg_filters[i]))
        }
        Expr::Aggregate(agg) => {
            let filters = rewrite(&mut agg.expr, cx);
            if let Some(param) = agg.param.as_mut() {
                rewrite(param, cx);
            }
            aggregate_filters(agg, filters)
        }
        Expr::Binary(be) => {
            let lhs = rewrite(&mut be.lhs, cx);
            let rhs = rewrite(&mut be.rhs, cx);
            let common = cx.binary_filters(be, lhs, rhs);
            if common.is_empty() || !cx.leaves.narrows(be) {
                return common;
            }
            push_into_binary(be, &common, cx)
        }
        Expr::Unary(unary) => rewrite(&mut unary.expr, cx),
        Expr::Paren(p) => rewrite(&mut p.expr, cx),
        Expr::Subquery(s) => rewrite(&mut s.expr, cx),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => vec![],
    }
}

/// The label filters every series of `e`'s result satisfies, with the leaf
/// rule supplied by `leaves`.
pub fn get_common_label_filters_with(e: &Expr, leaves: &dyn LeafFilters) -> Vec<Matcher> {
    common_filters_of(e, &Pass::new(leaves, PUSHDOWN_WORK_BUDGET))
}

fn common_filters_of(e: &Expr, cx: &Pass) -> Vec<Matcher> {
    if !cx.spend(1) {
        return vec![];
    }
    match e {
        Expr::VectorSelector(vs) => cx.common_filters(vs),
        Expr::MatrixSelector(ms) => cx.common_filters(&ms.vs),
        Expr::Call(fe) => call_filters(fe, |i| common_filters_of(&fe.args.args[i], cx)),
        Expr::Aggregate(agg) => aggregate_filters(agg, common_filters_of(&agg.expr, cx)),
        Expr::Binary(be) => cx.binary_filters(
            be,
            common_filters_of(&be.lhs, cx),
            common_filters_of(&be.rhs, cx),
        ),
        Expr::Unary(unary) => common_filters_of(&unary.expr, cx),
        Expr::Paren(p) => common_filters_of(&p.expr, cx),
        Expr::Subquery(s) => common_filters_of(&s.expr, cx),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => vec![],
    }
}

/// The common filters of a call's result, given `arg_filters(i)`: those of its
/// `i`-th argument. Asks for at most one argument.
fn call_filters(fe: &Call, arg_filters: impl FnOnce(usize) -> Vec<Matcher>) -> Vec<Matcher> {
    if !call_carries_labels(fe) {
        return vec![];
    }
    let Some(func) = resolve_function(fe.func.name) else {
        return vec![];
    };
    let args = &fe.args.args;
    match func.kind() {
        PromqlFunctionKind::LabelJoin | PromqlFunctionKind::LabelReplace => {
            if args.len() < 2 {
                return vec![];
            }
            drop_label_filters_for_label_name(&arg_filters(0), &args[1])
        }
        _ => {
            let Some(index) = series_arg_index(fe) else {
                return vec![];
            };
            let mut filters = arg_filters(index);
            if let Some(labels) = labels_rewritten_by_call(fe) {
                filters.retain(|m| !labels.contains(&m.name.as_str()));
            }
            filters
        }
    }
}

/// The common filters of an aggregation's result, given those of its input:
/// the ones on labels its grouping keeps.
fn aggregate_filters(agg: &AggregateExpr, mut filters: Vec<Matcher>) -> Vec<Matcher> {
    trim_filters_by_aggr_modifier(&mut filters, agg);
    filters
}

/// The common filters of a binary operation's result, given those of its
/// operands.
fn binary_filters(
    binary: &BinaryExpr,
    mut lfs_left: Vec<Matcher>,
    mut lfs_right: Vec<Matcher>,
) -> Vec<Matcher> {
    let (group_modifier, join_modifier) = match &binary.modifier {
        Some(modifier) => (&modifier.matching, &modifier.card),
        None => (&None, &VectorMatchCardinality::OneToOne),
    };

    // `fill()` emits a series from either side alone, like `or`: only
    // filters true of both sides hold for every output series.
    let fills = binary
        .modifier
        .as_ref()
        .is_some_and(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some());
    if fills {
        let mut common = intersect_label_filters(lfs_left, lfs_right);
        trim_filters_by_match_modifier(&mut common, group_modifier);
        return common;
    }

    match binary.op.id() {
        T_LOR => {
            // {fCommon, f1} or {fCommon, f2} -> {fCommon}
            // {fCommon, f1} or on() {fCommon, f2} -> {}
            // {fCommon, f1} or on(fCommon) {fCommon, f2} -> {fCommon}
            // {fCommon, f1} or on(f1) {fCommon, f2} -> {}
            // {fCommon, f1} or on(f2) {fCommon, f2} -> {}
            // {fCommon, f1} or on(f3) {fCommon, f2} -> {}
            lfs_left = intersect_label_filters(lfs_left, lfs_right);
            trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
            lfs_left
        }
        T_LUNLESS => {
            // {f1} unless {f2} -> {f1}
            // {f1} unless on() {f2} -> {}
            // {f1} unless on(f1) {f2} -> {f1}
            // {f1} unless on(f2) {f2} -> {}
            // {f1} unless on(f1, f2) {f2} -> {f1}
            // {f1} unless on(f3) {f2} -> {}
            trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
            lfs_left
        }
        _ => {
            match join_modifier {
                // group_left
                VectorMatchCardinality::ManyToOne(include) => {
                    // {f1} * group_left() {f2} -> {f1, f2}
                    // {f1} * on() group_left() {f2} -> {f1}
                    // {f1} * on(f1) group_left() {f2} -> {f1}
                    // {f1} * on(f2) group_left() {f2} -> {f1, f2}
                    // {f1} * on(f1, f2) group_left() {f2} -> {f1, f2}
                    // {f1} * on(f3) group_left() {f2} -> {f1}
                    // {f1} * on(f3) group_left(f1) {f2} -> {}
                    // {f1} * ignoring(f1) group_left(f1) {f2} -> {}
                    // {f1} * ignoring(f3) group_left(f1) {f2} -> {f1}
                    // {f1} * on(f3) group_left(f2) {f2} -> {f1, f2}
                    retain_many_side_label_filters(&mut lfs_left, include, group_modifier);
                    retain_one_side_label_filters(&mut lfs_right, include, group_modifier);
                    union_label_filters(lfs_left, lfs_right)
                }
                // group_right
                VectorMatchCardinality::OneToMany(include) => {
                    // {f1} * group_right() {f2} -> {f1, f2}
                    // {f1} * on() group_right() {f2} -> {f2}
                    // {f1} * on(f1) group_right() {f2} -> {f1, f2}
                    // {f1} * on(f2) group_right() {f2} -> {f2}
                    // {f1} * on(f1, f2) group_right() {f2} -> {f1, f2}
                    // {f1} * on(f3) group_right() {f2} -> {f2}
                    // {f1} * on(f3) group_right(f2) {f2} -> {}
                    // {f1} * on(f3) group_right(f1) {f2} -> {f1, f2}
                    retain_many_side_label_filters(&mut lfs_right, include, group_modifier);
                    retain_one_side_label_filters(&mut lfs_left, include, group_modifier);
                    union_label_filters(lfs_left, lfs_right)
                }
                _ => {
                    // {f1} * {f2} -> {f1, f2}
                    // {f1} * on() {f2} -> {}
                    // {f1} * on(f1) {f2} -> {f1}
                    // {f1} * on(f2) {f2} -> {f2}
                    // {f1} * on(f1, f2) {f2} -> {f2}
                    // {f1} * on(f3} {f2} -> {}
                    lfs_left = union_label_filters(lfs_left, lfs_right);
                    trim_filters_by_match_modifier(&mut lfs_left, group_modifier);
                    lfs_left
                }
            }
        }
    }
}

/// An aggregation keeps only its grouping labels: `by (...)` keeps those
/// named, `without (...)` all but those named, and no clause none.
fn trim_filters_by_aggr_modifier(lfs: &mut Vec<Matcher>, afe: &AggregateExpr) {
    match &afe.modifier {
        None => lfs.clear(),
        modifier => trim_filters_by_match_modifier(lfs, modifier),
    }
}

/// Whether a grouped join matches its operands on `label`, i.e. both sides
/// are known to carry the same value for it. `on()` and `ignoring()` are the
/// only cases: a group clause cannot be written without one of them.
fn is_matched_label(label: &str, matching: &Option<LabelModifier>) -> bool {
    match matching {
        None => true,
        Some(LabelModifier::Include(on)) => on.labels.iter().any(|l| l == label),
        Some(LabelModifier::Exclude(ignoring)) => !ignoring.labels.iter().any(|l| l == label),
    }
}

/// Of filters on the "many" side of a `group_left(...)`/`group_right(...)`
/// (the left of `group_left`, the right of `group_right`), keep those that
/// hold for the join's result. A grouped join keeps every label of the many
/// side except the ones the group clause names: those are written from the
/// "one" side, or deleted when it has none, so a many-side filter on one says
/// nothing about the result — unless the label is also matched on, and both
/// sides agree on it. `on()` may not share a label with the group clause
/// (the parser rejects it), so under `on()` every named label is overwritten.
///
/// The same set is what may be pushed *into* the many side from a filter
/// known of the result: its result labels are its own labels, bar those.
fn retain_many_side_label_filters(
    lfs: &mut Vec<Matcher>,
    include: &Labels,
    matching: &Option<LabelModifier>,
) {
    if include.labels.is_empty() {
        return;
    }
    lfs.retain(|m| !include.labels.contains(&m.name) || is_matched_label(&m.name, matching));
}

/// Of filters on the "one" side of a `group_left(...)`/`group_right(...)`,
/// keep those that hold for the join's result: filters on a matched label,
/// which the many side carries too, and filters on a label the group clause
/// names, which the result takes from this side. A label the one side lacks
/// is deleted from the result rather than copied, and a matcher treats a
/// missing label as the empty value, so the filter holds either way. Every
/// other label of the one side is absent from the result.
///
/// As with the many side, the same set is what may be pushed into the one
/// side from a filter known of the result.
fn retain_one_side_label_filters(
    lfs: &mut Vec<Matcher>,
    include: &Labels,
    matching: &Option<LabelModifier>,
) {
    lfs.retain(|m| include.labels.contains(&m.name) || is_matched_label(&m.name, matching));
}

/// Trims lfs by the specified be.modifier.matching (e.g., on() or ignoring()).
///
/// The following cases are possible:
/// - It returns lfs as is if be doesn't contain any group modifier
/// - It returns only filters specified in on()
/// - It drops filters specified inside ignoring()
pub fn trim_filters_by_match_modifier(
    lfs: &mut Vec<Matcher>,
    group_modifier: &Option<LabelModifier>,
) {
    match group_modifier {
        None => {}
        Some(LabelModifier::Include(on)) => lfs.retain(|m| on.labels.contains(&m.name)),
        Some(LabelModifier::Exclude(ignoring)) => {
            lfs.retain(|m| !ignoring.labels.contains(&m.name))
        }
    }
}

fn common_filters_without_metric_name(matchers: &Matchers) -> Vec<Matcher> {
    if let Some((head, rest)) = matchers.or_matchers.split_first() {
        let mut lfs_a = label_filters_without_metric_name(head);
        for lfs in rest {
            if lfs_a.is_empty() {
                return vec![];
            }
            lfs_a = intersect_label_filters(lfs_a, label_filters_without_metric_name(lfs));
        }
        return lfs_a;
    }
    label_filters_without_metric_name(&matchers.matchers)
}

fn label_filters_without_metric_name(lfs: &[Matcher]) -> Vec<Matcher> {
    lfs.iter()
        .filter(|&x| x.name != METRIC_NAME)
        .cloned()
        .collect()
}

/// Append `common_filters` to the selector `vs`, less those the leaf rule
/// knows would prune nothing there.
fn push_filters_to_selector(vs: &mut VectorSelector, common_filters: &[Matcher], cx: &Pass) {
    let written =
        vs.matchers.matchers.len() + vs.matchers.or_matchers.iter().map(Vec::len).sum::<usize>();
    if !cx.spend(common_filters.len() + written) {
        return;
    }
    // Owned: the retained set is this selector's, not the sibling's the
    // caller's list goes on to.
    let mut filters = common_filters.to_vec();
    cx.leaves.retain_pruning(vs, &mut filters);
    if !filters.is_empty() {
        push_filters_to_matchers(&mut vs.matchers, &filters);
    }
}

fn push_filters_to_matchers(matchers: &mut Matchers, common_filters: &[Matcher]) {
    fn sort(matchers: &mut [Matcher]) {
        matchers.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));
    }
    if !matchers.matchers.is_empty() {
        union_label_filters_internal(&mut matchers.matchers, common_filters);
        sort(&mut matchers.matchers);
    } else if !matchers.or_matchers.is_empty() {
        for matcher in matchers.or_matchers.iter_mut() {
            union_label_filters_internal(matcher, common_filters);
            sort(matcher);
        }
    } else {
        matchers.matchers = common_filters.to_vec();
        sort(&mut matchers.matchers);
    }
}

pub fn push_down_binary_op_filters_in_place(e: &mut Expr, common_filters: &[Matcher]) {
    push_down(
        e,
        common_filters,
        &Pass::new(&WrittenFilters, PUSHDOWN_WORK_BUDGET),
    );
}

/// Push `common_filters` into `e`, with the leaf rule deciding, at each
/// selector, which of them are worth adding, and return the common filters of
/// the rewritten `e` (see [`rewrite`]).
///
/// `common_filters` is read-only: a node that narrows the set for its own
/// subtree (a matching modifier, an aggregation's grouping, a relabeling
/// function) trims a copy. The operands of a binary operation are siblings
/// and must each see the set as their parent left it — when they shared one
/// list, whatever the left operand trimmed for itself was lost to the right.
fn push_down(e: &mut Expr, common_filters: &[Matcher], cx: &Pass) -> Vec<Matcher> {
    if common_filters.is_empty() {
        return common_filters_of(e, cx);
    }
    // A node may copy the list once or twice for its subtrees.
    if !cx.spend(1 + common_filters.len()) {
        return vec![];
    }

    match e {
        Expr::VectorSelector(vs) => {
            push_filters_to_selector(vs, common_filters, cx);
            cx.common_filters(vs)
        }
        Expr::MatrixSelector(ms) => {
            push_filters_to_selector(&mut ms.vs, common_filters, cx);
            cx.common_filters(&ms.vs)
        }
        Expr::Subquery(s) => push_down(&mut s.expr, common_filters, cx),
        Expr::Call(fe) => push_into_call(fe, common_filters, cx),
        Expr::Unary(unary) => push_down(&mut unary.expr, common_filters, cx),
        Expr::Binary(be) => push_into_binary(be, common_filters, cx),
        Expr::Aggregate(aggr) => {
            // Grouping labels pass through an aggregation unchanged, so a filter
            // on one is equally true of the input series. A label the aggregation
            // *synthesizes* is not: `count_values` writes its value label onto the
            // output, and the input series carry no such label, so pushing a
            // filter on it into the selector would match nothing.
            let mut filters = match aggr.param.as_deref() {
                Some(label_name) if aggr.op.id() == T_COUNT_VALUES => {
                    drop_label_filters_for_label_name(common_filters, label_name)
                }
                _ => common_filters.to_vec(),
            };
            trim_filters_by_aggr_modifier(&mut filters, aggr);
            // `aggr.param` is a scalar or string (the `k` of topk, the quantile,
            // the count_values label) — never an operand of the binary op's label
            // matching. Rewriting a selector under it, as in `topk(scalar(x), y)`,
            // would change the parameter's value rather than prune series.
            let filters = push_down(&mut aggr.expr, &filters, cx);
            aggregate_filters(aggr, filters)
        }
        Expr::Paren(p) => push_down(&mut p.expr, common_filters, cx),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => vec![],
    }
}

fn push_into_call(fe: &mut Call, common_filters: &[Matcher], cx: &Pass) -> Vec<Matcher> {
    if !call_carries_labels(fe) {
        return vec![];
    }
    let target: Option<(usize, Cow<[Matcher]>)> = match fe.func.name {
        "label_replace" | "label_join" if fe.args.args.len() >= 2 => Some((
            0,
            Cow::Owned(drop_label_filters_for_label_name(
                common_filters,
                &fe.args.args[1],
            )),
        )),
        "label_replace" | "label_join" | "absent" | "absent_over_time" => None,
        _ => series_arg_index(fe).map(|index| {
            let filters = match labels_rewritten_by_call(fe) {
                Some(labels) => Cow::Owned(
                    common_filters
                        .iter()
                        .filter(|m| !labels.contains(&m.name.as_str()))
                        .cloned()
                        .collect(),
                ),
                None => Cow::Borrowed(common_filters),
            };
            (index, filters)
        }),
    };
    let pushed =
        target.map(|(index, filters)| (index, push_down(&mut fe.args.args[index], &filters, cx)));
    let fe = &*fe;
    call_filters(fe, |i| match pushed {
        Some((index, filters)) if index == i => filters,
        _ => common_filters_of(&fe.args.args[i], cx),
    })
}

fn push_into_binary(bo: &mut BinaryExpr, common_filters: &[Matcher], cx: &Pass) -> Vec<Matcher> {
    let (lhs, rhs) = match &bo.modifier {
        None => (
            push_down(&mut bo.lhs, common_filters, cx),
            push_down(&mut bo.rhs, common_filters, cx),
        ),
        Some(modifier) => {
            // A grouped join's result carries the many side's labels and the
            // group clause's labels from the one side, so each side can take
            // the filters on its own labels, not only the matched ones. Not
            // with `fill()`: narrowing one side on an unmatched label could
            // leave a series of the other side unmatched, and the fill would
            // then synthesize a result for it that was not there before.
            let fills = modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some();
            let (mut lhs_filters, mut rhs_filters) =
                (common_filters.to_vec(), common_filters.to_vec());
            match &modifier.card {
                VectorMatchCardinality::ManyToOne(include) if !fills => {
                    retain_many_side_label_filters(&mut lhs_filters, include, &modifier.matching);
                    retain_one_side_label_filters(&mut rhs_filters, include, &modifier.matching);
                }
                VectorMatchCardinality::OneToMany(include) if !fills => {
                    retain_one_side_label_filters(&mut lhs_filters, include, &modifier.matching);
                    retain_many_side_label_filters(&mut rhs_filters, include, &modifier.matching);
                }
                _ => {
                    trim_filters_by_match_modifier(&mut lhs_filters, &modifier.matching);
                    trim_filters_by_match_modifier(&mut rhs_filters, &modifier.matching);
                }
            }
            (
                push_down(&mut bo.lhs, &lhs_filters, cx),
                push_down(&mut bo.rhs, &rhs_filters, cx),
            )
        }
    };
    cx.binary_filters(bo, lhs, rhs)
}

fn intersect_label_filters(first: Vec<Matcher>, second: Vec<Matcher>) -> Vec<Matcher> {
    if first.is_empty() || second.is_empty() {
        return vec![];
    }
    let set = matcher_set(&first);
    second.into_iter().filter(|m| set.contains(m)).collect()
}

fn union_label_filters(mut first: Vec<Matcher>, second: Vec<Matcher>) -> Vec<Matcher> {
    if first.is_empty() {
        return second;
    }
    union_label_filters_internal(&mut first, &second);
    first
}

/// Append to `first` each filter of `second` that `first` lacks. A filter
/// `second` holds twice is appended twice.
fn union_label_filters_internal(first: &mut Vec<Matcher>, second: &[Matcher]) {
    if second.is_empty() {
        return;
    }
    let missing: Vec<Matcher> = {
        let set = matcher_set(first);
        second
            .iter()
            .filter(|m| !set.contains(m))
            .cloned()
            .collect()
    };
    first.extend(missing);
}

fn matcher_set(filters: &[Matcher]) -> AHashSet<&Matcher> {
    let mut set = AHashSet::with_capacity(filters.len());
    set.extend(filters.iter());
    set
}

fn drop_label_filters_for_label_name(lfs: &[Matcher], label_name: &Expr) -> Vec<Matcher> {
    let Expr::StringLiteral(name) = label_name else {
        return vec![];
    };
    lfs.iter().filter(|x| x.name != name.val).cloned().collect()
}
