use promql_parser::label::METRIC_NAME;
use promql_parser::parser::BinaryExpr;
use promql_parser::parser::LabelModifier;
use promql_parser::parser::token::{T_ADD, T_ATAN2, T_DIV, T_MOD, T_MUL, T_POW, T_SUB, TokenType};

/// Whether `be` carries a `fill(...)` modifier for either side: unmatched
/// series then produce results too, rather than dropping out.
pub(in crate::promql) fn has_fill_modifier(be: &BinaryExpr) -> bool {
    be.modifier
        .as_ref()
        .is_some_and(|m| m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some())
}

/// Returns true if the binary operation changes the metric schema, meaning
/// `__name__` should be dropped from the result. Mirrors Prometheus's
/// `shouldDropMetricName` in engine.go, which lists `%`, `^` and `atan2`
/// alongside the four basic arithmetic operators.
pub(in crate::promql) fn changes_metric_schema(op: TokenType) -> bool {
    matches!(
        op.id(),
        T_ADD | T_SUB | T_MUL | T_DIV | T_POW | T_MOD | T_ATAN2
    )
}

/// Whether `name` is part of a binary operation's match key under `matching`,
/// as Prometheus' `Labels.MatchLabels` selects them: with `on(...)` just the
/// listed labels, otherwise everything but the ignored labels and `__name__`.
///
/// Not the rule for the *result* labels (`result_metric`), which keeps
/// `__name__` under `ignoring(...)` unless the operator drops it.
pub(super) fn match_key_keeps(matching: Option<&LabelModifier>, name: &str) -> bool {
    match matching {
        Some(LabelModifier::Include(on)) => on.labels.iter().any(|n| n == name),
        Some(LabelModifier::Exclude(ignoring)) => {
            name != METRIC_NAME && !ignoring.labels.iter().any(|n| n == name)
        }
        None => name != METRIC_NAME,
    }
}
