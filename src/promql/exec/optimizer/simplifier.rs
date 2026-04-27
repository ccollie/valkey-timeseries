// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// https://github.com/apache/arrow-datafusion/tree/main/datafusion/optimizer/src
// https://github.com/apache/arrow-datafusion/blob/e222bd627b6e7974133364fed4600d74b4da6811/datafusion/optimizer/src/utils.rs

use crate::parser::ParseResult;
use crate::promql::exec::optimizer::pushdown::optimize_in_place;
use crate::promql::exec::optimizer::utils::{expr_contains, is_null, is_number, is_one, is_op_with, is_zero};
use crate::promql::functions::{resolve_function, PromqlFunctionKind};
use promql_parser::parser::token::{TokenType, T_ADD, T_DIV, T_LAND, T_LOR, T_MOD, T_MUL};
use promql_parser::parser::{BinaryExpr, Expr};
use std::ops::Deref;
// https://prometheus.io/docs/prometheus/latest/querying/operators
// Expression simplification API

pub fn simplify_expression(expr: Expr) -> ParseResult<Expr> {
    let simplifier = ExprSimplifier::new();
    simplifier.simplify(expr)
}

pub fn optimize(expr: Expr) -> ParseResult<Expr> {
    simplify_expression(expr)
}

/// This structure handles API for expression simplification
#[derive(Default)]
pub struct ExprSimplifier {}

impl ExprSimplifier {
    /// Create a new `ExprSimplifier` with the given `info` such as an
    /// instance of [`SimplifyContext`]. See
    /// [`simplify`](Self::simplify) for an example.
    ///
    /// [`SimplifyContext`]: crate::simplify_expressions::context::SimplifyContext
    pub fn new() -> Self {
        Self {}
    }

    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying simplifications.
    ///
    /// The types of the expression must match what operators expect,
    /// or else an error may occur trying to evaluate.
    ///
    /// # Example:
    ///
    /// `b > 2 AND b > 2`
    ///
    /// can be written to
    ///
    /// `b > 2`
    ///
    /// ``` rust
    /// use crate::metricsql_parser::prelude::{selector, number, Expr, ExprSimplifier};
    ///
    /// // Create the simplifier
    /// let simplifier = ExprSimplifier::new();
    ///
    /// // b < 2
    /// let b_lt_2 = selector("b").lt(number(2.0));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = simplifier.simplify(expr).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    pub fn simplify(&self, expr: Expr) -> ParseResult<Expr> {
        let mut simplifier = Simplifier::new();
        let mut const_evaluator = ConstEvaluator::new();

        let expr = remove_parens(expr);

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        let mut result = expr
            .rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)?
            // run both passes twice to try and minimize simplifications that we missed
            .rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)?;

        // push down filters
        optimize_in_place(&mut result);
        Ok(result)
    }
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr == bool 1` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr != true` to `!expr` when `expr` is of boolean type
/// * `1 == bool 1` to `1`
/// * `0 == bool 1` to `0`
/// * `expr == NaN` and `expr != NaN` to `NaN`
#[derive(Default)]
pub struct Simplifier {}

impl Simplifier {
    pub fn new() -> Self {
        Self {}
    }

    pub fn simplify(&self, expr: Expr) -> Expr {

        match expr {
            Expr::Binary(BinaryExpr {
                             lhs,
                             rhs,
                             op,
                             modifier,
                         }) => {
                match op.id() {
                    //
                    // Rules for Add
                    //

                    // A + 0 --> A
                    // Valid only for NumberLiteral, since VectorSelector, Subquery, Aggregation,
                    // etc. can return vectors containing NaN
                    T_ADD if is_zero(&rhs) && is_number(&lhs) => *lhs,

                    // 0 + A --> A
                    // Valid only for NumberLiteral, since VectorSelector, Subquery, Aggregation,
                    // etc. can return vectors containing NaN
                    T_ADD if is_zero(&lhs) && is_number(&rhs) => *rhs,

                    // A + A --> A * 2
                    // Our use case envisions that this expression involving metric selectors
                    // will need to make network calls to evaluate. If both sides are the same
                    // we can optimize by multiplying by 2 and only making one network call.
                    T_ADD if *lhs == *rhs
                        && matches!(
                            lhs.deref(),
                            Expr::VectorSelector(_) | Expr::Subquery(_) | Expr::Aggregate(_)
                        ) =>
                        {
                            let two = Expr::from(2.0);
                            Expr::Binary(BinaryExpr {
                                rhs: Box::new(two),
                                lhs,
                                op: TokenType::new(T_MUL),
                                modifier,
                            })
                        }

                    // Rules for OR
                    //
                    // (..A..) OR A --> (..A..)
                    T_LOR if expr_contains(&lhs, &rhs, TokenType::new(T_LOR)) => *lhs,
                    // A OR (..A..) --> (..A..)
                    T_LOR if expr_contains(&rhs, &lhs, TokenType::new(T_LOR)) => *rhs,
                    // A OR (A AND B) --> A
                    T_LOR if is_op_with(TokenType::new(T_LAND), &rhs, &lhs) => *lhs,
                    // (A AND B) OR A --> A
                    T_LOR if is_op_with(TokenType::new(T_LAND), &lhs, &rhs) => *rhs,

                    //
                    // Rules for AND
                    //
                    // (..A..) AND A --> (..A..)
                    T_LAND if expr_contains(&lhs, &rhs, TokenType::new(T_LAND)) => *lhs,
                    // A AND (..A..) --> (..A..)
                    T_LAND if expr_contains(&rhs, &lhs, TokenType::new(T_LAND)) => *rhs,
                    // A AND (A OR B) --> A
                    T_LAND if is_op_with(TokenType::new(T_LOR), &rhs, &lhs) => *lhs,
                    // (A OR B) AND A --> A
                    T_LAND if is_op_with(TokenType::new(T_LOR), &lhs, &rhs) => *rhs,

                    //
                    // Rules for Mul
                    //
                    // A * 1 --> A
                    T_MUL if is_one(&rhs) && is_number(&lhs) => *lhs,
                    // 1 * A --> A
                    T_MUL if is_one(&lhs) && is_number(&rhs) => *rhs,
                    // A * NaN --> NaN
                    T_MUL if is_null(&rhs) => *rhs,
                    // NaN * A --> NaN
                    T_MUL if is_null(&lhs) => *lhs,
                    // A * 0 --> 0
                    T_MUL if is_zero(&rhs) && is_number(&lhs) => *rhs,
                    // 0 * A --> 0
                    T_MUL if is_zero(&lhs) && is_number(&rhs) => *lhs,

                    //
                    // Rules for Div
                    //
                    // A / 1 --> A
                    T_DIV if is_one(&rhs) => *lhs,
                    // NaN / A --> NaN
                    T_DIV if is_null(&lhs) => *lhs,
                    // A / NaN --> NaN
                    T_DIV if is_null(&rhs) => *rhs,
                    // A / A --> NAN if A.is_nan() else 1.0. The NaN comparison can be valid for
                    // NumberLiteral, but not for VectorSelector, Subquery, Aggregation, etc.
                    T_DIV if lhs == rhs && is_number(&lhs) => {
                        if is_null(&rhs) {
                            Expr::from(f64::NAN)
                        } else {
                            Expr::from(1.0)
                        }
                    }
                    // 0 / 0 -> NaN
                    T_DIV if is_zero(&lhs) && is_zero(&rhs) => Expr::from(f64::NAN),
                    // A / 0 -> NaN
                    T_DIV if is_zero(&rhs) => {
                        // if we have an instant vector or sample, check if we need to maintain
                        // the label set
                        let should_keep_metric_names = {
                            if let Expr::Call(fe) = &lhs.as_ref() {
                                if let Some(func) = resolve_function(fe.func.name) {
                                    use PromqlFunctionKind::*;
                                    let kind = func.kind();
                                    matches!(kind, LabelReplace | LabelJoin)
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };
                        if should_keep_metric_names {
                            return Expr::Binary(BinaryExpr {
                                lhs,
                                rhs,
                                op,
                                modifier,
                            });
                        }

                        Expr::from(f64::NAN)
                    }
                    // 0 / A -> 0
                    T_DIV if is_zero(&lhs) && is_number(&rhs) => *lhs,

                    //
                    // Rules for Mod
                    //
                    // A % NaN --> NaN
                    T_MOD if is_null(&rhs) => *rhs,
                    // NaN % A --> NaN
                    T_MOD if is_null(&lhs) => *lhs,
                    // A % 1 --> 0
                    T_MOD if is_one(&rhs) && is_number(&lhs) => Expr::from(0.0),
                    // A % 0 --> NaN
                    T_MOD if is_zero(&rhs) => Expr::from(f64::NAN),
                    // A % A --> 0
                    T_MOD if lhs == rhs && is_number(&lhs) => {
                        if is_null(&rhs) {
                            Expr::from(f64::NAN)
                        } else {
                            Expr::from(0.0)
                        }
                    }
                    // no additional rewrites possible
                    _ => Expr::Binary(BinaryExpr {
                        lhs,
                        rhs,
                        op,
                        modifier,
                    }),
                }
            }
            expr => expr,
        }
    }
}

// see https://prometheus.io/docs/prometheus/latest/querying/operators/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::exec::optimizer::utils::{number, selector};

    fn parse(expr: &str) -> Expr {
        promql_parser::parser::parse(expr).unwrap()
    }
    fn assert_expr_eq(expected: &Expr, actual: &Expr) {
        assert_eq!(
            expected, actual,
            "expected: \n{}\n but got: \n{}",
            expected, actual
        );
    }

    fn assert_string_expr_eq(expr: &str, expected: Expr) {
        let expr = parse(expr);
        let actual = simplify(expr);
        assert_eq!(expected, actual,
                   "expected: \n{}\n but got: \n{}",
                   expected, actual
        );
    }

    fn assert_string_simplify(expr: &str, expected: &str) {
        let expr = parse(expr);
        let expected_expr = parse(expected);
        let actual = simplify(expr);
        assert_eq!(expected_expr, actual,
                   "expected: \n{}\n but got: \n{}",
                   expected, actual
        );
    }

    // ------------------------------
    // --- ExprSimplifier tests -----
    // ------------------------------
    #[test]
    fn api_basic() {
        assert_string_simplify("1.0 + 2.0", "3.0");
    }

    #[test]
    fn simplify_and_constant_prop() {
        // should be able to simplify to false
        // (6 * (1 - 2)) > 0
        assert_string_simplify("(6.0 * (1.0 - 2.0)) > 0.0", "0.0");
    }

    // ------------------------------
    // --- Simplifier tests -----
    // ------------------------------

    #[test]
    fn test_simplify_or_same() {
        assert_string_simplify("c2 or c2", "c2");
    }

    #[test]
    fn test_simplify_and_same() {
        assert_string_simplify("c2 and c2", "c2");
    }

    #[test]
    fn test_simplify_selector_plus_selector_same() {
        let expr = "c2 + c2";
        let expected = "c2 * 2.0";
        assert_string_simplify(expr, expected);
    }

    #[test]
    fn test_simplify_mul_by_one() {
        assert_string_simplify("c2 * 1.0", "c2");
        assert_string_simplify("1.0 * c2", "c2");

        assert_string_simplify("45.0 * 1.0", "45.0");
        assert_string_simplify("1.0 * 89.0", "89.0");
    }

    #[test]
    fn test_simplify_mul_by_nan() {
        // A * NAN --> NAN
        assert_string_simplify("c2 * NaN", "NaN");
        // NAN * A --> NAN
        assert_string_simplify("NaN * c2", "NaN");
    }

    #[test]
    fn test_simplify_add_zero() {
        // 0 + A --> A, where A is numeric
        assert_string_simplify("0.0 + 5.0", "5.0");


        // 0 + A --> A
        // Only simplify when A is numeric
        assert_string_simplify("0.0 + c2", "c2");

        // A + 0 --> A if A
        // Only simplify when A is numeric
        assert_string_simplify("foo + 0.0", "foo");
    }

    #[test]
    fn test_simplify_mul_by_zero() {
        // 0 * A --> 0
        {
            // should remain unchanged if A is not numeric
            assert_string_simplify("0.0 * c2", "0.0");

            // should return 0.0 if it is numeric
            assert_string_simplify("0.0 * 12.5", "0.0");
        }

        // A * 0 --> 0
        {
            // should remain unchanged for non-numeric A
            let expr = "foo * 0.0";
            let expected = expr.clone();
            let actual = simplify(expr);
            assert_expr_eq(&expected, &actual);

            let expr = "0.0 * 65.4";
            assert_string_simplify(expr, "0.0");
        }
    }

    #[test]
    fn test_simplify_div_by_one() {
        // A / 1 = A
        // should remain unchanged for non-numeric A
        assert_string_simplify("c2 / 1.0", "c2");

        // return A for numeric A
        assert_string_simplify("42.0 / 1.0", "42.0");
    }

    #[test]
    fn test_simplify_div_nan() {
        // A / NAN --> NAN
        assert_string_simplify("c1 / NaN", "NaN");
        // NAN / A --> NAN
        assert_string_simplify("NaN / c2", "NaN");
        // NAN / NAN --> NAN
        assert_string_simplify("NaN / NaN", "NaN");
    }

    #[test]
    fn test_simplify_div_zero_by_zero() {
        // 0 / 0 -> NAN
        let expr = "0.0 / 0.0";
        assert_string_simplify(expr, "NaN");
    }

    #[test]
    fn test_simplify_div_by_zero() {
        // A / 0 -> NaN
        assert_string_simplify("c2 / 0.0", "NaN");
    }

    #[test]
    fn test_simplify_mod_by_nan() {
        // A % NaN --> NaN
        assert_string_simplify("c2 % NaN", "NaN");
        // NaN % A --> NaN
        assert_string_simplify("NaN % c2", "NaN");
    }

    #[test]
    fn test_simplify_mod_by_one() {
        assert_string_simplify("c2 % 1", "0.0");

        // test with number
        assert_string_simplify("789.0 % 1.0", "0.0");
    }

    #[test]
    fn test_simplify_mod_by_zero_non_nan() {
        assert_string_simplify("foo % 0.0", "NaN");
    }

    #[test]
    fn test_simplify_simple_and() {
        // (c > 5) AND (c > 5)
        assert_string_simplify("(c > 5) AND (c > 5)", "c > 5.0");
    }

    #[test]
    fn test_simplify_composed_and() {
        let expr = "((c > 5) AND (c1 < 6)) AND (c > 5)";
        let expected = selector("c2").gt(number(5.0)) & selector("c1").lt(number(6.0));

        let actual = simplify(expr);
        assert_expr_eq(&expected, &actual);
    }

    #[test]
    fn test_simplify_or_and() {
        let l = "c2 > 5.0";
        let r = "(c1 < 6.0) AND (c2 > 5.0)";
        let expr = format!("({l}) OR ({r})");

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5)) --> c2 > 5
        assert_string_simplify(&expr, l);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5) --> c2 > 5
        let expr = format!("({r}) OR ({l})");

        assert_string_simplify(&expr, l);
    }

    #[test]
    fn test_simplify_and_or() {
        let l = "c2 > 5.0";
        let r = "(c1 < 6.0) OR (c2 > 5.0)";

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = format!("({l}) AND ({r})");
        assert_string_simplify(&expr, l);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = format!("({r}) AND ({l})");
        assert_string_simplify(&expr, l);
    }

    #[test]
    fn test_simplify_nan_and_false() {
        assert_string_simplify("NaN and 0.0", "NaN");
    }

    #[test]
    fn test_simplify_div_nan_by_nan() {
        assert_string_simplify("NaN / NaN", "NaN");
    }

    #[test]
    fn test_simplify_simplify_arithmetic_expr() {
        assert_string_simplify("1.0 + 1.0", "2.0");
        assert_string_simplify("1.0 == 1.0", "1.0");
    }

    // ------------------------------
    // ----- Simplifier tests -------
    // ------------------------------

    fn try_simplify(expr: Expr) -> ParseResult<Expr> {
        let simplifier = ExprSimplifier::new();
        simplifier.simplify(expr)
    }

    fn simplify(expr: Expr) -> Expr {
        try_simplify(expr).unwrap()
    }

    #[test]
    fn simplify_expr_nan_comparison() {
        let nan = number(f64::NAN);
        let zero = number(0.0);
        let one = number(1.0);

        // scalar == bool NAN is always false
        let actual = simplify(number(1.0).eq(number(f64::NAN)));
        assert_expr_eq(&zero, &actual);

        let expr = parse("NaN == NaN").unwrap();
        let actual = simplify(expr);
        assert_expr_eq(&nan, &actual);

        let expr = parse("NaN == bool NaN").unwrap();
        let actual = simplify(expr);
        assert_expr_eq(&one, &actual);

        // NAN != NAN is always 0
        assert_string_simplify("NaN != NaN", "0.0");

        // scalar != NAN is always 1
        assert_string_simplify("1.0 != NaN", "1.0");
    }

    #[test]
    fn simplify_expr_eq() {
        // true == true -> true
        assert_string_simplify("1.0 == 1.0", "1.0");

        // true == false -> false
        assert_string_simplify("1.0 == 0.0", "0.0");
    }

    #[test]
    fn simplify_expr_eq_skip_non_boolean_type() {
        // don't fold c1 == foo
        let actual = "c1 == foo";
        assert_string_simplify(actual, actual);
    }

    #[test]
    fn simplify_expr_not_eq() {
        // test constant
        assert_string_simplify("1.0 != 1.0", "0.0");
        assert_string_simplify("1.0 != 0.0", "1.0");
    }

    #[test]
    fn simplify_expr_not_eq_skip_non_boolean_type() {
        let actual = "c1 != foo";
        let expected = "c1 != foo";
        assert_string_simplify(expected, actual);
    }

    #[test]
    fn test_simplify_parens() {
        let expr = parse("((foo))").unwrap();
        let expected = parse("foo").unwrap();
        let actual = simplify(expr);
        assert_expr_eq(&expected, &actual);
    }

    // TODO: BinaryExpr
}