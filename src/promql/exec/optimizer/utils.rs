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

//! Utility functions for expression simplification
use logos::Source;
use promql_parser::label::Matchers;
use promql_parser::parser::{BinaryExpr, Expr, NumberLiteral, ParenExpr, VectorSelector};
use promql_parser::parser::token::{Token, TokenType, T_LAND, T_LOR};
use promql_parser::util::walk_expr;


/// Create a selector expression based on a qualified or unqualified column name
///
/// Example:
/// ``` rust
/// use crate::metricsql_parser::ast::utils::*;
/// let c = selector("latency");
/// ```
pub(in crate::promql) fn selector(ident: impl Into<String>) -> Expr {
    Expr::VectorSelector(
        VectorSelector {
            name: Some(ident.into()),
            offset: None,
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            at: None,
        })
}

pub(in crate::promql) fn lit(str: &str) -> Expr {
    Expr::from(str)
}

pub(in crate::promql) fn number(val: f64) -> Expr {
    Expr::from(val)
}

/// Returns true if `needle` is found in a chain of `search_op`
/// expressions, such as `(A AND B) AND C`
pub(in crate::promql) fn expr_contains(expr: &Expr, needle: &Expr, search_op: TokenType) -> bool {
    match expr {
        Expr::Binary(BinaryExpr { lhs, op, rhs, .. }) if *op == search_op => {
            expr_contains(lhs, needle, search_op) || expr_contains(rhs, needle, search_op)
        }
        _ => expr == needle,
    }
}

pub(super) fn is_number(s: &Expr) -> bool {
    matches!(s, Expr::NumberLiteral(_))
}

pub(in crate::promql) fn is_number_value(s: &Expr, num: f64) -> bool {
    match s {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => val.total_cmp(&num) == std::cmp::Ordering::Equal,
        _ => false,
    }
}

pub(in crate::promql) fn is_zero(s: &Expr) -> bool {
    is_number_value(s, 0.0)
}

pub(in crate::promql) fn is_one(s: &Expr) -> bool {
    is_number_value(s, 1.0)
}

pub(in crate::promql) fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => val.is_nan(),
        _ => false,
    }
}


pub(in crate::promql) fn binop(lhs: Expr, op: TokenType, rhs: Expr) -> Expr {
    Expr::Binary(BinaryExpr {
        lhs: Box::new(lhs),
        op,
        rhs: Box::new(rhs),
        modifier: None,
    })
}

pub(in crate::promql) fn join_expr_and(lhs: Expr, rhs: Expr) -> Expr {
    binop(lhs, TokenType::new(T_LAND), rhs)
}

pub(in crate::promql) fn join_expr_or(lhs: Expr, rhs: Expr) -> Expr {
    binop(lhs, TokenType::new(T_LOR), rhs)
}

pub(in crate::promql) fn paren(expr: Expr) -> Expr {
    Expr::Paren(ParenExpr {
        expr: Box::new(expr),
    })
}

/// returns true if `haystack` looks like `(needle OP X)` or `(X OP needle)`
pub(in crate::promql) fn is_op_with(target_op: TokenType, haystack: &Expr, needle: &Expr) -> bool {
    matches!(haystack, Expr::Binary(BinaryExpr { lhs, op, rhs, .. })
        if op == &target_op && (needle == lhs.as_ref() || needle == rhs.as_ref()))
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical AND.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ``` rust
/// use crate::metricsql_parser::ast::utils::{selector, number, conjunction};
/// // a=1 AND b=2
/// let expr = selector("a").eq(number(1.0)).and(selector("b").eq(number(2.0)));
///
/// // [a=1, b=2]
/// let split = vec![
///   selector("a").eq(number(1.0)),
///   selector("b").eq(number(2.0)),
/// ];
///
/// // use conjunction to join them together with `AND`
/// assert_eq!(conjunction(split), Some(expr));
/// ```
pub(in crate::promql) fn conjunction(filters: impl IntoIterator<Item=Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| join_expr_and(accum, expr))
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical `OR`.
///
/// Returns None if the filters array is empty.
pub(in crate::promql) fn disjunction(filters: impl IntoIterator<Item=Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| join_expr_or(accum, expr))
}

// all this nonsense because f64 used in NumberExpr doesn't implement Eq
pub(in crate::promql) fn expr_equals(expr1: &Expr, expr2: &Expr) -> bool {
    use Expr::*;

    fn compare_parens(parens: &ParenExpr, expr: &Expr) -> bool {
        if let Some(other) = parens.innermost_expr() {
            return expr == other;
        }
        match expr {
            Paren(p) => parens == p,
            _ => false,
        }
    }

    match (expr1, expr2) {
        (Paren(p1), Paren(p2)) => {
            // println!("p1: {:?}, p2: {:?}", p1, p2);
            p1 == p2
        }
        // special case: (x) == x. I don't know if I like this
        (Paren(p), e) => compare_parens(p, e),
        (e, Paren(p)) => compare_parens(p, e),
        (a, b) => a == b,
    }
}

pub(super) fn string_vecs_equal_unordered(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let hash_a: ASmallSet<8, _> = a.iter().collect();
    b.iter().all(|x| hash_a.contains(x))
}


#[cfg(test)]
pub mod tests {
}