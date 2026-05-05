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
use promql_parser::parser::token::TokenType;
use promql_parser::parser::{BinaryExpr, Expr, NumberLiteral};

/// Returns true if `needle` is found in a chain of `search_op`
/// expressions, such as `(A AND B) AND C`
pub(super) fn expr_contains(expr: &Expr, needle: &Expr, search_op: TokenType) -> bool {
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

pub(super) fn is_number_value(s: &Expr, num: f64) -> bool {
    match s {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => {
            val.total_cmp(&num) == std::cmp::Ordering::Equal
        }
        _ => false,
    }
}

pub(super) fn is_zero(s: &Expr) -> bool {
    is_number_value(s, 0.0)
}

pub(super) fn is_one(s: &Expr) -> bool {
    is_number_value(s, 1.0)
}

pub(super) fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => val.is_nan(),
        _ => false,
    }
}

/// returns true if `haystack` looks like `(needle OP X)` or `(X OP needle)`
pub(super) fn is_op_with(target_op: TokenType, haystack: &Expr, needle: &Expr) -> bool {
    matches!(haystack, Expr::Binary(BinaryExpr { lhs, op, rhs, .. })
        if op == &target_op && (needle == lhs.as_ref() || needle == rhs.as_ref()))
}
