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
use promql_parser::parser::token::{T_LAND, TokenType};
use promql_parser::parser::{BinModifier, BinaryExpr, Expr, NumberLiteral, VectorMatchFillValues};

/// `expr` without the parentheses around it.
pub(super) fn unparen(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(p) => unparen(&p.expr),
        other => other,
    }
}

/// Whether a binary operation's `modifier` changes how its operands match: `on`, `ignoring`
/// or `fill`. A set operator without one matches series on all their labels but `__name__`.
pub(super) fn has_matching(modifier: &Option<BinModifier>) -> bool {
    modifier
        .as_ref()
        .is_some_and(|m| m.matching.is_some() || m.fill_values != VectorMatchFillValues::default())
}

/// `expr`'s operands, if it is (through parentheses) the binary operation `op`, and whether it
/// matches on all labels ([`has_matching`] does not hold for it).
pub(super) fn operands(expr: &Expr, op: TokenType) -> Option<(&Expr, &Expr, bool)> {
    match unparen(expr) {
        Expr::Binary(BinaryExpr {
            lhs,
            rhs,
            op: o,
            modifier,
        }) if *o == op => Some((unparen(lhs), unparen(rhs), !has_matching(modifier))),
        _ => None,
    }
}

/// Whether `expr` is `needle`, or a chain of `op` with no matching modifier anywhere in it
/// that has `needle` as an operand.
///
/// For a set operator that is what makes `needle` cover the chain: every series an
/// unmodified `and` chain returns has its labels in each operand, and an unmodified `or`
/// chain returns a series with the labels of every one of each operand's.
pub(super) fn plain_chain_contains(expr: &Expr, needle: &Expr, op: TokenType) -> bool {
    match operands(expr, op) {
        Some((lhs, rhs, true)) => {
            plain_chain_contains(lhs, needle, op) || plain_chain_contains(rhs, needle, op)
        }
        _ => unparen(expr) == unparen(needle),
    }
}

/// The operand an `and` chain takes its series from: its leftmost one, whatever the chain's
/// modifiers. `and` only ever drops series of its left operand, so every series the chain
/// returns is one of this one's, with its value.
pub(super) fn and_chain_source(expr: &Expr) -> &Expr {
    match operands(expr, TokenType::new(T_LAND)) {
        Some((lhs, _, _)) => and_chain_source(lhs),
        None => unparen(expr),
    }
}

pub(super) fn is_number(s: &Expr) -> bool {
    matches!(s, Expr::NumberLiteral(_))
}

pub(super) fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val, .. }) => val.is_nan(),
        _ => false,
    }
}
