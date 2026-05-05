mod const_evaluator;
mod pushdown;
#[cfg(test)]
mod pushdown_tests;
mod simplifier;
mod utils;

pub use simplifier::optimize_expr;