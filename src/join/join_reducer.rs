use crate::common::binop::{
    abs_diff, avg, cmp, compare_eq, compare_gt, compare_gte, compare_lt, compare_lte, compare_neq,
    max, min, op_and, op_default, op_div, op_if, op_if_not, op_minus, op_mod, op_mul, op_or,
    op_plus, op_pow, op_unless, op_xor, pct_change, sgn_diff, BinopFunc,
};
use std::fmt;
use std::str::FromStr;
use valkey_module::ValkeyError;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum JoinReducer {
    AbsDiff,
    And,
    Avg,
    Cmp,
    Default,
    Div,
    #[default]
    Eql,
    Mod,
    Mul,
    Pow,
    Sub,
    Gt,
    Gte,
    If,
    IfNot,
    Lt,
    Lte,
    Max,
    Min,
    NotEq,
    Or,
    PctChange,
    SgnDiff,
    Sum,
    Unless,
    Xor,
}

fn join_reducer_get(key: &str) -> Option<JoinReducer> {
    hashify::tiny_map_ignore_case! {
        key.as_bytes(),
        "abs_diff" => JoinReducer::AbsDiff,
        "cmp" => JoinReducer::Cmp,
        "eq" => JoinReducer::Eql,
        "gt" => JoinReducer::Gt,
        "gte" => JoinReducer::Gte,
        "sub" => JoinReducer::Sub,
        "mod" => JoinReducer::Mod,
        "mul" => JoinReducer::Mul,
        "ne"  => JoinReducer::NotEq,
        "lt" => JoinReducer::Lt,
        "lte" => JoinReducer::Lte,
        "div" => JoinReducer::Div,
        "pow" => JoinReducer::Pow,
        "sgn_diff" => JoinReducer::SgnDiff,
        "pct_change" => JoinReducer::PctChange,

        // logic set ops
        "and" => JoinReducer::And,
        "or" => JoinReducer::Or,
        "unless" => JoinReducer::Unless,

        "if" => JoinReducer::If,
        "ifnot" => JoinReducer::IfNot,
        "default" => JoinReducer::Default,

        "sum" => JoinReducer::Sum,
        "avg" => JoinReducer::Avg,
        "max" => JoinReducer::Max,
        "min" => JoinReducer::Min,
        "xor" => JoinReducer::Xor,
    }
}

impl JoinReducer {
    pub const fn as_str(&self) -> &'static str {
        use JoinReducer::*;
        match self {
            AbsDiff => "abs_diff",
            Sum => "+",
            And => "and",
            Cmp => "cmp",
            Default => "default",
            Div => "/",
            Eql => "==",
            Gt => ">",
            Gte => ">=",
            If => "if",
            IfNot => "ifNot",
            Mod => "%",
            Mul => "*",
            Lt => "<",
            Lte => "<=",
            NotEq => "!=",
            Or => "or",
            Pow => "^",
            SgnDiff => "sgn_diff",
            PctChange => "pct_change",
            Sub => "-",
            Unless => "unless",
            Avg => "avg",
            Max => "max",
            Min => "min",
            Xor => "xor",
        }
    }

    pub const fn get_handler(&self) -> BinopFunc {
        use JoinReducer::*;
        match self {
            Max => max,
            Min => min,
            Avg => avg,
            AbsDiff => abs_diff,
            Sum => op_plus,
            And => op_and,
            Cmp => cmp,
            Default => op_default,
            Div => op_div,
            Eql => compare_eq,
            Mod => op_mod,
            Mul => op_mul,
            Pow => op_pow,
            Sub => op_minus,
            Gt => compare_gt,
            Gte => compare_gte,
            If => op_if,
            IfNot => op_if_not,
            Lt => compare_lt,
            Lte => compare_lte,
            NotEq => compare_neq,
            Or => op_or,
            SgnDiff => sgn_diff,
            Unless => op_unless,
            PctChange => pct_change,
            Xor => op_xor,
        }
    }
}

impl FromStr for JoinReducer {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JoinReducer::try_from(s)
    }
}

impl TryFrom<&str> for JoinReducer {
    type Error = ValkeyError;

    fn try_from(op: &str) -> Result<Self, Self::Error> {
        match join_reducer_get(op) {
            Some(operator) => Ok(operator),
            None => Err(ValkeyError::String(format!(
                "TSDB: unknown binary op \"{op}\""
            ))),
        }
    }
}

impl fmt::Display for JoinReducer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
