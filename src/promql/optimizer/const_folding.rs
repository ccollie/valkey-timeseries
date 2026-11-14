use crate::promql::binops::apply_binary_op;
use crate::promql::functions::{
    DateTimePart, PromqlFunctionKind, datetime_from_seconds, unary_math_kernel,
};
use crate::promql::utils::map_children;
use num_traits::FloatConst;
use promql_parser::parser::token::TokenType;
use promql_parser::parser::{BinaryExpr, Call, Expr, UnaryExpr};

/// `expr` with every constant subexpression folded, children first.
pub(super) fn fold_constants(expr: Expr) -> Expr {
    match map_children(expr, fold_constants) {
        Expr::Unary(unary) => match *unary.expr {
            Expr::NumberLiteral(n) => Expr::from(-n.val),
            operand => Expr::Unary(UnaryExpr {
                expr: Box::new(operand),
            }),
        },
        Expr::Binary(binary) => handle_binary_expr(binary),
        Expr::Call(call) => handle_function_expr(call),
        Expr::Paren(paren) if matches!(*paren.expr, Expr::NumberLiteral(_)) => *paren.expr,
        expr => expr,
    }
}

fn handle_binary_expr(be: BinaryExpr) -> Expr {
    let is_bool = be.return_bool();

    match (be.lhs.as_ref(), be.rhs.as_ref(), be.op) {
        (Expr::NumberLiteral(ln), Expr::NumberLiteral(rn), op) => {
            handle_number_number(ln.val, rn.val, op, is_bool)
        }
        _ => Expr::Binary(be),
    }
}

fn handle_number_number(ln: f64, rn: f64, op: TokenType, is_bool: bool) -> Expr {
    // properly constructed expressions (from the parser) should not panic
    let n = apply_binary_op(op, ln, rn).expect("binary operation failed");
    if is_bool {
        return Expr::from(if n.is_nan() || n == 0.0 { 0.0 } else { 1.0 });
    }
    Expr::from(n)
}

fn get_single_scalar_arg(fe: &Call) -> Option<f64> {
    match fe.args.args.as_slice() {
        [arg] => match &**arg {
            Expr::NumberLiteral(n) => Some(n.val),
            _ => None,
        },
        _ => None,
    }
}

fn simplify_math_fn(fe: &Call, func: PromqlFunctionKind) -> Option<f64> {
    use PromqlFunctionKind::*;
    let arg = get_single_scalar_arg(fe)?;
    // The kernel the function itself evaluates with: `sgn(-0)` is 0, not -1,
    // and `cosh(710)` overflows to +Inf as Go's does.
    if let Some(kernel) = unary_math_kernel(func) {
        return Some(kernel(arg));
    }
    match func {
        DayOfMonth => extract_datetime_part(arg, DateTimePart::DayOfMonth),
        DayOfWeek => extract_datetime_part(arg, DateTimePart::DayOfWeek),
        DayOfYear => extract_datetime_part(arg, DateTimePart::DayOfYear),
        DaysInMonth => extract_datetime_part(arg, DateTimePart::DaysInMonth),
        Hour => extract_datetime_part(arg, DateTimePart::Hour),
        Minute => extract_datetime_part(arg, DateTimePart::Minute),
        Month => extract_datetime_part(arg, DateTimePart::Month),
        Year => extract_datetime_part(arg, DateTimePart::Year),
        _ => None,
    }
}

/// `expr` with every `vector(e)` replaced by `e`, through the arithmetic that
/// keeps a one-sample, label-less vector one: for the argument of `scalar(...)`
/// only, where the two read the same. A filtering comparison (no `bool`) or a
/// matching modifier stops the rewrite, since there the vector and the scalar
/// behave differently.
fn unwrap_vectors(expr: Expr) -> Expr {
    match expr {
        Expr::Call(mut fe) if fe.func.name == "vector" && fe.args.len() == 1 => {
            unwrap_vectors(*fe.args.args.remove(0))
        }
        // `abs(vector(-1))` reads as `abs(-1)` here: a math or date function
        // over a number folds to its value.
        Expr::Call(mut fe) => {
            fe.args.args = fe
                .args
                .args
                .into_iter()
                .map(|arg| Box::new(fold_constants(unwrap_vectors(*arg))))
                .collect();
            match PromqlFunctionKind::try_from(fe.func.name)
                .ok()
                .and_then(|kind| simplify_math_fn(&fe, kind))
            {
                Some(value) => Expr::from(value),
                None => Expr::Call(fe),
            }
        }
        Expr::Binary(mut be)
            if (!be.op.is_comparison_operator() || be.return_bool())
                && be.modifier.as_ref().is_none_or(|m| m.matching.is_none()) =>
        {
            be.lhs = Box::new(unwrap_vectors(*be.lhs));
            be.rhs = Box::new(unwrap_vectors(*be.rhs));
            Expr::Binary(be)
        }
        Expr::Paren(mut p) => {
            p.expr = Box::new(unwrap_vectors(*p.expr));
            Expr::Paren(p)
        }
        Expr::Unary(mut u) => {
            u.expr = Box::new(unwrap_vectors(*u.expr));
            Expr::Unary(u)
        }
        other => other,
    }
}

/// A call whose arguments are folded already. Unwrapping `vector(...)` is
/// `unwrap_vectors`' job, and happens only under `scalar(...)` below.
fn handle_function_expr(fe: Call) -> Expr {
    use PromqlFunctionKind::*;
    let arg_count = fe.args.len();
    let Ok(kind) = PromqlFunctionKind::try_from(fe.func.name) else {
        return Expr::Call(fe);
    };
    match kind {
        Scalar if arg_count == 1 => {
            // Inside `scalar(...)` a one-sample, label-less `vector(e)` and `e`
            // are interchangeable, so the argument folds with its `vector`
            // calls unwrapped: `scalar(9 + vector(4))` -> `13`. Only here:
            // anywhere else `vector(e)` is an instant vector, and unwrapping it
            // would retype `sum(vector(1))` or `-vector(1)`.
            let candidate = fold_constants(unwrap_vectors((*fe.args.args[0]).clone()));
            if let Expr::NumberLiteral(_) = candidate {
                candidate
            } else {
                Expr::Call(fe)
            }
        }
        Pi if arg_count == 0 => Expr::from(f64::PI()),
        // A math or date function takes an instant vector and returns one, so
        // folding it to a number is sound only inside `scalar(...)`, where
        // `unwrap_vectors` does it.
        _ => Expr::Call(fe),
    }
}

pub(crate) fn extract_datetime_part(epoch_secs: f64, part: DateTimePart) -> Option<f64> {
    datetime_from_seconds(epoch_secs).map(|dt| part.extract(dt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::number::parse_number;
    use promql_parser::parser::parse;

    // ------------------------------
    // --- ConstEvaluator tests -----
    // ------------------------------

    fn test_simplify(input_expr: &str, expected_expr: &str) {
        let input = parse(input_expr).expect("parse failed");
        let expected = parse(expected_expr).expect("parse failed");
        let simplified = fold_constants(input);
        assert_eq!(
            &simplified,
            &expected,
            "Mismatch simplifying {input_expr}\n  Expected:{expected_expr}\n  Got:{}",
            simplified.prettify()
        );
    }

    /// Constants fold wherever they sit: inside an aggregation's operand and
    /// under a unary minus, not only at the top of a binary expression. A
    /// vector-typed operand is never folded away (`x * NaN` stays).
    #[test]
    fn folds_inside_aggregations_and_under_unary_minus() {
        test_simplify("sum(x * (1 + 2))", "sum(x * 3)");
        test_simplify(
            "sum by (job) (rate(m[5m]) / (2 * 30))",
            "sum by (job) (rate(m[5m]) / 60)",
        );
        test_simplify("sum(x * NaN)", "sum(x * NaN)");

        let Expr::NumberLiteral(n) = fold_constants(parse("-(1 + 2)").unwrap()) else {
            panic!("-(1 + 2) did not fold to a number");
        };
        assert_eq!(n.val, -3.0);
        let Expr::NumberLiteral(n) = fold_constants(parse("-(-(2 * 2))").unwrap()) else {
            panic!("-(-(2 * 2)) did not fold to a number");
        };
        assert_eq!(n.val, 4.0);
    }

    #[test]
    fn test_const_evaluator() {
        // true --> true
        test_simplify("1.0", "1.0");
        // c == 1 --> c == 1
        test_simplify("c == 1.0", "c == 1.0");
        // c = 1 + 2 --> c + 3
        test_simplify("c == 1.0 + 2.0", "c == 3.0");
    }

    fn test_math_fn(name: &str, arg: f64, expected: f64) {
        let expr = format!("scalar({}(vector({})))", name, arg);
        let simplified = fold_constants(parse(&expr).expect("parse failed")).prettify();
        let simplified_value = parse_number(&simplified).unwrap();
        if expected.is_nan() {
            assert!(
                simplified_value.is_nan(),
                "Expected NaN for {expr}, but got {simplified_value}"
            );
        } else {
            assert_eq!(
                expected, simplified_value,
                "Mismatch evaluating {expr}\n  Expected:{expected}\n  Got:{simplified_value}"
            );
        }
    }

    #[test]
    fn test_const_evaluator_math_function() {
        test_math_fn("abs", -1.0, 1.0);
        test_math_fn("abs", 1.0, 1.0);

        test_math_fn("acos", 2.0, 2_f64.acos());

        // acosh
        test_math_fn("acosh", 2.0, 2_f64.acosh());

        // asin
        test_math_fn("asin", 1.0, 1_f64.asin());

        // asinh
        test_math_fn("asinh", 1.0, 1_f64.asinh());

        test_math_fn("atan", 1.0, 1_f64.atan());

        // atanh
        test_math_fn("atanh", 0.5, 0.5_f64.atanh());

        // ceil
        test_math_fn("ceil", 0.0, 0.0);
        test_math_fn("ceil", 1.1, 2.0);

        // cos
        test_math_fn("cos", 0.5, 0.5_f64.cos());

        // cosh
        test_math_fn("cosh", 1.0, 1_f64.cosh());

        // deg
        test_math_fn("deg", std::f64::consts::FRAC_PI_2, 90.0);

        // exp
        test_math_fn("exp", 1.0, 1_f64.exp());

        // floor
        test_math_fn("floor", 0.0, 0.0);
        test_math_fn("floor", 1.1, 1.0);

        // ln
        test_math_fn("ln", 1.0, 0.0);
        test_math_fn("ln", std::f64::consts::E, 1.0);

        // log10
        test_math_fn("log10", 1.0, 0.0);
        test_math_fn("log10", 10.0, 1.0);

        // log2
        test_math_fn("log2", 1.0, 0.0);

        // rad
        test_math_fn("rad", 90.0, std::f64::consts::FRAC_PI_2);

        // sgn
        test_math_fn("sgn", -4.5, -1.0);

        // sin
        test_math_fn("sin", std::f64::consts::FRAC_PI_2, 1.0);

        // sinh
        test_math_fn("sinh", 1.0, 1_f64.sinh());

        // sqrt
        test_math_fn("sqrt", 4.0, 2.0);

        // tan
        test_math_fn("tan", 0.75, 0.75_f64.tan());

        // tanh
        test_math_fn("tanh", 1.0, 1_f64.tanh());
    }

    /// A folded math function gives what evaluating it gives, bit for bit —
    /// signed zeros and overflow included, where Rust's own `signum`/`cosh`
    /// differ from PromQL's.
    #[test]
    fn folding_a_math_function_matches_evaluating_it() {
        let folded = |q: &str| match fold_constants(parse(q).expect("parse failed")) {
            Expr::NumberLiteral(n) => n.val,
            other => panic!("{q} did not fold: {}", other.prettify()),
        };
        // (query, what the function returns unoptimized)
        for (q, expected) in [
            ("scalar(sgn(vector(0)))", 0.0_f64),
            ("scalar(sgn(vector(-0)))", 0.0),
            ("scalar(sgn(vector(-3)))", -1.0),
            ("scalar(cosh(vector(710)))", f64::INFINITY),
            ("scalar(sinh(vector(-710)))", f64::NEG_INFINITY),
        ] {
            assert_eq!(folded(q).to_bits(), expected.to_bits(), "{q}");
        }
    }

    fn test_date_part_fn(name: &str, epoch_secs: f64, part: DateTimePart) {
        let expr = format!("scalar({name}(vector({epoch_secs})))");
        let simplified = fold_constants(parse(&expr).expect("parse failed")).prettify();
        let expected = extract_datetime_part(epoch_secs, part).unwrap();
        let actual = parse_number(&simplified).unwrap();
        assert_eq!(
            expected, actual,
            "Mismatch evaluating {expr}\n  Expected:{expected}\n  Got:{actual}"
        );
    }

    #[test]
    fn test_const_evaluator_date_parts() {
        // A fixed instant, so a failure reproduces: 2023-11-14 22:13:20 UTC.
        let epoch = 1_700_000_000.0;

        test_date_part_fn("day_of_month", epoch, DateTimePart::DayOfMonth);
        test_date_part_fn("days_in_month", epoch, DateTimePart::DaysInMonth);
        test_date_part_fn("day_of_week", epoch, DateTimePart::DayOfWeek);
        test_date_part_fn("day_of_year", epoch, DateTimePart::DayOfYear);

        test_date_part_fn("hour", epoch, DateTimePart::Hour);
        test_date_part_fn("minute", epoch, DateTimePart::Minute);
        test_date_part_fn("month", epoch, DateTimePart::Month);

        test_date_part_fn("year", epoch, DateTimePart::Year);
    }

    #[test]
    fn test_const_evaluator_date_parts_not_a_time() {
        // NaN and ±Inf are not a point in time: the call must stay unfolded so
        // evaluation returns NaN, not a calendar value for some fallback instant.
        for arg in ["NaN", "Inf", "-Inf"] {
            let expr = format!("scalar(year(vector({arg})))");
            let simplified = fold_constants(parse(&expr).expect("parse failed")).prettify();
            assert!(
                simplified.starts_with("scalar(year("),
                "{expr} folded to {simplified}"
            );
        }
    }

    #[test]
    fn test_scalar_vector() {
        struct TestCase {
            expr: &'static str,
            expected: &'static str,
        }

        let tests = vec![
            TestCase {
                expr: "(9+scalar(vector(-10)))",
                expected: "-1.0",
            },
            TestCase {
                expr: "scalar(9+vector(4)) / 2",
                expected: "6.5",
            },
            TestCase {
                expr: r#"scalar(
                scalar(
                    scalar(
                        vector( 20 - 4 ) * 0.5 - 2
                    ) - vector( 2 )
                ) + vector(2)
            ) * 9"#,
                expected: "54.0",
            },
            TestCase {
                expr: "5 - scalar(
                scalar(
                    scalar(
                        vector( 20 - 4 ) * vector(0.5) - vector(2)
                    ) - vector( 2 )
                ) + vector(2)
            )",
                expected: "-1.0",
            },
            TestCase {
                expr: "scalar(vector(1) + vector(2))",
                expected: "3.0",
            },
            TestCase {
                expr: "scalar(vector(1) + scalar(vector(1) + vector(2)))",
                expected: "4.0",
            },
            TestCase {
                expr: "scalar(vector(1) + scalar(vector(1) + scalar(vector(1) + vector(2))))",
                expected: "5.0",
            },
            TestCase {
                expr: "(scalar(9+vector(4)) * 4 - 9+scalar(vector(3)))",
                expected: "46.0",
            },
            TestCase {
                expr: "scalar(1 +vector(2 != bool 1))",
                expected: "2.0",
            },
            TestCase {
                expr: "scalar(1 +vector(1 != bool 1))",
                expected: "1.0",
            },
            TestCase {
                expr: "1 >= bool 1",
                expected: "1.0",
            },
            TestCase {
                expr: "1 >= bool 2",
                expected: "0.0",
            },
        ];

        for tt in tests {
            test_simplify(tt.expr, tt.expected);
        }
    }
}
