use crate::parser::number::parse_number;
use crate::promql::functions::{datetime_from_seconds, DateTimePart, PromqlFunctionKind};
use num_traits::FloatConst;
use promql_parser::parser::token::{TokenType, T_ADD, T_EQLC, T_NEQ, T_GT, T_LT, T_LTE, T_GTE};
use promql_parser::parser::{AggregateExpr, BinaryExpr, Call, Expr, SubqueryExpr, UnaryExpr};
use crate::promql::binops::apply_binary_op;

/// Can the expression be evaluated at plan time, (assuming all of
/// its children can also be evaluated)?
pub(super) fn can_evaluate(expr: &Expr) -> bool {
    // check for reasons we can't evaluate this node
    //
    // NOTE all expr types are listed here, so when new ones are
    //  added, they can be checked for their ability to be evaluated
    // at plan time
    match expr {
        Expr::Aggregate(_) => false,
        Expr::Paren(_) => true,
        // only handle immutable scalar functions
        Expr::Call(_) => true,
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Unary(_)
        | Expr::Binary(_) => true,
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => false,
        &Expr::Subquery(_) | &Expr::Extension(_) => todo!(),
    }
}

pub fn const_simplify(expr: Expr) -> Expr {
    match expr {
        Expr::Unary(uo) => handle_unary_expr(uo),
        Expr::Binary(_) => handle_binop_internal(expr),
        Expr::Call(fe) => handle_function_expr(fe),
        Expr::Aggregate(ae) => handle_aggregation_expr(ae),
        Expr::Subquery(s) => handle_rollup_expr(s.expr),
        Expr::Paren(p) => {
            let expr = const_simplify(&p.expr);
            Expr::Paren(expr)
        }
        _ => expr,
    }
}


fn handle_unary_expr(ue: UnaryExpr) -> Expr {
    let mut ue = ue;
    match ue.expr.as_mut() {
        Expr::NumberLiteral(n) => {
            return Expr::from(-n.val);
        }
        Expr::Unary(u2) => {
            return std::mem::take(&mut u2.expr);
        }
        _ => {}
    }
    Expr::Unary(u)
}

fn handle_binop_internal(be: Expr) -> Expr {
    if let Expr::Binary(BinaryExpr {
                            lhs,
                            rhs,
                            op,
                            modifier
                        }) = be {
        let left = if let Expr::Binary(_) = *lhs {
            handle_binop_internal(*lhs)
        } else {
            const_simplify(*lhs)
        };
        let right = if let Expr::Binary(_) = *rhs {
            handle_binop_internal(*rhs)
        } else {
            const_simplify(*rhs)
        };
        let new_be = BinaryExpr {
            lhs: Box::new(left),
            rhs: Box::new(right),
            op,
            modifier,
        };
        return handle_binary_expr(new_be);
    }
    be
}

fn handle_binary_expr(be: BinaryExpr) -> Expr {
    let is_bool = be.return_bool();

    match (be.lhs.as_ref(), be.rhs.as_ref(), be.op) {
        (Expr::NumberLiteral(ln), Expr::NumberLiteral(rn), op) => {
            handle_number_number(ln.val, rn.val, op, is_bool)
        }
        (Expr::StringLiteral(left), Expr::StringLiteral(right), op) => {
            handle_string_string(left, right, op, is_bool)
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

fn handle_string_string(left: &str, right: &str, op: TokenType, is_bool: bool) -> Expr {
    if op.id() == T_ADD {
        if left.is_empty() {
            return Expr::from(right);
        } else if right.is_empty() {
            return Expr::from(left);
        }
        let mut res = String::with_capacity(left.len() + right.len());
        res += left;
        res += right;
        Expr::from(res)
    } else if op.is_comparison_operator() {
        let n = string_compare(left, right, op, is_bool).expect("string compare failed");
        Expr::from(n)
    } else {
        Expr::Binary(BinaryExpr {
            lhs: Box::new(Expr::from(left)),
            rhs: Box::new(Expr::from(right)),
            op,
            modifier: None,
        })
    }
}

pub fn string_compare(a: &str, b: &str, op: TokenType, is_bool: bool) -> ParseResult<f64> {
    let res = match op {
        T_EQLC => a == b,
        T_NEQ => a != b,
        T_LT => a < b,
        T_GT => a > b,
        T_LTE => a <= b,
        T_GTE => a >= b,
        _ => {
            return Err(ParseError::Unsupported(format!(
                "unexpected operator {op} in string comparison"
            )))
        }
    };
    Ok(if res {
        1_f64
    } else if is_bool {
        0_f64
    } else {
        f64::NAN
    })
}

fn get_single_scalar_arg(fe: &Call) -> Option<f64> {
    if fe.args.len() == 1 {
        if let Expr::NumberLiteral(val) = &*fe.args.args[0] {
            return Some(val.val);
        }
    }
    None
}

fn simplify_math_fn(fe: &Call, func: PromqlFunctionKind) -> Option<f64> {
    use PromqlFunctionKind::*;
    let arg_count = fe.args.len();
    if func == Pi && arg_count == 0 {
        return Some(f64::PI());
    }
    let arg = get_single_scalar_arg(fe)?;
    match func {
        Abs => Some(arg.abs()),
        Acos => Some(arg.acos()),
        Acosh => Some(arg.acosh()),
        Asin => Some(arg.asin()),
        Asinh => Some(arg.asinh()),
        Atan => Some(arg.atan()),
        Atanh => Some(arg.atanh()),
        Ceil => Some(arg.ceil()),
        Cos => Some(arg.cos()),
        Cosh => Some(arg.cosh()),
        DayOfMonth => extract_datetime_part(arg, DateTimePart::DayOfMonth),
        DayOfWeek => extract_datetime_part(arg, DateTimePart::DayOfWeek),
        DayOfYear => extract_datetime_part(arg, DateTimePart::DayOfYear),
        DaysInMonth => extract_datetime_part(arg, DateTimePart::DaysInMonth),
        Deg => Some(arg.to_degrees()),
        Exp => Some(arg.exp()),
        Floor => Some(arg.floor()),
        Hour => extract_datetime_part(arg, DateTimePart::Hour),
        Ln => Some(arg.ln()),
        Log2 => Some(arg.log2()),
        Log10 => Some(arg.log10()),
        Minute => extract_datetime_part(arg, DateTimePart::Minute),
        Month => extract_datetime_part(arg, DateTimePart::Month),
        Rad => Some(arg.to_radians()),
        Sgn => Some(arg.signum()),
        Sin => Some(arg.sin()),
        Sinh => Some(arg.sinh()),
        Sqrt => Some(arg.sqrt()),
        Tan => Some(arg.tan()),
        Tanh => Some(arg.tanh()),
        Year => extract_datetime_part(arg, DateTimePart::Year),
        _ => None,
    }
}

fn handle_function_expr(fe: Call) -> Expr {
    use PromqlFunctionKind::*;
    let arg_count = fe.args.len();
    let Ok(kind) = PromqlFunctionKind::try_from(fe.func.name) else {
        return Expr::Call(fe);
    };
    match kind {
        Scalar if arg_count == 1 => {
            let arg = &*fe.args.args[0];
            match *arg {
                // Verify whether the arg is a string.
                // Then try converting the string to number.
                Expr::StringLiteral(s) => {
                    let n = parse_number(&s.val).unwrap_or(f64::NAN);
                    Expr::from(n)
                }
                Expr::NumberLiteral(n) => Expr::from(n.val),
                _ => {
                    let expr = const_simplify(*arg);
                    // `Scalar(q)` returns q if q contains only a single time series. Otherwise, it returns nothing.
                    // It's challenging to determine if a time series is a single time series from a vector selector.
                    match expr {
                        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => expr,
                        Expr::Binary(_) => handle_binop_internal(expr),
                        Expr::Call(fe1) => handle_function_expr(fe1),
                        _ => Expr::Call(fe),
                    }
                }
            }
        }
        Vector if arg_count == 1 => {
            let mut fe = fe;
            // ????
            fe.args.args.remove(0)
        }
        Pi if arg_count == 0 => Expr::from(f64::PI()),
        _ => {
            if let Some(value) = simplify_math_fn(&fe, kind) {
                Expr::from(value)
            } else {
                Expr::Call(fe)
            }
        }
    }
}

pub(crate) fn extract_datetime_part(epoch_secs: f64, part: DateTimePart) -> Option<f64> {
    datetime_from_seconds(epoch_secs)
        .map(|dt| part.extract(dt))
}

fn handle_aggregation_expr(ae: AggregateExpr) -> Expr {
    if let Some(param) = ae.param {
        let arg = const_simplify(*param);
        let ae = AggregateExpr { param: Some(Box::new(arg)), ..ae };
        return Expr::Aggregate(ae);
    };
    Expr::Aggregate(ae)
}

fn handle_rollup_expr(re: SubqueryExpr) -> Expr {
    let expr = const_simplify(*re.expr);
    let new_expr = SubqueryExpr {
        expr: Box::new(expr),
        ..re
    };
    Expr::Subquery(new_expr)
}

fn handle_expr_vecs(args: Vec<Expr>) -> Vec<Expr> {
    args.into_iter().map(const_simplify).collect::<Vec<Expr>>()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use promql_parser::parser::{NumberLiteral, StringLiteral, parse};

    use super::*;

    // ------------------------------
    // --- ConstEvaluator tests -----
    // ------------------------------
    fn test_const_simplify(input_expr: Expr, expected_expr: Expr) {
        let evaluated_expr = const_simplify(input_expr.clone());
        assert_eq!(&evaluated_expr, &expected_expr,
            "Mismatch evaluating {input_expr}\n  Expected:{expected_expr}\n  Got:{evaluated_expr}"
        );
    }

    fn test_simplify(input_expr: &str, expected_expr: &str) {
        let input = parse(input_expr).expect("parse failed");
        let expected = parse(expected_expr).expect("parse failed");
        let simplified = const_simplify(input);
        assert_eq!(&simplified, &expected,
                   "Mismatch simplifying {input_expr}\n  Expected:{expected_expr}\n  Got:{simplified_expr}"
        );
    }

    fn set_bool_modifier(expr: Expr) -> Expr {
        match expr {
            Expr::Binary(be) => Expr::Binary(be.return_bool()),
            _ => expr,
        }
    }

    fn remove_bool_modifier(expr: Expr) -> Expr {
        match expr {
            Expr::Binary(mut be) => {
                be.modifier = None;
                Expr::Binary(be)
            }
            _ => expr,
        }
    }

    fn number(v: f64) -> Expr {
        Expr::NumberLiteral(NumberLiteral { val: v })
    }

    fn lit(s: &str) -> Expr {
        Expr::StringLiteral(StringLiteral { val: s.to_string() })
    }

    #[test]
    fn test_const_evaluator() {
        // true --> true
        test_simplify("1.0", "1.0");
        // true or true --> true
        test_simplify("1.0 or 1.0", "1.0");
        // true or false --> true
        test_simplify("1.0 or 0.0", "1.0");

        // c == 1 --> c == 1
        test_simplify("c == 1.0", "c == 1.0");
        // c = 1 + 2 --> c + 3
        test_simplify("c = 1.0 + 2.0", "c = 3.0");
        // (foo != foo) OR (c == 1) --> false OR (c == 1)
        test_simplify(
            r#"( "foo" != bool "foo" ) OR ( c == 1.0 )"#,
            "false OR ( c == 1.0 )",
        );
    }

    #[test]
    fn test_const_evaluator_strings() {
        // "foo" + "bar" --> "foobar"
        test_simplify(r#""foo" + "bar""#, "foobar");

        // "foo" == bool "foo" --> 1.0
        test_simplify(r#""foo" == bool "foo""#, "1.0");

        // "foo" != bool "foo" --> 0.0
        test_simplify(r#""foo" != bool "foo""#, "0.0");

        // "foo" != "foo" --> NAN
        test_simplify(r#""foo" != "foo""#, "NAN"); // wrong

        // "foo" != bool "bar" --> 1.0
        test_simplify(r#""foo" != bool "bar""#, "1.0");

        // "foo" > bool "bar" --> 1.0
        test_simplify(r#""foo" > bool "bar""#, "1.0");

        // "foo" < bool "bar" --> 0.0
        test_simplify(r#""foo" < bool "bar""#, "0.0");

        // "foo" < "bar" --> NAN
        test_simplify(r#""foo" < "bar""#, "NAN"); // wrong

        // "foo" >= bool "foo" --> 1.0
        test_simplify(r#""foo" >= bool "foo""#, "1.0");

        // "foo_99" >= bool "foo" --> 1.0
        test_simplify(r#""foo_99" >= "foo""#, "1.0");

        // "foo" <= bool "foo1" --> 1.0
        test_simplify(r#""foo" <= bool "foo1""#, "1.0");

        // "foo" <= bool "foo" --> 1.0
        test_simplify(r#""foo" <= bool "foo""#, "1.0");
    }

    #[test]
    fn test_const_evaluator_scalar_functions() {
        let rand = Expr::call("rand", vec![]).expect("invalid function call");
        let expr = rand.clone() + (Expr::from(1.0) + Expr::from(2.0));
        let expected = rand + Expr::from(3.0);
        test_const_simplify(expr, expected);

        // parenthesization matters: can't rewrite
        // (rand() + 1) + 2 --> (rand() + 1) + 2)
        let rand = Expr::call("rand", vec![]).expect("invalid function call");
        let expr = (rand + Expr::from(1.0)) + Expr::from(2.0);
        test_const_simplify(expr.clone(), expr);
    }

    fn test_math_fn(name: &str, arg: f64, expected: f64) {
        let expr = Expr::call(name, vec![Expr::from(arg)]).expect("invalid function call");
        test_const_simplify(expr, number(expected));
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

    fn test_date_part_fn(name: &str, epoch_secs: f64, part: DateTimePart) {
        let expr = Expr::call(name, vec![number(epoch_secs)]).expect("invalid function call");
        let value = extract_datetime_part(epoch_secs, part);
        test_const_simplify(expr, number(value));
    }

    #[test]
    fn test_const_evaluator_date_parts() {
        let now = Utc::now();
        let epoch = now.timestamp() as f64;

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
    fn test_scalar_vector() {
        struct TestCase {
            expr: &'static str,
            expected: f64,
        }

        let tests = vec![
            TestCase {
                expr: "(9+scalar(vector(-10)))",
                expected: -1.0,
            },
            TestCase {
                expr: r#"(scalar("12.90"))"#,
                expected: 12.90,
            },
            TestCase {
                expr: "scalar(9+vector(4)) / 2",
                expected: 6.5,
            },
            TestCase {
                expr: r#"scalar(
                scalar(
                    scalar(
                        vector( 20 - 4 ) * 0.5 - 2
                    ) - vector( 2 )
                ) + vector(2)
            ) * 9"#,
                expected: 54.0,
            },
            TestCase {
                expr: "5 - scalar(
                scalar(
                    scalar(
                        vector( 20 - 4 ) * vector(0.5) - vector(2)
                    ) - vector( 2 )
                ) + vector(2)
            )",
                expected: -1.0,
            },
            TestCase {
                expr: "scalar(vector(1) + vector(2))",
                expected: 3.0,
            },
            TestCase {
                expr: "scalar(vector(1) + scalar(vector(1) + vector(2)))",
                expected: 4.0,
            },
            TestCase {
                expr: "scalar(vector(1) + scalar(vector(1) + scalar(vector(1) + vector(2))))",
                expected: 5.0,
            },
            TestCase {
                expr: "(scalar(9+vector(4)) * 4 - 9+scalar(vector(3)))",
                expected: 46.0,
            },
            TestCase {
                expr: "scalar(1 +vector(2 != bool 1))",
                expected: 2.0,
            },
            TestCase {
                expr: "scalar(1 +vector(1 != bool 1))",
                expected: 1.0,
            },
            TestCase {
                expr: "1 >= bool 1",
                expected: 1.0,
            },
            TestCase {
                expr: "1 >= bool 2",
                expected: 0.0,
            },
        ];

        for tt in tests {
            let expr = parse(tt.expr).expect("parse failed");
            test_const_simplify(expr, number(tt.expected));
        }
    }
}