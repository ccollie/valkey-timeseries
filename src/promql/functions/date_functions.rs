use crate::promql::functions::types::{FunctionCallContext, PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{exact_arity_error, max_arity_error};
use crate::promql::{EvalContext, EvalResult, EvalSample, ExprResult};
use chrono::{DateTime, Datelike, Timelike, Utc};
use promql_parser::parser::Expr;

#[derive(Copy, Clone)]
pub(in crate::promql) struct TimestampFunction;

impl TimestampFunction {
    fn timestamps(arg: PromQLArg, ctx: &EvalContext, sample_times: bool) -> EvalResult<ExprResult> {
        let mut samples = arg.into_instant_vector()?;
        for sample in &mut samples {
            let time = if sample_times {
                sample.timestamp_ms
            } else {
                ctx.evaluation_ts
            };
            sample.value = time as f64 / 1000.0;
            sample.timestamp_ms = ctx.evaluation_ts;
            sample.drop_name = true;
        }

        Ok(ExprResult::InstantVector(samples))
    }
}

/// Whether `timestamp()` reports its argument's sample times: only a vector selector
/// (in any parentheses, with any `offset` or `@`) has them. Anything computed from one
/// was computed at the evaluation time, even though its samples still carry the
/// selector's times, so `timestamp(abs(m))` is the evaluation time, as in Prometheus.
fn reports_sample_times(arg: &Expr) -> bool {
    match arg {
        Expr::VectorSelector(_) => true,
        Expr::Paren(paren) => reports_sample_times(&paren.expr),
        _ => false,
    }
}

impl PromQLFunction for TimestampFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Self::timestamps(arg, ctx, true)
    }

    fn apply_call(
        &self,
        evaluated_args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        let [arg] = <[PromQLArg; 1]>::try_from(evaluated_args)
            .map_err(|args| exact_arity_error("timestamp", 1, args.len()))?;
        // Without the unevaluated argument (direct callers), the samples' own times.
        let sample_times = ctx.raw_arg(0).is_none_or(reports_sample_times);
        Self::timestamps(arg, ctx.eval_context, sample_times)
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) enum DateTimePart {
    Year,
    Month,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    Hour,
    Minute,
    DaysInMonth,
}

impl DateTimePart {
    pub fn extract(&self, dt: DateTime<Utc>) -> f64 {
        match self {
            Self::Year => dt.year() as f64,
            Self::Month => dt.month() as f64,
            Self::DayOfMonth => dt.day() as f64,
            Self::DayOfYear => dt.ordinal() as f64,
            Self::DayOfWeek => dt.weekday().num_days_from_sunday() as f64,
            Self::Hour => dt.hour() as f64,
            Self::Minute => dt.minute() as f64,
            Self::DaysInMonth => days_in_month(dt) as f64,
        }
    }
}

pub(in crate::promql) fn datetime_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }

    let seconds = value.trunc();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&seconds) {
        return None;
    }

    DateTime::from_timestamp(seconds as i64, 0)
}

fn datetime_from_millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(value / 1000, 0)
}

/// The length of `dt`'s month. Not the distance to the 1st of the next month:
/// for a December in chrono's last representable year that date does not
/// exist, and `days_in_month(vector(8210265408000))` panicked the server.
pub(in crate::promql) fn days_in_month(dt: DateTime<Utc>) -> u32 {
    u32::from(dt.num_days_in_month())
}

pub(in crate::promql) fn eval_datetime_function(
    samples: &mut [EvalSample],
    eval_timestamp_ms: i64,
    part: DateTimePart,
) {
    for sample in samples {
        sample.value = datetime_from_seconds(sample.value)
            .map(|dt| part.extract(dt))
            .unwrap_or(f64::NAN);
        sample.timestamp_ms = eval_timestamp_ms;
        sample.drop_name = true;
    }
}

macro_rules! make_datetime_function {
    ( $name: ident, $func_name: literal, $part: expr ) => {
        #[derive(Copy, Clone)]
        pub(in crate::promql) struct $name;

        impl PromQLFunction for $name {
            fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
                let mut samples = arg.into_instant_vector()?;
                eval_datetime_function(&mut samples, ctx.evaluation_ts, $part);
                Ok(ExprResult::InstantVector(samples))
            }

            fn apply_args(
                &self,
                args: Vec<PromQLArg>,
                ctx: &EvalContext,
            ) -> EvalResult<ExprResult> {
                match args.len() {
                    0 => Ok(ExprResult::InstantVector(vec![EvalSample::unlabelled(
                        ctx.evaluation_ts,
                        datetime_from_millis(ctx.evaluation_ts)
                            .map(|dt| $part.extract(dt))
                            .unwrap_or(f64::NAN),
                    )])),
                    1 => self.apply(args.into_iter().next().expect("single arg"), ctx),
                    _ => Err(max_arity_error($func_name, 1, args.len())),
                }
            }
        }
    };
}

make_datetime_function!(YearFunction, "year", DateTimePart::Year);
make_datetime_function!(MonthFunction, "month", DateTimePart::Month);
make_datetime_function!(DayOfMonthFunction, "day_of_month", DateTimePart::DayOfMonth);
make_datetime_function!(DayOfYearFunction, "day_of_year", DateTimePart::DayOfYear);
make_datetime_function!(DayOfWeekFunction, "day_of_week", DateTimePart::DayOfWeek);
make_datetime_function!(HourFunction, "hour", DateTimePart::Hour);
make_datetime_function!(MinuteFunction, "minute", DateTimePart::Minute);
make_datetime_function!(
    DaysInMonthFunction,
    "days_in_month",
    DateTimePart::DaysInMonth
);

#[derive(Copy, Clone)]
pub(in crate::promql) struct TimeFunction;

impl PromQLFunction for TimeFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if !args.is_empty() {
            return Err(exact_arity_error("time", 0, args.len()));
        }

        Ok(ExprResult::InstantVector(vec![EvalSample::unlabelled(
            ctx.evaluation_ts,
            ctx.evaluation_ts as f64 / 1000.0,
        )]))
    }
}
