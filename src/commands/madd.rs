use crate::commands::arg_parse::{parse_timestamp, parse_value_arg};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::series::{
    PerSeriesSamples, SampleAddResult, SeriesGuardMut, TimeSeriesOptions, create_and_store_series,
    get_timeseries_mut, multi_series_merge_samples,
};
use ahash::AHashMap;
use smallvec::SmallVec;
use std::ops::DerefMut;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

#[derive(Debug)]
struct ParsedInput<'a> {
    key: &'a ValkeyString,
    raw_timestamp: &'a ValkeyString,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
    index: usize,
    res: SampleAddResult,
}

#[derive(Default)]
struct SeriesSamples<'a> {
    series: Option<SeriesGuardMut<'a>>,
    err: SampleAddResult,
    samples: Vec<ParsedInput<'a>>,
}

/// TS.MADD key timestamp value [key timestamp value ...]
///
/// The code is a bit involved, but the goal of this implementation is to parallelize the
/// processing of the samples. The idea is to split the input into groups of samples that
/// belong to the same series, and then process each group in parallel. `TimeSeries::merge_samples`
/// allows us to add multiple samples at once per series, while parallelizing across series blocks.
/// Because of that there is extra bookkeeping to do, including mapping results back to the
/// original input and returning results in input order.
pub fn madd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 3 {
        return Err(ValkeyError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(ValkeyError::WrongArity);
    }

    let sample_count = arg_count / 3;

    let now = current_time_millis();
    let current_ts = ctx.create_string(now.to_string());

    // parse the input arguments into a map of samples grouped by series
    let mut input_map = parse_args(ctx, &args[1..], &current_ts)?;

    let results = handle_update(ctx, &mut input_map)?;

    // reassemble the input back into the original order
    let mut all_inputs = Vec::with_capacity(sample_count);
    for (_, ss) in input_map.into_iter() {
        all_inputs.extend(ss.samples);
    }

    // sort the inputs back to the original order
    all_inputs.sort_by(|x, y| x.index.cmp(&y.index));

    // match results back to the original input
    for (index, res) in results.iter() {
        if let Some(input) = all_inputs.get_mut(*index) {
            input.res = *res;
        }
    }

    // handle replication
    handle_replication(ctx, &all_inputs);

    let result = results
        .into_iter()
        .map(|x| ValkeyValue::from(x.1))
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(result))
}

fn handle_update(
    ctx: &Context,
    input_map: &mut AHashMap<&ValkeyString, SeriesSamples>,
) -> ValkeyResult<SmallVec<(usize, SampleAddResult), 8>> {
    let mut per_series_samples: Vec<PerSeriesSamples> = Vec::with_capacity(4); // usually we have just a few series

    let mut errors: SmallVec<(usize, SampleAddResult), 8> = SmallVec::new();

    for (_key, samples) in input_map.iter_mut() {
        let res = samples.err;

        if !res.is_ok() {
            // if we have an error, we need to return it for each sample
            for input in samples.samples.iter() {
                errors.push((input.index, res));
            }
            continue;
        }

        let Some(series) = &mut samples.series else {
            continue;
        };

        let mut s = PerSeriesSamples::new(series.deref_mut());

        for input in samples.samples.iter() {
            if input.res.is_ok() {
                s.add_sample(Sample::new(input.timestamp, input.value), input.index);
            } else {
                // if we have an error, we need to return it for this sample
                errors.push((input.index, input.res));
            }
        }

        if !s.is_empty() {
            per_series_samples.push(s);
        }
    }

    let mut results = multi_series_merge_samples(per_series_samples, Some(ctx))?;
    // add errors to the results
    results.extend(errors);
    results.sort_by_key(|(index, _)| *index);
    Ok(results)
}

fn parse_args<'a>(
    ctx: &'a Context,
    args: &'a [ValkeyString],
    current_ts: &'a ValkeyString,
) -> ValkeyResult<AHashMap<&'a ValkeyString, SeriesSamples<'a>>> {
    let mut input_map: AHashMap<&ValkeyString, SeriesSamples> =
        AHashMap::with_capacity(args.len() / 3);

    let mut index: usize = 0;
    let mut arg_index: usize = 0;

    let options = TimeSeriesOptions::from_config();
    while index < args.len() {
        let key = &args[index];
        let mut raw_timestamp = &args[index + 1];
        let raw_value = &args[index + 2];
        let timestamp_str = raw_timestamp.try_as_str()?;

        let series_samples = input_map.entry(key).or_default();

        // if series_samples is new, we need to look up the series by key
        let mut res = if series_samples.samples.is_empty() {
            match get_timeseries_mut(ctx, key, false, Some(AclPermissions::UPDATE)) {
                Ok(Some(guard)) => {
                    series_samples.series = Some(guard);
                    series_samples.err = SampleAddResult::Ok(Sample::default());
                    SampleAddResult::Ok(Sample::default())
                }
                Ok(None) => {
                    let guard = create_and_store_series(ctx, key, options.clone(), true, true)?;
                    series_samples.series = Some(guard);
                    series_samples.err = SampleAddResult::Ok(Sample::default());
                    SampleAddResult::Ok(Sample::default())
                }
                Err(_) => SampleAddResult::InvalidPermissions,
            }
        } else {
            series_samples.err
        };

        // parsing value only if we have no errors with the key
        let (timestamp, value) = if !res.is_ok() {
            (0, 0.0)
        } else {
            let ts = match parse_timestamp(timestamp_str) {
                Ok(ts) => ts,
                Err(_) => {
                    res = SampleAddResult::InvalidTimestamp;
                    0
                }
            };
            let value = match parse_value_arg(raw_value) {
                Ok(v) => v,
                Err(_) => {
                    res = SampleAddResult::InvalidValue;
                    0.0
                }
            };
            (ts, value)
        };

        if timestamp_str == "*" {
            raw_timestamp = current_ts;
        }

        series_samples.samples.push(ParsedInput {
            key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
            index: arg_index,
            res,
        });

        arg_index += 1;
        index += 3;
    }

    Ok(input_map)
}

fn handle_replication(ctx: &Context, inputs: &[ParsedInput]) {
    let mut replication_args: SmallVec<_, 24> = SmallVec::new();
    for input in inputs.iter() {
        if input.res.is_ok() {
            replication_args.push(input.key);
            replication_args.push(input.raw_timestamp);
            replication_args.push(input.raw_value);
        }
    }

    if !replication_args.is_empty() {
        ctx.replicate("TS.MADD", &*replication_args);
        for key in replication_args.into_iter().step_by(3) {
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add", key);
        }
    }
}
