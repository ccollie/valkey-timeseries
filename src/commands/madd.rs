use crate::commands::arg_parse::{parse_timestamp, parse_value_arg};
use crate::common::parallel::items::Items;
use crate::common::parallel::join;
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::{get_timeseries_mut, SampleAddResult, SeriesGuardMut, TimeSeries};
use ahash::AHashMap;
use smallvec::{smallvec, SmallVec};
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

struct PerSeriesSamples<'a> {
    series: &'a mut TimeSeries,
    samples: SmallVec<Sample, 4>,
    indices: SmallVec<usize, 4>,
}

/// TS.MADD key timestamp value [key timestamp value ...]
///
/// The code is a bit involved, but the goal of this implementation is to parallelize the
/// processing of the samples. The idea is to split the input into groups of samples that
/// belong to the same series, and then process each group in parallel. Using `TimeSeries::merge_samples`
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
    let mut input_map = parse_args(ctx, &args, &current_ts)?;

    // map results to the input
    let results = handle_update(&mut input_map);

    // reassemble the input back into the original order
    let mut all_inputs = Vec::with_capacity(sample_count);
    for (_, ss) in input_map.into_iter() {
        all_inputs.extend(ss.samples);
    }

    // match results back to the original input
    for (index, res) in results {
        if let Some(input) = all_inputs.get_mut(index) {
            input.res = res;
        }
    }

    // sort the inputs back to the original order
    all_inputs.sort_by(|x, y| x.index.cmp(&y.index));

    // handle replication
    handle_replication(ctx, &all_inputs);

    let result = all_inputs
        .into_iter()
        .map(|x| ValkeyValue::from(x.res))
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(result))
}

fn add_samples_internal(
    input: &mut PerSeriesSamples,
) -> TsdbResult<SmallVec<(usize, SampleAddResult), 8>> {
    if input.samples.len() == 1 {
        let sample = input.samples.pop().unwrap();
        let index = input.indices.pop().unwrap();
        let result = input.series.add(sample.timestamp, sample.value, None);
        return Ok(smallvec![(index, result)]);
    }

    let add_results = input.series.merge_samples(&input.samples, None)?;

    let mut result: SmallVec<(usize, SampleAddResult), 8> = SmallVec::new();
    for item in add_results
        .iter()
        .zip(input.indices.iter())
        .map(|(res, index)| (*index, *res))
    {
        result.push(item);
    }

    Ok(result)
}

fn execute_grouped(groups: &mut [PerSeriesSamples]) -> SmallVec<(usize, SampleAddResult), 8> {
    match groups {
        [] => {
            smallvec![]
        }
        [first] => add_samples_internal(first).unwrap(),
        [left, right] => {
            let (mut l, r) = join(
                || add_samples_internal(left).unwrap(),
                || add_samples_internal(right).unwrap(),
            );

            l.extend(r);
            l
        }
        _ => {
            let (left, right) = groups.split_at(groups.len() / 2);
            let (mut l, r) = join(|| execute_grouped(left), || execute_grouped(right));
            l.extend(r);
            l
        }
    }
}

fn handle_update(
    input_map: &mut AHashMap<&ValkeyString, SeriesSamples>,
) -> SmallVec<(usize, SampleAddResult), 8> {
    let mut per_series_samples: SmallVec<PerSeriesSamples, 4> = SmallVec::new();

    // todo! Parallelize this if we involve multiple keys/chunks
    for (_key, samples) in input_map.iter_mut() {
        let res = samples.err;

        if !res.is_ok() {
            continue;
        }

        if let Some(series) = &mut samples.series {
            let mut s = PerSeriesSamples {
                series: series.deref_mut(),
                samples: SmallVec::new(),
                indices: SmallVec::new(),
            };

            for input in samples.samples.iter() {
                if input.res.is_ok() {
                    s.samples.push(Sample::new(input.timestamp, input.value));
                    s.indices.push(input.index);
                }
            }

            if !s.samples.is_empty() {
                per_series_samples.push(s);
            }
        }
    }

    execute_grouped(&mut per_series_samples)
}

fn parse_args<'a>(
    ctx: &'a Context,
    args: &'a [ValkeyString],
    current_ts: &'a ValkeyString,
) -> ValkeyResult<AHashMap<&'a ValkeyString, SeriesSamples<'a>>> {
    let mut input_map: AHashMap<&ValkeyString, SeriesSamples> =
        AHashMap::with_capacity(args.len() / 3);

    let mut index: usize = 1;
    let mut arg_index: usize = 0;
    while index <= args.len() {
        let key = &args[index];
        let mut raw_timestamp = &args[index + 1];
        let raw_value = &args[index + 2];
        let timestamp_str = raw_timestamp.try_as_str()?;

        let series_samples = input_map.entry(key).or_insert_with(SeriesSamples::default);

        // if series_samples is new, we need to look up the series by key
        let mut res = if series_samples.samples.is_empty() {
            match get_timeseries_mut(ctx, key, true, Some(AclPermissions::UPDATE)) {
                Ok(Some(guard)) => {
                    series_samples.series = Some(guard);
                    SampleAddResult::Ok(0)
                }
                Ok(None) => SampleAddResult::InvalidKey,
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
            raw_timestamp = &current_ts;
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
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.ADD", key);
        }
    }
}
