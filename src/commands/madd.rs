use crate::commands::arg_parse::{parse_timestamp, parse_value_arg};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::{get_timeseries_mut, SampleAddResult};
use ahash::AHashMap;
use smallvec::SmallVec;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

struct ParsedInput<'a> {
    key: &'a ValkeyString,
    key_buf: &'a [u8],
    raw_timestamp: &'a ValkeyString,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
    index: usize,
}

pub fn madd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 3 {
        return Err(ValkeyError::WrongArity);
    }

    if arg_count % 3 != 0 {
        return Err(ValkeyError::WrongArity);
    }

    let sample_count = arg_count / 3;

    let current_ts = ctx.create_string(current_time_millis().to_string());

    let mut inputs: Vec<ParsedInput> = Vec::with_capacity(sample_count);

    let mut index: usize = 1;
    while index <= arg_count {
        let key = &args[index];
        let mut raw_timestamp = &args[index + 1];
        let raw_value = &args[index + 2];
        let timestamp_str = raw_timestamp.try_as_str()?;
        let timestamp = parse_timestamp(timestamp_str)?;
        let value = parse_value_arg(raw_value)?;

        if timestamp_str == "*" {
            raw_timestamp = &current_ts;
        }

        inputs.push(ParsedInput {
            key,
            key_buf: key,
            raw_timestamp,
            raw_value,
            timestamp,
            value,
            index: inputs.len(),
        });

        index += 3;
    }

    // todo! Parallelize this if we involve multiple keys/chunks

    let mut temp = group(inputs.into_iter().map(|input| (input.key_buf, input)))
        .into_iter()
        .fold(vec![], |mut acc, inputs| {
            // todo: remove unwrap
            let key = inputs[0].key;
            let mut arr = add_samples_internal(ctx, key, &inputs).unwrap();
            acc.append(&mut arr);
            acc
        });

    temp.sort_by(|x, y| x.0.cmp(&y.0));
    let result = temp
        .into_iter()
        .map(|(_, res)| ValkeyValue::from(res))
        .collect::<Vec<_>>();

    // todo!!
    Ok(ValkeyValue::Array(result))
}

fn add_samples_internal(
    ctx: &Context,
    key: &ValkeyString,
    input: &Vec<ParsedInput>,
) -> TsdbResult<Vec<(usize, SampleAddResult)>> {
    if let Ok(Some(mut guard)) = get_timeseries_mut(ctx, key, true, Some(AclPermissions::UPDATE)) {
        let samples: SmallVec<Sample, 6> = input
            .iter()
            .map(|input| Sample {
                timestamp: input.timestamp,
                value: input.value,
            })
            .collect();

        let series = &mut guard;
        let add_results = series.merge_samples(&samples, None)?;
        let mut results = Vec::with_capacity(input.len());
        let mut replication_args: SmallVec<_, 16> = SmallVec::new();
        for (res, input) in add_results.iter().zip(input.iter()) {
            if res.is_ok() {
                replication_args.push(input.key);
                replication_args.push(input.raw_timestamp);
                replication_args.push(input.raw_value);
            }
            results.push((input.index, *res));
        }
        if !replication_args.is_empty() {
            ctx.replicate("TS.MADD", &*replication_args);
            let mut idx = 0;
            while idx < replication_args.len() {
                ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.ADD", replication_args[idx]);
                idx += 3;
            }
        }

        Ok(results)
    } else {
        Ok(input
            .iter()
            .map(|input| (input.index, SampleAddResult::InvalidKey))
            .collect())
    }
}

fn group<K, V, I>(iter: I) -> Vec<Vec<V>>
where
    K: Eq + std::hash::Hash,
    I: Iterator<Item = (K, V)>,
{
    let mut hash_map = match iter.size_hint() {
        (_, Some(len)) => AHashMap::with_capacity(len),
        (len, None) => AHashMap::with_capacity(len),
    };

    for (key, value) in iter {
        hash_map
            .entry(key)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(value);
    }

    hash_map.into_values().collect()
}
