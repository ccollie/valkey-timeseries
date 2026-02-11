use crate::commands::parse_series_options;
use crate::common::Sample;
use crate::series::{
    IngestedSamples, TimeSeries, bulk_insert_samples, create_and_store_series, get_timeseries_mut,
};
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.ADDBULK key data
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [ON_DUPLICATE policy_ovr]
///     [ENCODING <COMPRESSED|UNCOMPRESSED>]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///
pub fn add_bulk(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key = args[1].clone();
    // I don't like the copying here, but we need a mutable buffer
    let samples = {
        let data = args[2].as_slice();
        let mut buf = data.to_vec();
        let sample_data = IngestedSamples::from_json_lines(&mut buf)?;
        sample_data.samples
    };

    let options = parse_series_options(args, 4, &[])?;

    if let Some(mut guard) = get_timeseries_mut(ctx, &key, false, Some(AclPermissions::UPDATE))? {
        return process_series(ctx, &mut guard, samples);
    }

    let mut series = create_and_store_series(ctx, &key, options, true, true)?;
    process_series(ctx, &mut series, samples)
}

#[inline]
fn process_series(ctx: &Context, series: &mut TimeSeries, samples: Vec<Sample>) -> ValkeyResult {
    match handle_ingest(ctx, series, samples) {
        Ok(val) => {
            ctx.replicate_verbatim();
            Ok(val)
        }
        Err(err) => Err(err),
    }
}

fn handle_ingest(ctx: &Context, series: &mut TimeSeries, samples: Vec<Sample>) -> ValkeyResult {
    let sample_count = samples.len();
    let duplicate_policy = series.sample_duplicates.resolve_policy(None);

    let results = bulk_insert_samples(ctx, series, &samples, Some(duplicate_policy));

    let success_count = results.iter().filter(|res| res.is_ok()).count();

    let result = vec![
        ValkeyValue::Integer(success_count as i64),
        ValkeyValue::Integer(sample_count as i64),
    ];

    Ok(ValkeyValue::Array(result))
}
