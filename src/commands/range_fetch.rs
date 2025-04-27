use valkey_module::Context;
use crate::commands::arg_types::RangeOptions;
use crate::common::Timestamp;
use crate::series::{SeriesGuard, TimeSeries};
use crate::series::index::series_keys_by_matchers;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;

pub(crate) struct MRangeSeriesMeta<'a> {
    pub series: &'a TimeSeries,
    pub guard: SeriesGuard,
    pub source_key: String,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
}


pub fn fetch_mrange_metadata(ctx: &Context, options: &RangeOptions) {
    let matchers = std::mem::take(&mut options.series_selector);
    let keys = series_keys_by_matchers(ctx, &[matchers], None)?;

    // needed to keep valkey keys alive below
    let db_keys = keys.iter().map(|key| ctx.open_key(key)).collect::<Vec<_>>();

    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    let metas = db_keys
        .iter()
        .zip(keys)
        .filter_map(|(db_key, source_key)| {
            if let Ok(Some(series)) = db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                let meta = MRangeSeriesMeta {
                    series,
                    source_key: source_key.to_string_lossy(),
                    start_ts,
                    end_ts,
                };
                Some(meta)
            } else {
                None
            }
        })
        .collect();

    let result_rows = process_mrange_command(metas, &options);
    let mut result = result_rows
        .into_iter()
        .map(result_row_to_value)
        .collect::<Vec<_>>();

    if reverse {
        result.reverse();
    }
}