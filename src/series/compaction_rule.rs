use crate::aggregators::Aggregator;
use crate::common::{Sample, Timestamp};
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::index::with_timeseries_index;
use crate::series::TimeSeries;
use valkey_module::{Context, ValkeyString};

pub struct CompactionRule {
    pub dest: ValkeyString,
    pub aggregate: Aggregator,
    pub bucket_duration: u64, // ms
    pub ts_alignment: Timestamp,
    pub bucket_start: Timestamp //
}


fn run_compaction(rule: &mut CompactionRule, sample: Sample) {
    todo!()
}

fn run_compaction_internal(dest: &TimeSeries, rule: &mut CompactionRule, sample: Sample) {

}

fn run_compactions(ctx: &Context, rules: &[&mut CompactionRule], value: f64) {
    // collect all dest keys
    let mut dest_keys = Vec::new();
    with_timeseries_index(ctx, move |index| {
        // needed to keep valkey keys alive below
        let db_keys = rules.iter().map(|rule| ctx.open_key(&rule.dest).collect::<Vec<_>>();

        let metas = db_keys.iter()
            .zip(keys)
            .filter_map(|(db_key, source_key)| {
                if let Ok(Some(series)) = db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                    let (start_ts, end_ts) = options.date_range.get_series_range(series, None,false);
                    let meta = SeriesMeta {
                        series,
                        source_key,
                        start_ts,
                        end_ts,
                    };
                    Some(meta)
                } else {
                    None
                }
            }).collect();

        let result_rows = process_command(metas, &options);
        let mut result = result_rows
            .into_iter()
            .map(result_row_to_value)
            .collect::<Vec<_>>();

        if reverse {
            result.reverse();
        }

        Ok(ValkeyValue::from(result))
    })
}