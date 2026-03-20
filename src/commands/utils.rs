use super::fanout::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::common::replies::{
    reply_label_ex, reply_with_bulk_string, reply_with_labels, reply_with_sample_ex,
    reply_with_samples,
};
use crate::series::request_types::MRangeSeriesResult;
use std::os::raw::c_long;
use valkey_module::{Context, ValkeyResult, ValkeyValue, raw};

fn reply_with_array(ctx: &Context, len: usize) {
    raw::reply_with_array(ctx.ctx, len as c_long);
}

pub(super) fn reply_with_fanout_label(ctx: &Context, label: &FanoutLabel) {
    if label.name.is_empty() {
        raw::reply_with_null(ctx.ctx);
        return;
    }
    reply_label_ex(ctx, &label.name, Some(&label.value));
}

pub(super) fn reply_with_fanout_labels(ctx: &Context, v: &[FanoutLabel]) {
    reply_with_array(ctx, v.len());
    for label in v {
        reply_with_fanout_label(ctx, label);
    }
}

pub fn reply_with_fanout_sample(ctx: &Context, sample: &Option<FanoutSample>) {
    if let Some(s) = sample {
        reply_with_sample_ex(ctx, s.timestamp, s.value);
    } else {
        raw::reply_with_null(ctx.ctx);
    }
}

pub fn reply_with_mrange_series_result(ctx: &Context, series: &MRangeSeriesResult) {
    reply_with_array(ctx, 3);

    reply_with_bulk_string(ctx, &series.key);

    // series.labels has the same count as selected_labels
    reply_with_labels(ctx, &series.labels);

    reply_with_samples(ctx, series.data.iter());
}

pub(super) fn reply_with_mrange_series_results(
    ctx: &Context,
    series_results: &[MRangeSeriesResult],
) -> ValkeyResult {
    reply_with_array(ctx, series_results.len());
    for series in series_results {
        reply_with_mrange_series_result(ctx, series);
    }
    Ok(ValkeyValue::NoReply)
}
