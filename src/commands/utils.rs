use super::fanout::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::common::{Sample, Timestamp};
use crate::fanout::FanoutError;
use crate::labels::Label;
use crate::series::request_types::MRangeSeriesResult;
use std::os::raw::{c_char, c_long};
use std::{collections::BTreeSet, ffi::CString};
use valkey_module::{Context, VALKEYMODULE_POSTPONED_ARRAY_LEN, ValkeyResult, ValkeyValue, raw};

fn reply_with_array(ctx: &Context, len: usize) {
    raw::reply_with_array(ctx.ctx, len as c_long);
}

pub(super) fn reply_with_error(ctx: &Context, err: FanoutError) {
    if err.message.is_empty() {
        reply_with_str(ctx, err.kind.as_str());
    } else {
        reply_with_str(ctx, &err.message);
    }
}

pub(super) fn reply_with_str(ctx: &Context, s: &str) {
    let msg = CString::new(s).unwrap();
    raw::reply_with_simple_string(ctx.ctx, msg.as_ptr());
}

pub(super) fn reply_with_i64(ctx: &Context, v: i64) {
    raw::reply_with_long_long(ctx.ctx, v);
}

pub(super) fn reply_with_bulk_string(ctx: &Context, s: &str) {
    raw::reply_with_string_buffer(ctx.ctx, s.as_ptr().cast::<c_char>(), s.len());
}

pub fn reply_with_string_iter(ctx: &Context, v: impl Iterator<Item = String>) {
    raw::reply_with_array(ctx.ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long);
    let mut len = 0;
    for s in v {
        reply_with_bulk_string(ctx, &s);
        len += 1;
    }
    raw::reply_with_array(ctx.ctx, len as c_long);
}

pub fn reply_with_btree_set(ctx: &Context, v: &BTreeSet<String>) {
    reply_with_array(ctx, v.len());
    for s in v {
        reply_with_bulk_string(ctx, s);
    }
}

pub fn reply_label_ex(ctx: &Context, label: &str, value: Option<&str>) {
    reply_with_array(ctx, 2);
    reply_with_bulk_string(ctx, label);
    if let Some(value) = value {
        reply_with_bulk_string(ctx, value);
    } else {
        raw::reply_with_null(ctx.ctx);
    }
}

pub fn reply_label(ctx: &Context, label: &str, value: &str) {
    let value = if value.is_empty() { None } else { Some(value) };
    reply_label_ex(ctx, label, value);
}

pub fn reply_with_labels(ctx: &Context, labels: &[Label]) {
    reply_with_array(ctx, labels.len());
    for label in labels {
        reply_label(ctx, &label.name, &label.value);
    }
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

pub fn reply_with_sample_ex(ctx: &Context, timestamp: Timestamp, value: f64) {
    reply_with_array(ctx, 2);
    reply_with_i64(ctx, timestamp);
    raw::reply_with_double(ctx.ctx, value);
}

#[inline]
pub fn reply_with_sample(ctx: &Context, sample: &Sample) {
    reply_with_sample_ex(ctx, sample.timestamp, sample.value);
}

pub fn reply_with_fanout_sample(ctx: &Context, sample: &Option<FanoutSample>) {
    if let Some(s) = sample {
        reply_with_sample_ex(ctx, s.timestamp, s.value);
    } else {
        raw::reply_with_null(ctx.ctx);
    }
}

pub fn reply_with_samples(ctx: &Context, samples: impl Iterator<Item = Sample>) {
    raw::reply_with_array(ctx.ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN as c_long);

    let mut len = 0;
    for sample in samples {
        reply_with_sample(ctx, &sample);
        len += 1;
    }

    reply_with_array(ctx, len);
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
