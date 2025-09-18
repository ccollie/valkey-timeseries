use super::fanout::generated::{Label as FanoutLabel, Sample as FanoutSample};
use crate::common::Timestamp;
use crate::fanout::FanoutError;
use std::os::raw::c_char;
use std::{collections::BTreeSet, ffi::CString};
use valkey_module::{Context, Status, raw};

pub(super) fn reply_with_error(ctx: &Context, err: FanoutError) {
    if err.message.is_empty() {
        reply_with_str(ctx, err.kind.as_str());
    } else {
        reply_with_str(ctx, &err.message);
    }
}

pub(super) fn reply_with_str(ctx: &Context, s: &str) -> Status {
    let msg = CString::new(s).unwrap();
    raw::reply_with_simple_string(ctx.ctx, msg.as_ptr())
}

pub(super) fn reply_with_i64(ctx: &Context, v: i64) -> Status {
    raw::reply_with_long_long(ctx.ctx, v)
}

pub(super) fn reply_with_usize(ctx: &Context, v: usize) -> Status {
    raw::reply_with_long_long(ctx.ctx, v as i64)
}

pub(super) fn reply_with_bulk_string(ctx: &Context, s: &str) -> Status {
    raw::reply_with_string_buffer(ctx.ctx, s.as_ptr().cast::<c_char>(), s.len())
}

pub fn reply_with_string_iter(
    ctx: &Context,
    v: impl Iterator<Item = String>,
    count: usize,
) -> Status {
    let status = raw::reply_with_array(ctx.ctx, count as i64);
    if status != Status::Ok {
        return status;
    }
    for s in v {
        let cstr = CString::new(s).unwrap();
        let status = raw::reply_with_string_buffer(ctx.ctx, cstr.as_ptr(), cstr.as_bytes().len());
        if status != Status::Ok {
            return status;
        }
    }
    Status::Ok
}

pub fn reply_with_btree_set(ctx: &Context, v: &BTreeSet<String>) -> Status {
    let mut status = raw::reply_with_array(ctx.ctx, v.len() as i64);
    if status != Status::Ok {
        return status;
    }
    for s in v {
        status = reply_with_bulk_string(ctx, s);
        if status != Status::Ok {
            return status;
        }
    }
    Status::Ok
}

pub fn reply_with_fanout_label(ctx: &Context, label: &FanoutLabel) -> Status {
    if label.name.is_empty() {
        return raw::reply_with_null(ctx.ctx);
    }
    let mut status = reply_with_str(ctx, &label.name);
    if status != Status::Ok {
        return status;
    }
    status = reply_with_bulk_string(ctx, &label.value);
    if status != Status::Ok {
        return status;
    }
    Status::Ok
}

pub fn reply_with_fanout_labels(ctx: &Context, v: &[FanoutLabel]) -> Status {
    let status = raw::reply_with_array(ctx.ctx, v.len() as i64);
    if status != Status::Ok {
        return status;
    }
    for label in v {
        let status = reply_with_fanout_label(ctx, label);
        if status != Status::Ok {
            return status;
        }
    }
    Status::Ok
}

pub fn reply_with_sample_ex(ctx: &Context, timestamp: Timestamp, value: f64) -> Status {
    let mut status = raw::reply_with_array(ctx.ctx, 2);
    if status != Status::Ok {
        return status;
    }
    status = reply_with_i64(ctx, timestamp);
    if status != Status::Ok {
        return status;
    }
    status = raw::reply_with_double(ctx.ctx, value);
    if status != Status::Ok {
        return status;
    }
    Status::Ok
}

pub fn reply_with_fanout_sample(ctx: &Context, sample: &Option<FanoutSample>) -> Status {
    if let Some(s) = sample {
        reply_with_sample_ex(ctx, s.timestamp, s.value)
    } else {
        raw::reply_with_null(ctx.ctx)
    }
}
