use crate::common::Sample;
use std::cmp::Ordering;
use std::fmt::Display;
use valkey_module::ValkeyError;

// Copyright (c) 2020 Ritchie Vink
// Some portions Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
mod default;
mod join_asof_iter;

use crate::error_consts;
pub(crate) use default::*;
pub(crate) use join_asof_iter::*;

pub type IdxSize = usize;

#[inline]
fn ge_allow_eq<T: PartialOrd + Copy>(l: T, r: T, allow_eq: bool) -> bool {
    match l.partial_cmp(&r) {
        Some(Ordering::Equal) => allow_eq,
        Some(Ordering::Greater) => true,
        _ => false,
    }
}

#[inline]
fn lt_allow_eq<T: PartialOrd + Copy>(l: T, r: T, allow_eq: bool) -> bool {
    match l.partial_cmp(&r) {
        Some(Ordering::Equal) => allow_eq,
        Some(Ordering::Less) => true,
        _ => false,
    }
}

trait AsofJoinState: Default {
    fn new(allow_eq: bool) -> Self;

    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize>;
}

#[derive(Default)]
struct AsofJoinForwardState {
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsofJoinState for AsofJoinForwardState {
    fn new(allow_eq: bool) -> Self {
        AsofJoinForwardState {
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        while self.scan_offset < n_right {
            if let Some(right_val) = right(self.scan_offset) {
                if ge_allow_eq(right_val.timestamp, left_val.timestamp, self.allow_eq) {
                    return Some(self.scan_offset);
                }
            }
            self.scan_offset += 1;
        }
        None
    }
}

#[derive(Default)]
struct AsofJoinBackwardState {
    // best_bound is the greatest right index <= left_val.
    best_bound: Option<IdxSize>,
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsofJoinState for AsofJoinBackwardState {
    fn new(allow_eq: bool) -> Self {
        AsofJoinBackwardState {
            best_bound: None,
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        while self.scan_offset < n_right {
            if let Some(right_val) = right(self.scan_offset) {
                if lt_allow_eq(right_val.timestamp, left_val.timestamp, self.allow_eq) {
                    self.best_bound = Some(self.scan_offset);
                } else {
                    break;
                }
            }
            self.scan_offset += 1;
        }
        self.best_bound
    }
}

#[derive(Default)]
struct AsofJoinNearestState {
    // best_bound is the nearest value to left_val, with ties broken towards the last element.
    best_bound: Option<IdxSize>,
    scan_offset: IdxSize,
    allow_eq: bool,
}

impl AsofJoinState for AsofJoinNearestState {
    #[inline]
    fn new(allow_eq: bool) -> Self {
        AsofJoinNearestState {
            best_bound: None,
            scan_offset: 0,
            allow_eq,
        }
    }

    #[inline]
    fn next<F: FnMut(IdxSize) -> Option<Sample>>(
        &mut self,
        left_val: &Sample,
        mut right: F,
        n_right: IdxSize,
    ) -> Option<IdxSize> {
        // Skipping ahead to the first value greater than left_val. This is
        // cheaper than computing differences.
        while self.scan_offset < n_right {
            if let Some(scan_right_val) = right(self.scan_offset) {
                if lt_allow_eq(scan_right_val.timestamp, left_val.timestamp, self.allow_eq) {
                    self.best_bound = Some(self.scan_offset);
                } else {
                    // Now we must compute a difference to see if scan_right_val
                    // is closer than our current best bound.
                    let scan_is_better = if let Some(best_idx) = self.best_bound {
                        let best_right_val = unsafe { right(best_idx).unwrap_unchecked() };
                        let left_ts = left_val.timestamp;

                        let best_diff = left_ts.abs_diff(best_right_val.timestamp);
                        let scan_diff = left_ts.abs_diff(scan_right_val.timestamp);

                        lt_allow_eq(scan_diff, best_diff, self.allow_eq)
                    } else {
                        true
                    };

                    if scan_is_better {
                        self.best_bound = Some(self.scan_offset);
                        self.scan_offset += 1;

                        // It is possible there are later elements equal to our
                        // scan, so keep going on.
                        while self.scan_offset < n_right {
                            if let Some(next_right_val) = right(self.scan_offset) {
                                if next_right_val == scan_right_val && self.allow_eq {
                                    self.best_bound = Some(self.scan_offset);
                                } else {
                                    break;
                                }
                            }

                            self.scan_offset += 1;
                        }
                    }

                    break;
                }
            }

            self.scan_offset += 1;
        }

        self.best_bound
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Hash)]
pub enum AsofJoinStrategy {
    /// selects the last row in the right series whose ‘on’ key is less than or equal to the left’s key
    #[default]
    Backward,
    /// selects the first row in the right series whose ‘on’ key is greater than or equal to the left’s key.
    Forward,
    /// selects the right in the right series whose 'on' key is nearest to the left's key.
    Nearest,
}

impl Display for AsofJoinStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsofJoinStrategy::Backward => write!(f, "Backward"),
            AsofJoinStrategy::Forward => write!(f, "Forward"),
            AsofJoinStrategy::Nearest => write!(f, "Nearest"),
        }
    }
}

impl TryFrom<&str> for AsofJoinStrategy {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let strategy = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "forward" => AsofJoinStrategy::Forward,
            "next" => AsofJoinStrategy::Forward,
            "previous" => AsofJoinStrategy::Backward,
            "backward" => AsofJoinStrategy::Backward,
            "nearest" => AsofJoinStrategy::Nearest,
        };

        match strategy {
            Some(strategy) => Ok(strategy),
            None => Err(ValkeyError::Str(error_consts::INVALID_ASOF_STRATEGY)),
        }
    }
}
