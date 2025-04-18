use crate::common::Sample;
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
// https://github.com/pola-rs/polars/blob/main/crates/polars-ops/src/frame/join/asof/default.rs
use super::{
    AsofJoinBackwardState, AsofJoinForwardState, AsofJoinNearestState, AsofJoinState,
    AsofJoinStrategy, IdxSize,
};

fn join_asof_impl<S, F>(
    left: &[Sample],
    right: &[Sample],
    mut filter: F,
    allow_eq: bool,
) -> Vec<(Sample, Sample)>
where
    S: AsofJoinState,
    F: FnMut(&Sample, &Sample) -> bool,
{
    let mut out = Vec::with_capacity(left.len());
    let mut state = S::new(allow_eq);

    for left_val in left.iter() {
        if let Some(r_idx) = state.next(
            left_val,
            // SAFETY: next() only calls with indices < right.len().
            |j| Some(unsafe { *right.get_unchecked(j) }),
            right.len() as IdxSize,
        ) {
            // SAFETY: r_idx is non-null and valid.
            let right_val = unsafe { right.get_unchecked(r_idx) };
            if filter(left_val, right_val) {
                out.push((*left_val, *right_val));
            }
        }
    }

    out
}

pub fn join_asof_forward<F>(
    left: &[Sample],
    right: &[Sample],
    filter: F,
    allow_eq: bool,
) -> Vec<(Sample, Sample)>
where
    F: FnMut(&Sample, &Sample) -> bool,
{
    join_asof_impl::<AsofJoinForwardState, _>(left, right, filter, allow_eq)
}

pub fn join_asof_backward<F>(
    left: &[Sample],
    right: &[Sample],
    filter: F,
    allow_eq: bool,
) -> Vec<(Sample, Sample)>
where
    F: FnMut(&Sample, &Sample) -> bool,
{
    join_asof_impl::<AsofJoinBackwardState, _>(left, right, filter, allow_eq)
}

pub fn join_asof_nearest<F>(
    left: &[Sample],
    right: &[Sample],
    filter: F,
    allow_eq: bool,
) -> Vec<(Sample, Sample)>
where
    F: FnMut(&Sample, &Sample) -> bool,
{
    join_asof_impl::<AsofJoinNearestState, _>(left, right, filter, allow_eq)
}

pub(crate) fn join_asof_samples(
    left: &[Sample],
    right: &[Sample],
    strategy: AsofJoinStrategy,
    tolerance: Option<i64>,
    allow_eq: bool,
) -> Vec<(Sample, Sample)> {
    if let Some(t) = tolerance {
        let abs_tolerance = t.abs_diff(0);
        let filter = |l: &Sample, r: &Sample| l.timestamp.abs_diff(r.timestamp) <= abs_tolerance;
        match strategy {
            AsofJoinStrategy::Forward => join_asof_forward(left, right, filter, allow_eq),
            AsofJoinStrategy::Backward => join_asof_backward(left, right, filter, allow_eq),
            AsofJoinStrategy::Nearest => join_asof_nearest(left, right, filter, allow_eq),
        }
    } else {
        let filter = |_: &Sample, _: &Sample| true;
        match strategy {
            AsofJoinStrategy::Forward => join_asof_forward(left, right, filter, allow_eq),
            AsofJoinStrategy::Backward => join_asof_backward(left, right, filter, allow_eq),
            AsofJoinStrategy::Nearest => join_asof_nearest(left, right, filter, allow_eq),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::Sample;

    #[test]
    fn test_join_asof_backward() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 5,
                value: 10.0,
            },
            Sample {
                timestamp: 15,
                value: 20.0,
            },
            Sample {
                timestamp: 25,
                value: 30.0,
            },
            Sample {
                timestamp: 35,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 5);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 15);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 25);
        assert_eq!(result[3].0.timestamp, 40);
        assert_eq!(result[3].1.timestamp, 35);
    }

    #[test]
    fn test_join_asof_forward() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 15,
                value: 10.0,
            },
            Sample {
                timestamp: 25,
                value: 20.0,
            },
            Sample {
                timestamp: 35,
                value: 30.0,
            },
            Sample {
                timestamp: 45,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Forward, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 15);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 25);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 35);
        assert_eq!(result[3].0.timestamp, 40);
        assert_eq!(result[3].1.timestamp, 45);
    }

    #[test]
    fn test_join_asof_nearest() {
        let left = vec![
            Sample {
                timestamp: 12,
                value: 1.0,
            },
            Sample {
                timestamp: 22,
                value: 2.0,
            },
            Sample {
                timestamp: 32,
                value: 3.0,
            },
            Sample {
                timestamp: 42,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
            Sample {
                timestamp: 40,
                value: 40.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Nearest, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 12);
        assert_eq!(result[0].1.timestamp, 10);
        assert_eq!(result[1].0.timestamp, 22);
        assert_eq!(result[1].1.timestamp, 20);
        assert_eq!(result[2].0.timestamp, 32);
        assert_eq!(result[2].1.timestamp, 30);
        assert_eq!(result[3].0.timestamp, 42);
        assert_eq!(result[3].1.timestamp, 40);
    }

    #[test]
    fn test_join_asof_with_tolerance() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 8,
                value: 10.0,
            },
            Sample {
                timestamp: 18,
                value: 20.0,
            },
            Sample {
                timestamp: 28,
                value: 30.0,
            },
            Sample {
                timestamp: 35,
                value: 40.0,
            },
        ];

        // With tolerance 3
        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, Some(3), true);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 8);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 18);
        assert_eq!(result[2].0.timestamp, 30);
        assert_eq!(result[2].1.timestamp, 28);
        // No match for timestamp 40 because nearest backward (35) is outside tolerance
    }

    #[test]
    fn test_join_asof_empty_inputs() {
        let left: Vec<Sample> = vec![];
        let right = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, None, true);
        assert_eq!(result.len(), 0);

        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        let right: Vec<Sample> = vec![];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, None, true);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_join_asof_exact_matches() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
        ];

        // With allow_eq = true
        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, None, true);

        assert_eq!(result.len(), 3);
        for i in 0..3 {
            assert_eq!(result[i].0.timestamp, result[i].1.timestamp);
        }

        // With allow_eq = false
        // Note that we specify a tolerance here otherwise the test would fail. This is because
        // otherwise the algorithm would simply scan backwards and therefore find a non-equal match.
        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, Some(5), false);
        assert_eq!(result.len(), 0); // No matches because exact equality is not allowed
    }

    #[test]
    fn test_join_asof_duplicates() {
        let left = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            }, // Duplicate timestamp
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        let right = vec![
            Sample {
                timestamp: 5,
                value: 10.0,
            },
            Sample {
                timestamp: 15,
                value: 20.0,
            },
            Sample {
                timestamp: 15,
                value: 25.0,
            }, // Duplicate timestamp
            Sample {
                timestamp: 25,
                value: 30.0,
            },
        ];

        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Backward, None, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.timestamp, 10);
        assert_eq!(result[0].1.timestamp, 5);
        assert_eq!(result[1].0.timestamp, 20);
        assert_eq!(result[1].1.timestamp, 15);
        assert_eq!(result[2].0.timestamp, 20); // Second entry with same timestamp
        assert_eq!(result[2].1.timestamp, 15);
        assert_eq!(result[3].0.timestamp, 30);
        assert_eq!(result[3].1.timestamp, 25);
    }

    #[test]
    fn test_join_asof_nearest_equidistant() {
        let left = vec![
            Sample {
                timestamp: 15,
                value: 1.0,
            }, // Equidistant from 10 and 20
            Sample {
                timestamp: 25,
                value: 2.0,
            }, // Equidistant from 20 and 30
        ];

        let right = vec![
            Sample {
                timestamp: 10,
                value: 10.0,
            },
            Sample {
                timestamp: 20,
                value: 20.0,
            },
            Sample {
                timestamp: 30,
                value: 30.0,
            },
        ];

        // Nearest should prefer the later value when equidistant
        let result = join_asof_samples(&left, &right, AsofJoinStrategy::Nearest, None, true);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.timestamp, 15);
        assert_eq!(result[0].1.timestamp, 20); // Should take later value when equidistant
        assert_eq!(result[1].0.timestamp, 25);
        assert_eq!(result[1].1.timestamp, 30);
    }
}
