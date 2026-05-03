// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub st: i64,
    pub t: i64,
    pub v: f64,
}

#[derive(Clone, Copy, Debug)]
struct Triple {
    st: i64,
    t: i64,
    v: f64,
}



// XOR2 Chunk implementation
const MAX_FIRST_ST_CHANGE_ON: usize = 127;

#[cfg(test)]
mod tests {
    use crate::series::chunks::bstream::BStream;
    use crate::series::chunks::xor2::chunk::{is_stale_nan, XOR2Chunk, STALE_NAN};
    use super::*;

    fn write_xor2_new_window_payload(bs: &mut BStream, delta: u64) -> (u8, u8) {
        let (leading, trailing, sigbits) = xor2_delta_window(delta);
        let mut encoded_sigbits = sigbits;
        if sigbits == 64 {
            encoded_sigbits = 0;
        }

        bs.write_bits_fast(leading as u64, 5);
        bs.write_bits_fast(encoded_sigbits as u64, 6);
        bs.write_bits_fast(delta >> trailing, sigbits as usize);

        (leading, trailing)
    }

    fn xor2_delta_window(delta: u64) -> (u8, u8, u8) {
        let leading = delta.leading_zeros() as u8;
        let mut trailing = delta.trailing_zeros() as u8;
        if leading >= 32 {
            trailing = 31;
        }

        (leading, trailing, 64 - leading - trailing)
    }


    fn benchmark_xor2_write(b: &mut test::Bencher) {
        // Creating samples array
        let mut samples: Vec<(i64, f64)> = Vec::with_capacity(120);
        for i in 0..120 {
            samples.push((
                i as i64 * 1000,
                i as f64 + i as f64 / 10.0 + i as f64 / 100.0 + i as f64 / 1000.0,
            ));
        }

        b.iter(|| {
            // Create new XOR2 chunk
            let mut c = NewXOR2Chunk();
            let mut app = c.appender().unwrap();
            for s in &samples {
                // Append samples to the chunk
                app.append(0, s.0, s.1);
            }
        });
    }

    fn require_xor2_samples(samples: &[Triple]) {
        let mut chunk = XOR2Chunk::new();

        for sample in samples.iter() {
            chunk.append(sample.st, sample.t, sample.v);
        }

        let mut it = chunk.iterator();
        for want in samples.iter() {
            let Some(sample) = it.next() else {
                panic!("expected more samples");
            };
            assert_eq!(want.t, sample.timestamp);
            assert_eq!(want.v, sample.value);
        }
    }

    #[test]
    fn test_xor2_stale_with_dod_non_zero() {
        let mut c = XOR2Chunk::new();

        // Stale NaN samples where the timestamp dod is non-zero, exercising the
        // `111` value encoding path inside writeVDelta.
        let samples = vec![
            (1000, 1.0, false),
            (2000, 2.0, false),
            // dod = (1050 - 1000) - (2000 - 1000) = 50 - 1000 = -950: stale with dod≠0.
            (3050, f64::from_bits(STALE_NAN), true),
            (4050, 4.0, false),
            (5050, 5.0, false),
        ];

        for s in &samples {
            c.append(0, s.0, s.1);
        }

        let mut it = c.iterator();
        for expected in &samples {
            let Some(sample) = it.next() else {
                panic!("expected more samples");
            };
            assert_eq!(expected.0, sample.timestamp);
            if expected.2 {
                assert!(is_stale_nan(sample.value), "Expected stale NaN at ts={}", sample.timestamp);
            } else {
                assert_eq!(expected.1, sample.value);
            }
        }
    }

    fn test_xor2_large_dod_with_active_st() {
        require_xor2_samples(&[
            Triple { st: 0, t: 0, v: 1.0 },
            Triple { st: 900, t: 1000, v: 2.0 },
            Triple { st: 1000, t: 2000, v: 3.0 },
            Triple { st: 1047576, t: 1050576, v: 4.0 },
        ]);
    }

    fn test_xor2_active_st_fast_path_boundaries() {
        require_xor2_samples(&[
            Triple { st: 0, t: 1000, v: 1.0 },
            Triple { st: 1990, t: 2000, v: 1.0 },
            Triple { st: 2986, t: 3000, v: 1.0 },
            Triple { st: 3954, t: 4000, v: 1.0 },
            Triple { st: 4698, t: 5000, v: 1.0 },
        ]);
    }

    fn test_xor2_active_st_13_bit_dod_value_unchanged_st_delta_branches() {
        require_xor2_samples(&[
            Triple { st: 0, t: 1000, v: 1.0 },
            Triple { st: 0, t: 2000, v: 1.0 },
            Triple { st: 1500, t: 3000, v: 1.0 }, // First ST change: stDiff=500.
            Triple { st: 2500, t: 4001, v: 1.0 }, // Active ST, dod=1, deltaStDiff=0.
            Triple { st: 3497, t: 5003, v: 1.0 }, // Active ST, dod=1, deltaStDiff=4.
            Triple { st: 4467, t: 6004, v: 1.0 }, // Active ST, dod=-1, deltaStDiff=32.
            Triple { st: 5212, t: 7007, v: 1.0 }, // Active ST, dod=2, deltaStDiff=256.
            Triple { st: 5915, t: 8008, v: 1.0 }, // Active ST, dod=-2, deltaStDiff=300.
        ]);
    }

    fn test_xor2_encode_joint_value_unchanged_then_changed() {
        require_xor2_samples(&[
            Triple { st: 0, t: 1000, v: 1.0 },
            Triple { st: 0, t: 2000, v: 2.0 },
            Triple { st: 0, t: 7096, v: 2.0 },
            Triple { st: 0, t: 12192, v: 3.0 },
        ]);
    }

    fn test_xor2_constant_non_zero_st_fast_path() {
        require_xor2_samples(&[
            Triple { st: 500, t: 1000, v: 1.0 },
            Triple { st: 500, t: 2000, v: 2.0 },
            Triple { st: 500, t: 3000, v: 2.0 },
            Triple { st: 500, t: 4050, v: 2.0 },
            Triple { st: 500, t: 5100, v: 3.0 },
        ]);
    }

    fn test_xor2_active_st_dod_zero_value_change() {
        require_xor2_samples(&[
            Triple { st: 0, t: 1000, v: 1.0 },
            Triple { st: 500, t: 2000, v: 2.0 },
            Triple { st: 500, t: 3000, v: 3.0 }, // dod=0, value changed.
            Triple { st: 500, t: 4000, v: 4.0 }, // dod=0, value changed.
        ]);
    }

    #[test]
    fn test_xor2_irregular_timestamps() {
        let mut c = XOR2Chunk::new();

        // Timestamps with dod values spanning multiple encoding ranges.
        let timestamps = vec![
            1000, 2000, 3000,
            // dod in 13-bit range.
            3050, 4050, 5050,
            // dod in 20-bit range (large jitter).
            5050 + 100000, 5050 + 200000, 5050 + 300000,
            // Back to regular.
            5050 + 301000,
        ];

        for ts in &timestamps {
            c.append(0, *ts, 1.0);
        }

        let mut it = c.iterator();
        for expected in &timestamps {
            assert_eq!(Val::Float, it.next());
            let (ts, _) = it.at();
            assert_eq!(*expected, ts);
        }
        assert_eq!(None, it.next());
    }

    #[test]
    fn test_xor2_large_dod() {
        let mut c = XOR2Chunk::new();
        // Force the 64-bit escape path with a very large dod.
        let timestamps = vec![0, 1000, 2000, 2000 + (1 << 20)];
        for ts in &timestamps {
            c.append(0, *ts, 1.0);
        }

        let mut it = c.iterator();
        for expected in &timestamps {
            let sample = it.next();
            assert!(sample.is_some(), "expected more samples");
            assert_eq!(*expected, sample.unwrap().timestamp);
        }
        assert_eq!(None, it.next());
    }

    fn test_xor2_active_st_dod_zero_value_unchanged_zero_st_delta() {
        require_xor2_samples(
            &[
                Triple { st: 0, t: 1000, v: 1.0 },
                Triple { st: 0, t: 2000, v: 1.0 },
                Triple { st: 1500, t: 3000, v: 1.0 }, // First ST change: stDiff=500.
                Triple { st: 2500, t: 4000, v: 1.0 }, // Active ST, dod=0, value unchanged, deltaStDiff=0.
                Triple { st: 3500, t: 5000, v: 1.0 }, // Repeat to keep the path live on consecutive samples.
            ],
        );
    }

    fn test_xor2_active_st_value_change_inline_st_delta_branches() {
        require_xor2_samples(
            &[
                Triple { st: 0, t: 1000, v: 1.0 },
                Triple { st: 0, t: 2000, v: 2.0 },
                Triple { st: 1500, t: 3000, v: 3.0 }, // First ST change: stDiff=500.
                Triple { st: 2500, t: 4000, v: 4.0 }, // Active ST default case, deltaStDiff=0.
                Triple { st: 3497, t: 5000, v: 5.0 }, // Active ST default case, deltaStDiff=3.
                Triple { st: 4466, t: 6000, v: 6.0 }, // Active ST default case, deltaStDiff=31.
                Triple { st: 5211, t: 7000, v: 7.0 }, // Active ST default case, deltaStDiff=255.
            ],
        );
    }

    fn test_xor2_chunk_st(t: &mut Test) {
        test_chunk_st_handling(t, ValType::Float, || {
            Xor2Chunk::new()
        });
    }

    #[test]
    fn test_xor2_chunk_more_than_127_samples() {
        const AFTER_MAX: usize = MAX_FIRST_ST_CHANGE_ON + 3;

        // zero ST
        let mut chunk = XOR2Chunk::new();
        for i in 0..AFTER_MAX {
            chunk.append(0, (i * 10 + 1) as i64, i as f64 * 1.5);
        }

        let mut it = chunk.iterator();
        for i in 0..AFTER_MAX {
            let st = it.at_st();
            let (ts, v) = it.at();
            assert_eq!(0, st);
            assert_eq!(i as i64 * 10 + 1, ts);
            assert_eq!(i as f64 * 1.5, v);
        }

        assert!(it.err().is_none());

        // non-zero ST after 127

        let mut chunk = XOR2Chunk::new();
        for i in 0..AFTER_MAX {
            let mut st = 0;
            if i == AFTER_MAX - 1 {
                st = ((AFTER_MAX - 1) * 10) as i64;
            }
            chunk.append(st, (i * 10 + 1) as i64, i as f64 * 1.5);
        }

        let mut it = chunk.iterator();
        for i in 0..AFTER_MAX {
            let st = it.at_st();
            let (ts, v) = it.at();
            if i == AFTER_MAX - 1 {
                assert_eq!(((AFTER_MAX - 1) * 10) as i64, st);
            } else {
                assert_eq!(0, st);
            }
            assert_eq!((i * 10 + 1) as i64, ts);
            assert_eq!(i as f64 * 1.5, v);
        }

        assert_eq!(None, it.next());
    }



    #[test]
    fn test_xor2_basic() {
        let mut c = XOR2Chunk::new();

        let samples = vec![
            (1000, 1.0),
            (2000, 2.0),
            (3000, 3.0),
            (4000, 4.0),
            (5000, 5.0),
        ];

        for s in &samples {
            c.append(0, s.0, s.1);
        }

        let mut it = c.iterator();
        for expected in &samples {
            let Some(sample) = it.next() else {
                panic!("expected more samples");
            };
            assert_eq!(expected.0, sample.timestamp);
            assert_eq!(expected.1, sample.value);
        }
    }
    #[test]
    fn test_xor2_basic() {
        let mut chunk = XOR2Chunk::new();

        let samples = vec![
            (0, 1000, 1.0),
            (0, 2000, 2.0),
            (0, 3000, 3.0),
            (0, 4000, 4.0),
            (0, 5000, 5.0),
        ];

        for (st, t, v) in &samples {
            chunk.append(*st, *t, *v);
        }
        let mut iterator = chunk.iterator();
        for (expected_st, expected_t, expected_v) in samples {
            let Some(sample) = iterator.next() else {
                panic!("expected more samples");
            };
            assert_eq!(sample.timestamp, expected_t);
            assert_eq!(sample.value, expected_v);
        }
    }

    #[test]
    fn test_xor2_with_staleness() {
        let mut chunk = XOR2Chunk::new();

        let stale_nan_bits = 0x7ff0000000000002;
        let samples = vec![
            (0, 1000, 1.0, false),
            (0, 2000, 2.0, false),
            (0, 3000, f64::from_bits(stale_nan_bits), true),
            (0, 4000, 4.0, false),
            (0, 5000, f64::from_bits(stale_nan_bits), true),
            (0, 6000, 6.0, false),
        ];

        for (st, t, v, _) in &samples {
            chunk.append(*st, *t, *v);
        }

        let mut iterator = chunk.iterator();
        for (expected_st, expected_t, expected_v, is_stale) in samples {
            let Some(sample) = iterator.next() else {
                panic!("expected more samples");
            };
            let ts = sample.timestamp;
            let v = sample.value;
            if is_stale {
                assert!(v.to_bits() == stale_nan_bits);
            } else {
                assert_eq!(v, expected_v);
            }
        }
    }
}