#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::config::SPLIT_FACTOR;
    use crate::error::TsdbError;
    use crate::series::chunks::merge::merge_by_capacity;
    use crate::series::{
        chunks::{Chunk, ChunkEncoding, TimeSeriesChunk},
        DuplicatePolicy, SampleAddResult,
    };
    use crate::tests::generators::DataGenerator;
    use std::time::Duration;

    const CHUNK_TYPES: [ChunkEncoding; 3] = [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
        ChunkEncoding::Pco,
    ];

    fn generate_random_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .interval(Duration::from_millis(1000))
            .build()
            .generate()
    }

    #[test]
    fn test_clear_chunk_with_multiple_samples() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            chunk.clear();

            assert_eq!(chunk.len(), 0);
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_get_range_empty_chunk() {
        for chunk_type in CHUNK_TYPES {
            let chunk = TimeSeriesChunk::new(chunk_type, 100);

            assert!(chunk.is_empty());

            let result = chunk.get_range(0, 100).unwrap();

            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_get_range_single_sample() {
        let sample = Sample {
            timestamp: 10,
            value: 1.0,
        };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

            let result = chunk.get_range(0, 20).unwrap();
            assert_eq!(
                result.len(),
                1,
                "{}: get_range_single_sample - expected 1 sample, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(result[0], sample);

            let empty_result = chunk.get_range(20, 30).unwrap();
            assert!(empty_result.is_empty());
        }
    }

    #[test]
    fn test_get_range_start_equals_end() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let result = chunk.get_range(20, 20).unwrap();
            assert_eq!(
                result.len(),
                1,
                "{}: Expected 1 result, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(
                result[0],
                Sample {
                    timestamp: 20,
                    value: 2.0
                }
            );

            let empty_result = chunk.get_range(15, 15).unwrap();
            assert!(
                empty_result.is_empty(),
                "{}: Expected empty result, got {:?}",
                chunk_type,
                empty_result
            );
        }
    }

    #[test]
    fn test_get_range_full_range() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(
                chunk.len(),
                3,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let result = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                result.len(),
                3,
                "{}: Expected 3 results, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(result, samples);
        }
    }

    #[test]
    fn test_get_range_between_samples() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            let result = chunk.get_range(15, 35).unwrap();
            assert_eq!(
                result.len(),
                2,
                "{}: Expected 2 results, got {}",
                chunk_type,
                result.len()
            );
            assert_eq!(
                result[0],
                Sample {
                    timestamp: 20,
                    value: 2.0
                }
            );
            assert_eq!(
                result[1],
                Sample {
                    timestamp: 30,
                    value: 3.0
                }
            );
        }
    }

    #[test]
    fn test_remove_range_chunk() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 400);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 4);

            chunk.remove_range(20, 30).unwrap();

            assert_eq!(chunk.len(), 2);

            let _ = chunk.remove_range(20, 30).unwrap();
            let current = chunk.get_range(0, 100).unwrap();
            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 40,
                    value: 4.0,
                },
            ];

            assert_eq!(
                current, expected,
                "{chunk_type}: Expected range {:?} after removing [20, 30], got {:?}",
                expected, current
            );
        }
    }

    #[test]
    fn test_remove_range_single_sample() {
        let sample = Sample {
            timestamp: 10,
            value: 1.0,
        };

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            chunk.add_sample(&sample).unwrap();

            assert_eq!(chunk.len(), 1);

            chunk.remove_range(10, 20).unwrap();

            assert_eq!(
                chunk.len(),
                0,
                "{}: Expected 0 results, got {}",
                chunk_type,
                chunk.len()
            );
            assert_eq!(chunk.get_range(0, 100).unwrap(), vec![]);
        }
    }

    #[test]
    fn test_remove_range_same_timestamp() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk.set_data(&samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let removed = chunk.remove_range(10, 10).unwrap();

            assert_eq!(
                removed, 1,
                "{}: Expected 1 result, got {removed}",
                chunk_type
            );
            assert_eq!(
                chunk.get_range(0, 100).unwrap(),
                vec![
                    Sample {
                        timestamp: 20,
                        value: 2.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 3.0
                    }
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_keep_first() {
        let samples = vec![
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
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[Sample {
                    timestamp: 20,
                    value: 5.0,
                }])
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::KeepFirst))
                .unwrap();

            assert_eq!(result.len(), 3);
            assert_eq!(
                chunk.len(),
                4,
                "{chunk_type}: Expected 3 results, got {}",
                chunk.len()
            );

            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 5.0,
                },
                Sample {
                    timestamp: 20,
                    value: 3.0,
                },
                Sample {
                    timestamp: 30,
                    value: 4.0,
                },
            ];

            assert_eq!(chunk.get_range(0, 100).unwrap(), expected,);
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_keep_last() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 25,
                value: 2.5,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[Sample {
                    timestamp: 20,
                    value: 5.0,
                }])
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::KeepLast))
                .unwrap();

            assert_eq!(
                result.len(),
                4,
                "{chunk_type}: Expected 3 results. Found {}",
                result.len()
            );

            assert_eq!(
                chunk.len(),
                4,
                "{chunk_type}: Expected chunk len of 3. Found {}",
                result.len()
            );
            assert_eq!(
                chunk.get_range(0, 100).unwrap(),
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 20,
                        value: 2.0
                    },
                    Sample {
                        timestamp: 25,
                        value: 2.5,
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
            // assert_eq!(blocked.len(), 1);
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_sum() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 1.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 20,
                    value: 5.0,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Sum))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 1.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 8.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_min() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 4.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 15,
                    value: 15.0,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Min))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 4.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_max() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 4.5,
            },
            Sample {
                timestamp: 20,
                value: 3.0,
            },
            Sample {
                timestamp: 30,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .add_sample(&Sample {
                    timestamp: 15,
                    value: 16.5,
                })
                .unwrap();

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Max))
                .unwrap();

            assert_eq!(result.len(), samples.len());
            assert_eq!(
                chunk.len(),
                4,
                "{}: Expected 3 results, got {}",
                chunk_type,
                chunk.len()
            );

            let merged_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                merged_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 16.5,
                    },
                    Sample {
                        timestamp: 20,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 30,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_duplicate_policy_block() {
        let samples = vec![
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

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);

            chunk
                .set_data(&[
                    Sample {
                        timestamp: 20,
                        value: 5.0,
                    },
                    Sample {
                        timestamp: 50,
                        value: 5.0,
                    },
                ])
                .unwrap();

            // First merge should add all samples
            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Block))
                .unwrap();

            assert_eq!(
                result.len(),
                samples.len(),
                "{chunk_type}: Expected {} samples to be merged",
                samples.len()
            );

            let second = result[1];
            assert_eq!(second, SampleAddResult::Duplicate);

            let expected = vec![
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                Sample {
                    timestamp: 20,
                    value: 5.0,
                },
                Sample {
                    timestamp: 30,
                    value: 3.0,
                },
                Sample {
                    timestamp: 50,
                    value: 5.0,
                },
            ];

            let current_samples = chunk.get_range(0, 100).unwrap();
            assert_eq!(current_samples, expected,);
        }
    }

    #[test]
    fn test_merge_samples_outside_range() {
        let samples = vec![
            Sample {
                timestamp: 5,
                value: 1.0,
            },
            Sample {
                timestamp: 15,
                value: 2.0,
            },
            Sample {
                timestamp: 25,
                value: 3.0,
            },
            Sample {
                timestamp: 35,
                value: 4.0,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 100);
            chunk
                .set_data(&[
                    Sample {
                        timestamp: 10,
                        value: 0.0,
                    },
                    Sample {
                        timestamp: 20,
                        value: 0.0,
                    },
                ])
                .unwrap();

            assert_eq!(chunk.len(), 2);
            assert_eq!(chunk.first_timestamp(), 10);
            assert_eq!(chunk.last_timestamp(), 20);

            let result = chunk
                .merge_samples(&samples, Some(DuplicatePolicy::Block))
                .unwrap();

            assert_eq!(
                result.len(),
                samples.len(),
                "{chunk_type}: Expected 2 results, got {}",
                result.len()
            );
            assert_eq!(chunk.len(), 6);
            assert_eq!(
                chunk.first_timestamp(),
                5,
                "{chunk_type}: expected first timestamp 5, got {}",
                chunk.first_timestamp()
            );
            assert_eq!(
                chunk.last_timestamp(),
                35,
                "{chunk_type}: expected last timestamp 35, got {}",
                chunk.last_timestamp()
            );

            let range = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                range,
                vec![
                    Sample {
                        timestamp: 5,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 10,
                        value: 0.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 2.0,
                    },
                    Sample {
                        timestamp: 20,
                        value: 0.0
                    },
                    Sample {
                        timestamp: 25,
                        value: 3.0,
                    },
                    Sample {
                        timestamp: 35,
                        value: 4.0
                    },
                ]
            );
        }
    }

    #[test]
    fn test_merge_samples_with_mixed_timestamps() {
        let existing_samples = vec![
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

        let new_samples = vec![
            Sample {
                timestamp: 15,
                value: 1.5,
            },
            Sample {
                timestamp: 20,
                value: 2.5,
            },
            Sample {
                timestamp: 25,
                value: 2.5,
            },
            Sample {
                timestamp: 35,
                value: 3.5,
            },
        ];

        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 1024);
            chunk.set_data(&existing_samples).unwrap();

            assert_eq!(chunk.len(), 3);

            let result = chunk
                .merge_samples(&new_samples, Some(DuplicatePolicy::KeepLast))
                .unwrap();

            assert_eq!(
                result.len(),
                new_samples.len(),
                "{chunk_type}: Expected {} results, got {}",
                new_samples.len(),
                result.len()
            );

            assert_eq!(
                chunk.len(),
                6,
                "{chunk_type}: expected 6 results, found {}",
                chunk.len()
            );

            let all_samples = chunk.get_range(0, 40).unwrap();
            assert_eq!(
                all_samples,
                vec![
                    Sample {
                        timestamp: 10,
                        value: 1.0
                    },
                    Sample {
                        timestamp: 15,
                        value: 1.5
                    },
                    Sample {
                        timestamp: 20,
                        value: 2.5
                    },
                    Sample {
                        timestamp: 25,
                        value: 2.5
                    },
                    Sample {
                        timestamp: 30,
                        value: 3.0
                    },
                    Sample {
                        timestamp: 35,
                        value: 3.5
                    },
                ]
            );
        }
    }

    const ELEMENTS_PER_CHUNK: usize = 60;

    fn saturate_chunk(chunk: &mut TimeSeriesChunk) {
        loop {
            let samples = generate_random_samples(100);
            match chunk.merge_samples(&samples, Some(DuplicatePolicy::KeepLast)) {
                Ok(_) => {}
                Err(TsdbError::CapacityFull(_)) => break,
                Err(e) => panic!("unexpected error: {:?}", e),
            }
            let threshold = (chunk.max_size() as f64 * SPLIT_FACTOR) as usize;
            if chunk.size() > threshold {
                break;
            }
        }
    }

    #[test]
    fn test_merge_by_capacity_with_empty_source_chunk() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Ensure the source chunk is empty
        assert!(src_chunk.is_empty());

        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        );

        assert_eq!(result, Ok(None));
    }

    #[test]
    fn test_merge_by_capacity_partial_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        let capacity = dest_chunk.estimate_remaining_sample_capacity();

        // Fill the destination chunk to have more than a quarter but less than full capacity of source
        let dest_samples = generate_random_samples(capacity / 2);
        dest_chunk.set_data(&dest_samples).unwrap();

        // Fill the source chunk with samples
        saturate_chunk(&mut src_chunk);

        let saved_count = src_chunk.len();

        // Perform the merge
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        )
        .unwrap();

        // Check that a partial merge occurred
        assert!(result.is_some());

        let count_merged = dest_chunk.len() + src_chunk.len();

        // Verify that the source chunk still contains the remaining samples
        assert_eq!(count_merged, saved_count);
    }

    #[test]
    fn test_merge_by_capacity_with_empty_destination() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Add some samples to the source chunk
        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
            Sample {
                timestamp: 300,
                value: 3.0,
            },
        ];
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk is empty
        assert!(dest_chunk.is_empty());

        // Perform the merge
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            0,
            Some(DuplicatePolicy::KeepLast),
        );

        // Verify the merge result
        assert_eq!(result.unwrap(), Some(samples.len()));
        assert!(src_chunk.is_empty());
        assert_eq!(dest_chunk.len(), samples.len());

        // Verify the samples in the destination chunk
        let merged_samples = dest_chunk.get_range(100, 300).unwrap();
        assert_eq!(merged_samples, samples);
    }

    #[test]
    fn test_merge_by_capacity_clears_source_after_full_merge() {
        let mut dest_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);
        let mut src_chunk = TimeSeriesChunk::new(ChunkEncoding::Uncompressed, 1024);

        // Populate source chunk with samples
        let samples = generate_random_samples(ELEMENTS_PER_CHUNK);
        src_chunk.set_data(&samples).unwrap();

        // Ensure destination chunk has enough capacity for a full merge
        let remaining_capacity = dest_chunk.estimate_remaining_sample_capacity();
        assert!(remaining_capacity >= ELEMENTS_PER_CHUNK);

        // Perform the merge
        let min_timestamp = samples[0].timestamp;
        let result = merge_by_capacity(
            &mut dest_chunk,
            &mut src_chunk,
            min_timestamp,
            Some(DuplicatePolicy::KeepLast),
        );

        // Verify the result
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ELEMENTS_PER_CHUNK));
        assert!(src_chunk.is_empty());
    }

    #[test]
    fn test_iter_all() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(2500);
            chunk.set_data(&samples).unwrap();

            let actual_samples = chunk.iter().collect::<Vec<Sample>>();

            assert_eq!(
                samples.len(),
                actual_samples.len(),
                "{} : expected samples len {}, got {}",
                chunk_type,
                samples.len(),
                actual_samples.len()
            );
            assert_eq!(samples, actual_samples);
        }
    }

    #[test]
    fn test_samples_by_timestamps() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(100);

            for sample in samples.iter() {
                chunk.add_sample(sample).unwrap();
            }

            // Test with a subset of timestamps
            let timestamps: Vec<_> = samples.iter().map(|s| s.timestamp).collect();
            let selected_timestamps = &timestamps[10..20];
            let expected_samples: Vec<_> = samples[10..20].to_vec();

            let result_samples = chunk.samples_by_timestamps(selected_timestamps).unwrap();
            assert_eq!(result_samples, expected_samples);

            // Test with timestamps that are not present
            let missing_timestamps = vec![-2000, -3000, -4000];
            let result_samples = chunk.samples_by_timestamps(&missing_timestamps).unwrap();
            assert!(result_samples.is_empty());

            // Test with an empty timestamp list
            let result_samples = chunk.samples_by_timestamps(&[]).unwrap();
            assert!(result_samples.is_empty());
        }
    }

    #[test]
    fn test_samples_by_timestamps_partial_overlap() {
        for chunk_type in CHUNK_TYPES {
            let mut chunk = TimeSeriesChunk::new(chunk_type, 16384);
            let samples = generate_random_samples(100);

            for sample in samples.iter() {
                chunk.add_sample(sample).unwrap();
            }

            // Test with a mix of present and absent timestamps
            let timestamps = vec![
                samples[5].timestamp,
                2000, // not present
                samples[15].timestamp,
            ];
            let expected_samples = vec![samples[5], samples[15]];

            let result_samples = chunk.samples_by_timestamps(&timestamps).unwrap();
            assert_eq!(result_samples, expected_samples);
        }
    }
}
