#[cfg(test)]
mod tests {
    use crate::aggregators::{AggregationOptions, Aggregator, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::join::join_handler::join_internal;
    use crate::join::{create_join_iter, JoinOptions, JoinReducer, JoinResultType, JoinType};
    use joinkit::EitherOrBoth;

    fn create_basic_samples() -> (Vec<Sample>, Vec<Sample>) {
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(15, 10.0),
            Sample::new(20, 20.0),
            Sample::new(25, 30.0),
        ];
        (left, right)
    }

    fn create_basic_options() -> JoinOptions {
        JoinOptions {
            join_type: JoinType::Inner,
            reducer: None,
            aggregation: None,
            count: None,
            date_range: Default::default(),
            timestamp_filter: None,
            value_filter: None,
        }
    }

    #[test]
    fn test_join_inner_no_reducer() {
        let (left, right) = create_basic_samples();
        let options = create_basic_options();

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Only timestamp 20 exists in both series
            assert_eq!(values[0].timestamp, 20);
            match values[0].value {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l, 2.0);
                    assert_eq!(r, 20.0);
                }
                _ => panic!("Expected Both values"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All left timestamps

            // Check first value (left-only)
            assert_eq!(values[0].timestamp, 10);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 1.0);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            // Check second value (both)
            assert_eq!(values[1].timestamp, 20);
            match values[1].value {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l, 2.0);
                    assert_eq!(r, 20.0);
                }
                _ => panic!("Expected Both values for timestamp 20"),
            }

            // Check third value (left-only)
            assert_eq!(values[2].timestamp, 30);
            match values[2].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 3.0);
                }
                _ => panic!("Expected Left value for timestamp 30"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_right_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Right;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All right timestamps

            // Check timestamps and values
            assert_eq!(values[0].timestamp, 15);
            assert_eq!(values[1].timestamp, 20);
            assert_eq!(values[2].timestamp, 25);

            // Check the middle value (has both left and right)
            match values[1].value {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l, 2.0);
                    assert_eq!(r, 20.0);
                }
                _ => panic!("Expected Both values for timestamp 20"),
            }

            // Check the other values (right-only)
            match values[0].value {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r, 10.0);
                }
                _ => panic!("Expected Right value for timestamp 15"),
            }

            match values[2].value {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r, 30.0);
                }
                _ => panic!("Expected Right value for timestamp 25"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_full_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Full;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 5); // All unique timestamps from both series

            // Verify timestamps are in order
            assert_eq!(values[0].timestamp, 10);
            assert_eq!(values[1].timestamp, 15);
            assert_eq!(values[2].timestamp, 20);
            assert_eq!(values[3].timestamp, 25);
            assert_eq!(values[4].timestamp, 30);

            // Check value types at each timestamp
            match values[0].value {
                EitherOrBoth::Left(_) => (),
                _ => panic!("Expected Left value for timestamp 10"),
            }

            match values[1].value {
                EitherOrBoth::Right(_) => (),
                _ => panic!("Expected Right value for timestamp 15"),
            }

            match values[2].value {
                EitherOrBoth::Both(_, _) => (),
                _ => panic!("Expected Both values for timestamp 20"),
            }

            match values[3].value {
                EitherOrBoth::Right(_) => (),
                _ => panic!("Expected Right value for timestamp 25"),
            }

            match values[4].value {
                EitherOrBoth::Left(_) => (),
                _ => panic!("Expected Left value for timestamp 30"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 1); // Only timestamp 20 exists in both series
            assert_eq!(samples[0].timestamp, 20);
            assert_eq!(samples[0].value, 22.0); // 2.0 + 20.0 = 22.0
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_reducer_left_join() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 3); // All left timestamps

            // Check values - NaN for missing right values
            assert_eq!(samples[0].timestamp, 10);
            assert!(samples[0].value.is_nan()); // 1.0 + NaN = NaN

            assert_eq!(samples[1].timestamp, 20);
            assert_eq!(samples[1].value, 22.0); // 2.0 + 20.0 = 22.0

            assert_eq!(samples[2].timestamp, 30);
            assert!(samples[2].value.is_nan()); // 3.0 + NaN = NaN
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_reducer_and_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.reducer = Some(JoinReducer::Sum);
        options.aggregation = Some(AggregationOptions {
            aggregator: Aggregator::Sum(Default::default()),
            bucket_duration: 15,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            // With bucket size 15, we should get buckets [15-30), [30-45)
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp, 15);
            assert_eq!(samples[0].value, 22.0); // Only value is 22.0 from timestamp 20
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_with_count_limit() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Full;
        options.count = Some(3);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // Limited to 3 values
            assert_eq!(values[0].timestamp, 10);
            assert_eq!(values[1].timestamp, 15);
            assert_eq!(values[2].timestamp, 20);
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_with_different_reducer_operations() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();

        // Test Plus reducer
        options.reducer = Some(JoinReducer::Sum);
        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 22.0); // 2.0 + 20.0 = 22.0
        }

        // Test Minus reducer
        options.reducer = Some(JoinReducer::Sub);
        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, -18.0); // 2.0 - 20.0 = -18.0
        }

        // Test Mul reducer
        options.reducer = Some(JoinReducer::Mul);
        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 40.0); // 2.0 * 20.0 = 40.0
        }

        // Test Div reducer
        options.reducer = Some(JoinReducer::Div);
        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples[0].value, 0.1); // 2.0 / 20.0 = 0.1
        }
    }

    #[test]
    fn test_join_empty_inputs() {
        let left: Vec<Sample> = vec![];
        let right: Vec<Sample> = vec![];
        let options = create_basic_options();

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0);
        } else {
            panic!("Expected empty Values result type");
        }
    }

    #[test]
    fn test_join_empty_left_input() {
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];
        let mut options = create_basic_options();
        // right outer join
        options.join_type = JoinType::Right;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);

            // All should be right-only values
            for value in values {
                match value.value {
                    EitherOrBoth::Right(_) => (),
                    _ => panic!("Expected only Right values"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_empty_right_input() {
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];
        let mut options = create_basic_options();
        options.join_type = JoinType::Left;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);

            // All should be left-only values
            for value in values {
                match value.value {
                    EitherOrBoth::Left(_) => (),
                    _ => panic!("Expected only Left values"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_exclusive_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::LeftExclusive;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // All left timestamps except the matching one at 20

            // Check first value (left-only)
            assert_eq!(values[0].timestamp, 10);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 1.0);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            // Check second value (left-only)
            assert_eq!(values[1].timestamp, 30);
            match values[1].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 3.0);
                }
                _ => panic!("Expected Left value for timestamp 30"),
            }

            // The timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_right_exclusive_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::RightExclusive;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // All right timestamps except the matching one at 20

            // Check timestamps and values
            assert_eq!(values[0].timestamp, 15);
            assert_eq!(values[1].timestamp, 25);

            // Check the right-only values
            match values[0].value {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r, 10.0);
                }
                _ => panic!("Expected Right value for timestamp 15"),
            }

            match values[1].value {
                EitherOrBoth::Right(r) => {
                    assert_eq!(r, 30.0);
                }
                _ => panic!("Expected Right value for timestamp 25"),
            }

            // The timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_exclusive_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::LeftExclusive;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 2); // All left timestamps except the matching one at 20

            // Values should be NaN since there are no right values to add
            assert_eq!(samples[0].timestamp, 10);
            assert!(samples[0].value.is_nan()); // 1.0 + NaN = NaN

            assert_eq!(samples[1].timestamp, 30);
            assert!(samples[1].value.is_nan()); // 3.0 + NaN = NaN

        // The timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_right_exclusive_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::RightExclusive;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 2); // All right timestamps except the matching one at 20

            // Values should be NaN since there are no left values to add
            assert_eq!(samples[0].timestamp, 15);
            assert!(samples[0].value.is_nan()); // NaN + 10.0 = NaN

            assert_eq!(samples[1].timestamp, 25);
            assert!(samples[1].value.is_nan()); // NaN + 30.0 = NaN

        // The timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_exclusive_with_non_matching_samples() {
        // Test when no samples match between left and right
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(30, 3.0),
            Sample::new(50, 5.0),
        ];
        let right = vec![
            Sample::new(20, 2.0),
            Sample::new(40, 4.0),
            Sample::new(60, 6.0),
        ];

        // Test LeftExclusive - should return all left samples
        let mut options = create_basic_options();
        options.join_type = JoinType::LeftExclusive;

        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All left samples should be returned
            for (i, value) in values.iter().enumerate() {
                match value.value {
                    EitherOrBoth::Left(l) => {
                        assert_eq!(l, left[i].value);
                    }
                    _ => panic!("Expected Left value"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }

        // Test RightExclusive - should return all right samples
        let mut options = create_basic_options();
        options.join_type = JoinType::RightExclusive;

        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All right samples should be returned
            for (i, value) in values.iter().enumerate() {
                match value.value {
                    EitherOrBoth::Right(r) => {
                        assert_eq!(r, right[i].value);
                    }
                    _ => panic!("Expected Right value"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_exclusive_with_empty_inputs() {
        // Test with empty left input for LeftExclusive
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::LeftExclusive;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No left samples, so no results
        } else {
            panic!("Expected Values result type");
        }

        // Test with empty right input for RightExclusive
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];

        let mut options = create_basic_options();
        options.join_type = JoinType::RightExclusive;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No right samples, so no results
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_left_exclusive_with_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::LeftExclusive;
        options.reducer = Some(JoinReducer::Sum);
        options.aggregation = Some(AggregationOptions {
            aggregator: Aggregator::Sum(Default::default()),
            bucket_duration: 35, // Bucket size 35 to combine samples at 10 and 30
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let join_iter = create_join_iter(left.clone(), right.clone(), options.join_type);
        let items = join_iter.collect::<Vec<_>>();

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            // Should have one bucket [0-35) with NaN value
            // (since both values within bucket are NaN from not having matching right values)
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp, 0);
            assert!(samples[0].value.is_nan());
        } else {
            panic!("Expected Samples result type");
        }
    }
}
