#[cfg(test)]
mod tests {
    use crate::aggregators::{Aggregator, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use crate::join::join_handler::join_internal;
    use crate::join::{JoinOptions, JoinReducer, JoinResultType, JoinType};
    use crate::series::request_types::AggregationOptions;
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
            assert_eq!(values.len(), 3); // All the left timestamps

            // Check the first value (left-only)
            assert_eq!(values[0].timestamp, 10);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 1.0);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            // Check the second value (both)
            assert_eq!(values[1].timestamp, 20);
            match values[1].value {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l, 2.0);
                    assert_eq!(r, 20.0);
                }
                _ => panic!("Expected Both values for timestamp 20"),
            }

            // Check the third value (left-only)
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
            assert_eq!(samples.len(), 3); // All the left values

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
            assert_eq!(samples[0].value, 22.0); // The only value is 22.0 from timestamp 20
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

    /// SEMI JOIN
    #[test]
    fn test_join_semi_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Only timestamp 20 exists in both series
            assert_eq!(values[0].timestamp, 20);

            // In a semi-join, only left values are returned for matches
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 2.0);
                }
                _ => panic!("Expected Left value for timestamp 20"),
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 1); // Only timestamp 20 exists in both series
            assert_eq!(samples[0].timestamp, 20);

            // With a reducer, the left value is combined with NaN (since semi join only returns left values)
            assert!(samples[0].value.is_nan()); // 2.0 + NaN = NaN
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_semi_with_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.reducer = Some(JoinReducer::Sum);
        options.aggregation = Some(AggregationOptions {
            aggregator: Aggregator::Sum(Default::default()),
            bucket_duration: 30,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            // With bucket size 30, we expect one bucket [0-30) containing timestamp 20
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp, 0);

            // Since the semi-join with reducer produces NaN, the aggregation will also be NaN
            assert!(samples[0].value.is_nan());
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_semi_no_matches() {
        // Create datasets with no matching timestamps
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

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No matching timestamps, so no results
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_multiple_matches() {
        // Create datasets with multiple matching timestamps
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
            Sample::new(50, 50.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Two matching timestamps: 20 and 30

            // Check first match
            assert_eq!(values[0].timestamp, 20);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 2.0);
                }
                _ => panic!("Expected Left value for timestamp 20"),
            }

            // Check the second match
            assert_eq!(values[1].timestamp, 30);
            match values[1].value {
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
    fn test_join_semi_empty_inputs() {
        // Test with empty left input
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No left samples, so no results
        } else {
            panic!("Expected Values result type");
        }

        // Test with empty right input
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No right samples to match with, so no results
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_with_count_limit() {
        // Create datasets with multiple matching timestamps
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
            Sample::new(40, 40.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;
        options.count = Some(2); // Limit to first 2 results

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Limited to 2 results
            assert_eq!(values[0].timestamp, 20);
            assert_eq!(values[1].timestamp, 30);
            // 40 is also a match but should be excluded due to the count limit
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_semi_vs_inner_join() {
        // Check that semi-join returns only left values for matching timestamps,
        // unlike inner join which returns both left and right values
        let left = vec![Sample::new(20, 2.0)];
        let right = vec![Sample::new(20, 20.0)];

        // Test semi-join
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 2.0);
                }
                _ => panic!("Expected Left value for semi join"),
            }
        }

        // Test inner join for comparison
        let mut options = create_basic_options();
        options.join_type = JoinType::Inner;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            match values[0].value {
                EitherOrBoth::Both(l, r) => {
                    assert_eq!(l, 2.0);
                    assert_eq!(r, 20.0);
                }
                _ => panic!("Expected Both values for inner join"),
            }
        }
    }

    #[test]
    fn test_join_semi_vs_anti_join() {
        // Create datasets to compare semi and anti joins
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        // Test semi-join (should return rows where timestamps match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].timestamp, 20); // Only timestamp 20 matches
        }

        // Test anti-join (should return rows where timestamps don't match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].timestamp, 10); // Timestamps 10 and 30 don't match
            assert_eq!(values[1].timestamp, 30);
        }
    }

    // ANTI JOIN
    #[test]
    fn test_join_anti_no_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // Timestamps 10 and 30 from left don't exist in right

            // Check the first value
            assert_eq!(values[0].timestamp, 10);
            match values[0].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 1.0);
                }
                _ => panic!("Expected Left value for timestamp 10"),
            }

            // Check the second value
            assert_eq!(values[1].timestamp, 30);
            match values[1].value {
                EitherOrBoth::Left(l) => {
                    assert_eq!(l, 3.0);
                }
                _ => panic!("Expected Left value for timestamp 30"),
            }

            // Timestamp 20 should not be present since it exists in both left and right
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_with_reducer() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.reducer = Some(JoinReducer::Sum);

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            assert_eq!(samples.len(), 2); // Timestamps 10 and 30 from the left don't exist in right

            // For anti-join with reducer, it applies the reducer function with NaN for the missing right value
            assert_eq!(samples[0].timestamp, 10);
            assert!(samples[0].value.is_nan()); // 1.0 + NaN = NaN

            assert_eq!(samples[1].timestamp, 30);
            assert!(samples[1].value.is_nan()); // 3.0 + NaN = NaN
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_anti_with_aggregation() {
        let (left, right) = create_basic_samples();
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.reducer = Some(JoinReducer::Sum);
        options.aggregation = Some(AggregationOptions {
            aggregator: Aggregator::Sum(Default::default()),
            bucket_duration: 25, // Bucket size to group timestamps 10 and 30
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        });

        let result = join_internal(left, right, &options);

        if let JoinResultType::Samples(samples) = result {
            // With bucket size 25, we should get two buckets: [0-25) and [25-50)
            assert_eq!(samples.len(), 2);

            // First bucket contains timestamp 10
            assert_eq!(samples[0].timestamp, 0);
            assert!(samples[0].value.is_nan()); // NaN from timestamp 10

            // The second bucket contains timestamp 30
            assert_eq!(samples[1].timestamp, 25);
            assert!(samples[1].value.is_nan()); // NaN from timestamp 30
        } else {
            panic!("Expected Samples result type");
        }
    }

    #[test]
    fn test_join_anti_no_matches() {
        // Test when all left timestamps match right timestamps
        // In this case, anti-join should return no rows
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(10, 10.0),
            Sample::new(20, 20.0),
            Sample::new(30, 30.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No rows should be returned
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_all_matches() {
        // Test when no left timestamps match right timestamps
        // In this case, anti-join should return all left rows
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![
            Sample::new(15, 15.0),
            Sample::new(25, 25.0),
            Sample::new(35, 35.0),
        ];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 3); // All left rows should be returned

            // Check all timestamps and values
            assert_eq!(values[0].timestamp, 10);
            assert_eq!(values[1].timestamp, 20);
            assert_eq!(values[2].timestamp, 30);

            // Verify all values are from the left side
            for value in values {
                match value.value {
                    EitherOrBoth::Left(_) => (),
                    _ => panic!("Expected Left value"),
                }
            }
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_empty_inputs() {
        // Test with empty left input
        let left: Vec<Sample> = vec![];
        let right = vec![Sample::new(15, 10.0), Sample::new(20, 20.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 0); // No left samples, so no results
        } else {
            panic!("Expected Values result type");
        }

        // Test with empty right input
        // Anti join should return all left rows when right is empty
        let left = vec![Sample::new(10, 1.0), Sample::new(20, 2.0)];
        let right: Vec<Sample> = vec![];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2); // All left samples should be returned
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_with_count_limit() {
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
            Sample::new(40, 4.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;
        options.count = Some(1); // Limit to the first result only

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1); // Limited to 1 result
            assert_eq!(values[0].timestamp, 10); // Only the first non-matching timestamp
        } else {
            panic!("Expected Values result type");
        }
    }

    #[test]
    fn test_join_anti_vs_semi_join() {
        // Create datasets to compare anti and semi joins
        let left = vec![
            Sample::new(10, 1.0),
            Sample::new(20, 2.0),
            Sample::new(30, 3.0),
        ];
        let right = vec![Sample::new(20, 20.0), Sample::new(40, 40.0)];

        // Test anti-join (should return rows where timestamps don't match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left.clone(), right.clone(), &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].timestamp, 10); // Timestamps 10 and 30 don't match
            assert_eq!(values[1].timestamp, 30);
        }

        // Test semi-join (should return rows where timestamps match)
        let mut options = create_basic_options();
        options.join_type = JoinType::Semi;

        let result = join_internal(left, right, &options);
        if let JoinResultType::Values(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].timestamp, 20); // Only timestamp 20 matches
        }
    }
    
    // #[test]
    // fn test_join_anti_different_reducer_operations() {
    //     let left = vec![Sample::new(10, 5.0)];
    //     let right: Vec<Sample> = vec![]; // Empty right to ensure all left rows are included
    //
    //     // Test with different reducers to ensure they handle NaN correctly
    //     let reducers = [
    //         JoinReducer::Sum, JoinReducer::Sub, JoinReducer::Mul, JoinReducer::Div,
    //         JoinReducer::Max, JoinReducer::Min, JoinReducer::Avg
    //     ];
    //
    //     for reducer in reducers {
    //         let mut options = create_basic_options();
    //         options.join_type = JoinType::Anti;
    //         options.reducer = Some(reducer);
    //
    //         let result = join_internal(left.clone(), right.clone(), &options);
    //
    //         if let JoinResultType::Samples(samples) = result {
    //             assert_eq!(samples.len(), 1);
    //             assert_eq!(samples[0].timestamp, 10);
    //
    //             // For any reducer operation with NaN as one operand, the result should be NaN
    //             assert!(samples[0].value.is_nan(),
    //                     "Expected NaN result for reducer {:?}, got {}", reducer, samples[0].value);
    //         } else {
    //             panic!("Expected Samples result type");
    //         }
    //     }
    // }

    #[test]
    fn test_join_anti_large_datasets() {
        // Test with larger datasets to verify performance and correctness
        let mut left = Vec::with_capacity(100);
        let mut right = Vec::with_capacity(50);

        // Create left samples with timestamps 0, 2, 4, ..., 198
        for i in 0..100 {
            left.push(Sample::new(i * 2, i as f64));
        }

        // Create right samples with timestamps 0, 4, 8, ..., 196
        for i in 0..50 {
            right.push(Sample::new(i * 4, (i * 10) as f64));
        }

        let mut options = create_basic_options();
        options.join_type = JoinType::Anti;

        let result = join_internal(left, right, &options);

        if let JoinResultType::Values(values) = result {
            // Anti-join should return left rows with timestamps that are not multiples of 4
            // So we expect timestamps 2, 6, 10, ..., 198 (50 values)
            assert_eq!(values.len(), 50);

            // Verify that all returned timestamps are not multiples of 4
            for value in values {
                assert_eq!(
                    value.timestamp % 4,
                    2,
                    "Timestamp {} should not be a multiple of 4",
                    value.timestamp
                );
            }
        } else {
            panic!("Expected Values result type");
        }
    }
}
