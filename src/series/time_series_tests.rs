#[cfg(test)]
mod tests {
    use crate::common::{Sample, Timestamp};
    use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
    use crate::error::TsdbError;
    use crate::series::chunks::{Chunk, GorillaChunk, TimeSeriesChunk};
    use crate::series::TimeSeries;
    use std::time::Duration;

    fn create_chunk(size: Option<usize>) -> TimeSeriesChunk {
        TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(size.unwrap_or(1024)))
    }

    fn create_chunk_with_samples(samples: Vec<Sample>) -> TimeSeriesChunk {
        let mut chunk = create_chunk(None);
        for sample in samples {
            chunk.add_sample(&sample).unwrap();
        }
        chunk
    }
    
    fn create_chunk_with_timestamps(start: Timestamp, end: Timestamp) -> TimeSeriesChunk {
        let mut chunk = create_chunk(None);
        for ts in start..=end {
            chunk.add_sample(&Sample { timestamp: ts, value: 1.0 + ts as f64 }).unwrap();
        }
        chunk
    }

    #[test]
    fn test_samples_by_timestamps_exact_match_one_chunk() {
        // Setup a TimeSeries instance with a single chunk containing specific timestamps
        let mut time_series = TimeSeries::default();
        let mut chunk = TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(4096));
        let timestamps = vec![1000, 2000, 3000];
        for &ts in &timestamps {
            chunk.add_sample(&Sample { timestamp: ts, value: 1.0 }).unwrap();
        }
        time_series.chunks.push(chunk);

        let mut chunk = TimeSeriesChunk::Gorilla(GorillaChunk::with_max_size(4096));
        chunk.add_sample(&Sample { timestamp: 4000, value: 1.0 }).unwrap();
        time_series.chunks.push(chunk);
        
        time_series.update_state_from_chunks();
        
        // Define the timestamps to fetch, which match exactly one chunk
        let fetch_timestamps = vec![1000, 2000, 3000];
        
        let result = time_series.samples_by_timestamps(&fetch_timestamps);

        // Verify the results
        assert!(result.is_ok());
        let samples = result.unwrap();
        assert_eq!(samples.len(), fetch_timestamps.len());
        for (i, sample) in samples.iter().enumerate() {
            assert_eq!(sample.timestamp, fetch_timestamps[i]);
            assert_eq!(sample.value, 1.0);
        }
    }

    #[test]
    fn test_samples_by_timestamps_multiple_chunks() {
        // Setup a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();

        // Assume create_chunk_with_samples is a helper function to create a chunk with given samples
        let chunk1 = create_chunk_with_samples(vec![
            Sample { timestamp: 100, value: 1.0 },
            Sample { timestamp: 200, value: 2.0 },
        ]);
        let chunk2 = create_chunk_with_samples(vec![
            Sample { timestamp: 300, value: 3.0 },
            Sample { timestamp: 400, value: 4.0 },
        ]);

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.update_state_from_chunks();

        // Timestamps that match samples across multiple chunks
        let timestamps = vec![100, 300, 400];

        // Fetch samples by timestamps
        let result = time_series.samples_by_timestamps(&timestamps).unwrap();

        // Expected samples
        let expected_samples = vec![
            Sample { timestamp: 100, value: 1.0 },
            Sample { timestamp: 300, value: 3.0 },
            Sample { timestamp: 400, value: 4.0 },
        ];

        // Assert that the fetched samples match the expected samples
        assert_eq!(result, expected_samples);
    }

    #[test]
    fn test_samples_by_timestamps_no_matching_timestamps() {
        // Create a TimeSeries instance with no chunks
        let time_series = TimeSeries::default();

        // Define a set of timestamps that do not match any chunk
        let timestamps = vec![100, 200, 300];

        // Call the samples_by_timestamps method
        let result = time_series.samples_by_timestamps(&timestamps);

        // Assert that the result is an empty vector
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_samples_by_timestamps_with_duplicates_in_same_chunk() {
        // Setup a TimeSeries instance with a single chunk containing duplicate timestamps
        let mut time_series = TimeSeries::default();
        let timestamp = 1000;
        let sample1 = Sample { timestamp, value: 1.0 };
        let sample2 = Sample { timestamp, value: 2.0 };

        // Add samples to the time series
        time_series.add_sample_internal(sample1);
        time_series.add_sample_internal(sample2);

        // Request samples by timestamps, including duplicates
        let timestamps = vec![timestamp, timestamp];
        let result = time_series.samples_by_timestamps(&timestamps).unwrap();

        // Verify that the result contains both samples with the duplicate timestamp
        assert_eq!(result.len(), 2);
        assert!(result.contains(&sample1));
        assert!(result.contains(&sample2));
    }

    #[test]
    fn test_samples_by_timestamps_across_multiple_chunks() {
        // Setup a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();
        
        let chunk1 = create_chunk(None);
        let chunk2 = create_chunk(None);
        let chunk3 = create_chunk(None);

        // Add chunks to the time series
        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.chunks.push(chunk3);

        // Add samples to each chunk
        time_series.chunks[0].add_sample(&Sample { timestamp: 1, value: 10.0 }).unwrap();
        time_series.chunks[1].add_sample(&Sample { timestamp: 2, value: 20.0 }).unwrap();
        time_series.chunks[2].add_sample(&Sample { timestamp: 3, value: 30.0 }).unwrap();
        time_series.total_samples = 3;

        // Define timestamps to fetch
        let timestamps = vec![1, 2, 3];

        // Fetch samples by timestamps
        let result = time_series.samples_by_timestamps(&timestamps);

        // Verify the result
        assert!(result.is_ok());
        let samples = result.unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].timestamp, 1);
        assert_eq!(samples[0].value, 10.0);
        assert_eq!(samples[1].timestamp, 2);
        assert_eq!(samples[1].value, 20.0);
        assert_eq!(samples[2].timestamp, 3);
        assert_eq!(samples[2].value, 30.0);
    }

    #[test]
    fn test_trim_on_empty_timeseries() {
        let mut timeseries = TimeSeries::new();
        let result = timeseries.trim();
        assert_eq!(result.unwrap(), 0);
        assert_eq!(timeseries.total_samples, 0);
        assert!(timeseries.chunks.is_empty());
    }

    #[test]
    fn test_trim_remove_range_error() {
        // Setup a TimeSeries with a chunk that will cause remove_range to fail
        let mut time_series = TimeSeries::default();
        let mut chunk = TimeSeriesChunk::new(Default::default(), DEFAULT_CHUNK_SIZE_BYTES);

        // Assuming remove_range will fail if the range is invalid, we simulate this by adding a sample
        // with a timestamp that will not be removed by the range, causing an error.
        let sample = Sample { timestamp: 100, value: 1.0 };
        chunk.add_sample(&sample).unwrap();
        time_series.chunks.push(chunk);

        // Set the retention so that the min_timestamp is greater than the sample's timestamp
        time_series.retention = Duration::from_secs(1);
        time_series.last_sample = Some(sample);

        // Attempt to trim, expecting an error due to remove_range failure
        let result = time_series.trim();

        // Assert that the result is an error
        assert!(matches!(result, Err(TsdbError::RemoveRangeError)));
    }

    #[test]
    fn test_trim_all_chunks_before_min_timestamp() {
        let mut time_series = TimeSeries::new();
        let mut chunk1 = create_chunk(None);
        let mut chunk2 = create_chunk(None);

        // Add samples to chunks such that they are all before the min_timestamp
        let sample1 = Sample { timestamp: 10, value: 1.0 };
        let sample2 = Sample { timestamp: 20, value: 2.0 };
        chunk1.add_sample(&sample1).unwrap();
        chunk2.add_sample(&sample2).unwrap();

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        
        time_series.update_state_from_chunks();

        // Set retention so that min_timestamp is greater than any sample timestamp
        time_series.retention = Duration::from_millis(30);

        // Perform the trim operation
        let deleted_count = time_series.trim().unwrap();

        // Check that all chunks are removed
        assert_eq!(deleted_count, 2);
        assert!(time_series.chunks.is_empty());
        assert_eq!(time_series.total_samples, 0);
        assert_eq!(time_series.first_timestamp, 0);
        assert_eq!(time_series.last_sample, None);
    }

    #[test]
    fn test_trim_partial_chunks() {
        // Setup a TimeSeries with chunks such that some are before the min_timestamp
        let mut time_series = TimeSeries::default();

        // Assume we have a helper function to create a chunk with given timestamps
        let chunk1 = create_chunk_with_timestamps(0, 10); // Entirely before min_timestamp
        let chunk2 = create_chunk_with_timestamps(14, 18); // Partially before min_timestamp
        let chunk3 = create_chunk_with_timestamps(20, 30); // After min_timestamp

        let chunk1_len = chunk1.len();
        
        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);
        time_series.chunks.push(chunk3);
        time_series.update_state_from_chunks();

        // Set a retention period such that min_timestamp is 15
        time_series.retention = Duration::from_millis(15);
        time_series.last_sample = Some(Sample { timestamp: 30, value: 0.0 });

        // Call trim and check the results
        let deleted_count = time_series.trim().expect("Trim should succeed");

        // Verify that the first chunk is removed and the second chunk is trimmed
        assert_eq!(deleted_count, chunk1_len + 2); // all from chunk1 and 2 from chunk2
        assert_eq!(time_series.chunks.len(), 2); // Only chunk2 and chunk3 should remain
        assert_eq!(time_series.chunks[0].first_timestamp(), 16); // chunk2 should be trimmed
        assert_eq!(time_series.chunks[1].first_timestamp(), 20); // chunk3 remains unchanged
    }

    #[test]
    fn test_trim_no_chunks_before_min_timestamp() {
        let mut time_series = TimeSeries::new();

        // Create a chunk with timestamps starting from 1000
        let mut chunk = create_chunk(None);
        chunk.add_sample(&Sample { timestamp: 1000, value: 1.0 }).unwrap();
        chunk.add_sample(&Sample { timestamp: 1001, value: 2.0 }).unwrap();
        time_series.chunks.push(chunk);
        
        time_series.update_state_from_chunks();

        // Call trim and assert that no samples are deleted
        let deleted_count = time_series.trim().unwrap();
        assert_eq!(deleted_count, 0);
        assert_eq!(time_series.total_samples, 2);
        assert_eq!(time_series.chunks.len(), 1);
    }

    #[test]
    fn test_trim_adjusts_total_samples_correctly() {
        let mut time_series = TimeSeries::new();

        // Set up chunks with samples
        let mut chunk1 = create_chunk(None);
        let mut chunk2 = create_chunk(None);

        // Assuming add_sample is a method to add samples to a chunk
        chunk1.add_sample(&Sample { timestamp: 1, value: 10.0 }).unwrap();
        chunk1.add_sample(&Sample { timestamp: 2, value: 20.0 }).unwrap();
        chunk2.add_sample(&Sample { timestamp: 3, value: 30.0 }).unwrap();

        time_series.chunks.push(chunk1);
        time_series.chunks.push(chunk2);

        time_series.update_state_from_chunks();

        // Set retention to remove the first chunk
        time_series.retention = Duration::from_millis(2);

        let deleted_count = time_series.trim().unwrap();

        assert_eq!(deleted_count, 2);
        assert_eq!(time_series.total_samples, 1);
        // todo: check last_sample, first_timestamp, etc.
    }

    #[test]
    fn test_trim_partial_overlap_with_min_timestamp() {
        let mut time_series = TimeSeries::new();

        // Create a chunk that is completely before min_timestamp
        let mut chunk = create_chunk(None);
        chunk.add_sample(&Sample { timestamp: 50, value: 0.5 }).unwrap();
        time_series.chunks.push(chunk);
        
        // Create a chunk that partially overlaps with min_timestamp
        let mut chunk1 = create_chunk(None);
        chunk1.add_sample(&Sample { timestamp: 100, value: 1.0 }).unwrap();
        chunk1.add_sample(&Sample { timestamp: 200, value: 2.0 }).unwrap();
        time_series.chunks.push(chunk1);

        // Set retention to ensure min_timestamp is 150
        time_series.retention = Duration::from_millis(50);

        time_series.update_state_from_chunks();

        // Perform trim operation
        let deleted_count = time_series.trim().unwrap();

        // Verify the results
        assert_eq!(deleted_count, 1); // Only one sample should be deleted from chunk1
        assert_eq!(time_series.total_samples, 1); // Total samples should reflect the deletion
        assert_eq!(time_series.chunks.len(), 1); // Only one chunk should remain
        assert_eq!(time_series.chunks[0].first_timestamp(), 200); // First timestamp should be updated
    }

    #[test]
    fn test_remove_range_partial_overlap_multiple_chunks() {
        // Setup a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();
        let chunk_size = 5; // Assume each chunk can hold 5 samples

        // Create and add samples to the time series
        for i in 0..15 {
            time_series.add(i as Timestamp, i as f64, None);
        }

        // Remove a range that partially overlaps multiple chunks
        let start_ts = 3;
        let end_ts = 11;
        let removed_samples = time_series.remove_range(start_ts, end_ts).unwrap();

        // Verify the correct number of samples were removed
        assert_eq!(removed_samples, 9);

        // Verify the remaining samples are correct
        let remaining_samples: Vec<_> = time_series.iter().collect();
        let expected_samples = vec![
            Sample { timestamp: 0, value: 0.0 },
            Sample { timestamp: 1, value: 1.0 },
            Sample { timestamp: 2, value: 2.0 },
            Sample { timestamp: 12, value: 12.0 },
            Sample { timestamp: 13, value: 13.0 },
            Sample { timestamp: 14, value: 14.0 },
        ];
        assert_eq!(remaining_samples, expected_samples);
    }

    #[test]
    fn test_remove_range_no_overlap() {
        // Arrange
        let mut time_series = TimeSeries::default();
        let sample1 = Sample { timestamp: 1000, value: 1.0 };
        let sample2 = Sample { timestamp: 2000, value: 2.0 };
        let sample3 = Sample { timestamp: 3000, value: 3.0 };

        time_series.add(sample1.timestamp, sample1.value, None);
        time_series.add(sample2.timestamp, sample2.value, None);
        time_series.add(sample3.timestamp, sample3.value, None);

        // Act
        let deleted_samples = time_series.remove_range(4000, 5000).unwrap();

        // Assert
        assert_eq!(deleted_samples, 0);
        assert_eq!(time_series.total_samples, 3);
        assert_eq!(time_series.first_timestamp, 1000);
        assert_eq!(time_series.last_timestamp(), 3000);
    }

    #[test]
    fn test_remove_range_updates_total_samples_correctly() {
        // Setup a TimeSeries with multiple chunks and samples
        let mut time_series = TimeSeries::default();
        let sample1 = Sample { timestamp: 1, value: 10.0 };
        let sample2 = Sample { timestamp: 2, value: 20.0 };
        let sample3 = Sample { timestamp: 3, value: 30.0 };
        let sample4 = Sample { timestamp: 4, value: 40.0 };
        let sample5 = Sample { timestamp: 5, value: 50.0 };

        // Add samples to the time series
        time_series.add(sample1.timestamp, sample1.value, None);
        time_series.add(sample2.timestamp, sample2.value, None);
        time_series.add(sample3.timestamp, sample3.value, None);
        time_series.add(sample4.timestamp, sample4.value, None);
        time_series.add(sample5.timestamp, sample5.value, None);

        // Ensure total_samples is correct before removal
        assert_eq!(time_series.total_samples, 5);

        // Remove a range of samples
        let removed_samples = time_series.remove_range(2, 4).unwrap();

        // Verify the number of samples removed
        assert_eq!(removed_samples, 3);

        // Verify total_samples is updated correctly
        assert_eq!(time_series.total_samples, 2);

        // Verify remaining samples are correct
        let remaining_samples = time_series.get_range(1, 5);
        assert_eq!(remaining_samples.len(), 2);
        assert_eq!(remaining_samples[0].timestamp, 1);
        assert_eq!(remaining_samples[1].timestamp, 5);
    }

    #[test]
    fn test_remove_range_exactly_matches_chunk_boundaries() {
        // Setup a TimeSeries with multiple chunks
        let mut time_series = TimeSeries::default();

        // Assume each chunk can hold 2 samples for simplicity
        time_series.chunk_size_bytes = 2 * size_of::<Sample>();

        // Add samples to create multiple chunks
        let samples = vec![
            Sample { timestamp: 1, value: 10.0 },
            Sample { timestamp: 2, value: 20.0 },
            Sample { timestamp: 3, value: 30.0 },
            Sample { timestamp: 4, value: 40.0 },
        ];

        for sample in samples {
            time_series.add(sample.timestamp, sample.value, None);
        }

        // Verify initial state
        assert_eq!(time_series.chunks.len(), 1);
        assert_eq!(time_series.total_samples, 4);

        // Remove range that exactly matches the boundaries of the chunks
        let removed_samples = time_series.remove_range(1, 4).unwrap();

        // Verify that all samples are removed
        assert_eq!(removed_samples, 4);
        assert_eq!(time_series.total_samples, 0);
        assert_eq!(time_series.chunks.len(), 1); // One empty chunk should remain
        assert!(time_series.chunks[0].is_empty());
    }

    #[test]
    fn test_remove_range_updates_first_last_timestamps() {
        let mut time_series = TimeSeries::default();
        let samples = vec![
            Sample { timestamp: 1, value: 10.0 },
            Sample { timestamp: 2, value: 20.0 },
            Sample { timestamp: 3, value: 30.0 },
            Sample { timestamp: 4, value: 40.0 },
        ];

        for sample in &samples {
            time_series.add(sample.timestamp, sample.value, None);
        }

        // Remove samples with timestamps 2 and 3
        let removed_count = time_series.remove_range(2, 3).expect("Failed to remove range");
        assert_eq!(removed_count, 2);

        // Check if first and last timestamps are updated correctly
        assert_eq!(time_series.first_timestamp, 1);
        assert_eq!(time_series.last_timestamp(), 4);
    }
}