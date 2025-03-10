use crate::common::{Sample, Timestamp};
use crate::error_consts;
use pco::data_types::Number;
use pco::errors::PcoError;
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::FULL_BATCH_N;
use valkey_module::{ValkeyError, ValkeyResult};

const EMPTY_SLICE: [u8; 0] = [];
struct StreamState<'a, T: Number> {
    decompressor: FileDecompressor,
    chunk_decompressor: MaybeChunkDecompressor<T, &'a [u8]>,
    values: [T; FULL_BATCH_N],
    cursor: &'a [u8],
    count: usize,
    idx: usize,
    is_finished: bool,
    finished_chunk: bool,
}

impl<'a, T: Number + Default> StreamState<'a, T> {
    fn new(src: &'a [u8]) -> ValkeyResult<Self> {
        let (decompressor, cursor) = FileDecompressor::new(src)
            .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?;

        let state = Self {
            decompressor,
            chunk_decompressor: MaybeChunkDecompressor::EndOfData(&EMPTY_SLICE),
            cursor,
            count: 0,
            idx: 0,
            is_finished: false,
            finished_chunk: false,
            values: [T::default(); FULL_BATCH_N],
        };

        Ok(state)
    }

    // https://docs.rs/pco/0.3.1/src/pco/standalone/decompressor.rs.html
    fn next_chunk(&mut self) -> ValkeyResult<bool> {
        if self.is_finished {
            return Ok(false);
        }
        self.count = 0;
        if let MaybeChunkDecompressor::Some(chunk_decompressor) = &mut self.chunk_decompressor {
            let progress = chunk_decompressor
                .decompress(&mut self.values)
                .map_err(convert_error)?;
            self.finished_chunk = progress.finished;
            self.count = progress.n_processed;
            if self.finished_chunk {
                let mut replacement = MaybeChunkDecompressor::<T, &[u8]>::EndOfData(&EMPTY_SLICE);
                std::mem::swap(&mut self.chunk_decompressor, &mut replacement);
                match replacement {
                    MaybeChunkDecompressor::Some(replacement) => {
                        self.cursor = replacement.into_src();
                    }
                    MaybeChunkDecompressor::EndOfData(_) => {
                        self.is_finished = true;
                    }
                }
            }
            return Ok(true);
        }
        if self.next_chunk_decompressor()? {
            self.next_chunk()
        } else {
            Ok(false)
        }
    }

    fn next_chunk_decompressor(&mut self) -> ValkeyResult<bool> {
        match self.decompressor.chunk_decompressor::<T, _>(self.cursor) {
            Ok(MaybeChunkDecompressor::EndOfData(_)) => {
                self.is_finished = true;
                Ok(false)
            }
            Ok(decompressor) => {
                self.chunk_decompressor = decompressor;
                self.finished_chunk = false;
                Ok(true)
            }
            Err(err) => Err(convert_error(err)),
        }
    }

    fn next_value(&mut self) -> ValkeyResult<Option<T>> {
        if self.idx >= self.count {
            if self.is_finished {
                return Ok(None);
            }
            if !self.next_chunk()? {
                return Ok(None);
            }
            self.idx = 0;
        }

        let value = self.values[self.idx];
        self.idx += 1;
        Ok(Some(value))
    }
}

pub struct PcoSampleIterator<'a> {
    timestamp_state: StreamState<'a, Timestamp>,
    values_state: StreamState<'a, f64>,
    first_ts: Timestamp,
    last_ts: Timestamp,
    filtered: bool,
}

impl<'a> PcoSampleIterator<'a> {
    pub fn new(timestamps: &'a [u8], values: &'a [u8]) -> ValkeyResult<Self> {
        let timestamp_state = StreamState::new(timestamps)?;
        let values_state = StreamState::new(values)?;

        Ok(Self {
            timestamp_state,
            values_state,
            first_ts: 0,
            last_ts: 0,
            filtered: false,
        })
    }

    pub fn new_range(
        timestamps: &'a [u8],
        values: &'a [u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> ValkeyResult<Self> {
        let mut iter = Self::new(timestamps, values)?;
        iter.filtered = true;
        iter.first_ts = start_ts;
        iter.last_ts = end_ts;
        Ok(iter)
    }

    fn next_item<T: Number + Default>(state: &mut StreamState<'a, T>) -> Option<T> {
        match state.next_value() {
            Ok(Some(v)) => Some(v),
            Ok(None) => None,
            Err(err) => {
                eprintln!("Error {:?}", err);
                None
            }
        }
    }
}

impl Iterator for PcoSampleIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (
                Self::next_item(&mut self.timestamp_state),
                Self::next_item(&mut self.values_state),
            ) {
                (Some(ts), Some(val)) => {
                    if self.filtered {
                        if ts < self.first_ts {
                            continue;
                        }
                        if ts > self.last_ts {
                            return None;
                        }
                    }
                    break Some(Sample {
                        timestamp: ts,
                        value: val,
                    });
                }
                _ => break None,
            }
        }
    }
}

fn convert_error(_err: PcoError) -> ValkeyError {
    // todo: log error
    ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION)
}

#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::series::chunks::pco::pco_utils::{compress_timestamps, compress_values};
    use crate::series::chunks::pco::PcoSampleIterator;
    use std::time::Duration;
    use crate::tests::generators::DataGenerator;
    use crate::tests::generators::RandAlgo::MackeyGlass;

    #[test]
    fn test_pco_sample_iterator() {
        const SAMPLE_COUNT: usize = 1000;

        let epoch = 10000;
        let values = DataGenerator::builder()
            .start(epoch)
            .samples(SAMPLE_COUNT)
            .significant_digits(6)
            .maybe_interval(Some(Duration::from_millis(5000)))
            .algorithm(MackeyGlass)
            .build()
            .generate();

        let timestamps = values.iter().map(|v| v.timestamp).collect::<Vec<_>>();
        let values = values.iter().map(|v| v.value).collect::<Vec<_>>();

        let mut timestamp_buf: Vec<u8> = Vec::with_capacity(1024);
        let mut values_buf: Vec<u8> = Vec::with_capacity(1024);

        compress_timestamps(&mut timestamp_buf, &timestamps)
            .expect("Unable to compress timestamps");
        compress_values(&mut values_buf, &values).expect("Unable to compress values");

        let expected: Vec<_> = timestamps
            .iter()
            .zip(values.iter())
            .map(|(ts, val)| Sample {
                timestamp: *ts,
                value: *val,
            })
            .collect();

        let iterator =
            PcoSampleIterator::new(&timestamp_buf, &values_buf).expect("Unable to create iterator");
        let actual: Vec<Sample> = iterator.collect();

        assert_eq!(actual.len(), expected.len());
        for (i, sample) in actual.iter().enumerate() {
            assert_eq!(sample, &expected[i]);
        }
    }
}
