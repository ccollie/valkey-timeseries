use crate::common::parallel::Parallel;
use crate::common::Sample;
use crate::series::request_types::RangeOptions;

pub struct MultiSeriesRangeFetcher<'a> {
    start_timestamp: i64,
    end_timestamp: i64,
    options: &'a RangeOptions,
    items: Vec<Vec<Sample>>
}

impl MultiSeriesRangeFetcher<'_> {
    // pub fn new(
    //     start_timestamp: i64,
    //     end_timestamp: i64,
    //     options: &'a RangeOptions,
    // ) -> Self {
    //     Self {
    //         start_timestamp,
    //         end_timestamp,
    //         options,
    //         items: Vec::new(),
    //     }
    // }
}

impl Parallel for MultiSeriesRangeFetcher<'_> {
    fn create(&self) -> Self {
        Self {
            start_timestamp: self.start_timestamp,
            end_timestamp: self.end_timestamp,
            options: self.options,
            items: Vec::new(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.items.extend(other.items);
    }
}