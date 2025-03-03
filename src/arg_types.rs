use crate::aggregators::Aggregator;
use crate::common::Timestamp;
use crate::iterators::aggregator::AggregationOptions;
use crate::labels::matchers::Matchers;
use crate::series::{TimestampRange, TimestampValue, ValueFilter};

#[derive(Default)]
pub struct MatchFilterOptions {
    pub date_range: Option<TimestampRange>,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

impl MatchFilterOptions {
    pub fn is_empty(&self) -> bool {
        self.date_range.is_none() && self.matchers.is_empty()
    }
}

impl From<Vec<Matchers>> for MatchFilterOptions {
    fn from(matchers: Vec<Matchers>) -> Self {
        Self {
            matchers,
            ..Default::default()
        }
    }
}

impl From<Matchers> for MatchFilterOptions {
    fn from(matcher: Matchers) -> Self {
        Self {
            matchers: vec![matcher],
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub(crate) aggregator: Aggregator,
    pub(crate) group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub series_selector: Matchers,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
}

impl RangeOptions {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            date_range: TimestampRange {
                start: TimestampValue::Specific(start),
                end: TimestampValue::Specific(end),
            },
            ..Default::default()
        }
    }

    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
}

#[cfg(test)]
mod tests {}
