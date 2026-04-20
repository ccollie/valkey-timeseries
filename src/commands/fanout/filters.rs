use super::generated::{
    filter_predicate_value::LabelValue, label_filter::Matcher, FilterList as FanoutFilterList,
    FilterListValue, FilterPredicateValue as FanoutPredicateValue, LabelFilter as FanoutFilter,
    MatcherOpType, MetaDateRangeFilter as FanoutMetaDateRangeFilter, OrMatcherList,
    RegexFilterValue as FanoutRegexFilterValue, SeriesSelector as FanoutSeriesSelector,
};
use crate::commands::fanout::series_selector::Filters;
use crate::labels::filters::{
    FilterList, LabelFilter, OrFiltersList, PredicateMatch, PredicateValue, RegexMatcher,
    SeriesSelector, ValueList,
};
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::DateRange;
use valkey_module::{ValkeyError, ValkeyResult};

fn predicate_value_to_fanout(value: &PredicateValue) -> FanoutPredicateValue {
    let label_value = match value {
        PredicateValue::Empty => LabelValue::Empty(true),
        PredicateValue::String(v) => LabelValue::Single(v.clone()),
        PredicateValue::List(list) => LabelValue::List(FilterListValue {
            values: list.iter().cloned().collect(),
        }),
    };

    FanoutPredicateValue {
        label_value: Some(label_value),
    }
}

fn predicate_value_from_fanout(value: &FanoutPredicateValue) -> PredicateValue {
    match value.label_value.as_ref() {
        None | Some(LabelValue::Empty(_)) => PredicateValue::Empty,
        Some(LabelValue::Single(v)) => PredicateValue::String(v.clone()),
        Some(LabelValue::List(values)) => match values.values.len() {
            0 => PredicateValue::Empty,
            1 => PredicateValue::String(values.values[0].clone()),
            _ => {
                let mut items: ValueList = ValueList::with_capacity(values.values.len());
                for item in values.values.iter() {
                    items.push(item.clone());
                }
                PredicateValue::List(items)
            }
        },
    }
}

fn regex_matcher_to_fanout(value: &RegexMatcher) -> FanoutRegexFilterValue {
    FanoutRegexFilterValue {
        regex: value.value.clone(),
        prefix: value.prefix.clone().unwrap_or_default(),
    }
}

fn regex_matcher_from_fanout(value: &FanoutRegexFilterValue) -> ValkeyResult<RegexMatcher> {
    let mut matcher = RegexMatcher::create(value.regex.as_str())
        .map_err(|e| ValkeyError::String(format!("TSDB: regex value error: {e:?}")))?;
    matcher.prefix = (!value.prefix.is_empty()).then(|| value.prefix.clone());
    Ok(matcher)
}

impl From<&LabelFilter> for FanoutFilter {
    fn from(source: &LabelFilter) -> Self {
        let (matcher, op) = match &source.matcher {
            PredicateMatch::Equal(value) => {
                let value = predicate_value_to_fanout(value);
                (Matcher::Predicate(value), MatcherOpType::Equal)
            }
            PredicateMatch::NotEqual(value) => {
                let value = predicate_value_to_fanout(value);
                (Matcher::Predicate(value), MatcherOpType::NotEqual)
            }
            PredicateMatch::RegexEqual(regex) => {
                let value = regex_matcher_to_fanout(regex);
                (Matcher::Regex(value), MatcherOpType::RegexEqual)
            }
            PredicateMatch::RegexNotEqual(regex) => {
                let value = regex_matcher_to_fanout(regex);
                (Matcher::Regex(value), MatcherOpType::RegexNotEqual)
            }
            PredicateMatch::NotStartsWith(prefix) => {
                let value = prefix.clone();
                (Matcher::Prefix(value), MatcherOpType::NotStartsWith)
            }
            PredicateMatch::StartsWith(prefix) => {
                let value = prefix.clone();
                (Matcher::Prefix(value), MatcherOpType::StartsWith)
            }
        };

        FanoutFilter {
            label: source.label.clone(),
            op: op.into(),
            matcher: Some(matcher),
        }
    }
}

impl From<&SeriesSelector> for FanoutSeriesSelector {
    fn from(value: &SeriesSelector) -> Self {
        fn decompose_and_matchers(dest: &mut Vec<FanoutFilter>, matchers: &FilterList) {
            for matcher in matchers.iter() {
                dest.push(matcher.into());
            }
        }

        match &value {
            SeriesSelector::Or(lists) => {
                let mut or_matchers = Vec::with_capacity(lists.len());
                for matcher_list in lists.iter() {
                    let mut matchers = Vec::with_capacity(matcher_list.len());
                    decompose_and_matchers(&mut matchers, matcher_list);
                    let filters = FanoutFilterList { matchers };
                    or_matchers.push(filters);
                }
                let or_matchers = OrMatcherList {
                    filters: or_matchers,
                };
                FanoutSeriesSelector {
                    filters: Some(Filters::OrFilters(or_matchers)),
                }
            }
            SeriesSelector::And(items) => {
                let mut matchers = Vec::with_capacity(items.len());
                decompose_and_matchers(&mut matchers, items);
                let filters = FanoutFilterList { matchers };
                FanoutSeriesSelector {
                    filters: Some(Filters::AndFilters(filters)),
                }
            }
        }
    }
}

pub(crate) fn serialize_matchers_list(
    filters: &[SeriesSelector],
) -> ValkeyResult<Vec<FanoutSeriesSelector>> {
    let mut result: Vec<FanoutSeriesSelector> = Vec::with_capacity(filters.len());
    for filter in filters.iter() {
        let item: FanoutSeriesSelector = filter.into();
        result.push(item);
    }
    Ok(result)
}

impl TryFrom<&FanoutSeriesSelector> for SeriesSelector {
    type Error = ValkeyError;

    fn try_from(value: &FanoutSeriesSelector) -> Result<Self, Self::Error> {
        let mut result = SeriesSelector::And(FilterList::default());

        fn convert_list(matchers: &[FanoutFilter]) -> ValkeyResult<FilterList> {
            let mut result = Vec::with_capacity(matchers.len());
            for matcher in matchers.iter() {
                let item: LabelFilter = matcher.try_into().map_err(|e| {
                    ValkeyError::String(format!("TSDB: failed to convert matcher: {e:?}"))
                })?;
                result.push(item);
            }
            Ok(FilterList::new(result))
        }

        if let Some(filters) = &value.filters {
            match filters {
                Filters::AndFilters(and_filters) => {
                    result = SeriesSelector::And(convert_list(&and_filters.matchers)?);
                }
                Filters::OrFilters(or_filters) => {
                    let mut or_matchers: OrFiltersList = Default::default();
                    for matcher_list in or_filters.filters.iter() {
                        let items = convert_list(&matcher_list.matchers)?;
                        or_matchers.push(items);
                    }
                    result = SeriesSelector::Or(or_matchers);
                }
            }
        }

        Ok(result)
    }
}

pub(crate) fn deserialize_matchers_list(
    filter_vec: Option<Vec<FanoutSeriesSelector>>,
) -> ValkeyResult<Vec<SeriesSelector>> {
    if let Some(filter_vec) = filter_vec {
        let mut filters = Vec::with_capacity(filter_vec.len());
        for filter in filter_vec.iter() {
            filters.push(filter.try_into()?);
        }
        Ok(filters)
    } else {
        Err(ValkeyError::Str("TSDB: missing filters"))
    }
}

impl TryFrom<&FanoutFilter> for LabelFilter {
    type Error = ValkeyError;

    fn try_from(value: &FanoutFilter) -> Result<Self, Self::Error> {
        let op: MatcherOpType = value.op.try_into().map_err(|_| {
            ValkeyError::Str("TSDB: invalid matcher operation, cannot convert from i32")
        })?;
        let matcher = match (op, value.matcher.as_ref()) {
            (MatcherOpType::Equal, Some(Matcher::Predicate(predicate))) => {
                PredicateMatch::Equal(predicate_value_from_fanout(predicate))
            }
            (MatcherOpType::NotEqual, Some(Matcher::Predicate(predicate))) => {
                PredicateMatch::NotEqual(predicate_value_from_fanout(predicate))
            }
            (MatcherOpType::RegexEqual, Some(Matcher::Regex(regex))) => {
                PredicateMatch::RegexEqual(regex_matcher_from_fanout(regex)?)
            }
            (MatcherOpType::RegexNotEqual, Some(Matcher::Regex(regex))) => {
                PredicateMatch::RegexNotEqual(regex_matcher_from_fanout(regex)?)
            }
            (MatcherOpType::StartsWith, Some(Matcher::Prefix(prefix))) => {
                PredicateMatch::StartsWith(prefix.clone())
            }
            (MatcherOpType::NotStartsWith, Some(Matcher::Prefix(prefix))) => {
                PredicateMatch::NotStartsWith(prefix.clone())
            }
            _ => {
                return Err(ValkeyError::Str(
                    "TSDB: matcher operation does not match matcher payload",
                ));
            }
        };

        if value.label.is_empty() {
            return Err(ValkeyError::Str("TSDB: matcher label cannot be empty"));
        }

        Ok(LabelFilter {
            label: value.label.clone(),
            matcher,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_filter_round_trip_for_equality_variants() {
        let cases = vec![
            LabelFilter {
                label: "env".into(),
                matcher: PredicateMatch::Equal(PredicateValue::Empty),
            },
            LabelFilter {
                label: "env".into(),
                matcher: PredicateMatch::Equal(PredicateValue::String("prod".into())),
            },
            LabelFilter {
                label: "env".into(),
                matcher: PredicateMatch::NotEqual(PredicateValue::List(
                    vec!["prod".to_string(), "staging".to_string()].into(),
                )),
            },
        ];

        for case in cases {
            let fanout: FanoutFilter = (&case).into();
            let decoded = LabelFilter::try_from(&fanout).expect("label filter should decode");
            assert_eq!(decoded, case);
        }
    }

    #[test]
    fn test_label_filter_round_trip_for_regex_and_prefix_variants() {
        let mut regex = RegexMatcher::create("server.*").expect("regex should compile");
        regex.prefix = Some("server".into());

        let regex_filter = LabelFilter {
            label: "instance".into(),
            matcher: PredicateMatch::RegexEqual(regex.clone()),
        };
        let fanout_regex: FanoutFilter = (&regex_filter).into();
        let decoded_regex =
            LabelFilter::try_from(&fanout_regex).expect("regex filter should decode");

        match decoded_regex.matcher {
            PredicateMatch::RegexEqual(decoded) => {
                assert_eq!(decoded.value, regex.value);
                assert_eq!(decoded.prefix, regex.prefix);
            }
            other => panic!("expected regex matcher, got {other:?}"),
        }

        let prefix_filter = LabelFilter {
            label: "instance".into(),
            matcher: PredicateMatch::NotStartsWith("server".into()),
        };
        let fanout_prefix: FanoutFilter = (&prefix_filter).into();
        let decoded_prefix =
            LabelFilter::try_from(&fanout_prefix).expect("prefix filter should decode");
        assert_eq!(decoded_prefix, prefix_filter);
    }

    #[test]
    fn test_label_filter_rejects_mismatched_op_and_payload() {
        let fanout = FanoutFilter {
            label: "instance".into(),
            op: MatcherOpType::RegexEqual as i32,
            matcher: Some(Matcher::Predicate(FanoutPredicateValue {
                label_value: Some(LabelValue::Single("server1".into())),
            })),
        };

        let err = LabelFilter::try_from(&fanout).expect_err("mismatched matcher should fail");
        match err {
            ValkeyError::Str(msg) => {
                assert!(msg.contains("operation does not match matcher payload"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

impl From<MetaDateRangeFilter> for FanoutMetaDateRangeFilter {
    fn from(value: MetaDateRangeFilter) -> Self {
        match value {
            MetaDateRangeFilter::Includes(r) => FanoutMetaDateRangeFilter {
                start: r.start,
                end: r.end,
                exclude: false,
            },
            MetaDateRangeFilter::Excludes(r) => FanoutMetaDateRangeFilter {
                start: r.start,
                end: r.end,
                exclude: true,
            },
        }
    }
}

impl From<FanoutMetaDateRangeFilter> for MetaDateRangeFilter {
    fn from(value: FanoutMetaDateRangeFilter) -> Self {
        let range = DateRange {
            start: value.start,
            end: value.end,
        };
        if value.exclude {
            MetaDateRangeFilter::Excludes(range)
        } else {
            MetaDateRangeFilter::Includes(range)
        }
    }
}
