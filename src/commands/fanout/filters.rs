use super::generated::{
    FilterList as FanoutFilterList, FilterListValue, FilterPredicateValue as FanoutPredicateValue,
    LabelFilter as FanoutFilter, MatcherOpType, MetaDateRangeFilter as FanoutMetaDateRangeFilter,
    OrMatcherList, RegexFilterValue as FanoutRegexFilterValue,
    SeriesSelector as FanoutSeriesSelector, filter_predicate_value::LabelValue,
    label_filter::Matcher,
};
use crate::commands::fanout::series_selector::Filters;
use crate::labels::filters::{
    FilterList, LabelFilter, MatchOp, OrFiltersList, PredicateMatch, PredicateValue, RegexMatcher,
    SeriesSelector, ValueList,
};
use crate::series::DateRange;
use crate::series::request_types::MetaDateRangeFilter;
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

fn regex_matcher_from_fanout(
    value: &FanoutRegexFilterValue,
    op: MatchOp,
) -> ValkeyResult<RegexMatcher> {
    let matcher = LabelFilter::create(op, "__fanout_regex__", value.regex.as_str())
        .map_err(|e| ValkeyError::String(format!("TSDB: regex value error: {e:?}")))?
        .matcher;

    let matcher = match matcher {
        PredicateMatch::RegexEqual(matcher) | PredicateMatch::RegexNotEqual(matcher) => matcher,
        _ => {
            return Err(ValkeyError::String(
                "TSDB: regex value error: expected regex matcher".to_string(),
            ));
        }
    };

    if !value.prefix.is_empty() && matcher.prefix.as_deref() != Some(value.prefix.as_str()) {
        return Err(ValkeyError::String(format!(
            "TSDB: regex value error: invalid prefix '{}'",
            value.prefix
        )));
    }

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
                PredicateMatch::RegexEqual(regex_matcher_from_fanout(regex, MatchOp::RegexEqual)?)
            }
            (MatcherOpType::RegexNotEqual, Some(Matcher::Regex(regex))) => {
                PredicateMatch::RegexNotEqual(regex_matcher_from_fanout(
                    regex,
                    MatchOp::RegexNotEqual,
                )?)
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
        let regex_filter = LabelFilter::create(
            MatchOp::RegexEqual,
            "instance",
            "server[0-9]+\\.(prod|staging)",
        )
        .expect("regex filter should compile");
        let fanout_regex: FanoutFilter = (&regex_filter).into();
        let decoded_regex =
            LabelFilter::try_from(&fanout_regex).expect("regex filter should decode");

        match (&regex_filter.matcher, &decoded_regex.matcher) {
            (PredicateMatch::RegexEqual(expected), PredicateMatch::RegexEqual(decoded)) => {
                assert_eq!(decoded.value, expected.value);
                assert_eq!(decoded.prefix, expected.prefix);
                for value in ["server123.prod", "server42.staging", "serverabc.prod"] {
                    assert_eq!(decoded.is_match(value), expected.is_match(value));
                }
            }
            (_, other) => panic!("expected regex matcher, got {other:?}"),
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
    fn test_series_selector_round_trip_with_list_equality() {
        // Build a selector with an equality matcher that contains a list of values
        let lf = LabelFilter {
            label: "node".into(),
            matcher: PredicateMatch::Equal(PredicateValue::List(
                vec!["node1".to_string(), "node2".to_string()].into(),
            )),
        };

        let selector = SeriesSelector::with_filters(vec![lf.clone()]);

        let selector_clone = selector.clone();

        // Serialize to fanout representation
        let fanout = serialize_matchers_list(&[selector_clone]).expect("serialize failed");

        // Deserialize back
        let decoded = deserialize_matchers_list(Some(fanout)).expect("deserialize failed");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], selector);

        // Also validate LabelFilter round-trip directly
        let fanout_filter: FanoutFilter = (&lf).into();
        let decoded_lf = LabelFilter::try_from(&fanout_filter).expect("label filter decode failed");
        assert_eq!(decoded_lf, lf);
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
