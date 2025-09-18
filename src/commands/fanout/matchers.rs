use super::generated::{
    Matcher as FanoutMatcher, MatcherList, MatcherListValue, MatcherOpType,
    Matchers as FanoutMatchers, OrMatcherList, matcher, matchers,
};
use crate::labels::matchers::{
    Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue, RegexMatcher, ValueList,
};
use valkey_module::{ValkeyError, ValkeyResult};

impl From<&Matcher> for FanoutMatcher {
    fn from(source: &Matcher) -> Self {
        fn get_predicate_value(value: &PredicateValue) -> matcher::Value {
            match value {
                PredicateValue::Empty => matcher::Value::Empty(true),
                PredicateValue::String(v) => matcher::Value::Single(v.into()),
                PredicateValue::List(list) => {
                    let mut items: MatcherListValue = MatcherListValue {
                        values: Vec::with_capacity(list.len()),
                    };
                    for item in list.iter() {
                        items.values.push(item.into());
                    }
                    matcher::Value::List(items)
                }
            }
        }

        let (value, op) = match &source.matcher {
            PredicateMatch::Equal(value) => {
                let value = get_predicate_value(value);
                (value, MatcherOpType::NotEqual)
            }
            PredicateMatch::NotEqual(value) => {
                let items = get_predicate_value(value);
                (items, MatcherOpType::NotEqual)
            }
            PredicateMatch::RegexEqual(regex) => {
                let value = matcher::Value::Single(regex.value.clone());
                (value, MatcherOpType::RegexEqual)
            }
            PredicateMatch::RegexNotEqual(regex) => {
                let value = matcher::Value::Single(regex.value.clone());
                (value, MatcherOpType::RegexNotEqual)
            }
        };

        FanoutMatcher {
            label: source.label.clone(),
            op: op.into(),
            value: Some(value),
        }
    }
}

impl From<&Matchers> for FanoutMatchers {
    fn from(value: &Matchers) -> Self {
        fn decompose_and_matchers(dest: &mut Vec<FanoutMatcher>, matchers: &[Matcher]) {
            for matcher in matchers.iter() {
                dest.push(matcher.into());
            }
        }

        match &value.matchers {
            MatcherSetEnum::Or(lists) => {
                let mut or_matchers = Vec::with_capacity(lists.len());
                for matcher_list in lists.iter() {
                    let mut converted = Vec::with_capacity(matcher_list.len());
                    decompose_and_matchers(&mut converted, matcher_list);
                    let matcher_list = MatcherList {
                        matchers: converted,
                    };
                    or_matchers.push(matcher_list);
                }
                let or_matchers = OrMatcherList {
                    filters: or_matchers,
                };
                FanoutMatchers {
                    name: value.name.clone(),
                    filters: Some(matchers::Filters::OrFilters(or_matchers)),
                }
            }
            MatcherSetEnum::And(items) => {
                let mut converted = Vec::with_capacity(items.len());
                decompose_and_matchers(&mut converted, items);
                let matcher_list = MatcherList {
                    matchers: converted,
                };
                FanoutMatchers {
                    name: value.name.clone(),
                    filters: Some(matchers::Filters::AndFilters(matcher_list)),
                }
            }
        }
    }
}

pub(crate) fn serialize_matchers_list(filters: &[Matchers]) -> ValkeyResult<Vec<FanoutMatchers>> {
    let mut result: Vec<FanoutMatchers> = Vec::with_capacity(filters.len());
    for filter in filters.iter() {
        let item: FanoutMatchers = filter.into();
        result.push(item);
    }
    Ok(result)
}

impl TryFrom<&FanoutMatchers> for Matchers {
    type Error = ValkeyError;

    fn try_from(value: &FanoutMatchers) -> Result<Self, Self::Error> {
        let mut result = Matchers {
            name: value.name.clone(),
            ..Default::default()
        };

        fn convert_list(matchers: &[FanoutMatcher]) -> ValkeyResult<Vec<Matcher>> {
            let mut result = Vec::with_capacity(matchers.len());
            for matcher in matchers.iter() {
                let item: Matcher = matcher.try_into().map_err(|e| {
                    ValkeyError::String(format!("TSDB: failed to convert matcher: {e:?}"))
                })?;
                result.push(item);
            }
            Ok(result)
        }

        if let Some(filters) = &value.filters {
            match filters {
                matchers::Filters::AndFilters(and_filters) => {
                    result.matchers = MatcherSetEnum::And(convert_list(&and_filters.matchers)?);
                }
                matchers::Filters::OrFilters(or_filters) => {
                    let mut or_matchers = Vec::new();
                    for matcher_list in or_filters.filters.iter() {
                        let items = convert_list(&matcher_list.matchers)?;
                        or_matchers.push(items);
                    }
                    result.matchers = MatcherSetEnum::Or(or_matchers);
                }
            }
        }

        Ok(result)
    }
}

pub(crate) fn deserialize_matchers_list(
    filter_vec: Option<Vec<FanoutMatchers>>,
) -> ValkeyResult<Vec<Matchers>> {
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

impl TryFrom<&FanoutMatcher> for Matcher {
    type Error = ValkeyError;

    fn try_from(value: &FanoutMatcher) -> Result<Self, Self::Error> {
        fn get_predicate_value(value: &FanoutMatcher) -> PredicateValue {
            match &value.value {
                None => PredicateValue::Empty,
                Some(matcher::Value::Empty(_)) => PredicateValue::Empty,
                Some(matcher::Value::Single(v)) => PredicateValue::String(v.into()),
                Some(matcher::Value::List(values)) => match values.values.len() {
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

        fn get_regex_value(value: &FanoutMatcher) -> ValkeyResult<RegexMatcher> {
            match &value.value {
                Some(matcher::Value::List(items)) => {
                    if items.values.len() == 1 {
                        return RegexMatcher::create(items.values[0].as_str()).map_err(|e| {
                            ValkeyError::String(format!("TSDB: regex value error: {e:?}"))
                        });
                    }
                }
                Some(matcher::Value::Single(v)) => {
                    return RegexMatcher::create(v.as_str()).map_err(|e| {
                        ValkeyError::String(format!("TSDB: regex value error: {e:?}"))
                    });
                }
                _ => {}
            }
            Err(ValkeyError::Str("TSDB: invalid or empty regex value"))
        }
        let op: MatcherOpType = value.op.try_into().map_err(|_| {
            ValkeyError::Str("TSDB: invalid matcher operation, cannot convert from i32")
        })?;
        let matcher = match op {
            MatcherOpType::Equal => {
                let value = get_predicate_value(value);
                PredicateMatch::Equal(value)
            }
            MatcherOpType::NotEqual => {
                let value = get_predicate_value(value);
                PredicateMatch::NotEqual(value)
            }
            MatcherOpType::RegexEqual => PredicateMatch::RegexEqual(get_regex_value(value)?),
            MatcherOpType::RegexNotEqual => PredicateMatch::RegexNotEqual(get_regex_value(value)?),
        };

        if value.label.is_empty() {
            return Err(ValkeyError::Str("TSDB: matcher label cannot be empty"));
        }

        Ok(Matcher {
            label: value.label.clone(),
            matcher,
        })
    }
}
