use super::request_generated::{
    MatchOpType, Matcher as FBMatcher, MatcherArgs, Matchers as FBMatchers, MatchersArgs,
    MatchersCondition,
};
use crate::labels::matchers::{
    Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue, RegexMatcher, ValueList,
};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use smallvec::SmallVec;
use valkey_module::{ValkeyError, ValkeyResult};

#[derive(Default, Debug)]
struct LocalMatcher {
    group_idx: usize,
    label: String,
    op: MatchOpType,
    value: SmallVec<String, 4>,
}

fn decompose_matcher(matcher: &Matcher) -> LocalMatcher {
    fn get_predicate_value(value: &PredicateValue) -> SmallVec<String, 4> {
        let mut items: SmallVec<String, 4> = SmallVec::new();
        match value {
            PredicateValue::Empty => items,
            PredicateValue::String(v) => {
                let string_value = v.as_str();
                items.push(string_value.to_string());
                items
            }
            PredicateValue::List(list) => {
                let mut items: SmallVec<_, 4> = SmallVec::new();
                for item in list.iter() {
                    items.push(item.clone());
                }
                items
            }
        }
    }

    let (items, op) = match &matcher.matcher {
        PredicateMatch::Equal(value) => {
            let items = get_predicate_value(value);
            (items, MatchOpType::Equal)
        }
        PredicateMatch::NotEqual(value) => {
            let items = get_predicate_value(value);
            (items, MatchOpType::NotEqual)
        }
        PredicateMatch::RegexEqual(regex) => {
            let string_value = regex.value.to_string();
            let mut items: SmallVec<String, 4> = SmallVec::new();
            items.push(string_value);
            (items, MatchOpType::RegexEqual)
        }
        PredicateMatch::RegexNotEqual(regex) => {
            let string_value = regex.value.to_string();
            let mut items: SmallVec<String, 4> = SmallVec::new();
            items.push(string_value);
            (items, MatchOpType::RegexNotEqual)
        }
    };

    LocalMatcher {
        group_idx: 0,
        label: matcher.label.clone(),
        op,
        value: items,
    }
}

pub(super) fn serialize_matchers<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    matchers: &Matchers,
) -> WIPOffset<FBMatchers<'a>> {
    fn decompose_and_matchers(dest: &mut Vec<LocalMatcher>, index: usize, matchers: &[Matcher]) {
        for matcher in matchers.iter() {
            let mut item = decompose_matcher(matcher);
            item.group_idx = index;
            dest.push(item);
        }
    }

    fn serialize_local_matchers<'a>(
        bldr: &mut FlatBufferBuilder<'a>,
        matchers: &[LocalMatcher],
    ) -> SmallVec<WIPOffset<FBMatcher<'a>>, 6> {
        let mut arg_items: SmallVec<_, 6> = SmallVec::new();
        let mut values: SmallVec<_, 6> = SmallVec::new();
        for item in matchers.iter() {
            let label = bldr.create_string(item.label.as_str());
            values.clear();
            for v in item.value.iter() {
                let string_value = bldr.create_string(v.as_str());
                values.push(string_value);
            }
            let values_ = bldr.create_vector(&values);

            let args = MatcherArgs {
                group_id: item.group_idx as u16,
                label: Some(label),
                op: item.op,
                value: Some(values_),
            };
            let obj = FBMatcher::create(bldr, &args);
            arg_items.push(obj);
        }
        arg_items
    }

    // Create name string if present
    let name = matchers
        .name
        .as_ref()
        .map(|name| bldr.create_string(name.as_str()));

    let mut local_matchers = Vec::with_capacity(6);
    let condition = match &matchers.matchers {
        MatcherSetEnum::Or(lists) => {
            // First, serialize all matchers and collect their offsets
            for (i, matcher_list) in lists.iter().enumerate() {
                decompose_and_matchers(&mut local_matchers, i, matcher_list);
            }
            MatchersCondition::Or
        }
        MatcherSetEnum::And(items) => {
            decompose_and_matchers(&mut local_matchers, 0, items);
            MatchersCondition::And
        }
    };

    let matchers_args = {
        let matchers = serialize_local_matchers(bldr, &local_matchers);

        let matcher_vector = bldr.create_vector(&matchers);
        MatchersArgs {
            matchers: Some(matcher_vector),
            condition,
            name,
        }
    };

    FBMatchers::create(bldr, &matchers_args)
}

/// Serializes a collection of Matchers filters into a FlatBuffer vector.
///
/// # Arguments
/// * `bldr` - A mutable reference to a FlatBufferBuilder
/// * `filters` - A slice of Matchers to serialize
///
/// # Returns
/// A flatbuffers::WIPOffset that can be used in a parent FlatBuffer object
pub(super) fn serialize_matchers_list<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    filters: &[Matchers],
) -> WIPOffset<Vector<'a, ForwardsUOffset<FBMatchers<'a>>>> {
    let mut serialized_filters = Vec::with_capacity(filters.len());

    for filter in filters.iter() {
        serialized_filters.push(serialize_matchers(bldr, filter));
    }

    bldr.create_vector(&serialized_filters)
}

pub(super) fn deserialize_matchers(filter: &FBMatchers) -> ValkeyResult<Matchers> {
    let mut result = Matchers::default();

    if let Some(name) = filter.name() {
        result.name = Some(name.to_string());
    }

    if filter.matchers().is_some() {
        let type_ = filter.condition();
        match type_ {
            MatchersCondition::And => {
                let items = filter
                    .matchers()
                    .unwrap_or_default()
                    .iter()
                    .map(|x| deserialize_matcher(&x))
                    .collect::<ValkeyResult<Vec<_>>>()?;

                result.matchers = MatcherSetEnum::And(items);
            }
            MatchersCondition::Or => {
                let mut or_matchers = Vec::new();
                let mut items = filter.matchers().unwrap_or_default().iter().enumerate();
                let mut group_idx = 0;
                let mut and_matchers = Vec::new();
                for (idx, matcher) in items.by_ref() {
                    let converted = deserialize_matcher(&matcher)?;
                    if idx == 0 {
                        group_idx = matcher.group_id();
                    }
                    if group_idx != matcher.group_id() {
                        group_idx = matcher.group_id();
                        if !and_matchers.is_empty() {
                            or_matchers.push(and_matchers);
                            and_matchers = Vec::new();
                        }
                    }
                    and_matchers.push(converted);
                }
                if !and_matchers.is_empty() {
                    or_matchers.push(and_matchers);
                }

                result.matchers = MatcherSetEnum::Or(or_matchers);
            }
            _ => {
                return Err(ValkeyError::Str("TSDB: unsupported matcher type in stream"));
            }
        }
    }

    if result.is_empty() {
        return Err(ValkeyError::Str("TSDB: matcher cannot be empty"));
    }

    Ok(result)
}

pub(super) fn deserialize_matchers_list(
    filter_vec: Option<Vector<ForwardsUOffset<FBMatchers>>>,
) -> ValkeyResult<Vec<Matchers>> {
    if let Some(filter_vec) = filter_vec {
        let mut filters = Vec::with_capacity(filter_vec.len());
        for filter in filter_vec.iter() {
            let item = deserialize_matchers(&filter)?;
            filters.push(item);
        }
        Ok(filters)
    } else {
        Err(ValkeyError::Str("TSDB: missing filters"))
    }
}

fn deserialize_matcher(request_matcher: &FBMatcher) -> ValkeyResult<Matcher> {
    let label = request_matcher.label();
    let op = request_matcher.op();

    fn get_predicate_value(value: &FBMatcher) -> PredicateValue {
        let mut items: ValueList = value
            .value()
            .unwrap_or_default()
            .iter()
            .map(|s| s.to_string())
            .collect();

        if items.len() == 1 {
            PredicateValue::String(items.pop().unwrap())
        } else {
            PredicateValue::List(items)
        }
    }

    fn get_regex_value(value: &FBMatcher) -> ValkeyResult<RegexMatcher> {
        if let Some(text) = value.value() {
            if text.len() == 1 {
                let item = text.get(0);
                return RegexMatcher::create(item)
                    .map_err(|e| ValkeyError::String(format!("TSDB: regex value error: {e:?}")));
            }
        }
        Err(ValkeyError::Str("TSDB: regex value is not a string"))
    }

    let matcher = match op {
        MatchOpType::Equal => {
            let value = get_predicate_value(request_matcher);
            PredicateMatch::Equal(value)
        }
        MatchOpType::NotEqual => {
            let value = get_predicate_value(request_matcher);
            PredicateMatch::NotEqual(value)
        }
        MatchOpType::RegexEqual => PredicateMatch::RegexEqual(get_regex_value(request_matcher)?),
        MatchOpType::RegexNotEqual => {
            PredicateMatch::RegexNotEqual(get_regex_value(request_matcher)?)
        }
        _ => return Err(ValkeyError::Str("Unsupported matcher")),
    };
    let label = label.unwrap_or_default().to_string();
    if label.is_empty() {
        return Err(ValkeyError::Str("TSDB: matcher label cannot be empty"));
    }

    Ok(Matcher { label, matcher })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Import items from the parent module (where serialize/deserialize_matchers are)
    use crate::labels::matchers::{
        Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
    };
    use flatbuffers::FlatBufferBuilder;

    // Helper function to perform the serialize -> deserialize round trip
    fn assert_round_trip(original_matchers: &Matchers) {
        let mut bldr = FlatBufferBuilder::with_capacity(1024);

        // Serialize
        let fb_matchers_offset = serialize_matchers(&mut bldr, original_matchers);
        bldr.finish(fb_matchers_offset, None); // Finish the buffer with FBMatchers as root
        let buf = bldr.finished_data();

        let fb_matchers =
            flatbuffers::root::<crate::fanout::request::request_generated::Matchers>(buf)
                .expect("Failed to get root FBMatchers");
        // Deserialize
        let deserialized_matchers =
            deserialize_matchers(&fb_matchers).expect("Deserialization failed");

        // Assert equality
        assert_eq!(
            original_matchers, &deserialized_matchers,
            "Round trip failed for: {original_matchers:?}"
        );
    }

    #[test]
    fn test_serialize_deserialize_and_simple() {
        let matchers = Matchers {
            name: None,
            matchers: MatcherSetEnum::And(vec![Matcher {
                label: "job".to_string(),
                matcher: PredicateMatch::Equal(PredicateValue::String("node".to_string())),
            }]),
        };
        assert_round_trip(&matchers);
    }

    #[test]
    fn test_serialize_deserialize_and_multiple() {
        let matchers = Matchers {
            name: Some("my_and_matcher".to_string()),
            matchers: MatcherSetEnum::And(vec![
                Matcher {
                    label: "job".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("node".to_string())),
                },
                Matcher {
                    label: "instance".to_string(),
                    matcher: PredicateMatch::NotEqual(PredicateValue::String(
                        "localhost:9090".to_string(),
                    )),
                },
                Matcher {
                    label: "region".to_string(),
                    matcher: PredicateMatch::RegexEqual("eu-.*".try_into().unwrap()),
                },
            ]),
        };
        assert_round_trip(&matchers);
    }

    #[test]
    fn test_serialize_deserialize_or_simple() {
        let matchers = Matchers {
            name: None,
            matchers: MatcherSetEnum::Or(vec![vec![Matcher {
                label: "status".to_string(),
                matcher: PredicateMatch::Equal(PredicateValue::String("success".to_string())),
            }]]),
        };
        assert_round_trip(&matchers);
    }

    #[test]
    fn test_serialize_deserialize_or_multiple_groups() {
        let matchers = Matchers {
            name: Some("my_or_matcher".to_string()),
            matchers: MatcherSetEnum::Or(vec![
                vec![
                    // Group 1
                    Matcher {
                        label: "env".to_string(),
                        matcher: PredicateMatch::Equal(PredicateValue::String("prod".to_string())),
                    },
                    Matcher {
                        label: "dc".to_string(),
                        matcher: PredicateMatch::Equal(PredicateValue::String(
                            "us-east-1".to_string(),
                        )),
                    },
                ],
                vec![
                    // Group 2
                    Matcher {
                        label: "env".to_string(),
                        matcher: PredicateMatch::Equal(PredicateValue::String(
                            "staging".to_string(),
                        )),
                    },
                    Matcher {
                        label: "owner".to_string(),
                        matcher: PredicateMatch::RegexNotEqual("test.*".try_into().unwrap()),
                    },
                ],
            ]),
        };
        assert_round_trip(&matchers);
    }

    #[test]
    fn test_serialize_deserialize_or_empty() {
        let matchers = Matchers {
            name: Some("empty_or".to_string()),
            matchers: MatcherSetEnum::Or(vec![]), // Empty OR list
        };
        assert_round_trip(&matchers);
    }

    #[test]
    fn test_serialize_deserialize_with_list_predicate() {
        let mut list = ValueList::new();
        list.push("200".to_string());
        list.push("201".to_string());
        list.push("204".to_string());
        let matchers = Matchers {
            name: None,
            matchers: MatcherSetEnum::And(vec![Matcher {
                label: "code".to_string(),
                matcher: PredicateMatch::Equal(PredicateValue::List(list)),
            }]),
        };
        assert_round_trip(&matchers);
    }
}
