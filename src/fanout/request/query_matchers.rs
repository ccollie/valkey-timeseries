use super::matchers::{serialize_local_matchers, LocalMatcher};
use super::request_generated::{
    MatchOpType, Matcher as FBMatcher, Matchers as FBMatchers, MatchersArgs, MatchersCondition,
};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, Vector, WIPOffset};
use metricsql_parser::label::{MatchOp, Matcher, Matchers};
use smallvec::smallvec;
use valkey_module::{ValkeyError, ValkeyResult};

fn match_op_to_fb(op: MatchOp) -> MatchOpType {
    match op {
        MatchOp::Equal => MatchOpType::Equal,
        MatchOp::NotEqual => MatchOpType::NotEqual,
        MatchOp::RegexEqual => MatchOpType::RegexEqual,
        MatchOp::RegexNotEqual => MatchOpType::RegexNotEqual,
    }
}

fn fb_to_match_op(op: MatchOpType) -> MatchOp {
    match op {
        MatchOpType::Equal => MatchOp::Equal,
        MatchOpType::NotEqual => MatchOp::NotEqual,
        MatchOpType::RegexEqual => MatchOp::RegexEqual,
        MatchOpType::RegexNotEqual => MatchOp::RegexNotEqual,
        _ => panic!("Unsupported match operation type"),
    }
}

fn decompose_matcher(matcher: &Matcher) -> LocalMatcher {
    let op = match_op_to_fb(matcher.op);
    LocalMatcher {
        group_idx: 0,
        label: matcher.label.clone(),
        op,
        value: smallvec![matcher.value.clone()],
    }
}

pub(super) fn serialize_matchers<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    matchers: &Matchers,
) -> WIPOffset<FBMatchers<'a>> {
    fn handle_and_matchers(dest: &mut Vec<LocalMatcher>, index: usize, matchers: &[Matcher]) {
        for matcher in matchers.iter() {
            let mut item = decompose_matcher(matcher);
            item.group_idx = index;
            dest.push(item);
        }
    }

    // Create a name string if present
    let name = matchers
        .name
        .as_ref()
        .map(|name| bldr.create_string(name.as_str()));

    let mut local_matchers = Vec::with_capacity(6);
    // Determine the condition type and handle matchers accordingly

    let condition: MatchersCondition = if !matchers.matchers.is_empty() {
        handle_and_matchers(&mut local_matchers, 0, &matchers.matchers);
        MatchersCondition::And
    } else if !matchers.or_matchers.is_empty() {
        // First, serialize all matchers and collect their offsets
        for (i, matcher_list) in matchers.or_matchers.iter().enumerate() {
            handle_and_matchers(&mut local_matchers, i, matcher_list);
        }
        MatchersCondition::Or
    } else {
        // No matchers provided, return an empty FBMatchers
        return FBMatchers::create(
            bldr,
            &MatchersArgs {
                matchers: None,
                condition: MatchersCondition::And,
                name,
            },
        );
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
    let name = filter.name().map(|n| n.to_string());
    let Some(matchers) = filter.matchers() else {
        return Err(ValkeyError::Str("TSDB: matchers cannot be empty"));
    };

    match filter.condition() {
        MatchersCondition::And => {
            let items = matchers
                .iter()
                .map(|x| deserialize_matcher(&x))
                .collect::<ValkeyResult<Vec<_>>>()?;

            Ok(Matchers::with_matchers(name, items))
        }
        MatchersCondition::Or => {
            let mut or_matchers = Vec::new();
            let mut items = matchers.iter().enumerate();
            let mut group_idx = 0;
            let mut and_matchers = Vec::new();
            for (idx, matcher) in items.by_ref() {
                let converted = deserialize_matcher(&matcher)?;
                let group_id = matcher.group_id();
                if idx == 0 {
                    group_idx = group_id;
                }
                if group_idx != group_id {
                    group_idx = group_id;
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
            Ok(Matchers::with_or_matchers(name, or_matchers))
        }
        _ => Err(ValkeyError::Str("TSDB: unsupported matcher type in stream")),
    }
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

    let label = label.unwrap_or_default().to_string();
    if label.is_empty() {
        return Err(ValkeyError::Str("TSDB: matcher label cannot be empty"));
    }

    fn get_predicate_value(value: &FBMatcher) -> ValkeyResult<String> {
        let items: Vec<_> = value.value().unwrap_or_default().iter().take(1).collect();

        if items.is_empty() {
            return Err(ValkeyError::Str("TSDB: matcher value cannot be empty"));
        }

        Ok(items[0].to_string())
    }

    let value = get_predicate_value(request_matcher)?;
    let match_op = fb_to_match_op(op);

    match match_op {
        MatchOp::Equal => Ok(Matcher::equal(label, value)),
        MatchOp::NotEqual => Ok(Matcher::not_equal(label, value)),
        MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
            if value.is_empty() {
                return Err(ValkeyError::Str("TSDB: regex value cannot be empty"));
            }
            let matcher = if match_op == MatchOp::RegexEqual {
                Matcher::regex_equal(label, value)
            } else if match_op == MatchOp::RegexNotEqual {
                Matcher::regex_notequal(label, value)
            } else {
                return Err(ValkeyError::Str("TSDB: unsupported regex match operation"));
            };

            let res = matcher.map_err(|_e| ValkeyError::Str("TSDB: invalid regex value"))?;

            Ok(res)
        }
    }
}
