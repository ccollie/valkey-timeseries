use crate::labels::matchers::{
    MatchOp, Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue, RegexMatcher,
};
use metricsql_parser::prelude::{
    MatchOp as ParserMatchOp, Matcher as ParserMatcher, Matchers as ParserMatchers,
};
use regex::Regex;
use valkey_module::ValkeyError;

const NON_MATCHING_REGEX: &str = r"\b\B"; // Default regex for non-matching cases
const ALL_MATCHING_REGEX: &str = r".*"; // Default regex for non-matching cases

pub(super) fn convert_vec(matchers: Vec<ParserMatcher>) -> Vec<Matcher> {
    matchers.into_iter().map(|matcher| matcher.into()).collect()
}

impl TryFrom<ParserMatchers> for Matchers {
    type Error = ValkeyError;
    fn try_from(parser_matchers: ParserMatchers) -> Result<Self, Self::Error> {
        if !parser_matchers.matchers.is_empty() {
            let matchers = convert_vec(parser_matchers.matchers);
            Ok(Matchers::with_matchers(None, matchers))
        } else if !parser_matchers.or_matchers.is_empty() {
            let or_matchers = parser_matchers.or_matchers;
            // Convert each group of matchers for OR logic
            let mut matchers = Vec::with_capacity(or_matchers.len());
            for matcher_group in or_matchers {
                let converted_matchers = convert_vec(matcher_group);
                matchers.push(converted_matchers);
            }
            Ok(Matchers::with_or_matchers(None, matchers))
        } else {
            // Both matchers and or_matchers are empty - return default
            Ok(Matchers::default())
        }
    }
}

impl From<ParserMatcher> for Matcher {
    fn from(parser_matcher: ParserMatcher) -> Self {
        let match_op = parser_matcher.op.into();
        let label = parser_matcher.label;
        let value = parser_matcher.value;
        let predicate_match = match match_op {
            MatchOp::Equal => PredicateMatch::Equal(PredicateValue::String(value)),
            MatchOp::NotEqual => PredicateMatch::NotEqual(PredicateValue::String(value)),
            MatchOp::RegexEqual => {
                let regex = Regex::new(&value).unwrap_or_else(|_| {
                    Regex::new(NON_MATCHING_REGEX).expect("Invalid non-matching regex")
                });
                let regex_matcher = RegexMatcher { regex, value };
                PredicateMatch::RegexEqual(regex_matcher)
            }
            MatchOp::RegexNotEqual => {
                let regex = Regex::new(&value).unwrap_or_else(|_| {
                    Regex::new(ALL_MATCHING_REGEX).expect("Invalid matching regex")
                });
                let regex_matcher = RegexMatcher { regex, value };
                PredicateMatch::RegexNotEqual(regex_matcher)
            }
        };
        Matcher {
            label,
            matcher: predicate_match,
        }
    }
}

impl From<ParserMatchOp> for MatchOp {
    fn from(parser_op: ParserMatchOp) -> Self {
        match parser_op {
            ParserMatchOp::Equal => MatchOp::Equal,
            ParserMatchOp::NotEqual => MatchOp::NotEqual,
            ParserMatchOp::RegexEqual => MatchOp::RegexEqual,
            ParserMatchOp::RegexNotEqual => MatchOp::RegexNotEqual,
        }
    }
}

impl TryFrom<Matchers> for ParserMatchers {
    type Error = ValkeyError;

    fn try_from(matchers: Matchers) -> Result<Self, Self::Error> {
        match matchers.matchers {
            MatcherSetEnum::And(and_matchers) => {
                let mut parser_matchers = Vec::with_capacity(and_matchers.len());
                for matcher in and_matchers {
                    let parser_matcher: ParserMatcher = matcher.try_into()?;
                    parser_matchers.push(parser_matcher);
                }
                Ok(ParserMatchers::with_matchers(
                    matchers.name,
                    parser_matchers,
                ))
            }
            MatcherSetEnum::Or(or_matchers) => {
                let mut parser_or_matchers = Vec::with_capacity(or_matchers.len());
                for matcher_group in or_matchers {
                    let parser_group: Vec<ParserMatcher> = matcher_group
                        .into_iter()
                        .map(|m| m.try_into())
                        .collect::<Result<Vec<_>, _>>()?;
                    parser_or_matchers.push(parser_group);
                }
                Ok(ParserMatchers::with_or_matchers(
                    matchers.name,
                    parser_or_matchers,
                ))
            }
        }
    }
}

impl TryFrom<Matcher> for ParserMatcher {
    type Error = ValkeyError;

    fn try_from(matcher: Matcher) -> Result<Self, Self::Error> {
        match matcher.matcher {
            PredicateMatch::Equal(PredicateValue::String(val)) => {
                Ok(ParserMatcher::equal(matcher.label, val))
            }
            PredicateMatch::NotEqual(PredicateValue::String(val)) => {
                Ok(ParserMatcher::not_equal(matcher.label, val))
            }
            PredicateMatch::RegexEqual(regex_matcher) => {
                let matcher = ParserMatcher::regex_equal(matcher.label, regex_matcher.value)
                    .map_err(|e| ValkeyError::String(format!("Invalid regex value: {e}")))?;
                Ok(matcher)
            }
            PredicateMatch::RegexNotEqual(regex_matcher) => {
                let matcher = ParserMatcher::regex_notequal(matcher.label, regex_matcher.value)
                    .map_err(|e| ValkeyError::String(format!("Invalid regex value: {e}")))?;
                Ok(matcher)
            }
            PredicateMatch::Equal(PredicateValue::Empty) => {
                Ok(ParserMatcher::equal(matcher.label, "".to_string()))
            }
            PredicateMatch::NotEqual(PredicateValue::Empty) => {
                Ok(ParserMatcher::not_equal(matcher.label, "".to_string()))
            }
            PredicateMatch::Equal(PredicateValue::List(_))
            | PredicateMatch::NotEqual(PredicateValue::List(_)) => Err(ValkeyError::String(
                "ParserMatcher does not support list values".into(),
            )),
        }
    }
}

impl From<MatchOp> for ParserMatchOp {
    fn from(match_op: MatchOp) -> Self {
        match match_op {
            MatchOp::Equal => ParserMatchOp::Equal,
            MatchOp::NotEqual => ParserMatchOp::NotEqual,
            MatchOp::RegexEqual => ParserMatchOp::RegexEqual,
            MatchOp::RegexNotEqual => ParserMatchOp::RegexNotEqual,
        }
    }
}
