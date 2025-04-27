use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::regex::parse_regex_anchored;
use crate::parser::lex::Token;
use crate::parser::utils::escape_ident;
use crate::parser::ParseError;
use enquote::enquote;
use regex::Regex;
use smallvec::SmallVec;
use std::cmp::PartialEq;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

const EMPTY_TEXT: &str = "";

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

impl MatchOp {
    pub fn is_regex(&self) -> bool {
        matches!(self, MatchOp::RegexEqual | MatchOp::RegexNotEqual)
    }
}

impl Display for MatchOp {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            MatchOp::Equal => write!(f, "="),
            MatchOp::NotEqual => write!(f, "!="),
            MatchOp::RegexEqual => write!(f, "=~"),
            MatchOp::RegexNotEqual => write!(f, "!~"),
        }
    }
}

impl TryFrom<Token> for MatchOp {
    type Error = ParseError;

    fn try_from(value: Token) -> Result<Self, Self::Error> {
        match value {
            Token::Equal => Ok(MatchOp::Equal),
            Token::OpNotEqual => Ok(MatchOp::NotEqual),
            Token::RegexEqual => Ok(MatchOp::RegexEqual),
            Token::RegexNotEqual => Ok(MatchOp::RegexNotEqual),
            _ => Err(ParseError::InvalidMatchOperator(value.to_string())),
        }
    }
}

pub type ValueList = SmallVec<String, 4>;

#[derive(Clone, Debug)]
pub enum PredicateValue {
    Empty,
    List(ValueList),
    String(String),
}

impl Display for PredicateValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PredicateValue::Empty => write!(f, ""),
            PredicateValue::String(s) => write!(f, "{}", enquote('"', s)),
            PredicateValue::List(values) => {
                let mut first = true;
                write!(f, "(")?;
                for value in values {
                    if first {
                        first = false;
                    } else {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", enquote('"', value))?;
                }
                write!(f, ")")?;
                Ok(())
            }
        }
    }
}

impl PredicateValue {
    pub fn matches(&self, value: &str) -> bool {
        match self {
            PredicateValue::Empty => value.is_empty(),
            PredicateValue::String(s) => s == value,
            PredicateValue::List(list) => {
                if value.is_empty() {
                    return list.is_empty();
                }
                list.iter().any(|x| x.as_str() == value)
            }
        }
    }

    pub fn text(&self) -> Option<&str> {
        match self {
            PredicateValue::Empty => None,
            PredicateValue::String(s) => Some(s),
            PredicateValue::List(list) => {
                if list.len() == 1 {
                    return list.first().map(|x| x.as_str());
                }
                None
            }
        }
    }

    fn cost(&self) -> usize {
        match self {
            PredicateValue::Empty => 0,
            PredicateValue::String(_s) => 1,
            PredicateValue::List(s) => s.len(),
        }
    }
}

impl From<Vec<String>> for PredicateValue {
    fn from(value: Vec<String>) -> Self {
        PredicateValue::List(value.into())
    }
}

impl From<&str> for PredicateValue {
    fn from(value: &str) -> Self {
        PredicateValue::String(value.to_string())
    }
}

impl From<String> for PredicateValue {
    fn from(value: String) -> Self {
        PredicateValue::String(value)
    }
}

impl PartialEq for PredicateValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PredicateValue::Empty, PredicateValue::Empty) => true,
            (PredicateValue::String(s1), PredicateValue::String(s2)) => s1 == s2,
            (PredicateValue::List(v1), PredicateValue::List(v2)) => v1 == v2,
            _ => false,
        }
    }
}

impl Hash for PredicateValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PredicateValue::Empty => state.write_u8(1),
            PredicateValue::String(s) => {
                state.write_u8(2);
                s.hash(state);
            }
            PredicateValue::List(values) => {
                state.write_u8(3);
                for value in values {
                    value.hash(state);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegexMatcher {
    pub(crate) regex: Regex,
    pub value: String, // original (possibly unanchored) string
}

impl Hash for RegexMatcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.regex.as_str().hash(state);
        self.value.hash(state);
    }
}

impl RegexMatcher {
    fn new(regex: Regex, value: String) -> Self {
        Self { regex, value }
    }

    pub fn create(value: &str) -> Result<Self, ParseError> {
        let (regex, unanchored) = parse_regex_anchored(value)?;
        Ok(Self::new(regex, unanchored.to_string()))
    }

    pub fn is_match(&self, other: &str) -> bool {
        if other.is_empty() {
            return is_empty_regex_matcher(&self.regex);
        }
        self.regex.is_match(other)
    }
}

impl TryFrom<&str> for RegexMatcher {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::create(value)
    }
}

impl Display for RegexMatcher {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", enquote('"', &self.value))
    }
}

impl Eq for RegexMatcher {}

impl PartialEq for RegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
    }
}

#[derive(Clone, Debug)]
pub enum PredicateMatch {
    Equal(PredicateValue),
    NotEqual(PredicateValue),
    RegexEqual(RegexMatcher),
    RegexNotEqual(RegexMatcher),
}

impl PredicateMatch {
    pub fn op(&self) -> MatchOp {
        match self {
            PredicateMatch::Equal(_) => MatchOp::Equal,
            PredicateMatch::NotEqual(_) => MatchOp::NotEqual,
            PredicateMatch::RegexEqual(_) => MatchOp::RegexEqual,
            PredicateMatch::RegexNotEqual(_) => MatchOp::RegexNotEqual,
        }
    }

    pub fn matches(&self, other: &str) -> bool {
        match self {
            PredicateMatch::Equal(value) => value.matches(other),
            PredicateMatch::NotEqual(value) => !value.matches(other),
            PredicateMatch::RegexEqual(re) => re.is_match(other),
            PredicateMatch::RegexNotEqual(re) => !re.is_match(other),
        }
    }

    pub fn inverse(self) -> Self {
        match self {
            PredicateMatch::Equal(value) => PredicateMatch::NotEqual(value),
            PredicateMatch::NotEqual(value) => PredicateMatch::Equal(value),
            PredicateMatch::RegexEqual(re) => PredicateMatch::RegexNotEqual(re),
            PredicateMatch::RegexNotEqual(re) => PredicateMatch::RegexEqual(re),
        }
    }

    pub(crate) fn cost(&self) -> usize {
        match &self {
            PredicateMatch::Equal(value) => value.cost(),
            PredicateMatch::NotEqual(value) => value.cost(),
            PredicateMatch::RegexEqual(_) => 50,
            PredicateMatch::RegexNotEqual(_) => 55,
        }
    }

    fn text(&self) -> Option<&str> {
        match self {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => value.text(),
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => Some(&re.value),
        }
    }
}

impl Hash for PredicateMatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PredicateMatch::Equal(value) => {
                state.write_u8(1);
                value.hash(state);
            }
            PredicateMatch::NotEqual(value) => {
                state.write_u8(2);
                value.hash(state);
            }
            PredicateMatch::RegexEqual(re) => {
                state.write_u8(3);
                re.hash(state);
            }
            PredicateMatch::RegexNotEqual(re) => {
                state.write_u8(4);
                re.hash(state);
            }
        }
    }
}

impl Eq for PredicateMatch {}

impl PartialEq for PredicateMatch {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PredicateMatch::Equal(v1), PredicateMatch::Equal(v2)) => v1 == v2,
            (PredicateMatch::NotEqual(v1), PredicateMatch::NotEqual(v2)) => v1 == v2,
            (PredicateMatch::RegexEqual(re1), PredicateMatch::RegexEqual(re2)) => re1 == re2,
            (PredicateMatch::RegexNotEqual(re1), PredicateMatch::RegexNotEqual(re2)) => re1 == re2,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matcher {
    pub label: String,
    pub matcher: PredicateMatch,
}

impl Matcher {
    pub fn create<N, V>(match_op: MatchOp, label: N, value: V) -> Result<Self, ParseError>
    where
        N: Into<String>,
        V: Into<String>,
    {
        let label = label.into();
        let value = value.into();

        match match_op {
            MatchOp::Equal => Ok(Self::equals(label, &value)),
            MatchOp::NotEqual => Ok(Self::not_equals(label, &value)),
            MatchOp::RegexEqual => {
                let re = RegexMatcher::create(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::RegexEqual(re),
                })
            }
            MatchOp::RegexNotEqual => {
                let re = RegexMatcher::create(&value)?;
                Ok(Self {
                    label,
                    matcher: PredicateMatch::RegexNotEqual(re),
                })
            }
        }
    }

    pub fn equals(label: String, value: &str) -> Self {
        Self {
            label,
            matcher: PredicateMatch::Equal(value.into()),
        }
    }

    pub fn not_equals(label: String, value: &str) -> Self {
        Self {
            label,
            matcher: PredicateMatch::NotEqual(value.into()),
        }
    }

    pub fn op(&self) -> MatchOp {
        self.matcher.op()
    }

    pub fn inverse(self) -> Self {
        Self {
            label: self.label,
            matcher: self.matcher.inverse(),
        }
    }

    pub fn regex_text(&self) -> Option<&str> {
        match &self.matcher {
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => Some(&re.value),
            _ => None,
        }
    }

    pub fn is_empty_matcher(&self) -> bool {
        self.matches("")
    }

    pub fn is_metric_name_filter(&self) -> bool {
        self.label == METRIC_NAME_LABEL && self.op() == MatchOp::Equal
    }

    pub fn matches(&self, value: &str) -> bool {
        self.matcher.matches(value)
    }

    pub(crate) fn cost(&self) -> usize {
        self.matcher.cost()
    }

    fn text(&self) -> Option<&str> {
        self.matcher.text()
    }
}

impl Display for Matcher {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}{}", self.label, self.op())?;
        match &self.matcher {
            PredicateMatch::Equal(value) | PredicateMatch::NotEqual(value) => {
                write!(f, "{}", value)
            }
            PredicateMatch::RegexEqual(re) | PredicateMatch::RegexNotEqual(re) => {
                write!(f, "{}", enquote('"', &re.value))
            }
        }
    }
}

impl Hash for Matcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.matcher.hash(state);
    }
}

fn is_empty_regex_matcher(re: &Regex) -> bool {
    // cheap check
    let value = re.as_str();
    let matches_empty = match value.len() {
        0 => true,
        1 => value == "^" || value == "$",
        2 => value == ".*" || value == "^$" || value == "?:",
        3 => value == "^.*" || value == ".*$",
        4 => value == "^.*$" || value == "^?:$",
        _ => false,
    };
    matches_empty || re.is_match("")
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub enum MatcherSetEnum {
    Or(Vec<Vec<Matcher>>),
    And(Vec<Matcher>),
}

impl MatcherSetEnum {
    pub fn is_empty(&self) -> bool {
        match self {
            MatcherSetEnum::Or(or_matchers) => or_matchers.is_empty(),
            MatcherSetEnum::And(and_matchers) => and_matchers.is_empty(),
        }
    }

    fn normalize(self, name: Option<String>) -> (Self, Option<String>) {
        // now normalize the matchers
        let mut name = name;
        let mut matchers = self;
        match matchers {
            MatcherSetEnum::And(ref mut and_matchers) => {
                if !and_matchers.is_empty() {
                    for (i, matcher) in and_matchers.iter_mut().enumerate() {
                        if matcher.is_metric_name_filter() {
                            if name.is_none() {
                                name = matcher.text().map(|text| text.to_string())
                            }
                            and_matchers.remove(i);
                            break;
                        }
                    }
                }
            }
            MatcherSetEnum::Or(ref mut or_matchers) => {
                if !or_matchers.is_empty() {
                    if let Some(metric_name) = Self::normalize_matcher_list(or_matchers) {
                        if name.is_none() {
                            name = Some(metric_name);
                        }
                        if or_matchers.len() == 1 {
                            let and_matchers = or_matchers.pop().expect("or_matchers is not empty");
                            return (MatcherSetEnum::And(and_matchers), name);
                        }
                    }
                }
            }
        }

        (matchers, name)
    }

    fn normalize_matcher_list(matchers: &mut Vec<Vec<Matcher>>) -> Option<String> {
        // if we have a __name__ filter, we need to ensure that all matchers have the same name
        // if so, we pull out the name and return it while removing the __name__ filter from all matchers

        // track name filters. Use Smallvec instead of HashSet to avoid allocations
        let mut to_remove: SmallVec<(usize, usize, bool), 4> = SmallVec::new();

        let name = {
            let mut metric_name: &str = "";

            let first = matchers.first()?;
            for (i, m) in first.iter().enumerate() {
                if m.is_metric_name_filter() {
                    metric_name = m.text().unwrap_or(EMPTY_TEXT);
                    to_remove.push((0, i, first.len() == 1));
                    break;
                }
            }

            if metric_name.is_empty() {
                return None;
            }

            let mut i: usize = 1;

            for match_list in matchers.iter().skip(1) {
                let mut found = false;
                for (j, m) in match_list.iter().enumerate() {
                    if m.is_metric_name_filter() {
                        let value = m.text().unwrap_or(EMPTY_TEXT);
                        if value != metric_name {
                            return None;
                        }
                        found = true;
                        to_remove.push((i, j, match_list.len() == 1));
                        break;
                    }
                }
                if !found && metric_name.is_empty() {
                    return None;
                }
                i += 1;
            }

            metric_name.to_string()
        };

        // remove the __name__ filter from all matchers
        for (i, j, remove) in to_remove.iter().rev() {
            if *remove {
                matchers.remove(*i);
            } else {
                matchers[*i].remove(*j);
            }
        }

        Some(name)
    }
}

impl Display for MatcherSetEnum {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self {
            MatcherSetEnum::Or(or_matchers) => {
                for (i, matchers) in or_matchers.iter().enumerate() {
                    if i > 0 {
                        write!(f, " or ")?;
                    }
                    join_matchers(f, matchers)?;
                }
                Ok(())
            }
            MatcherSetEnum::And(and_matchers) => join_matchers(f, and_matchers),
        }
    }
}

impl Default for MatcherSetEnum {
    fn default() -> Self {
        MatcherSetEnum::And(vec![])
    }
}

#[derive(Debug, Clone, Default)]
#[derive(PartialEq)]
pub struct Matchers {
    pub name: Option<String>,
    pub matchers: MatcherSetEnum,
}

impl Matchers {
    pub fn with_matchers(name: Option<String>, matchers: Vec<Matcher>) -> Self {
        let (matchers, name) = MatcherSetEnum::And(matchers).normalize(name);
        Matchers { name, matchers }
    }

    pub fn with_or_matchers(name: Option<String>, or_matchers: Vec<Vec<Matcher>>) -> Self {
        if or_matchers.len() == 1 {
            let mut or_matchers = or_matchers;
            return Self::with_matchers(name, or_matchers.pop().expect("or_matchers is not empty"));
        }
        let (matchers, name) = MatcherSetEnum::Or(or_matchers).normalize(name);
        Matchers { name, matchers }
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_none() && self.matchers.is_empty()
    }

    pub fn is_only_metric_name(&self) -> bool {
        self.name.is_some() && self.matchers.is_empty()
    }
}

const MATCHER_HASH_ID: u8 = 1;
const NAME_HASH_ID: u8 = 3;
const OR_HASH_ID: u8 = 5;
const AND_HASH_ID: u8 = 7;

impl Hash for Matchers {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(name) = &self.name {
            state.write_u8(NAME_HASH_ID);
            name.hash(state);
        }
        if !self.matchers.is_empty() {
            state.write_u8(MATCHER_HASH_ID);
            // constants added here since an empty Vec<Matcher> is equivalent tp an empty Vec<Vec<Matcher>>()
            match &self.matchers {
                MatcherSetEnum::Or(_) => {
                    state.write_u8(OR_HASH_ID);
                }
                MatcherSetEnum::And(_) => {
                    state.write_u8(AND_HASH_ID);
                }
            }
            self.matchers.hash(state);
        }
    }
}

impl Display for Matchers {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.is_empty() {
            write!(f, "{{}}")?;
            return Ok(());
        }

        if let Some(name) = &self.name {
            write!(f, "{}", escape_ident(name))?;
        }

        if self.is_only_metric_name() {
            return Ok(());
        }

        write!(f, "{{{}}}", &self.matchers)?;
        Ok(())
    }
}

fn join_matchers(f: &mut Formatter<'_>, v: &[Matcher]) -> fmt::Result {
    for (i, matcher) in v.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", matcher)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::labels::matchers::RegexMatcher;

    #[test]
    fn test_match_anchoring() {
        // Test anchoring behavior - should match full string only
        let matcher = RegexMatcher::create("abc.*").unwrap();
        assert!(matcher.is_match("abc123"));
        assert!(!matcher.is_match("xabc123"));

        let matcher = RegexMatcher::create(".*xyz$").unwrap();
        assert!(matcher.is_match("123xyz"));
        assert!(!matcher.is_match("123xyzx"));

        let matcher = RegexMatcher::create("abc").unwrap();

        assert!(matcher.is_match("abc"));
        assert!(!matcher.is_match("xabc"));
        assert!(!matcher.is_match("abcx"));
    }
}
