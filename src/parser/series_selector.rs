use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::matchers::{
    FilterList, LabelFilter, MatchOp, PredicateMatch, PredicateValue, SeriesSelector, ValueList,
};
use crate::parser::lex::{Token, expect_one_of_tokens, expect_token};
use crate::parser::parse_error::unexpected;
use crate::parser::utils::{extract_string_value, unescape_ident};
use crate::parser::{ParseError, ParseResult};
use logos::{Lexer, Logos};

const INITIAL_TOKENS: &[Token] = &[
    Token::Identifier,
    Token::OpOr,
    Token::LeftBrace,
    Token::StringLiteral,
];

const SECONDARY_TOKENS: &[Token] = &[
    Token::Equal,
    Token::OpNotEqual,
    Token::RegexEqual,
    Token::RegexNotEqual,
    Token::LeftBrace,
    Token::Eof,
];

/// Parses a series selector, either using RedisTimeSeries or Prometheus syntax.
///
///    I.e., it handles queries of the form (RedisTimeSeries):
///
///   * `region=(us-east-1,us-west-1)`
///   * `service="billing"`
///   * `sensor=bedroom`
///
///   Or (Prometheus):
///
///   * `request_latency{service="billing", env=~"staging|production", region=~"us-east-.*"}`
///   * `{service="inference", metric="request-count", env="prod"}`
///   * `{"my.dotted.metric", region="east"}` # New style
///
///  Produces a list of matchers, where each matcher is a label filter.
pub fn parse_series_selector(s: &str) -> ParseResult<SeriesSelector> {
    if s.is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }
    let mut lex = Token::lexer(s);
    let result = parse_series_selector_internal(&mut lex)?;
    expect_token(&mut lex, Token::Eof)?;
    Ok(result)
}

pub(crate) fn parse_series_selector_internal(p: &mut Lexer<Token>) -> ParseResult<SeriesSelector> {
    use Token::*;

    let (tok, text) = expect_one_of_tokens(p, INITIAL_TOKENS)?;

    if tok == LeftBrace {
        return parse_prometheus_selector_internal(p, String::new());
    }

    let text = match tok {
        Identifier => unescape_ident(text)?.to_string(),
        StringLiteral => {
            let value = extract_string_value(text)?;
            value.to_string()
        }
        _ => text.to_string(),
    };

    let (tok, _) = expect_one_of_tokens(p, SECONDARY_TOKENS)?;
    match tok {
        Eof => {
            let matcher = LabelFilter::equals(METRIC_NAME_LABEL.into(), &text);
            Ok(SeriesSelector::with_matchers(vec![matcher]))
        }
        LeftBrace => parse_prometheus_selector_internal(p, text),
        _ => {
            let matcher = parse_redis_ts_predicate(text, tok, p)?;
            Ok(SeriesSelector::with_matchers(vec![matcher]))
        }
    }
}

fn parse_prometheus_selector_internal(
    lex: &mut Lexer<Token>,
    name: String,
) -> ParseResult<SeriesSelector> {
    let name = if name.is_empty() { None } else { Some(name) };
    // LeftBrace already consumed
    parse_label_filters(lex, name)
}

/// support RedisTimeseries style selectors
fn parse_redis_ts_predicate(
    label: String,
    operator_token: Token,
    lex: &mut Lexer<Token>,
) -> ParseResult<LabelFilter> {
    let op: MatchOp = operator_token.try_into()?;

    if op.is_regex() {
        // we expect a string literal
        let value = parse_string_literal(lex)?;
        LabelFilter::create(op, label, value)
    } else {
        // value can be a string or a list of strings, or empty
        let value = parse_matcher_value(lex)?;
        match op {
            MatchOp::Equal => Ok(LabelFilter {
                label,
                matcher: PredicateMatch::Equal(value),
            }),
            MatchOp::NotEqual => Ok(LabelFilter {
                label,
                matcher: PredicateMatch::NotEqual(value),
            }),
            _ => unreachable!("parse_redis_ts_predicate: unexpected operator"),
        }
    }
}

/// `parse_label_filters` parses a set of label matchers.
///
/// `{` [ <label_name> <match_op> <match_string>, ... [or <label_name> <match_op> <match_string>, ...] `}`
///
fn parse_label_filters(p: &mut Lexer<Token>, name: Option<String>) -> ParseResult<SeriesSelector> {
    use Token::*;

    // left brace already consumed

    let mut or_matchers: Vec<FilterList> = Vec::new();
    let mut matchers: Vec<LabelFilter> = Vec::new();
    let mut has_or_matchers = false;

    // the underscore here is ugly but gets rid of the unused_assignment warning
    let mut _last_token = LeftBrace;
    let mut _has_metric_name_filter = false;

    loop {
        if has_or_matchers && !matchers.is_empty() {
            let last_matchers = std::mem::take(&mut matchers);
            let matchers = FilterList::new(last_matchers);
            or_matchers.push(matchers);
        }

        (matchers, _last_token, _has_metric_name_filter) = parse_label_filters_internal(p)?;
        // if name has a value, it must be added to each or_matchers
        if let Some(name) = &name
            && !_has_metric_name_filter
        {
            let metric_name_matcher = LabelFilter::equals(METRIC_NAME_LABEL.into(), name);
            matchers.push(metric_name_matcher);
        }

        match _last_token {
            RightBrace => {
                break;
            }
            OpOr => {
                has_or_matchers = true;
            }
            _ => {
                return Err(unexpected(
                    "label filter",
                    _last_token.as_str(),
                    "OR or }",
                    None,
                ));
            }
        }
    }

    if has_or_matchers {
        if !matchers.is_empty() {
            let matchers = FilterList::new(matchers);
            or_matchers.push(matchers);
        }
        // todo: validate name
        return Ok(SeriesSelector::Or(or_matchers));
    }

    Ok(SeriesSelector::with_matchers(matchers))
}

/// parse_label_filters parses a set of label matchers.
///
/// [ <label_name> <match_op> <match_string>, ... ]
///
fn parse_label_filters_internal(
    p: &mut Lexer<Token>,
) -> ParseResult<(Vec<LabelFilter>, Token, bool)> {
    use Token::*;

    let mut matchers: Vec<LabelFilter> = vec![];
    let mut metric_name_seen = false;

    loop {
        let (matcher, tok) = parse_label_filter(p, !metric_name_seen)?;
        if matcher.is_metric_name_filter() {
            if metric_name_seen {
                return Err(unexpected(
                    "metric name",
                    matcher.label.as_str(),
                    "only one metric name allowed",
                    None,
                ));
            }
            metric_name_seen = true;
            if matchers.is_empty() {
                matchers.push(matcher);
            } else {
                matchers.insert(0, matcher)
            }
        } else {
            matchers.push(matcher);
        }

        let tok = match tok {
            Some(tok) => tok,
            None => {
                let (tok, _) = expect_one_of_tokens(p, &[Comma, RightBrace, OpOr])?;
                tok
            }
        };

        if tok == RightBrace || tok == OpOr {
            return Ok((matchers, tok, metric_name_seen));
        }
    }
}

/// parse_label_filter parses a single label matcher.
///
///   <label_name> <match_op> <match_string> | <identifier> | <quoted_string>
///
fn parse_label_filter(
    p: &mut Lexer<Token>,
    accept_single: bool,
) -> ParseResult<(LabelFilter, Option<Token>)> {
    use Token::*;

    let label = expect_label_name(p)?;

    let (tok, _) = if accept_single {
        expect_one_of_tokens(
            p,
            &[
                Equal,
                OpNotEqual,
                RegexEqual,
                RegexNotEqual,
                Comma,
                RightBrace,
                OpOr,
            ],
        )
    } else {
        expect_one_of_tokens(
            p,
            &[
                Equal,
                OpNotEqual,
                RegexEqual,
                RegexNotEqual,
                Comma,
                RightBrace,
            ],
        )
    }?;

    match tok {
        Comma | RightBrace | OpOr => {
            let matcher = LabelFilter {
                label: METRIC_NAME_LABEL.into(),
                matcher: PredicateMatch::Equal(PredicateValue::String(label)),
            };
            return Ok((matcher, Some(tok)));
        }
        _ => {}
    }
    let op: MatchOp = tok.try_into()?;

    if op.is_regex() {
        let value = parse_string_literal(p)?;
        Ok((LabelFilter::create(op, label, value)?, None))
    } else {
        let value = parse_matcher_value(p)?;
        match op {
            MatchOp::Equal => Ok((
                LabelFilter {
                    label,
                    matcher: PredicateMatch::Equal(value),
                },
                None,
            )),
            MatchOp::NotEqual => Ok((
                LabelFilter {
                    label,
                    matcher: PredicateMatch::NotEqual(value),
                },
                None,
            )),
            _ => unreachable!("parse_label_filter: unexpected operator"),
        }
    }
}

fn expect_label_name(lex: &mut Lexer<Token>) -> ParseResult<String> {
    let (tok, text) = expect_one_of_tokens(lex, &[Token::Identifier, Token::StringLiteral])?;
    match tok {
        Token::Identifier => Ok(unescape_ident(text)?.to_string()),
        Token::StringLiteral => {
            let value = extract_string_value(text)?;
            Ok(value.to_string())
        }
        _ => {
            unreachable!("expect_label_name: unexpected token. Need an identifier or quoted string")
        }
    }
}

fn parse_string_literal(lexer: &mut Lexer<Token>) -> ParseResult<String> {
    let value = expect_token(lexer, Token::StringLiteral)?;
    let extracted = extract_string_value(value)?;
    Ok(extracted.to_string())
}

pub(crate) fn parse_matcher_value(lexer: &mut Lexer<Token>) -> ParseResult<PredicateValue> {
    use Token::*;

    let (tok, text) =
        expect_one_of_tokens(lexer, &[StringLiteral, Identifier, Number, LeftParen, Eof])?;
    match tok {
        Eof => return Ok(PredicateValue::Empty),
        Identifier | Number => {
            return Ok(PredicateValue::String(text.to_string()));
        }
        StringLiteral => {
            let value = extract_string_value(text)?;
            return Ok(PredicateValue::String(value.to_string()));
        }
        _ => {}
    }
    let mut was_value = false;

    let mut values: ValueList = ValueList::new();
    loop {
        let (tok, name) = if was_value {
            expect_one_of_tokens(lexer, &[Comma, RightParen])?
        } else {
            expect_one_of_tokens(
                lexer,
                &[StringLiteral, Identifier, Number, Comma, RightParen],
            )?
        };

        match tok {
            Identifier | Number => {
                values.push(name.to_string());
                was_value = true;
            }
            StringLiteral => {
                let value = extract_string_value(name)?;
                values.push(value.to_string());
                was_value = true;
            }
            Comma => {
                was_value = false;
                continue;
            }
            RightParen => {
                break;
            }
            _ => return Err(unexpected("metric name label", name, ", or }", None)),
        }
    }

    match values.len() {
        0 => Ok(PredicateValue::Empty),
        1 => {
            let value = values.pop().expect("values is not empty");
            Ok(PredicateValue::String(value))
        }
        _ => Ok(PredicateValue::List(values)),
    }
}
