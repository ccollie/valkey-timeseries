use crate::labels::matchers::{
    MatchOp, Matcher, Matchers, PredicateMatch, PredicateValue, ValueList,
};
use crate::parser::lex::{expect_one_of_tokens, expect_token, Token};
use crate::parser::parse_error::unexpected;
use crate::parser::utils::{extract_string_value, unescape_ident};
use crate::parser::{ParseError, ParseResult};
use logos::{Lexer, Logos};
use crate::common::constants::METRIC_NAME_LABEL;

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

/// parses a series selector, either using RedisTimeSeries or Prometheus syntax.
///
///    i.e, it handles queries of the form (RedisTimeSeries):
///
///   * `region=(us-east-1,us-west-1)`
///   * `service="billing"`
///
///   or (Prometheus):
///
///   * `request_latency{service="billing", env=~"staging|production", region=~"us-east-.*"}`
///   * `{service="inference", metric="request-count", env="prod"}`
///   * `{"my.dotted.metric", region="east"}` # New style
///
///  Produces a list of matchers, where each matcher is a label filter.
pub fn parse_series_selector(s: &str) -> ParseResult<Matchers> {
    if s.is_empty() {
        return Err(ParseError::EmptySeriesSelector);
    }
    let mut lex = Token::lexer(s);
    let result = parse_series_selector_internal(&mut lex)?;
    expect_token(&mut lex, Token::Eof)?;
    Ok(result)
}

pub(crate) fn parse_series_selector_internal(p: &mut Lexer<Token>) -> ParseResult<Matchers> {
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
        Eof => Ok(Matchers {
            name: Some(text),
            ..Default::default()
        }),
        LeftBrace => parse_prometheus_selector_internal(p, text),
        _ => {
            let matcher = parse_redis_ts_predicate(text, tok, p)?;
            Ok(Matchers::with_matchers(None, vec![matcher]))
        }
    }
}

fn parse_prometheus_selector_internal(
    lex: &mut Lexer<Token>,
    name: String,
) -> ParseResult<Matchers> {
    let name = if name.is_empty() { None } else { Some(name) };
    // LeftBrace already consumed
    parse_label_filters(lex, name)
}

/// support RedisTimeseries style selectors
/// See https://redis.io/docs/latest/commands/ts.mget/
fn parse_redis_ts_predicate(
    label: String,
    operator_token: Token,
    lex: &mut Lexer<Token>,
) -> ParseResult<Matcher> {
    let op: MatchOp = operator_token.try_into()?;

    if op.is_regex() {
        // we expect a string literal
        let value = parse_string_literal(lex)?;
        Matcher::create(op, label, value)
    } else {
        // value can be a string or a list of strings, or empty
        let value = parse_matcher_value(lex)?;
        match op {
            MatchOp::Equal => Ok(Matcher {
                label,
                matcher: PredicateMatch::Equal(value),
            }),
            MatchOp::NotEqual => Ok(Matcher {
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
fn parse_label_filters(p: &mut Lexer<Token>, name: Option<String>) -> ParseResult<Matchers> {
    use Token::*;

    // left brace already consumed

    let mut or_matchers: Vec<Vec<Matcher>> = Vec::new();
    let mut matchers: Vec<Matcher> = Vec::new();
    let mut has_or_matchers = false;

    // underscore here is ugly, but gets rid of the unused_assignment warning
    let mut _last_token = LeftBrace;

    loop {
        if has_or_matchers && !matchers.is_empty() {
            let last_matchers = std::mem::take(&mut matchers);
            or_matchers.push(last_matchers);
        }

        (matchers, _last_token) = parse_label_filters_internal(p)?;

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
                ))
            }
        }
    }

    if has_or_matchers {
        if !matchers.is_empty() {
            or_matchers.push(matchers);
        }
        // todo: validate name
        return Ok(Matchers::with_or_matchers(name, or_matchers));
    }

    Ok(Matchers::with_matchers(name, matchers))
}

/// parse_label_filters parses a set of label matchers.
///
/// [ <label_name> <match_op> <match_string>, ... ]
///
fn parse_label_filters_internal(p: &mut Lexer<Token>) -> ParseResult<(Vec<Matcher>, Token)> {
    use Token::*;

    let mut matchers: Vec<Matcher> = vec![];
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
            return Ok((matchers, tok));
        }
    }
}

/// parse_label_filter parses a single label matcher.
///
///   <label_name> <match_op> <match_string> | <identifier> | <quoted_string>
///
fn parse_label_filter(p: &mut Lexer<Token>, accept_single: bool) -> ParseResult<(Matcher, Option<Token>)> {
    use Token::*;

    let label = expect_label_name(p)?;

    let (tok, _) = if accept_single {
        expect_one_of_tokens(p, &[Equal, OpNotEqual, RegexEqual, RegexNotEqual, Comma, RightBrace, OpOr])
    } else {
        expect_one_of_tokens(p, &[Equal, OpNotEqual, RegexEqual, RegexNotEqual, Comma, RightBrace])
    }?;

    match tok {
        Comma | RightBrace | OpOr => {
            let matcher = Matcher {
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
        Ok((Matcher::create(op, label, value)?, None))
    } else {
        let value = parse_matcher_value(p)?;
        match op {
            MatchOp::Equal => Ok((Matcher { label, matcher: PredicateMatch::Equal(value) }, None)),
            MatchOp::NotEqual => Ok((Matcher { label, matcher: PredicateMatch::NotEqual(value) }, None)),
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

    let (tok, text) = expect_one_of_tokens(lexer, &[StringLiteral, Identifier, LeftParen, Eof])?;
    match tok {
        Eof => return Ok(PredicateValue::Empty),
        Identifier => {
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
            expect_one_of_tokens(lexer, &[StringLiteral, Identifier, Comma, RightParen])?
        };

        match tok {
            Identifier => {
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
