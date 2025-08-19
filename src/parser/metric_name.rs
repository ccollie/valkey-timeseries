use super::lex::{Token, expect_one_of_tokens, expect_token, get_next_token};
use super::parse_error::unexpected;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::Label;
use crate::parser::ParseResult;
use crate::parser::utils::extract_string_value;
use logos::{Lexer, Logos};

/// specialized parser for a `Prometheus` compatible metric name (as opposed to a metric selector).
///
///    <label_set>
///
///    <metric_identifier> [<label_set>]
///
pub fn parse_metric_name(s: &str) -> ParseResult<Vec<Label>> {
    let mut lex = Token::lexer(s);

    let mut labels: Vec<Label> = Vec::new();

    let measurement = expect_token(&mut lex, Token::Identifier)?;
    labels.push(Label::new(
        METRIC_NAME_LABEL.to_string(),
        measurement.to_string(),
    ));
    let res = get_next_token(&mut lex)?;
    if res.0 == Token::Eof {
        return Ok(labels);
    }
    if res.0 == Token::LeftBrace {
        parse_label_filters(&mut lex, &mut labels)?;
    }
    // todo: expect eof
    Ok(labels)
}

/// parse a set of label matchers.
///
/// '{' [ <label_name> <match_op> <match_string>, ... '}'
///
pub(crate) fn parse_label_filters(
    lex: &mut Lexer<Token>,
    labels: &mut Vec<Label>,
) -> ParseResult<()> {
    use Token::*;

    let mut was_value = false;

    loop {
        let (tok, name) = if was_value {
            expect_one_of_tokens(lex, &[Comma, RightBrace])
        } else {
            expect_one_of_tokens(lex, &[Identifier, Comma, RightBrace])
        }?;
        match tok {
            Identifier => {
                let name = name.to_string();
                let _ = expect_token(lex, Equal)?;
                let value = expect_token(lex, StringLiteral)?;
                let contents = extract_string_value(value)?;
                labels.push(Label::new(name, contents.to_string()));
                was_value = true;
            }
            Comma => {
                was_value = false;
            }
            RightBrace => break,
            _ => return Err(unexpected("metric name label", name, ", or }", None)),
        }
    }
    // make sure we're at eof

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metric_name() {
        let cases = vec![
            (
                "foo{}",
                vec![Label::new("__name__".to_string(), "foo".to_string())],
            ),
            (
                "foo",
                vec![Label::new("__name__".to_string(), "foo".to_string())],
            ),
            (
                "foo{bar=\"baz\"}",
                vec![
                    Label::new("__name__".to_string(), "foo".to_string()),
                    Label::new("bar".to_string(), "baz".to_string()),
                ],
            ),
            (
                "foo{bar=\"baz\", qux=\"quux\"}",
                vec![
                    Label::new("__name__".to_string(), "foo".to_string()),
                    Label::new("bar".to_string(), "baz".to_string()),
                    Label::new("qux".to_string(), "quux".to_string()),
                ],
            ),
            (
                "metric_name{label1=\"value1\", label2=\"value2\"}",
                vec![
                    Label::new("__name__".to_string(), "metric_name".to_string()),
                    Label::new("label1".to_string(), "value1".to_string()),
                    Label::new("label2".to_string(), "value2".to_string()),
                ],
            ),
            (
                "http_requests_total{method=\"post\", code=\"200\"}",
                vec![
                    Label::new("__name__".to_string(), "http_requests_total".to_string()),
                    Label::new("code".to_string(), "200".to_string()),
                    Label::new("method".to_string(), "post".to_string()),
                ],
            ),
            (
                "up{instance=\"localhost:9090\", job=\"prometheus\"}",
                vec![
                    Label::new("__name__".to_string(), "up".to_string()),
                    Label::new("instance".to_string(), "localhost:9090".to_string()),
                    Label::new("job".to_string(), "prometheus".to_string()),
                ],
            ),
        ];

        for (input, expected) in cases {
            let mut got = parse_metric_name(input).unwrap();
            got.sort();
            assert_eq!(got, expected);
        }
    }
}
