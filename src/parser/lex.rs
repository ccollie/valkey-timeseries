use crate::parser::parse_error::unexpected;
use crate::parser::{ParseErr, ParseError, ParseResult};
use logos::{Lexer, Logos, Span};
use std::fmt::{Display, Formatter};

fn unterminated_string_literal(_: &mut Lexer<Token>) -> ParseResult<()> {
    Err(ParseError::SyntaxError(
        "unterminated string literal".to_string(),
    ))
}

#[derive(Logos, Debug, PartialEq, Clone, Copy)]
#[logos(error = ParseError)]
#[logos(skip r"[ \t\n\f\r]+")]
#[logos(skip r"#[^\r\n]*(\r\n|\n)?")] // single line comment
pub enum Token {
    #[token("or", ignore(ascii_case))]
    OpOr,

    #[regex(r"[_a-zA-Z][_a-zA-Z0-9:\.]*")]
    Identifier,

    #[regex("'(?s:[^'\\\\]|\\\\.)*'")]
    #[regex("`(?s:[^`\\\\]|\\\\.)*`")]
    #[regex("\"(?s:[^\"\\\\]|\\\\.)*\"")]
    StringLiteral,

    #[token("{")]
    LeftBrace,

    #[token("}")]
    RightBrace,

    #[token(",")]
    Comma,

    #[token("(")]
    LeftParen,

    #[token(")")]
    RightParen,

    #[token("=")]
    Equal,

    #[token("==")]
    OpEqual,

    #[token("!=")]
    OpNotEqual,

    #[token("=~")]
    RegexEqual,

    #[token("!~")]
    RegexNotEqual,

    #[regex("\"(?s:[^\"\\\\]|\\\\.)*", unterminated_string_literal)]
    #[regex("'(?s:[^'\\\\]|\\\\.)*", unterminated_string_literal)]
    #[regex("@[^\"'\\s]\\S+", unterminated_string_literal)]
    ErrorStringUnterminated,

    /// Marker for end of stream.
    Eof,
}

impl Token {
    pub const fn as_str(&self) -> &'static str {
        match self {
            // keywords
            Self::StringLiteral => "<string literal>",

            Self::Identifier => "<identifier>",

            // symbols
            Self::LeftBrace => "{{",
            Self::RightBrace => "}}",
            Self::Comma => ",",
            Self::LeftParen => "(",
            Self::RightParen => ")",
            Self::Equal => "=",

            // operators
            Self::OpOr => "or",
            Self::OpEqual => "==",
            Self::OpNotEqual => "!=",
            Self::RegexEqual => "=~",
            Self::RegexNotEqual => "!~",

            Self::Eof => "<eof>",

            // other
            Self::ErrorStringUnterminated => "<unterminated string literal>",
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(crate) fn get_next_token<'a>(lex: &'a mut Lexer<Token>) -> ParseResult<(Token, &'a str, Span)> {
    match lex.next() {
        Some(Ok(tok)) => Ok((tok, lex.slice(), lex.span())),
        Some(Err(_)) => {
            let span = lex.span();
            let inner = ParseErr::new(
                format!("unexpected token \"{}\"", lex.slice().trim()).as_str(),
                span,
            );
            Err(ParseError::Unexpected(inner))
        }
        None => Ok((Token::Eof, "", Span::default())),
    }
}

pub(crate) fn expect_token<'a>(lex: &'a mut Lexer<Token>, expected: Token) -> ParseResult<&'a str> {
    let res = get_next_token(lex)?;
    if res.0 == expected {
        Ok(res.1)
    } else {
        let actual = res.1.to_string();
        // let span = lex.span();
        Err(unexpected(
            "label name",
            &actual,
            expected.as_str(),
            Some(&res.2),
        ))
    }
}

pub(crate) fn expect_one_of_tokens<'a>(
    lex: &'a mut Lexer<Token>,
    expected: &[Token],
) -> ParseResult<(Token, &'a str)> {
    let res = get_next_token(lex)?;
    if expected.contains(&res.0) {
        Ok((res.0, res.1))
    } else {
        let actual = res.1.to_string();
        // let span = lex.span();
        let expected = expected
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let msg = format!("one of: {}", expected);
        Err(unexpected("label name", &actual, &msg, Some(&res.2)))
    }
}

#[cfg(test)]
mod tests {
    use logos::Logos;
    use test_case::test_case;

    use super::Token;
    use super::Token::*;

    macro_rules! test_tokens {
    ($src:expr, [$(
      $tok:expr
      $(=> $val:expr)?
    ),*$(,)?]) => {#[allow(unused)] {
      let src: &str = $src;
      let mut lex = Token::lexer(src);
      let mut index = 0;
      let mut offset = 0;
      $({
        let actual = lex.next().expect(&format!("Expected token {}", index + 1)).unwrap();
        let expected = $tok;
        let text = lex.slice();
        assert_eq!(actual, expected);
        $(
          assert_eq!(text, $val, "index: {}", index);
        )?

        index += 1;
        offset += text.len();
      })*

      match lex.next() {
        None => (),
        Some(t) => panic!("Expected exactly {} tokens, but got {:#?} when expecting Eof", index, t),
      }
    }};
  }

    #[test]
    fn empty() {
        test_tokens!("", []);
    }

    #[test]
    fn whitespace() {
        // whitespace is skipped
        test_tokens!("  \t\n\r\r\n", []);
    }

    #[test_case("{", LeftBrace)]
    #[test_case("}", RightBrace)]
    #[test_case("(", LeftParen)]
    #[test_case(")", RightParen)]
    #[test_case(",", Comma)]
    #[test_case("=", Equal)]
    fn symbol(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    #[test_case("==", OpEqual)]
    #[test_case("!=", OpNotEqual)]
    #[test_case("=~", RegexEqual)]
    #[test_case("!~", RegexNotEqual)]
    fn operator(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    fn expect_error_containing(s: &str, needle: &str) {
        let mut lex = Token::lexer(s);
        let actual = lex.next().unwrap();
        assert_eq!(actual.is_err(), true);
        let msg = format!("{}", actual.unwrap_err());
        assert!(msg.contains(needle));
    }

    #[test_case("\"hi\"", StringLiteral; "double_1")]
    #[test_case("\"hi\n\"", StringLiteral; "double_2")]
    #[test_case("\"hi\\\"\"", StringLiteral; "double_3")]
    #[test_case("'hi'", StringLiteral; "single_1")]
    #[test_case("'hi\n'", StringLiteral; "single_2")]
    #[test_case("'hi\\''", StringLiteral; "single_3")]
    fn string(src: &str, tok: Token) {
        test_tokens!(src, [tok]);
    }

    #[test]
    fn string_unterminated() {
        fn check(s: &str) {
            expect_error_containing(s, "unterminated string literal");
        }

        check("\"hi");
        check("\'hi");
    }

    #[test]
    fn identifier() {
        test_tokens!("foobar123", [Identifier]);
    }

    #[test]
    fn identifiers() {
        // short
        test_tokens!(
          "m y f i h d s",
          [
            Identifier=>"m",
            Identifier=>"y",
            Identifier=>"f",
            Identifier=>"i",
            Identifier=>"h",
            Identifier=>"d",
            Identifier=>"s"
          ]
        );

        test_tokens!(
          "foo bar123",
          [
            Identifier=>"foo",
            Identifier=>"bar123",
          ]
        );
    }

    #[test]
    fn py_comment() {
        // comments are skipped
        test_tokens!("# hi", []);
    }

    #[test]
    fn junk() {
        let src = "💩";
        let mut lex = Token::lexer(src);
        let actual = lex.next().unwrap();
        assert_eq!(actual.is_err(), true);
    }

    #[test]
    fn metric_name() {
        // Just metric name
        test_success("metric", &["metric"]);

        // Metric name with spec chars
        test_success("foo.bar_", &["foo.bar_"]);
    }


    #[test]
    fn metric_name_with_tag_filters() {
        // Metric name with tag filters
        let s = r#"  metric:12.34{a="foo", b != "bar", c=~ "x.+y", d !~ "zzz"}"#;
        let expected = vec![
            "metric:12.34",
            "{",
            "a",
            "=",
            r#""foo""#,
            ",",
            "b",
            "!=",
            r#""bar""#,
            ",",
            "c",
            "=~",
            r#""x.+y""#,
            ",",
            "d",
            "!~",
            r#""zzz""#,
            "}",
        ];

        test_success(s, &expected);
    }
    
    #[test]
    fn comments() {
        let s = r"# comment # sdf
		foobar # comment
		baz
		# yet another comment";
        test_success(s, &["foobar", "baz"])
    }

    #[test]
    fn strings() {
        // An empty string
        test_success("", &[]);

        // String with whitespace
        let s = "  \n\t\r ";
        test_success(s, &[]);

        // Strings
        let s = r#""''"#.to_owned() + "``" + r#"\\"  '\\'  "\"" '\''"\\\"\\""#;
        let expected = vec![
            r#""""#,
            "''",
            "``",
            r#"\\"", `'\\'", `"\""", `'\''" "\\\"\\"#,
        ];
        test_success(&s, &expected);
    }

    #[test]
    fn miscellaneous() {
        let s = "   `foo\\\\\\`бар`  ";
        let expected = vec!["`foo\\\\\\`бар`"];
        test_success(s, &expected);
    }

    fn test_success(s: &str, expected_tokens: &[&str]) {
        let mut lexer = Token::lexer(s);
        let mut tokens = vec![];
        while let Some(tok) = lexer.next() {
            match tok {
                Ok(_) => {
                    tokens.push(lexer.slice());
                }
                Err(e) => {
                    panic!("unexpected error {:?}\n {}", e, lexer.slice())
                }
            }
        }

        assert_eq!(
            tokens.len(),
            expected_tokens.len(),
            "expected {:?} tokens, got {:?}",
            expected_tokens.len(),
            tokens.len()
        );

        for i in 0..tokens.len() {
            let actual = tokens[i];
            let expected = expected_tokens[i];
            assert_eq!(
                actual, expected,
                "expected {:?} at index {}, got {:?}",
                expected, i, actual
            );
        }
    }
}
