/// Go and Rust handle the repeat pattern differently
/// in Go the following is valid: `aaa{bbb}ccc`
/// in Rust {bbb} is seen as an invalid repeat and must be escaped \{bbb}
/// This escapes the opening "{" if it's not followed by valid repeat pattern (e.g. 4,6).
pub fn try_escape_for_repeat_re(re: &str) -> String {
    fn is_repeat(chars: &mut std::str::Chars<'_>) -> (bool, String) {
        let mut buf = String::new();
        let mut comma_seen = false;
        for c in chars.by_ref() {
            buf.push(c);
            match c {
                ',' if comma_seen => {
                    return (false, buf); // ",," is invalid
                }
                ',' if buf == "," => {
                    return (false, buf); // {, is invalid
                }
                ',' if !comma_seen => comma_seen = true,
                '}' if buf == "}" => {
                    return (false, buf); // {} is invalid
                }
                '}' => {
                    return (true, buf);
                }
                _ if c.is_ascii_digit() => continue,
                _ => {
                    return (false, buf); // false if visit non-digit char
                }
            }
        }
        (false, buf) // not ended with "}"
    }

    let mut result = String::with_capacity(re.len() + 1);
    let mut chars = re.chars();

    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(cc) = chars.next() {
                    result.push(c);
                    result.push(cc);
                }
            }
            '{' => {
                let (is, s) = is_repeat(&mut chars);
                if !is {
                    result.push('\\');
                }
                result.push(c);
                result.push_str(&s);
            }
            _ => result.push(c),
        }
    }
    result
}

#[test]
fn test_convert_re() {
    assert_eq!(try_escape_for_repeat_re("abc{}"), r"abc\{}");
    assert_eq!(try_escape_for_repeat_re("abc{def}"), r"abc\{def}");
    assert_eq!(try_escape_for_repeat_re("abc{def"), r"abc\{def");
    assert_eq!(try_escape_for_repeat_re("abc{1}"), "abc{1}");
    assert_eq!(try_escape_for_repeat_re("abc{1,}"), "abc{1,}");
    assert_eq!(try_escape_for_repeat_re("abc{1,2}"), "abc{1,2}");
    assert_eq!(try_escape_for_repeat_re("abc{,2}"), r"abc\{,2}");
    assert_eq!(try_escape_for_repeat_re("abc{{1,2}}"), r"abc\{{1,2}}");
    assert_eq!(try_escape_for_repeat_re(r"abc\{abc"), r"abc\{abc");
    assert_eq!(try_escape_for_repeat_re("abc{1a}"), r"abc\{1a}");
    assert_eq!(try_escape_for_repeat_re("abc{1,a}"), r"abc\{1,a}");
    assert_eq!(try_escape_for_repeat_re("abc{1,2a}"), r"abc\{1,2a}");
    assert_eq!(try_escape_for_repeat_re("abc{1,2,3}"), r"abc\{1,2,3}");
    assert_eq!(try_escape_for_repeat_re("abc{1,,2}"), r"abc\{1,,2}");
}
