// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The scoring algorithm is inspired by two JavaScript libraries:
// https://github.com/Nexucis/fuzzy (MIT License), used by the Prometheus UI,
// which itself was inspired by https://github.com/mattyork/fuzzy (MIT License).

use std::sync::OnceLock;

/// SubsequenceMatcher pre-computes the encoding of a fixed search pattern so
/// that it can be scored against many candidate strings without repeating the
/// ASCII check or char conversion on the pattern for every call. The first
/// [`score`] call with a Unicode candidate lazily caches the pattern's char
/// vec in a thread-safe [`OnceLock`], allowing concurrent scoring via `&self`.
pub struct SubsequenceMatcher {
    pattern: String,
    /// Byte length; used for the pre-check `pattern.len() > text.len()`.
    pattern_len: usize,
    pattern_ascii: bool,
    /// Pre-converted chars, initialized eagerly for Unicode patterns or lazily
    /// on first Unicode text for ASCII patterns.
    pattern_chars: OnceLock<Vec<char>>,
}

impl SubsequenceMatcher {
    pub fn new(pattern: &str) -> Self {
        let pattern_ascii = pattern.is_ascii();
        let pattern_chars = OnceLock::new();
        if !pattern_ascii {
            let _ = pattern_chars.set(pattern.chars().collect());
        }

        if pattern_ascii {
            Self {
                pattern: pattern.to_owned(),
                pattern_len: pattern.len(),
                pattern_ascii: true,
                pattern_chars,
            }
        } else {
            Self {
                pattern: pattern.to_owned(),
                pattern_len: pattern.len(),
                pattern_ascii: false,
                pattern_chars,
            }
        }
    }

    /// Computes a fuzzy match score between the matcher's pattern and `text`
    /// using a greedy subsequence-matching algorithm.
    ///
    /// The score is normalized to `[0.0, 1.0]` where:
    /// - `1.0` means an exact match.
    /// - `0.0` means no match (pattern is not a subsequence of text).
    /// - Intermediate values reward consecutive matches and penalize gaps.
    ///
    /// Raw formula: `Σ(interval²) − Σ(gap / text_len) − trailing_gap / (2 × text_len)`,
    /// normalized by `pattern_len²`.
    pub fn score(&self, text: &str) -> f64 {
        if self.pattern.is_empty() {
            return 1.0;
        }
        if text.is_empty() {
            return 0.0;
        }
        if self.pattern == text {
            return 1.0;
        }
        // Byte length ≥ char count, so this is a safe early exit.
        if self.pattern_len > text.len() {
            return 0.0;
        }

        let text_ascii = text.is_ascii();
        match (self.pattern_ascii, text_ascii) {
            (true, true) => match_subsequence_bytes(self.pattern.as_bytes(), text.as_bytes()),
            // A non-ASCII pattern char can never match in a pure-ASCII text.
            (false, true) => 0.0,
            _ => {
                // Pattern is ASCII but text is Unicode: convert and cache pattern chars.
                let pattern_chars = self
                    .pattern_chars
                    .get_or_init(|| self.pattern.chars().collect());
                let text_chars: Vec<char> = text.chars().collect();
                match_subsequence_chars(pattern_chars, &text_chars)
            }
        }
    }
}

/// Byte-slice implementation of the scoring algorithm for pure-ASCII inputs.
fn match_subsequence_bytes(pattern: &[u8], text: &[u8]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;
    let max_start = text_len - pattern_len;

    // Scores a match starting at `start`, where `text[start] == pattern[0]`
    // is guaranteed by the caller. Returns `None` if the pattern cannot be
    // completed from this position.
    let score_from = |start: usize| -> Option<f64> {
        let mut i = start;
        let from = i;
        let mut to = i;
        let mut pi = 1; // pattern index
        i += 1;

        // Extend the initial consecutive run.
        while pi < pattern_len && i < text_len && text[i] == pattern[pi] {
            to = i;
            pi += 1;
            i += 1;
        }

        let mut score = 0.0_f64;
        if from > 0 {
            score -= from as f64 * inv_text_len;
        }
        let size = (to - from + 1) as f64;
        score += size * size;
        let mut prev_to = to;

        while pi < pattern_len {
            // Jump to the next occurrence of `pattern[pi]`.
            let j = text[i..].iter().position(|&b| b == pattern[pi])?;
            i += j;
            let from = i;
            let mut to = i;
            pi += 1;
            i += 1;
            // Extend the consecutive run.
            while pi < pattern_len && i < text_len && text[i] == pattern[pi] {
                to = i;
                pi += 1;
                i += 1;
            }
            let gap = from as isize - prev_to as isize - 1;
            if gap > 0 {
                score -= gap as f64 * inv_text_len;
            }
            let size = (to - from + 1) as f64;
            score += size * size;
            prev_to = to;
        }

        // Penalise unmatched trailing characters at half the leading/inner rate.
        let trailing = text_len as isize - 1 - prev_to as isize;
        if trailing > 0 {
            score -= trailing as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0_f64;
    let mut i = 0;
    while i <= max_start {
        // Scan for the first pattern character within the reachable window.
        let j = match text[i..=max_start].iter().position(|&b| b == pattern[0]) {
            Some(j) => j,
            None => break,
        };
        i += j;
        match score_from(i) {
            // If the pattern cannot be completed from i, no later start can
            // succeed: text[i+1..] is a strict subset of text[i..].
            None => break,
            Some(s) if s > best_score => best_score = s,
            _ => {}
        }
        i += 1;
    }

    if best_score < 0.0 {
        return 0.0;
    }
    best_score / (pattern_len * pattern_len) as f64
}

/// Char-slice implementation of the scoring algorithm for Unicode inputs.
fn match_subsequence_chars(pattern: &[char], text: &[char]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;
    let max_start = text_len - pattern_len;

    // Tries to match all pattern chars as a subsequence starting at `start`.
    // Returns the raw score on success, or `None` if the pattern cannot be
    // fully matched.
    let match_from = |start: usize| -> Option<f64> {
        let mut pi = 0;
        let mut i = start;
        let mut score = 0.0_f64;
        let mut prev_to: isize = -1;

        while i < text_len && pi < pattern_len {
            if text[i] == pattern[pi] {
                let from = i;
                let mut to = i;
                pi += 1;
                i += 1;
                while i < text_len && pi < pattern_len && text[i] == pattern[pi] {
                    to = i;
                    pi += 1;
                    i += 1;
                }
                let gap = if prev_to < 0 {
                    from as isize
                } else {
                    from as isize - prev_to - 1
                };
                if gap > 0 {
                    score -= gap as f64 * inv_text_len;
                }
                let size = (to - from + 1) as f64;
                score += size * size;
                prev_to = to as isize;
            } else {
                i += 1;
            }
        }

        if pi < pattern_len {
            return None;
        }

        // Penalise unmatched trailing characters at half the leading/inner rate.
        let trailing = text_len as isize - 1 - prev_to;
        if trailing > 0 {
            score -= trailing as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0_f64;
    for i in 0..=max_start {
        if text[i] != pattern[0] {
            continue;
        }
        match match_from(i) {
            // If matching fails from this position, no later position can
            // succeed since the remaining text is a strict subset.
            None => break,
            Some(s) if s > best_score => best_score = s,
            _ => {}
        }
    }

    if best_score < 0.0 {
        return 0.0;
    }
    // Normalise by pattern_len² (the maximum possible raw score).
    best_score / (pattern_len * pattern_len) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ── Unit tests ────────────────────────────────────────────────────────────

    struct Case {
        name: &'static str,
        pattern: &'static str,
        text: &'static str,
        /// `Some(expected)` for a non-zero score, `None` for "must be 0.0".
        want: Option<f64>,
    }

    #[test]
    fn subsequence_score() {
        let cases = [
            Case {
                name: "empty pattern",
                pattern: "",
                text: "anything",
                want: Some(1.0),
            },
            Case {
                name: "empty text",
                pattern: "abc",
                text: "",
                want: None,
            },
            Case {
                name: "exact match",
                pattern: "my awesome text",
                text: "my awesome text",
                want: Some(1.0),
            },
            Case {
                name: "prefix match",
                pattern: "my",
                text: "my awesome text",
                // intervals [0,1], leading=0, trailing=13. raw = 4 - 13/30, normalised by 4.
                want: Some(107.0 / 120.0),
            },
            Case {
                name: "substring match",
                pattern: "tex",
                text: "my awesome text",
                // intervals [11,13], leading=11, trailing=1. raw = 9 - 11/15 - 1/30, normalised by 9.
                want: Some(247.0 / 270.0),
            },
            Case {
                name: "fuzzy match picks best starting position",
                pattern: "met",
                text: "my awesome text",
                // intervals [8,9] and [11,11], leading=8, inner gap=1, trailing=3.
                // raw = 5 - 9/15 - 3/30, normalised by 9.
                want: Some(43.0 / 90.0),
            },
            Case {
                name: "prefers later position with better consecutive run",
                pattern: "bac",
                text: "babac",
                // match at [2,4], leading gap=2, trailing=0. raw = 9 - 2/5, normalised by 9.
                want: Some(43.0 / 45.0),
            },
            Case {
                name: "pattern longer than text",
                pattern: "abcd",
                text: "abc",
                want: None,
            },
            Case {
                name: "pattern longer in runes than multi-byte text",
                pattern: "abc",
                text: "éé",
                want: None,
            },
            Case {
                name: "non-ASCII pattern with ASCII text",
                pattern: "é",
                text: "ab",
                want: None,
            },
            Case {
                name: "no subsequence match",
                pattern: "xyz",
                text: "abc",
                want: None,
            },
            Case {
                name: "unicode exact match",
                pattern: "éàü",
                text: "éàü",
                want: Some(1.0),
            },
            Case {
                name: "unicode prefix match",
                pattern: "éà",
                text: "éàü",
                // intervals [0,1], leading=0, trailing=1. raw = 4 - 1/6, normalised by 4.
                want: Some(23.0 / 24.0),
            },
            Case {
                name: "unicode no match",
                pattern: "üé",
                text: "éàü",
                want: None,
            },
            Case {
                name: "unicode first char matches but pattern cannot complete",
                pattern: "éàx",
                text: "éàü",
                want: None,
            },
            Case {
                name: "unicode fuzzy match with gap between intervals",
                pattern: "éü",
                text: "éàü",
                // intervals [0,0] and [2,2], leading=0, inner gap=1, trailing=0.
                // raw = 1 + 1 - 1/3, normalised by 4.
                want: Some(5.0 / 12.0),
            },
            Case {
                name: "mixed ascii and unicode",
                pattern: "aé",
                text: "aéb",
                // intervals [0,1], leading=0, trailing=1. raw = 4 - 1/6, normalised by 4.
                want: Some(23.0 / 24.0),
            },
            Case {
                // 'é' (U+00E9) encodes as [0xC3 0xA9] and 'ã' (U+00E3) as [0xC3 0xA3].
                // They share the leading byte but must not be treated as equal.
                name: "unicode chars sharing leading utf-8 byte do not match",
                pattern: "é",
                text: "ã",
                want: None,
            },
            Case {
                name: "single char exact match",
                pattern: "a",
                text: "a",
                want: Some(1.0),
            },
            Case {
                name: "consecutive match with leading gap",
                pattern: "oa",
                text: "goat",
                // 'o'(1),'a'(2) form interval [1,2], leading gap=1, trailing=1.
                // raw = 2² - 1/4 - 1/8 = 29/8, normalised by 2² = 4.
                want: Some(29.0 / 32.0),
            },
            Case {
                name: "repeated chars use greedy match",
                pattern: "abaa",
                text: "abbaa",
                // Matches 'a'(0),'b'(1),'a'(3),'a'(4): intervals [0,1] and [3,4].
                // raw = 2² + 2² - 1/5, normalised by 4² = 16.
                // A better match exists at 'a'(0),'b'(2),'a'(3),'a'(4) (score 49/80),
                // but this documents the current greedy behaviour.
                want: Some(39.0 / 80.0),
            },
        ];

        const EPSILON: f64 = 1e-9;

        for c in &cases {
            let got = SubsequenceMatcher::new(c.pattern).score(c.text);
            match c.want {
                None => assert_eq!(got, 0.0, "[{}] expected 0.0, got {got}", c.name),
                Some(want) => assert!(
                    (got - want).abs() < EPSILON,
                    "[{}] expected {want}, got {got} (delta {})",
                    c.name,
                    (got - want).abs()
                ),
            }
        }
    }

    #[test]
    fn subsequence_score_properties() {
        const EPSILON: f64 = 1e-9;

        // Prefix match scores below 1.0; only an exact match scores 1.0.
        // "pro" in "prometheus": intervals [0,2], trailing=7. raw = 9 - 7/20, normalised by 9.
        let got = SubsequenceMatcher::new("pro").score("prometheus");
        assert!(
            (got - 173.0 / 180.0).abs() < EPSILON,
            "prefix score: expected {}, got {got}",
            173.0 / 180.0
        );

        // Exact match always scores 1.0.
        assert_eq!(
            1.0,
            SubsequenceMatcher::new("prometheus").score("prometheus")
        );

        // Score is always in [0.0, 1.0] and never NaN.
        let range_cases: &[(&str, &str)] = &[
            ("abc", "xaxbxcx"),
            ("z", "aaaaaz"),
            ("ab", "ba"),
            ("met", "my awesome text"),
        ];
        for &(pattern, text) in range_cases {
            let score = SubsequenceMatcher::new(pattern).score(text);
            assert!(
                !score.is_nan(),
                "score is NaN for pattern={pattern:?} text={text:?}"
            );
            assert!(
                (0.0..=1.0).contains(&score),
                "score {score} out of [0,1] for pattern={pattern:?} text={text:?}"
            );
        }

        // Prefix scores higher than a suffix match of the same pattern.
        let prefix_score = SubsequenceMatcher::new("abc").score("abcdef");
        let suffix_score = SubsequenceMatcher::new("abc").score("defabc");
        assert!(
            prefix_score > suffix_score,
            "expected prefix ({prefix_score}) > suffix ({suffix_score})"
        );

        // Consecutive chars score higher than scattered chars.
        let consecutive = SubsequenceMatcher::new("abc").score("xabcx");
        let scattered = SubsequenceMatcher::new("abc").score("xaxbxcx");
        assert!(
            consecutive > scattered,
            "expected consecutive ({consecutive}) > scattered ({scattered})"
        );
    }

    #[test]
    fn subsequence_matcher_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SubsequenceMatcher>();
    }

    #[test]
    fn concurrent_score_calls_are_safe() {
        let matcher = Arc::new(SubsequenceMatcher::new("aé"));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let matcher = Arc::clone(&matcher);
            handles.push(std::thread::spawn(move || {
                let unicode_score = matcher.score("aéb");
                let ascii_score = matcher.score("aeb");
                (unicode_score, ascii_score)
            }));
        }

        const EPSILON: f64 = 1e-9;
        for handle in handles {
            let (unicode_score, ascii_score) = handle.join().expect("thread panicked");
            assert!((unicode_score - 23.0 / 24.0).abs() < EPSILON);
            assert_eq!(ascii_score, 0.0);
        }
    }

    #[test]
    fn ascii_fast_path_does_not_initialize_unicode_cache() {
        let matcher = SubsequenceMatcher::new("abc");
        assert!(matcher.pattern_chars.get().is_none());

        let score = matcher.score("xabcx");
        assert!(score > 0.0);
        assert!(matcher.pattern_chars.get().is_none());
    }
}
