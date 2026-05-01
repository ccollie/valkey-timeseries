use crate::common::strings::jaro_winkler;

pub struct JaroWinklerMatcher {
    pattern: String,
}

impl JaroWinklerMatcher {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    pub fn score(&self, value: &str) -> f64 {
        let s1 = jaro_winkler(&self.pattern, value) as f64;
        let s2 = jaro_winkler(value, &self.pattern) as f64;
        s1.max(s2)
    }
}
