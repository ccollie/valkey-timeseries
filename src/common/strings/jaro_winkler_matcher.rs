use crate::common::strings::jaro_winkler;

pub struct JaroWinklerMatcher {
    pattern: String,
}

impl JaroWinklerMatcher {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    pub fn score(&self, value: &str) -> f64 {
        jaro_winkler(&self.pattern, value) as f64
    }
}
