#[cfg(test)]
mod tests {
    use crate::labels::matchers::{
        MatchOp, Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
    };
    use crate::labels::parse_series_selector;

    fn assert_matcher(matcher: &Matcher, label: &str, op: MatchOp, value: &str) {
        let expected = Matcher::create(op, label, value).unwrap();
        assert_eq!(
            matcher, &expected,
            "expected matcher: {}, found {}",
            &expected, matcher
        );
    }

    fn assert_list_matcher(matcher: &Matcher, label: &str, op: MatchOp, values: &[&str]) {
        let values = values.iter().map(|s| s.to_string()).collect();
        if op.is_regex() {
            panic!("regex matchers are not supported in list matchers");
        }
        let expected = if op == MatchOp::Equal {
            Matcher {
                label: label.to_string(),
                matcher: PredicateMatch::Equal(PredicateValue::List(values)),
            }
        } else {
            Matcher {
                label: label.to_string(),
                matcher: PredicateMatch::NotEqual(PredicateValue::List(values)),
            }
        };
        assert_eq!(
            matcher, &expected,
            "expected matcher: {}, found {}",
            &expected, matcher
        );
    }

    fn with_and_matchers<F>(matchers: &Matchers, f: F)
    where
        F: Fn(&Vec<Matcher>),
    {
        match matchers.matchers {
            MatcherSetEnum::And(ref m) => f(m),
            _ => panic!("expected AND matcher"),
        }
    }

    fn with_or_matchers<F>(matchers: &Matchers, f: F)
    where
        F: Fn(&Vec<Vec<Matcher>>),
    {
        match matchers.matchers {
            MatcherSetEnum::Or(ref m) => f(m),
            _ => panic!("expected OR matcher"),
        }
    }

    #[test]
    fn test_parse_series_selector_empty_input() {
        let result = parse_series_selector("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Empty series selector");
    }

    #[test]
    fn test_parse_series_selector_single_label_matcher_without_metric_name() {
        let input = "{job=\"prometheus\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.name, None);

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_eq!(matcher.label, "job");
            assert!(
                matches!(matcher.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "prometheus")
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_or_conditions() {
        let selector = r#"{__name__="metric_name",label1="value1" or label2=~"value.*"}"#;
        let matchers = parse_series_selector(selector).unwrap();

        assert_eq!(matchers.name, Some("metric_name".to_string()));
        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);
            assert_eq!(or_matchers[0].len(), 1);
            assert_eq!(or_matchers[1].len(), 1);
            assert_matcher(&or_matchers[0][0], "label1", MatchOp::Equal, "value1");
            assert_matcher(&or_matchers[1][0], "label2", MatchOp::RegexEqual, "value.*");
        });
    }

    #[test]
    fn test_parse_series_selector_multiple_or_conditions() {
        let selector =
            r#"{job="prometheus",env="prod" or datacenter=~"us-.*" or instance!="localhost"}"#;
        let matchers = parse_series_selector(selector).unwrap();

        assert!(matchers.name.is_none());
        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 3);

            assert_eq!(or_matchers[0].len(), 2);
            assert_eq!(or_matchers[1].len(), 1);
            assert_eq!(or_matchers[2].len(), 1);
            assert_matcher(&or_matchers[0][0], "job", MatchOp::Equal, "prometheus");
            assert_matcher(&or_matchers[0][1], "env", MatchOp::Equal, "prod");
            assert_matcher(
                &or_matchers[1][0],
                "datacenter",
                MatchOp::RegexEqual,
                "us-.*",
            );
            assert_matcher(
                &or_matchers[2][0],
                "instance",
                MatchOp::NotEqual,
                "localhost",
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_regex_not_equal_matchers() {
        let input = r#"{job!~"prom.*",instance!~"local.*"}"#;
        let matchers = parse_series_selector(input).unwrap();

        assert!(matchers.name.is_none());

        with_and_matchers(&matchers, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);
            assert_matcher(&and_matchers[0], "job", MatchOp::RegexNotEqual, "prom.*");
            assert_matcher(
                &and_matchers[1],
                "instance",
                MatchOp::RegexNotEqual,
                "local.*",
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_negated_label_matchers() {
        let input = r#"{job!="prometheus",instance!="localhost:9090"}"#;
        let matchers = parse_series_selector(input).unwrap();

        assert!(matchers.name.is_none());
        with_and_matchers(&matchers, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);
            assert_matcher(&and_matchers[0], "job", MatchOp::NotEqual, "prometheus");
            assert_matcher(
                &and_matchers[1],
                "instance",
                MatchOp::NotEqual,
                "localhost:9090",
            );
        });
    }

    #[test]
    fn test_parse_series_selector_with_special_characters() {
        let input = "metric_name:with.special_characters";
        let matchers = parse_series_selector(input).unwrap();

        assert_eq!(
            matchers.name,
            Some("metric_name:with.special_characters".to_string())
        );
        assert!(matchers.matchers.is_empty());
    }

    #[test]
    fn test_parse_series_selector_with_metric_name_and_labels() {
        let input = "http_requests_total{method=\"GET\", status=\"200\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.name, Some("http_requests_total".to_string()));

        with_and_matchers(&result, |and_matchers| {
            assert_eq!(and_matchers.len(), 2);

            let method_matcher = &and_matchers[0];
            assert_matcher(method_matcher, "method", MatchOp::Equal, "GET");

            let status_matcher = &and_matchers[1];
            assert_matcher(status_matcher, "status", MatchOp::Equal, "200");
        });
    }

    #[test]
    fn test_prometheus_selector_with_list_matcher() {
        let input = "http_requests_total{method=(GET,SET,\"POST\"), status=~\"2[0-9]{2}\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.name, Some("http_requests_total".to_string()));
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 2);

            let method_matcher = &matchers[0];
            assert_list_matcher(
                method_matcher,
                "method",
                MatchOp::Equal,
                &["GET", "SET", "POST"],
            );

            let status_matcher = &matchers[1];
            assert_matcher(status_matcher, "status", MatchOp::RegexEqual, "2[0-9]{2}");
        });
    }

    #[test]
    fn test_parse_series_selector_metric_name_only() {
        let input = "metric_name";
        let matchers = parse_series_selector(input).unwrap();

        assert_eq!(matchers.name, Some("metric_name".to_string()));
        assert!(matchers.matchers.is_empty());
        assert!(matchers.is_only_metric_name());
    }

    #[test]
    fn test_parse_series_selector_redis_ts_style() {
        let input = "temperature=hot";
        let result = parse_series_selector(input).unwrap();

        assert!(result.name.is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let temperature_matcher = &matchers[0];
            assert_matcher(temperature_matcher, "temperature", MatchOp::Equal, "hot");
        });
    }

    // https://redis.io/docs/latest/commands/ts.queryindex/
    #[test]
    fn redis_ts_selector_equal_with_lists() {
        let input = "size=(small,medium,large)";
        let result = parse_series_selector(input).unwrap();

        assert!(result.name.is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_list_matcher(
                matcher,
                "size",
                MatchOp::Equal,
                &["small", "medium", "large"],
            );
        });
    }

    #[test]
    fn redis_ts_selector_not_equal_with_lists() {
        let input = "flavor!=(original,cajun,\"extra spicy\")";
        let result = parse_series_selector(input).unwrap();

        assert!(result.name.is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_list_matcher(
                matcher,
                "flavor",
                MatchOp::NotEqual,
                &["original", "cajun", "extra spicy"],
            );
        });
    }
}
