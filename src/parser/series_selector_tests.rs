#[cfg(test)]
mod tests {
    use crate::labels::filters::{
        FilterList, LabelFilter, MatchOp, PredicateMatch, PredicateValue, SeriesSelector,
    };
    use crate::labels::parse_series_selector;

    fn assert_matcher(matcher: &LabelFilter, label: &str, op: MatchOp, value: &str) {
        let expected = LabelFilter::create(op, label, value).unwrap();
        assert_eq!(
            matcher, &expected,
            "expected matcher: {}, found {}",
            &expected, matcher
        );
    }

    fn assert_contains_matcher(matchers: &[LabelFilter], label: &str, op: MatchOp, value: &str) {
        let expected = LabelFilter::create(op, label, value).unwrap();
        assert!(
            matchers.contains(&expected),
            "expected matcher: {}, not found in {:?}",
            &expected,
            matchers
        );
    }

    fn assert_list_matcher(matcher: &LabelFilter, label: &str, op: MatchOp, values: &[&str]) {
        let values = values.iter().map(|s| s.to_string()).collect();
        if op.is_regex() {
            panic!("regex matchers are not supported in list matchers");
        }
        let expected = if op == MatchOp::Equal {
            LabelFilter {
                label: label.to_string(),
                matcher: PredicateMatch::Equal(PredicateValue::List(values)),
            }
        } else {
            LabelFilter {
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

    fn with_and_matchers<F>(matchers: &SeriesSelector, f: F)
    where
        F: Fn(&[LabelFilter]),
    {
        match matchers {
            SeriesSelector::And(m) => f(m),
            _ => panic!("expected AND matcher"),
        }
    }

    fn with_or_matchers<F>(matchers: &SeriesSelector, f: F)
    where
        F: Fn(&[FilterList]),
    {
        match matchers {
            SeriesSelector::Or(m) => f(m),
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
    fn test_series_selector_number_literal_value() {
        let input = "job=1234";
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let matcher = &matchers[0];
            assert_eq!(matcher.label, "job");
            assert!(
                matches!(matcher.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "1234")
            );
        });
    }

    #[test]
    fn test_parse_series_selector_single_label_matcher_without_metric_name() {
        let input = "{job=\"prometheus\"}";
        let result = parse_series_selector(input).unwrap();

        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, None);

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

        let metric_name = matchers.get_metric_name();
        assert_eq!(metric_name, Some("metric_name"));

        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);
            assert_eq!(or_matchers[0].len(), 2);
            assert_eq!(or_matchers[1].len(), 1);
            assert_contains_matcher(&or_matchers[0], "label1", MatchOp::Equal, "value1");
            assert_contains_matcher(&or_matchers[1], "label2", MatchOp::RegexEqual, "value.*");
        });
    }

    #[test]
    fn test_parse_series_selector_multiple_or_conditions() {
        let selector =
            r#"{job="prometheus",env="prod" or datacenter=~"us-.*" or instance!="localhost"}"#;
        let matchers = parse_series_selector(selector).unwrap();

        assert!(matchers.get_metric_name().is_none());
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

        assert!(matchers.get_metric_name().is_none());

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

        assert!(matchers.get_metric_name().is_none());
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

        let metric_name = matchers.get_metric_name();
        assert_eq!(metric_name, Some("metric_name:with.special_characters"));
        assert_eq!(matchers.len(), 1);
    }

    #[test]
    fn test_parse_series_selector_with_metric_name_and_labels() {
        let input = "http_requests_total{method=\"GET\", status=\"200\"}";
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.get_metric_name(), Some("http_requests_total"));
        assert_eq!(result.len(), 3);

        with_and_matchers(&result, |and_matchers| {
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

        assert_eq!(result.get_metric_name(), Some("http_requests_total"));
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

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
    fn test_metric_name_distributed_across_or_conditions() {
        // test that the metric name is distributed across each OR branch; i.e.,
        // "http_requests{status="400" or method="POST"}"
        // gets parsed to
        // {__name__="http_requests", status="400"} or {__name__="http_requests", method="POST"}
        let matchers =
            parse_series_selector("http_requests{status=\"400\" or method=\"POST\"}").unwrap();

        assert_eq!(matchers.get_metric_name(), Some("http_requests"));

        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);

            // Each OR branch should include the metric name matcher
            for branch in or_matchers {
                let has_metric_name = branch.iter().any(|m| {
                    m.label == "__name__" && matches!(m.matcher, PredicateMatch::Equal(PredicateValue::String(ref s)) if s == "http_requests")
                });
                assert!(
                    has_metric_name,
                    "Each OR branch must include metric name matcher"
                );
            }
        });
    }

    #[test]
    fn test_parse_series_selector_metric_name_only() {
        let input = "metric_name";
        let matchers = parse_series_selector(input).unwrap();

        assert_eq!(matchers.get_metric_name(), Some("metric_name"));
        assert_eq!(matchers.len(), 1);
        assert!(matchers.is_only_metric_name());

        let input = "metric_name{}";
        let matchers = parse_series_selector(input).unwrap();
        assert_eq!(matchers.get_metric_name(), Some("metric_name"));
        assert_eq!(matchers.len(), 1);
        assert!(matchers.is_only_metric_name());
    }

    #[test]
    fn test_parse_series_selector_redis_ts_style() {
        let input = "temperature=hot";
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let temperature_matcher = &matchers[0];
            assert_matcher(temperature_matcher, "temperature", MatchOp::Equal, "hot");
        });
    }

    #[test]
    fn test_selector_with_newline() {
        let input = r#"label="\n""#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 1);

            let temperature_matcher = &matchers[0];
            assert_matcher(temperature_matcher, "label", MatchOp::Equal, "\n");
        });
    }

    // https://redis.io/docs/latest/commands/ts.queryindex/
    #[test]
    fn redis_ts_selector_equal_with_lists() {
        let input = "size=(small,medium,large)";
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
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

    #[test]
    fn test_parse_series_selector_with_quoted_labels() {
        let input = r#"{"metric.name"="value", "label-with-dash"="foo", "quoted.label"=~"val.*"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

            assert_matcher(&matchers[0], "metric.name", MatchOp::Equal, "value");
            assert_matcher(&matchers[1], "label-with-dash", MatchOp::Equal, "foo");
            assert_matcher(&matchers[2], "quoted.label", MatchOp::RegexEqual, "val.*");
        });
    }

    #[test]
    fn test_parse_series_selector_with_escaped_quotes() {
        let input =
            r#"{"metric.name"="value", "label-with-dash"="foo", "quoted.label"=~"val\".*"}"#;
        let result = parse_series_selector(input).unwrap();

        with_and_matchers(&result, |matchers| {
            assert_eq!(matchers.len(), 3);

            assert_matcher(&matchers[0], "metric.name", MatchOp::Equal, "value");
            assert_matcher(&matchers[1], "label-with-dash", MatchOp::Equal, "foo");
            assert_matcher(
                &matchers[2],
                "quoted.label",
                MatchOp::RegexEqual,
                r##"val".*"##,
            );
        });
    }

    #[test]
    fn test_parse_single_identifier_matcher() {
        let input = r#"{"foo"}"#;
        let result = parse_series_selector(input).unwrap();

        assert_eq!(result.get_metric_name(), Some("foo"));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_parse_single_identifier_matcher_with_normal() {
        let input = r#"{"foo", a="bc"}"#;
        let result = parse_series_selector(input).unwrap();
        // the metric name should be set to "foo"
        let metric_name = result.get_metric_name();

        assert_eq!(metric_name, Some("foo"));
        assert_eq!(result.len(), 2);
        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "a", MatchOp::Equal, "bc");
        });
    }

    #[test]
    fn test_parse_reserved_symbols_as_labels() {
        let input = r#"foo{NaN="inf"}"#;
        let result = parse_series_selector(input).unwrap();

        // the metric name should be set to "foo"
        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, Some("foo"));
        assert_eq!(result.len(), 2);

        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "NaN", MatchOp::Equal, "inf");
        });
    }

    #[test]
    fn test_parse_metric_name_in_the_middle_of_selector_list() {
        let input = r#"{a="b", foo!="bar", "metric_name", test=~"test", bar!~"baz"}"#;
        let result = parse_series_selector(input).unwrap();

        let metric_name = result.get_metric_name();
        assert_eq!(metric_name, Some("metric_name"));
        assert_eq!(result.len(), 5);

        with_and_matchers(&result, |matchers| {
            assert_contains_matcher(matchers, "a", MatchOp::Equal, "b");
            assert_contains_matcher(matchers, "foo", MatchOp::NotEqual, "bar");
            assert_contains_matcher(matchers, "test", MatchOp::RegexEqual, "test");
            assert_contains_matcher(matchers, "bar", MatchOp::RegexNotEqual, "baz");
        });
    }

    // OR tests
    #[test]
    fn test_parse_or_with_list_matchers() {
        let input = r#"{a="b", foo!="bar" or size=(small,medium,large) or flavor!=(original,cajun,"extra spicy")}"#;
        let result = parse_series_selector(input).unwrap();

        assert!(result.get_metric_name().is_none());
        with_or_matchers(&result, |or_matchers| {
            assert_eq!(or_matchers.len(), 3);

            // First OR branch
            assert_eq!(or_matchers[0].len(), 2);
            assert_contains_matcher(&or_matchers[0], "a", MatchOp::Equal, "b");
            assert_contains_matcher(&or_matchers[0], "foo", MatchOp::NotEqual, "bar");

            // Second OR branch
            assert_eq!(or_matchers[1].len(), 1);
            assert_list_matcher(
                &or_matchers[1][0],
                "size",
                MatchOp::Equal,
                &["small", "medium", "large"],
            );

            // Third OR branch
            assert_eq!(or_matchers[2].len(), 1);
            assert_list_matcher(
                &or_matchers[2][0],
                "flavor",
                MatchOp::NotEqual,
                &["original", "cajun", "extra spicy"],
            );
        });
    }

    #[test]
    fn test_or_with_prometheus_style_matchers() {
        let selector =
            r#"http_status{status="500"} or api_host{service="auth", env=~"prod|staging"}"#;
        let matchers = parse_series_selector(selector).unwrap();
        assert!(matchers.get_metric_name().is_none());
        with_or_matchers(&matchers, |or_matchers| {
            assert_eq!(or_matchers.len(), 2);

            // First OR branch
            let first = &or_matchers[0];
            assert_eq!(first.len(), 2); // metric name + status matcher
            assert_contains_matcher(first, "__name__", MatchOp::Equal, "http_status");
            assert_contains_matcher(first, "status", MatchOp::Equal, "500");

            // Second OR branch
            let second = &or_matchers[1];
            assert_eq!(second.len(), 3); // metric name + 2 matchers
            assert_contains_matcher(second, "__name__", MatchOp::Equal, "api_host");
            assert_contains_matcher(second, "service", MatchOp::Equal, "auth");
            assert_contains_matcher(second, "env", MatchOp::RegexEqual, "prod|staging");
        });
    }
}
