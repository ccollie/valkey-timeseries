// Copyright 2017 The Prometheus Authors
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

// https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings_test.go
// https://github.com/prometheus/prometheus/blob/main/tsdb/querier_test.go (see func TestPostingsForMatchers())
#[cfg(test)]
mod tests {
    use crate::labels::matchers::{MatchOp, Matcher, Matchers};
    use crate::labels::{InternedMetricName, Label};
    use crate::parser::metric_name::parse_metric_name;
    use crate::series::index::{TimeSeriesIndex, next_timeseries_id, postings_for_matchers};
    use crate::series::{SeriesRef, TimeSeries};
    use std::collections::{HashMap, HashSet};

    fn labels_from_strings<S: Into<String> + Clone>(ss: &[S]) -> Vec<Label> {
        if ss.is_empty() {
            return vec![];
        }
        if ss.len() % 2 != 0 {
            panic!("labels_from_strings: odd number of strings")
        }
        let mut labels = vec![];
        for i in (0..ss.len()).step_by(2) {
            let name = <S as Clone>::clone(&ss[i]).into();
            let value = <S as Clone>::clone(&ss[i + 1]).into();
            labels.push(Label { name, value })
        }
        labels
    }

    fn add_series(
        ix: &mut TimeSeriesIndex,
        labels_map: &mut HashMap<SeriesRef, Vec<Label>>,
        series_ref: SeriesRef,
        labels: &[Label],
    ) {
        let mut state = ();
        ix.with_postings_mut(&mut state, |p, _| {
            for label in labels.iter() {
                p.add_posting_for_label_value(series_ref, &label.name, &label.value);
            }
            p.add_id_to_all_postings(series_ref);
        });

        labels_map.insert(series_ref, labels.to_vec());
    }

    fn to_label_vec(labels: &[Vec<Label>]) -> Vec<Vec<Label>> {
        labels
            .iter()
            .map(|items| {
                let mut items = items.clone();
                items.sort();
                items
            })
            .collect::<Vec<_>>()
    }

    fn get_labels_by_matcher(
        ix: &TimeSeriesIndex,
        matchers: &[Matcher],
        series_data: &HashMap<SeriesRef, Vec<Label>>,
    ) -> Vec<Vec<Label>> {
        let copy = matchers.to_vec().clone();
        let filter = Matchers::with_matchers(None, copy);
        let p = postings_for_matchers(ix, &filter).unwrap();
        let mut actual: Vec<_> = p
            .iter()
            .flat_map(|id| series_data.get(&id))
            .cloned()
            .collect();

        actual.sort();
        actual
    }

    fn label_vec_to_string(labels: &[Label]) -> String {
        let mut items = labels.to_vec();
        items.sort();

        items
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    #[test]
    fn test_postings_for_matchers() {
        use MatchOp::*;

        let mut ix: TimeSeriesIndex = TimeSeriesIndex::default();
        let mut labels_map: HashMap<SeriesRef, Vec<Label>> = HashMap::new();

        let series_data = HashMap::from([
            (1, labels_from_strings(&["n", "1"])),
            (2, labels_from_strings(&["n", "1", "i", "a"])),
            (3, labels_from_strings(&["n", "1", "i", "b"])),
            (4, labels_from_strings(&["n", "1", "i", "\n"])),
            (5, labels_from_strings(&["n", "2"])),
            (6, labels_from_strings(&["n", "2.5"])),
        ]);

        for (series_ref, labels) in series_data.iter() {
            add_series(&mut ix, &mut labels_map, *series_ref, labels);
        }

        struct TestCase {
            matchers: Vec<Matcher>,
            exp: Vec<Vec<Label>>,
        }

        let cases = vec![
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // ----------------------------------------------------------------
            TestCase {
                matchers: vec![Matcher::create(Equal, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(Equal, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[labels_from_strings(&["n", "1", "i", "a"])]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(Equal, "i", "missing").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![Matcher::create(Equal, "missing", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Not equals
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "missing", "").unwrap()],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(NotEqual, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            //----------------------------------------------------------------------------
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(Equal, "i", "missing").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![Matcher::create(Equal, "missing", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Not equals.
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(NotEqual, "missing", "").unwrap()],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(NotEqual, "i", "a").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(NotEqual, "i", "").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // Regex.
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "n", "^1$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^a$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^a?$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", "^$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1"])],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexEqual, "i", "^.+$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            // Not regex.
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "i", "").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "n", "^1$").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "n", "1").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "n", "1|2.5").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "n", "(1|2.5)").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^a$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^a?$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                ]),
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^.*$").unwrap(),
                ],
                exp: vec![],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^.+$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1"])],
            },
            // Combinations.
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(NotEqual, "i", "").unwrap(),
                    Matcher::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(NotEqual, "i", "b").unwrap(),
                    Matcher::create(RegexEqual, "i", "^(b|a).*$").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            // Set optimization for Regex.
            // Refer to https://github.com/prometheus/prometheus/issues/2651.
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "n", "1|2").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", "a|b").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", "(a|b)").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "n", "x1|2").unwrap()],
                exp: vec![labels_from_strings(&["n", "2"])],
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "n", "2|2.5").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Empty value.
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", "c||d").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", "(c||d)").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for i=~".*"
            TestCase {
                matchers: vec![Matcher::create(RegexEqual, "i", ".*").unwrap()],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for n=~".*" and i=~"^.*$"
            TestCase {
                matchers: vec![
                    Matcher::create(RegexEqual, "n", ".*").unwrap(),
                    Matcher::create(RegexEqual, "i", "^.*$").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "1", "i", "a"]),
                    labels_from_strings(&["n", "1", "i", "b"]),
                    labels_from_strings(&["n", "1", "i", "\n"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut for n=~"^.*$"
            TestCase {
                matchers: vec![
                    Matcher::create(RegexEqual, "n", "^.*$").unwrap(),
                    Matcher::create(Equal, "i", "a").unwrap(),
                ],
                exp: vec![labels_from_strings(&["n", "1", "i", "a"])],
            },
            // Test shortcut for i!~".*"
            TestCase {
                matchers: vec![Matcher::create(RegexNotEqual, "i", ".*").unwrap()],
                exp: vec![],
            },
            // Test shortcut for n!~"^.*$",  i!~".*". First one triggers empty result.
            TestCase {
                matchers: vec![
                    Matcher::create(RegexNotEqual, "n", "^.*$").unwrap(),
                    Matcher::create(RegexNotEqual, "i", ".*").unwrap(),
                ],
                exp: vec![],
            },
            // Test shortcut i!~".*"
            TestCase {
                matchers: vec![
                    Matcher::create(RegexEqual, "n", ".*").unwrap(),
                    Matcher::create(RegexNotEqual, "i", ".*").unwrap(),
                ],
                exp: vec![],
            },
            // Test shortcut i!~".+"
            TestCase {
                matchers: vec![
                    Matcher::create(RegexEqual, "n", ".*").unwrap(),
                    Matcher::create(RegexNotEqual, "i", ".+").unwrap(),
                ],
                exp: to_label_vec(&[
                    labels_from_strings(&["n", "1"]),
                    labels_from_strings(&["n", "2"]),
                    labels_from_strings(&["n", "2.5"]),
                ]),
            },
            // Test shortcut i!~"^.*$"
            TestCase {
                matchers: vec![
                    Matcher::create(Equal, "n", "1").unwrap(),
                    Matcher::create(RegexNotEqual, "i", "^.*$").unwrap(),
                ],
                exp: vec![],
            },
        ];

        let mut exp: HashSet<String> = HashSet::new();

        for case in cases {
            let name = case
                .matchers
                .iter()
                .map(|matcher| matcher.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            let name = format!("{{{name}}}");
            exp.clear();

            let mut case = case;
            case.exp.sort();

            for labels in case.exp {
                let val = label_vec_to_string(&labels);
                exp.insert(val);
            }

            let actual = get_labels_by_matcher(&ix, &case.matchers, &series_data);

            for labels in actual {
                let current = label_vec_to_string(&labels);
                let found = exp.remove(&current);
                assert!(found, "Evaluating {name}\n unexpected result {current}");
            }

            assert!(
                exp.is_empty(),
                "Evaluating {name}\nextra result(s): {exp:?}"
            );
        }
    }

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        let labels = parse_metric_name(prometheus_name).unwrap();
        ts.labels = InternedMetricName::new(&labels);
        ts
    }

    #[test]
    fn test_querying_after_reindex() {
        let index = TimeSeriesIndex::new();

        // Create a time series with specific labels
        let ts = create_series_from_metric_name(
            r#"cpu_usage{instance="server1",region="us-west-2",env="prod"}"#,
        );
        let old_key = b"ts:cpu_usage:old";

        // Index the series with the old key
        index.index_timeseries(&ts, old_key);

        // Verify we can query by labels before rename
        let labels = ts.labels.to_label_vec();
        let result_before = index.posting_by_labels(&labels).unwrap();
        assert_eq!(result_before, Some(ts.id));

        // Verify we can query by specific label combinations before rename
        let instance_labels = vec![Label {
            name: "instance".to_string(),
            value: "server1".to_string(),
        }];
        let postings_before = index.posting_by_labels(&instance_labels).unwrap();
        assert_eq!(postings_before, Some(ts.id));

        // Test querying with matchers before rename
        let mut state = ();
        index.with_postings(&mut state, |postings, _| {
            let instance_postings = postings.postings_for_label_value("instance", "server1");
            assert!(instance_postings.contains(ts.id));

            let region_postings = postings.postings_for_label_value("region", "us-west-2");
            assert!(region_postings.contains(ts.id));

            let env_postings = postings.postings_for_label_value("env", "prod");
            assert!(env_postings.contains(ts.id));
        });

        // Now rename the series
        let new_key = b"ts:cpu_usage:new";
        index.reindex_timeseries(&ts, new_key);

        // Verify the same queries still work after rename
        let result_after = index.posting_by_labels(&labels).unwrap();
        assert_eq!(
            result_after,
            Some(ts.id),
            "Should still find series by labels after rename"
        );

        // Verify querying by specific label combinations still works
        let postings_after = index.posting_by_labels(&instance_labels).unwrap();
        assert_eq!(
            postings_after,
            Some(ts.id),
            "Should still find series by instance label after rename"
        );

        // Test querying with matchers after rename
        index.with_postings(&mut state, |postings, _| {
            let instance_postings = postings.postings_for_label_value("instance", "server1");
            assert!(
                instance_postings.contains(ts.id),
                "Should find series by instance after rename"
            );

            let region_postings = postings.postings_for_label_value("region", "us-west-2");
            assert!(
                region_postings.contains(ts.id),
                "Should find series by region after rename"
            );

            let env_postings = postings.postings_for_label_value("env", "prod");
            assert!(
                env_postings.contains(ts.id),
                "Should find series by env after rename"
            );

            // Verify series ID maps to the new key
            let expected_new_key = new_key.to_vec().into_boxed_slice();
            assert_eq!(
                postings.id_to_key.get(&ts.id),
                Some(&expected_new_key),
                "Series ID should map to new key"
            );
        });

        // Test more queries after rename
        let matchers = vec![
            Matcher::create(MatchOp::Equal, "instance", "server1").unwrap(),
            Matcher::create(MatchOp::Equal, "region", "us-west-2").unwrap(),
        ];
        let filter = Matchers::with_matchers(None, matchers);

        let complex_query_result = postings_for_matchers(&index, &filter).unwrap();
        assert!(
            complex_query_result.contains(ts.id),
            "Query should still work after rename"
        );

        let regex_matchers = vec![
            Matcher::create(MatchOp::RegexEqual, "instance", "server.*").unwrap(),
            Matcher::create(MatchOp::Equal, "env", "prod").unwrap(),
        ];
        let regex_filter = Matchers::with_matchers(None, regex_matchers);

        let regex_query_result = postings_for_matchers(&index, &regex_filter).unwrap();
        assert!(
            regex_query_result.contains(ts.id),
            "Query should still work after rename"
        );

        // Verify that the new key is present in the index
        index.with_postings(&mut state, |postings, _| {
            // Verify that the series ID maps to the new key
            assert_eq!(
                postings.id_to_key.get(&ts.id),
                Some(&new_key.to_vec().into_boxed_slice()),
                "Series ID should map to new key"
            );
        });
    }
}
