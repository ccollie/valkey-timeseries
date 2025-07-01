// Copyright 2015 The Prometheus Authors
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
use crate::common::Timestamp;
use crate::query::test_metric_storage::TestMetricStorage;
use metricsql_runtime::prelude::Context as QueryContext;
use std::sync::Arc;
use valkey_module::ValkeyResult;

pub(super) fn setup_range_query_test_data(
    stor: &mut TestMetricStorage,
    interval: i64,
    num_intervals: usize,
) -> ValkeyResult<()> {
    let mut metrics = Vec::new();
    // Generating test series: a_X and b_X where X can take values of one, ten, or a hundred,
    // representing the number of series each metric name contains.
    // Metric a_X and b_X are simple metrics.
    // These metrics will have data for all test time ranges

    fn create_metric(name: &str, i: usize) -> String {
        let mut result = name.to_string() + "{l=\"";
        let v = i.to_string();
        result.push_str(&v);
        result.push_str("\"}");
        result
    }

    fn add_metric(metrics: &mut Vec<String>, name: &str, i: usize) {
        let result = create_metric(name, i);
        metrics.push(result)
    }

    metrics.push("a_one{}".to_string());
    metrics.push("b_one{}".to_string());

    for i in 0..10 {
        add_metric(&mut metrics, "a_ten", i);
        add_metric(&mut metrics, "b_ten", i);
    }

    for i in 0..100 {
        add_metric(&mut metrics, "a_hundred", i);
        add_metric(&mut metrics, "b_hundred", i);
    }

    // Number points for each different label value of "l" for the sparse series
    let points_per_sparse_series = num_intervals / 50;

    for s in 0..num_intervals {
        let ts = (s as i64 * interval) as Timestamp;
        for (i, metric) in metrics.iter().enumerate() {
            let value = ((s + i) as i64 / metrics.len() as i64) as f64;
            stor.add(metric.as_str(), ts, value)?;
        }
        // Generating a sparse time series: each label value of "l" will contain data only for
        // points_per_sparse_series points
        let v = s / points_per_sparse_series;
        let metric = create_metric("sparse", v);
        let value = (s as f64) / (metrics.len() as f64);
        stor.add(metric.as_str(), ts, value)?;
    }

    Ok(())
}

pub(super) fn create_context(storage: TestMetricStorage) -> QueryContext {
    let provider = Arc::new(storage);
    let ctx = QueryContext::new();
    ctx.with_metric_storage(provider)
}

pub(super) struct BenchCase {
    pub(crate) expr: String,
    pub(crate) steps: usize,
}

pub(super) fn range_query_cases() -> Vec<BenchCase> {
    let mut cases = vec![
        // Plain retrieval.
        BenchCase {
            expr: "a_X".to_string(),
            steps: 0,
        },
        // Simple rate.
        BenchCase {
            expr: "rate(a_X[1m])".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "rate(a_X[1m])".to_string(),
            steps: 10000,
        },
        BenchCase {
            expr: "rate(sparse[1m])".to_string(),
            steps: 10000,
        },
        // Holt-Winters and long ranges.
        BenchCase {
            expr: "holt_winters(a_X[1d], 0.3, 0.3)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "changes(a_X[1d])".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "rate(a_X[1d])".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "absent_over_time(a_X[1d])".to_string(),
            steps: 0,
        },
        // Unary operators.
        BenchCase {
            expr: "-a_X".to_string(),
            steps: 0,
        },
        // Binary operators.
        BenchCase {
            expr: "a_X - b_X".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "a_X - b_X".to_string(),
            steps: 10000,
        },
        BenchCase {
            expr: "a_X and b_X{l=~'.*[0-4]$'}".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "a_X or b_X{l=~'.*[0-4]$'}".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "a_X unless b_X{l=~'.*[0-4]$'}".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "a_X and b_X{l='notfound'}".to_string(),
            steps: 0,
        },
        // Simple functions.
        BenchCase {
            expr: "abs(a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "label_replace(a_X, 'l2', '$1', 'l', '(.*)')".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "label_join(a_X, 'l2', '-', 'l', 'l')".to_string(),
            steps: 0,
        },
        // Simple aggregations.
        BenchCase {
            expr: "sum(a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "avg(a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum without (l)(h_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum without (le)(h_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum by (l)(h_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum by (le)(h_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "count_values('value', h_X)".to_string(),
            steps: 100,
        },
        BenchCase {
            expr: "topk(1, a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "topk(5, a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "limitk(1, a_X)".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "limitk(5, a_X)".to_string(),
            steps: 0,
        },
        // Combinations.
        BenchCase {
            expr: "rate(a_X[1m]) + rate(b_X[1m])".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum without (l)(rate(a_X[1m]))".to_string(),
            steps: 0,
        },
        BenchCase {
            expr: "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))".to_string(),
            steps: 0,
        },
        // Many-to-one join.
        BenchCase {
            expr: "a_X + on(l) group_right a_one".to_string(),
            steps: 0,
        },
        // Label compared to blank string.
        BenchCase {
            expr: "count({__name__!=\"\"})".to_string(),
            steps: 1,
        },
        BenchCase {
            expr: "count({__name__!=\"\",l=\"\"})".to_string(),
            steps: 1,
        },
        // Functions which have special handling inside eval()
        BenchCase {
            expr: "timestamp(a_X)".to_string(),
            steps: 0,
        },
    ];

    // X in an expr will be replaced by different metric sizes.
    let mut tmp = Vec::new();
    for c in cases {
        if !c.expr.contains("X") {
            tmp.push(c);
        } else {
            tmp.push(BenchCase {
                expr: c.expr.replace("X", "one"),
                steps: c.steps,
            });
            tmp.push(BenchCase {
                expr: c.expr.replace("X", "ten"),
                steps: c.steps,
            });
            tmp.push(BenchCase {
                expr: c.expr.replace("X", "hundred"),
                steps: c.steps,
            });
        }
    }
    cases = tmp;

    // No step will be replaced by cases with the standard step.
    let mut tmp = Vec::new();
    for c in cases {
        if c.steps != 0 {
            tmp.push(c);
        } else {
            tmp.push(BenchCase {
                expr: c.expr.clone(),
                steps: 1,
            });
            tmp.push(BenchCase {
                expr: c.expr.clone(),
                steps: 100,
            });
            tmp.push(BenchCase {
                expr: c.expr.clone(),
                steps: 1000,
            });
        }
    }
    tmp
}
