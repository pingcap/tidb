// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/metrics`: the Prometheus families every layer bumps, one module
//! per Go file (`executor.go`, `session.go`, ...), each with the
//! `init*Metrics` registration of its file.

pub mod bindinfo;
pub mod client_go_metrics;
pub mod ddl;
pub mod distsql;
pub mod domain;
pub mod executor;
pub mod gc_worker;
pub mod globalsort;
pub mod memory;
pub mod meta;
pub mod owner;
pub mod panic;
pub mod server;
pub mod session;
pub mod stats;
pub mod stmtsummary;
pub mod telemetry;
pub mod topsql;

/// client_golang's `MetricVec.DeletePartialMatch(labels)`: removes every
/// series of the vector whose labels include all of `labels`, and returns
/// how many it removed.
pub fn delete_partial_match<M: prometheus::core::Metric + Clone, B: prometheus::core::MetricVecBuilder<M = M>>(
    vec: &prometheus::core::MetricVec<B>,
    labels: &[(&str, &str)],
) -> usize {
    use prometheus::core::Collector as _;
    let mut removed = 0;
    for family in vec.collect() {
        for metric in family.get_metric() {
            let pairs = metric.get_label();
            let matches = labels.iter().all(|(name, value)| {
                pairs.iter().any(|pair| pair.name() == *name && pair.value() == *value)
            });
            if !matches {
                continue;
            }
            // A collected series lists its labels by name, not in the
            // vector's declaration order, so it is removed by name.
            let series: std::collections::HashMap<&str, &str> =
                pairs.iter().map(|pair| (pair.name(), pair.value())).collect();
            if vec.remove(&series).is_ok() {
                removed += 1;
            }
        }
    }
    removed
}

#[cfg(test)]
mod delete_partial_match_tests {
    use prometheus::{CounterVec, Opts};

    /// client_golang's `DeletePartialMatch` contract: only the series
    /// carrying the given label values go.
    #[test]
    fn delete_partial_match_removes_only_matching_series() {
        let vec = CounterVec::new(Opts::new("dpm_test", "test"), &["task_id", "event"]).unwrap();
        vec.with_label_values(&["1", "retry"]).inc();
        vec.with_label_values(&["1", "cleanup-failed"]).inc();
        vec.with_label_values(&["2", "retry"]).inc();
        assert_eq!(super::delete_partial_match(&vec, &[("task_id", "1")]), 2);
        let remaining: Vec<String> = prometheus::core::Collector::collect(&vec)[0]
            .get_metric()
            .iter()
            .map(|metric| metric.get_label().iter().map(|pair| pair.value().to_owned()).collect::<Vec<_>>().join("/"))
            .collect();
        // Collected labels are listed by name: event, then task_id.
        assert_eq!(remaining, ["retry/2"]);
    }
}
