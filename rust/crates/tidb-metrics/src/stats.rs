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

//! Go `pkg/metrics/stats.go`: the statistics families.

use std::sync::LazyLock;

use prometheus::{CounterVec, Opts};

/// Registers one collector with the process default registry, once: Go's
/// `pkg/metrics.RegisterMetrics` makes every family visible to `/metrics`.
fn register_once<C>(collector: prometheus::Result<C>, what: &'static str) -> C
where
    C: prometheus::core::Collector + Clone + 'static,
{
    let metric = collector.unwrap_or_else(|error| panic!("{what} is constructible: {error}"));
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| panic!("{what} is registered once: {error}"));
    metric
}

/// Go `metrics.PseudoEstimation` (the statistics family this package binds
/// its two pseudo-estimation children from).
pub static PSEUDO_ESTIMATION: LazyLock<CounterVec> = LazyLock::new(|| {
    register_once(
        CounterVec::new(
            Opts::new(
                "pseudo_estimation_total",
                "Counter of pseudo estimation caused by outdated stats.",
            )
            .namespace("tidb")
            .subsystem("statistics"),
            &["type"],
        ),
        "pseudo estimation counter",
    )
});
