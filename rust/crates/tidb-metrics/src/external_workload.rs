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

//! Go `pkg/metrics/external_workload.go`.

use prometheus::{CounterVec, Opts};
use std::sync::LazyLock;

/// Go `ExternalWorkloadTaskCounter`: counts register / recycle / init /
/// abort events emitted by the external workload manager, broken down by
/// worker type.
pub static EXTERNAL_WORKLOAD_TASK_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    let collector = CounterVec::new(
        Opts::new(
            "tidb_external_workload_task_total",
            "Total external workload manager events by worker type and action.",
        ),
        &["type", "action"],
    )
    .expect("valid metric definition");
    prometheus::default_registry().register(Box::new(collector.clone())).expect("metric registered once");
    collector
});
