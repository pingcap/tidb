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

//! Source-backed tests for session metric label identity.

use tidb_exec::session_metrics::nontransactional_dml_metric_kinds;

#[test]
fn nontransactional_metric_labels_preserve_source_order() {
    // Source: pkg/session/metrics/metrics.go:18-24,88-92 and
    // pkg/session/test/nontransactionaltest/nontransactional_test.go:429-435
    // (TestNonTransactionalMetrics).
    let labels: Vec<_> = nontransactional_dml_metric_kinds()
        .into_iter()
        .map(|kind| kind.label())
        .collect();
    assert_eq!(labels, vec!["delete", "insert", "update"]);
}
