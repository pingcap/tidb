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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_planner::physical::{BasePhysicalPlan, PhysicalLimit, RedactMode};

fn limit(offset: u64, count: u64) -> PhysicalLimit {
    PhysicalLimit {
        base: BasePhysicalPlan::with_id(1, "Limit", 0),
        offset,
        count,
        ..PhysicalLimit::default()
    }
}

/// Rust side of `pkg/planner/core/tests/redact/redact_test.go:23
/// TestRedactExplain` — the Limit rows of the MARKER and ON arms.
///
/// The Go test (under `set global tidb_redact_log=MARKER`, :42) explains
/// `select * from t where a > 1 limit 10 offset 10` on
/// `t(a int primary key, b int)` and expects exactly
/// `Limit root  offset:‹10›, count:‹10›` (:55) over
/// `Limit cop[tikv]  offset:‹0›, count:‹20›` (:57); under
/// `tidb_redact_log=ON` (:115) the same query expects
/// `Limit root  offset:?, count:?` (:128) over
/// `Limit cop[tikv]  offset:?, count:?` (:130). The operator-name/`root`/
/// `cop[tikv]` prefix comes from the explain driver; the value-bearing tail
/// is `PhysicalLimit.ExplainInfo` (`pkg/planner/core/operator/physicalop/
/// physical_limit.go:126-155`), which this crate models as
/// the wired `PhysicalLimit::explain_info`. The golden root limit carries
/// `offset=10, count=10` and the cop-side limit `offset=0, count=20` (the
/// pushed-down child folds the offset into its count), so both goldens are
/// pinned verbatim in both redaction modes.
#[test]
fn redact_explain_limit_rows_track_marker_and_on_modes() {
    // MARKER arm — redact_test.go:53-58 (tidb_redact_log=MARKER set at :42).
    let root = limit(10, 10);
    assert_eq!(
        root.explain_info(RedactMode::Marker),
        "offset:‹10›, count:‹10›",
        "root Limit row under MARKER (redact_test.go:55)"
    );
    let cop = limit(0, 20);
    assert_eq!(
        cop.explain_info(RedactMode::Marker),
        "offset:‹0›, count:‹20›",
        "cop Limit row under MARKER (redact_test.go:57)"
    );

    // ON arm — redact_test.go:126-131 (tidb_redact_log=ON set at :115).
    assert_eq!(
        root.explain_info(RedactMode::Enable),
        "offset:?, count:?",
        "root Limit row under ON (redact_test.go:128)"
    );
    assert_eq!(
        cop.explain_info(RedactMode::Enable),
        "offset:?, count:?",
        "cop Limit row under ON (redact_test.go:130)"
    );
}
