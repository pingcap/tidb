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

//! Go `pkg/ddl/placement_policy_test.go` (`TestAlterRangePlacementPolicy`),
//! on the mock store through the session, as Go's testkit runs it.

use crate::support::testkit::*;

fn get_rule_bundle(name: &str) -> tidb_placement::Bundle {
    tidb_domain_infosync::placement_manager::get_rule_bundle(name).unwrap()
}

/// Go `TestAlterRangePlacementPolicy`.
#[test]
fn test_alter_range_placement_policy() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy fiveReplicas followers=4");
    tk.must_exec("alter range global placement policy fiveReplicas");
    let bundle = get_rule_bundle(tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL);
    assert_eq!(bundle.rules.len(), 1);
    assert_eq!(bundle.rules[0].location_labels.len(), 0);
    tk.must_exec("alter range meta placement policy fiveReplicas");
    tk.must_query("show placement;").sort().check([
        "POLICY fiveReplicas FOLLOWERS=4 NULL",
        "RANGE TiDB_GLOBAL FOLLOWERS=4 PENDING",
        "RANGE TiDB_META FOLLOWERS=4 PENDING",
    ]);
    let bundle = get_rule_bundle(tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_META);
    assert_eq!(bundle.rules.len(), 1);
    assert_eq!(bundle.rules[0].location_labels.len(), 0);

    // Test Issue #51712
    tk.must_exec("alter placement policy fiveReplicas followers=4 SURVIVAL_PREFERENCES=\"[region]\"");
    tk.must_query("show placement;").sort().check([
        "POLICY fiveReplicas FOLLOWERS=4 SURVIVAL_PREFERENCES=\"[region]\" NULL",
        "RANGE TiDB_GLOBAL FOLLOWERS=4 SURVIVAL_PREFERENCES=\"[region]\" PENDING",
        "RANGE TiDB_META FOLLOWERS=4 SURVIVAL_PREFERENCES=\"[region]\" PENDING",
    ]);
    let bundle = get_rule_bundle(tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_GLOBAL);
    assert_eq!(bundle.rules.len(), 1);
    assert_eq!(bundle.rules[0].location_labels.len(), 1);
    assert_eq!(bundle.rules[0].location_labels[0], "region");
    let bundle = get_rule_bundle(tidb_placement::TIDB_BUNDLE_RANGE_PREFIX_FOR_META);
    assert_eq!(bundle.rules.len(), 1);
    assert_eq!(bundle.rules[0].location_labels.len(), 1);
    assert_eq!(bundle.rules[0].location_labels[0], "region");
    // Test Issue #52257
    tk.must_exec("create placement policy fiveRepl followers=4 SURVIVAL_PREFERENCES=\"[region]\"");
    tk.must_exec("drop placement policy fiveRepl");

    let err = tk.exec_to_err("drop placement policy fiveReplicas").expect("the policy is in use");
    assert_eq!(err.error_string(), "[ddl:8241]Placement policy 'fiveReplicas' is still in use");
    tk.must_exec("alter range global placement policy default");
    tk.must_exec("alter range meta placement policy default");
    assert!(tk.exec_to_err("drop placement policy fiveReplicas").is_none());
}
