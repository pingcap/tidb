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

//! Dependency-closed tests for `pkg/planner/implementation/base.go:22`.
//!
//! The Go anchor is `TestBaseImplementation` at
//! `pkg/planner/implementation/base_test.go:28`. Plan and memo attachment are
//! intentionally outside this scalar cost adapter.

use tidb_planner::implementation_cost::ImplementationCost;

#[test]
fn base_cost_resets_sums_and_supports_explicit_override() {
    let mut implementation = ImplementationCost::new();
    assert_eq!(implementation.cost(), 0.0);
    assert_eq!(implementation.calc_cost(&[]), 0.0);
    assert_eq!(implementation.calc_cost(&[1.5, 2.5, 3.0]), 7.0);
    assert_eq!(implementation.cost(), 7.0);
    assert_eq!(implementation.calc_cost(&[4.0]), 4.0);
    implementation.set_cost(6.0);
    assert_eq!(implementation.cost(), 6.0);
}

#[test]
fn cost_limits_preserve_identity_and_subtract_children() {
    assert_eq!(ImplementationCost::scale_cost_limit(12.0), 12.0);
    assert_eq!(ImplementationCost::get_cost_limit(12.0, &[]), 12.0);
    assert_eq!(ImplementationCost::get_cost_limit(12.0, &[2.0, 3.5]), 6.5);
    assert_eq!(ImplementationCost::get_cost_limit(2.0, &[4.0]), -2.0);
}
