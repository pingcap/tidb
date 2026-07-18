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

//! Dependency-closed vectors for LogicalCTETable's DeriveStats contract.
//!
//! The Go anchor is `TestPlanStatsLoadForCTE` at
//! `pkg/planner/core/casetest/planstats/plan_stats_test.go:281`.

use tidb_planner::logical_cte_table::{LogicalCteTableStats, StatsIdentity};

#[test]
fn reload_false_keeps_existing_stats_and_reports_no_change() {
    let existing = StatsIdentity::new(10);
    let mut state = LogicalCteTableStats::new(Some(existing), Some(StatsIdentity::new(20)));
    let (stats, changed) = state.derive_stats(&[false]);
    assert_eq!(stats, Some(existing));
    assert!(!changed);
}

#[test]
fn reload_true_installs_seed_stats_and_reports_change() {
    let mut state =
        LogicalCteTableStats::new(Some(StatsIdentity::new(10)), Some(StatsIdentity::new(20)));
    let (stats, changed) = state.derive_stats(&[true]);
    assert_eq!(stats.map(StatsIdentity::value), Some(20));
    assert!(changed);
}

#[test]
fn only_single_reload_entries_are_considered() {
    let existing = StatsIdentity::new(30);
    let mut state = LogicalCteTableStats::new(Some(existing), Some(StatsIdentity::new(40)));
    assert_eq!(state.derive_stats(&[]), (Some(existing), false));
    assert_eq!(state.derive_stats(&[true, false]), (Some(existing), false));
}
