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

//! Documentary gap ports for `pkg/planner/cascades/cascades_test.go`
//! (`pkg/planner.part2` items 63-64 on `origin/master`).
//!
//! Both Go tests drive a live `testkit` mock-store session: they flip the
//! session variable `SetEnableCascadesPlanner`, run DDL/DML/ANALYZE, and diff
//! `explain format='brief'` outputs between the legacy planner and the
//! (legacy) cascades engine. The Rust workspace has no session/executor
//! stack and no cascades optimizer driver (only the dependency-closed leaves
//! in this crate), so these ports are documented gaps; behavior is never
//! approximated.

/// GO PORT of `pkg/planner/cascades/cascades_test.go:24 TestCascadesDrive`.
///
/// Re-derived contract (against origin/master): with
/// `tidb_enable_cascades_planner=on` over `t1(a,b,key(a,b))` holding five
/// rows, `select 1` returns one row "1" and its brief explain is the
/// two-line golden `Projection 1.00 root 1->Column#1` above
/// `TableDual 1.00 root rows:1` — i.e. the cascades driver must at least
/// answer a constant projection.
#[test]
#[ignore = "go-parity-gap: needs testkit mock-store session + SetEnableCascadesPlanner var + explain format='brief' rendering; the cascades driver itself is unported"]
fn cascades_drive_select_one_brief_explain_golden() {
    // Restore: create/drop/populate t1, enable cascades planning, run
    // tk.MustQuery("explain format = 'brief' select 1") and compare both rows.
}

/// GO PORT of `pkg/planner/cascades/cascades_test.go:41
/// TestXFormedOperatorShouldDeriveTheirStatsOwn`.
///
/// Re-derived contract: after ANALYZE of t1/t2 (t2 grown 3 -> 3072 rows via
/// ten self-inserts), four query shapes — plain EXISTS-correlated subquery,
/// plus `inl_join` / `inl_hash_join` / (`set tidb_hash_join_version=optimized`
/// then) `hash_join` hinted variants — must produce byte-identical
/// `explain format="brief"` output with cascades off vs on (:57-:94). The pin
/// is that every xformed operator (apply lifted into join) derives its own
/// stats so cost-based choices do not drift between engines.
#[test]
#[ignore = "go-parity-gap: needs live dual-engine EXPLAIN comparison (session vars, analyze pipeline, correlated-subquery build); unported surface"]
fn xformed_operators_derive_stats_own_matches_classic_explain() {
    // Restore: per shape, capture brief-explain text with the variable off
    // then on, require.Equal(res1, res2) as at cascades_test.go:60/:72/:84/:94.
}
