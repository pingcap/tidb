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

//! Documentary gap port for `pkg/planner/core/casetest/schema/cannot_find_column_test.go::TestSchemaCannotFindColumnRegression`
//! (`pkg/planner.part9` item 512 on `origin/master`) plus its bootstrap
//! (`main_test.go:29` TestMain registers only the cannot_find_column_suite
//! book; skipped-reason).
//!
//! The Go test needs views, prepare/execute metadata fields, and multi-table
//! UPDATE through a live session — all beyond today's Rust planner surface.

/// GO PORT of
/// `pkg/planner/core/casetest/schema/cannot_find_column_test.go:24
/// TestSchemaCannotFindColumnRegression`.
///
/// Regression net for "column not found" schema bugs: t1/t3/t4 joins plus two
/// views (v_issue65892_topn with an ORDER BY-limit projection over an
/// arithmetic sort key, v_issue65892_lookup) replay the cannot_find_column_suite
/// book comparing plan_tree AND results per entry (:66-79). Then hand-written
/// issue-#66272 probes assert USING(id) resolves to t3's side: plain/having/
/// order-by/ALL-subquery queries each return exactly '10' (:86-96); prepared
/// statement FIELDS must name table t3 and column id (PrepareStmt :98 + field
/// checks :100-102); mixed-type USING('01' vs int) still resolves and returns
/// '1' (:112-120); a USING-join UPDATE mutates t_up_l row id=2 to a=1100
/// (:126-129).
#[test]
#[ignore = "go-parity-gap: views, USING resolution over prepared fields and join-UPDATE need the unported pipeline"]
fn schema_cannot_find_column_regression_golden() {}
