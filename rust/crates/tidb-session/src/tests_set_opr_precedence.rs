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

//! Execution-level pins for set-operation precedence.
//!
//! Neither parser encodes `INTERSECT`'s higher precedence in the AST — the
//! flat term list defers to the planner consumer, Go's `buildSetOpr`
//! (`logical_plan_builder.go:2123`) that cuts INTERSECT runs first and then
//! folds UNION/EXCEPT left-to-right. The planner shape is pinned by
//! `set_opr_tests.rs::test_intersect_binds_tighter_than_union`; these pins
//! carry the same rule through execution, where a grouping mistake changes
//! the ANSWER, not just the plan.

use crate::tests_support::*;
use crate::*;

/// `1 UNION 2 INTERSECT 2` is `1 UNION (2 INTERSECT 2)` — two rows. A
/// left-to-right reading would answer `(1 UNION 2) INTERSECT 2` — one row.
#[test]
fn intersect_binds_tighter_than_union_in_execution() {
    let mut session = Session::new();
    assert_eq!(
        row_text(session.run("select 1 union select 2 intersect select 2 order by 1")),
        [["1"], ["2"]]
    );
}

/// UNION and EXCEPT share precedence and fold left-to-right: `1 EXCEPT 2
/// UNION 2` is `(1 EXCEPT 2) UNION 2` — two rows. A tighter-UNION reading
/// would answer `1 EXCEPT (2 UNION 2)` — one row.
#[test]
fn union_and_except_fold_left_to_right() {
    let mut session = Session::new();
    assert_eq!(
        row_text(session.run("select 1 except select 2 union select 2 order by 1")),
        [["1"], ["2"]]
    );
}
