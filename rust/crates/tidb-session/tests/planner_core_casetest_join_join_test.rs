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

//! Go `pkg/planner/core/casetest/join/join_test.go`.

use crate::support::testkit::TestKit;

/// Go `TestIssue70757IndexJoinInnerIndexSelection`: an IndexJoin inner
/// candidate compares its per-probe access count (`CountAfterAccess / NDV`
/// of the join key) in skyline pruning's row-count-ratio rule, so idx_c's
/// smaller static count no longer prunes idx_k.
#[test]
fn test_issue70757_index_join_inner_index_selection() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec(
        "create table t_outer (id int not null, c varchar(36) not null, primary key (id), key idx_c (c))",
    );
    tk.must_exec(
        "create table t_inner (id int not null auto_increment, k int not null, c varchar(36) not null, \
         primary key (id), key idx_k (k), key idx_c (c))",
    );
    const ROWS_PER_C: usize = 101;
    const C_NDV: usize = 3;
    let outer_values: Vec<String> = (1..=ROWS_PER_C).map(|id| format!("({id}, 'cust-1')")).collect();
    tk.must_exec(&format!("insert into t_outer (id, c) values {}", outer_values.join(",")));
    let inner_values: Vec<String> = (1..=ROWS_PER_C * C_NDV)
        .map(|k| format!("({k}, 'cust-{}')", (k - 1) / ROWS_PER_C + 1))
        .collect();
    tk.must_exec(&format!("insert into t_inner (k, c) values {}", inner_values.join(",")));
    tk.must_exec("analyze table t_outer, t_inner");

    let query = "select /*+ inl_join(i) */ * from t_outer o join t_inner i on i.k = o.id and i.c = o.c where o.c = 'cust-1'";
    // Go's test leaves `set tidb_opt_fix_control = '45132:2'` commented out;
    // only the lowered threshold makes this small data set reach the ratio
    // rule (idx_k 303 rows vs idx_c 101), so both settings are checked. Go
    // master answers idx_k under each.
    for fix_control in ["", "45132:2"] {
        tk.must_exec(&format!("set tidb_opt_fix_control = '{fix_control}'"));
        let plan = format!("{:?}", tk.must_query(&format!("explain format='plan_tree' {query}")).rows());
        assert!(plan.contains("outer key:test.t_outer.id, inner key:test.t_inner.k"), "{fix_control}: {plan}");
        assert!(plan.contains("table:i, index:idx_k(k)"), "{fix_control}: {plan}");
        assert!(!plan.contains("table:i, index:idx_c(c)"), "{fix_control}: {plan}");
    }
}
