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

//! Go `plan_cost_ver1.go` under `tidb_cost_model_version = 1`: each
//! statement's root operator and its estimated cost, captured from a Go
//! tidb-server on the same tables (pseudo statistics).

use super::sql_kit::{exec_all, q, session};
use super::*;

#[test]
fn cost_model_ver1_plans_and_costs_follow_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "create table t1 (a int primary key, b int, c varchar(20), key(b))",
            "create table t2 (a int primary key, b int, c varchar(20), key(b))",
            "create table t3 (a int, b int, c int, key(a,b))",
            "set @@tidb_cost_model_version = 1",
        ],
    );
    for (sql, root, cost) in [
        ("select * from t1 where b > 1", "TableReader_7", "45418.00"),
        ("select * from t1 where a > 1", "TableReader_6", "18084.67"),
        ("select * from t1", "TableReader_5", "54251.33"),
        ("select * from t1 order by a desc limit 5", "Limit_12", "408.27"),
        ("select b from t1 where b = 3", "IndexReader_6", "35.75"),
        ("select * from t1 where a = 3", "Point_Get_1", "0.00"),
        ("select * from t1 where a in (1,2,3)", "Batch_Point_Get_1", "0.00"),
        ("select * from t1 join t2 on t1.b = t2.b where t1.a > 1", "HashJoin_11", "87489.83"),
        ("select * from t1 join t2 on t1.a = t2.a", "MergeJoin_11", "146002.67"),
        ("select /*+ inl_join(t2) */ * from t1 join t2 on t1.b = t2.b", "IndexJoin_13", "558055.94"),
        ("select /*+ inl_hash_join(t2) */ * from t1 join t2 on t1.b = t2.b", "IndexHashJoin_17", "558055.94"),
        ("select /*+ merge_join(t1,t2) */ * from t1 join t2 on t1.b = t2.b", "MergeJoin_11", "621953.25"),
        ("select /*+ hash_join(t1,t2) */ * from t1 left join t2 on t1.b = t2.b", "HashJoin_8", "147976.91"),
        ("select b, count(*) from t1 group by b", "Projection_4", "45885.00"),
        ("select /*+ stream_agg() */ b, count(*) from t1 group by b", "Projection_4", "70886.00"),
        ("select c, sum(b) from t1 group by c", "Projection_4", "58815.53"),
        ("select * from t1 order by c", "Sort_4", "458910.70"),
        ("select * from t1 order by c limit 10", "TopN_7", "44761.11"),
        ("select a from t1 union all select a from t2", "Union_8", "49445.00"),
        ("select * from t1 where exists (select 1 from t2 where t2.b = t1.b)", "HashJoin_9", "120616.66"),
        ("select * from t1 where t1.b in (select b from t2 where t2.c = 'x')", "IndexJoin_18", "40411.26"),
        ("select (select max(t2.a) from t2 where t2.b = t1.b) from t1", "HashJoin_11", "118357.56"),
        ("select * from t3 where a = 1 and b > 2", "IndexLookUp_7", "642.18"),
        ("select * from t1 where b = 1 or c = 'x'", "TableReader_7", "40033.82"),
        ("select * from t1 use index(b) where b > 1 order by b", "IndexLookUp_14", "91191.44"),
        ("select count(distinct c) from t1", "StreamAgg_9", "73426.00"),
    ] {
        let plan = q(&mut tk, &format!("explain format = 'verbose' {sql}"));
        assert_eq!((plan[0][0].as_str(), plan[0][2].as_str()), (root, cost), "{sql}: {plan:?}");
    }
}
