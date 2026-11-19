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

//! Go `pkg/executor/test/jointest/hashjoin/hash_join_test.go`, on the mock store through the session, as Go's testkit
//! runs it.

#![allow(unused_mut, unused_variables)]

use crate::support::testkit::TestKit;

/// Go `TestOuterTableBuildHashTableIsuse13933`.
#[test]
fn test_outer_table_build_hash_table_isuse13933() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t, s");
    tk.must_exec("create table t (a int,b int)");
    tk.must_exec("create table s (a int,b int)");
    tk.must_exec("insert into t values (11,11),(1,2)");
    tk.must_exec("insert into s values (1,2),(2,1),(11,11)");
    tk.must_query("select /*+ HASH_JOIN_BUILD(t) */ * from t left join s on s.a > t.a").sort().check(["1 2 11 11", "1 2 2 1", "11 11 <nil> <nil>"]);
    tk.must_query("explain format = 'brief' select /*+ HASH_JOIN_BUILD(t) */ * from t left join s on s.a > t.a").check(["HashJoin 99900000.00 root  CARTESIAN left outer join, left side:TableReader, other cond:gt(test.s.a, test.t.a)", "├─TableReader(Build) 10000.00 root  data:TableFullScan", "│ └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo", "└─TableReader(Probe) 9990.00 root  data:Selection", "  └─Selection 9990.00 cop[tikv]  not(isnull(test.s.a))", "    └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo"]);
    tk.must_exec("drop table if exists t, s");
    tk.must_exec("Create table s (a int, b int, key(b))");
    tk.must_exec("Create table t (a int, b int, key(b))");
    tk.must_exec("Insert into s values (1,2),(2,1),(11,11)");
    tk.must_exec("Insert into t values (11,2),(1,2),(5,2)");
    tk.must_query("select /*+ INL_HASH_JOIN(s) */ * from t left join s on s.b=t.b and s.a < t.a;").sort().check(["1 2 <nil> <nil>", "11 2 1 2", "5 2 1 2"]);
    tk.must_query("explain format = 'brief' select /*+ INL_HASH_JOIN(s) */ * from t left join s on s.b=t.b and s.a < t.a;").check(["IndexHashJoin 12475.01 root  left outer join, inner:IndexLookUp, left side:TableReader, outer key:test.t.b, inner key:test.s.b, equal cond:eq(test.t.b, test.s.b), other cond:lt(test.s.a, test.t.a)", "├─TableReader(Build) 10000.00 root  data:TableFullScan", "│ └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo", "└─IndexLookUp(Probe) 12475.01 root  ", "  ├─Selection(Build) 12487.50 cop[tikv]  not(isnull(test.s.b))", "  │ └─IndexRangeScan 12500.00 cop[tikv] table:s, index:b(b) range: decided by [eq(test.s.b, test.t.b)], keep order:false, stats:pseudo", "  └─Selection(Probe) 12475.01 cop[tikv]  not(isnull(test.s.a))", "    └─TableRowIDScan 12487.50 cop[tikv] table:s keep order:false, stats:pseudo"]);
}

/// Go `TestInlineProjection4HashJoinIssue15316`.
#[test]
fn test_inline_projection4_hash_join_issue15316() {
    // Two necessary factors to reproduce this issue:
    // (1) taking HashLeftJoin, i.e., letting the probing tuple lay at the left side of joined tuples
    // (2) the projection only contains a part of columns from the build side, i.e., pruning the same probe side
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists S, T");
    tk.must_exec("create table S (a int not null, b int, c int);");
    tk.must_exec("create table T (a int not null, b int, c int);");
    tk.must_exec("insert into S values (0,1,2),(0,1,null),(0,1,2);");
    tk.must_exec("insert into T values (0,10,2),(0,10,null),(1,10,2);");
    tk.must_query("select /*+ HASH_JOIN_BUILD(T) */ T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").check(["0 0 <nil>", "0 0 <nil>", "0 0 <nil>", "0 0 2", "0 0 2", "0 0 2"]);
    // NOTE: the HashLeftJoin should be kept
    tk.must_query("explain format = 'brief' select /*+ HASH_JOIN_BUILD(T) */ T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").check(["Projection 12487.50 root  test.t.a, test.t.a, test.t.c", "└─Sort 12487.50 root  test.t.a, test.t.c", "  └─HashJoin 12487.50 root  inner join, equal:[eq(test.s.a, test.t.a)], other cond:lt(test.s.b, test.t.b)", "    ├─TableReader(Build) 9990.00 root  data:Selection", "    │ └─Selection 9990.00 cop[tikv]  not(isnull(test.t.b))", "    │   └─TableFullScan 10000.00 cop[tikv] table:T keep order:false, stats:pseudo", "    └─TableReader(Probe) 9990.00 root  data:Selection", "      └─Selection 9990.00 cop[tikv]  not(isnull(test.s.b))", "        └─TableFullScan 10000.00 cop[tikv] table:S keep order:false, stats:pseudo"]);
}

/// Go `TestIssue54755`.
#[test]
fn test_issue54755() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec("drop table if exists t2;");
    tk.must_exec("create table t1(pk INTEGER AUTO_INCREMENT, col_int_nokey INTEGER, col_int_key INTEGER, col_varchar_key VARCHAR(1), col_varchar_nokey VARCHAR(1), PRIMARY KEY (pk), KEY (col_int_key), KEY (col_varchar_key, col_int_key))");
    tk.must_exec("create table t2(pk INTEGER AUTO_INCREMENT, col_int_nokey INTEGER, col_int_key INTEGER, col_varchar_key VARCHAR(1), col_varchar_nokey VARCHAR(1), PRIMARY KEY (pk), KEY (col_int_key), KEY (col_varchar_key, col_int_key))");
    tk.must_exec("insert into t1(col_int_key, col_int_nokey,col_varchar_key, col_varchar_nokey) values(4,2,'v','v'),(62,150,'v','v')");
    tk.must_exec("insert into t2(col_int_key, col_int_nokey,col_varchar_key, col_varchar_nokey) values(8,null,'x','x'),(7,8,'d','d')");
    tk.must_exec("set tidb_hash_join_version = optimized");
    // right join
    tk.must_query("select max(SQ1_alias2.col_int_nokey) as SQ1_field1 from ( t2 as SQ1_alias1 right join t1 as SQ1_alias2 on ( SQ1_alias2.col_varchar_key = SQ1_alias1.col_varchar_nokey ))").check(["150"]);
    // left join
    tk.must_query("select max(SQ1_alias2.col_int_nokey) as SQ1_field1 from ( t1 as SQ1_alias2 left join t2 as SQ1_alias1 on ( SQ1_alias2.col_varchar_key = SQ1_alias1.col_varchar_nokey ))").check(["150"]);
}

/// Go `TestIssue55016`.
#[test]
fn test_issue55016() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(a varchar(10), b char(10))");
    tk.must_exec("insert into t values('aa','a')");
    for hash_join_v2 in ["set tidb_hash_join_version = legacy", "set tidb_hash_join_version = optimized"] {
    tk.must_exec(hash_join_v2);
    tk.must_query("select count(*) from t t1 join t t2 on t1.a = t2.b and t2.a = t1.b").check(["0"]);
    }
}

/// Go `TestIssue56825`.
#[test]
fn test_issue56825() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec("drop table if exists t2;");
    tk.must_exec("create table t1(id int, col1 int)");
    tk.must_exec("create table t2(id int, col1 int, col2 int, col3 int, col4 int, col5 int)");
    tk.must_exec("insert into t1 values(1,2),(2,3)");
    tk.must_exec("insert into t2 values(1,2,3,4,5,6),(3,4,5,6,7,8),(4,5,6,7,8,9)");
    tk.must_exec("analyze table t1");
    tk.must_exec("analyze table t2");
    // t1 as build side
    for hash_join_v2 in ["set tidb_hash_join_version = legacy", "set tidb_hash_join_version = optimized"] {
    tk.must_exec(hash_join_v2);
    tk.must_query("select * from t1 left join t2 on t1.id = t2.id and t1.col1 <= t2.col1 order by t1.id").check(["1 2 1 2 3 4 5 6", "2 3 <nil> <nil> <nil> <nil> <nil> <nil>"]);
    tk.must_query("select * from t1 right join t2 on t1.id = t2.id and t1.col1 <= t2.col1 order by t2.id").check(["1 2 1 2 3 4 5 6", "<nil> <nil> 3 4 5 6 7 8", "<nil> <nil> 4 5 6 7 8 9"]);
    }
    tk.must_exec("insert into t1 values(10,20),(11,21),(12,22),(13,23),(14,24),(15,25)");
    tk.must_exec("analyze table t1");
    // t2 as build side
    for hash_join_v2 in ["set tidb_hash_join_version = legacy", "set tidb_hash_join_version = optimized"] {
    tk.must_exec(hash_join_v2);
    tk.must_query("select * from t1 left join t2 on t1.id = t2.id and t1.col1 <= t2.col1 order by t1.id").check(["1 2 1 2 3 4 5 6", "2 3 <nil> <nil> <nil> <nil> <nil> <nil>", "10 20 <nil> <nil> <nil> <nil> <nil> <nil>", "11 21 <nil> <nil> <nil> <nil> <nil> <nil>", "12 22 <nil> <nil> <nil> <nil> <nil> <nil>", "13 23 <nil> <nil> <nil> <nil> <nil> <nil>", "14 24 <nil> <nil> <nil> <nil> <nil> <nil>", "15 25 <nil> <nil> <nil> <nil> <nil> <nil>"]);
    tk.must_query("select * from t1 right join t2 on t1.id = t2.id and t1.col1 <= t2.col1 order by t2.id").check(["1 2 1 2 3 4 5 6", "<nil> <nil> 3 4 5 6 7 8", "<nil> <nil> 4 5 6 7 8 9"]);
    }
}
