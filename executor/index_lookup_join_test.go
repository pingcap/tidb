// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestIndexLookupJoinHang(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table idxJoinOuter (a int unsigned)")
	tk.MustExec("create table idxJoinInner (a int unsigned unique)")
	tk.MustExec("insert idxJoinOuter values (1), (1), (1), (1), (1)")
	tk.MustExec("insert idxJoinInner values (1)")
	tk.Se.GetSessionVars().IndexJoinBatchSize = 1
	tk.Se.GetSessionVars().IndexLookupJoinConcurrency = 1

	rs, err := tk.Exec("select /*+ TIDB_INLJ(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req := rs.NewRecordBatch()
	for i := 0; i < 5; i++ {
		rs.Next(context.Background(), req)
	}
	rs.Close()
}

func (s *testSuite1) TestIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t1(id int primary key, a int)")
	tk.MustExec("create table t2(id int primary key, a int, b int, key idx_a(a))")
	tk.MustExec("insert into t2 values (1,1,1),(4,2,4)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(2,2)")
	tk.MustExec("insert into t2 values(2,2,2), (3,3,3)")
	// TableScan below UnionScan
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"IndexJoin_11 12487.50 root inner join, inner:UnionScan_10, outer key:test.t1.a, inner key:test.t2.id",
		"├─UnionScan_12 9990.00 root not(isnull(test.t1.a))",
		"│ └─TableReader_15 9990.00 root data:Selection_14",
		"│   └─Selection_14 9990.00 cop not(isnull(test.t1.a))",
		"│     └─TableScan_13 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─UnionScan_10 1.00 root ",
		"  └─TableReader_9 1.00 root data:TableScan_8",
		"    └─TableScan_8 1.00 cop table:t2, range: decided by [test.t1.a], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	// IndexLookUp below UnionScan
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"IndexJoin_13 12487.50 root inner join, inner:UnionScan_12, outer key:test.t1.a, inner key:test.t2.a",
		"├─UnionScan_14 9990.00 root not(isnull(test.t1.a))",
		"│ └─TableReader_17 9990.00 root data:Selection_16",
		"│   └─Selection_16 9990.00 cop not(isnull(test.t1.a))",
		"│     └─TableScan_15 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─UnionScan_12 0.00 root not(isnull(test.t2.a))",
		"  └─IndexLookUp_11 0.00 root ",
		"    ├─Selection_10 0.00 cop not(isnull(test.t2.a))",
		"    │ └─IndexScan_8 10.00 cop table:t2, index:a, range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo",
		"    └─TableScan_9 0.00 cop table:t2, keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// IndexScan below UnionScan
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"Projection_7 12487.50 root test.t1.a, test.t2.a",
		"└─IndexJoin_12 12487.50 root inner join, inner:UnionScan_11, outer key:test.t1.a, inner key:test.t2.a",
		"  ├─UnionScan_13 9990.00 root not(isnull(test.t1.a))",
		"  │ └─TableReader_16 9990.00 root data:Selection_15",
		"  │   └─Selection_15 9990.00 cop not(isnull(test.t1.a))",
		"  │     └─TableScan_14 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─UnionScan_11 0.00 root not(isnull(test.t2.a))",
		"    └─IndexReader_10 0.00 root index:Selection_9",
		"      └─Selection_9 0.00 cop not(isnull(test.t2.a))",
		"        └─IndexScan_8 10.00 cop table:t2, index:a, range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2",
		"2 2",
	))
	tk.MustExec("rollback")
}

func (s *testSuite1) TestBatchIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t1(id int primary key, a int)")
	tk.MustExec("create table t2(id int primary key, a int, key idx_a(a))")
	tk.MustExec("set @@session.tidb_init_chunk_size=1")
	tk.MustExec("set @@session.tidb_index_join_batch_size=1")
	tk.MustExec("set @@session.tidb_index_lookup_join_concurrency=4")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1,1),(2,1),(3,1),(4,1)")
	tk.MustExec("insert into t2 values(1,1)")
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"StreamAgg_13 1.00 root funcs:count(1)",
		"└─IndexJoin_27 12487.50 root inner join, inner:UnionScan_26, outer key:test.t1.a, inner key:test.t2.a",
		"  ├─UnionScan_19 9990.00 root not(isnull(test.t1.a))",
		"  │ └─TableReader_22 9990.00 root data:Selection_21",
		"  │   └─Selection_21 9990.00 cop not(isnull(test.t1.a))",
		"  │     └─TableScan_20 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─UnionScan_26 0.00 root not(isnull(test.t2.a))",
		"    └─IndexReader_25 0.00 root index:Selection_24",
		"      └─Selection_24 0.00 cop not(isnull(test.t2.a))",
		"        └─IndexScan_23 10.00 cop table:t2, index:a, range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"4",
	))
	tk.MustExec("rollback")
}

func (s *testSuite1) TestInapplicableIndexJoinHint(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint, b bigint);`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t1, t2) */ * from t1, t2;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ TIDB_INLJ(t1, t2) */ is inapplicable without column equal ON condition`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ TIDB_INLJ(t1, t2) */ is inapplicable`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint, b bigint, index idx_a(a));`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ TIDB_INLJ(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ TIDB_INLJ(t2) */ is inapplicable`))
}

func (s *testSuite) TestIndexJoinOverflow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`insert into t1 values (-1)`)
	tk.MustExec(`create table t2(a int unsigned, index idx(a));`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Rows())
}
