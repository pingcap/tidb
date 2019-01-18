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
		"IndexJoin_11 12500.00 root inner join, inner:UnionScan_10, outer key:test.t1.a, inner key:test.t2.id",
		"├─UnionScan_12 10000.00 root ",
		"│ └─TableReader_14 10000.00 root data:TableScan_13",
		"│   └─TableScan_13 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─UnionScan_10 10.00 root ",
		"  └─TableReader_9 10.00 root data:TableScan_8",
		"    └─TableScan_8 10.00 cop table:t2, range: decided by [test.t1.a], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	// IndexLookUp below UnionScan
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"IndexJoin_12 12500.00 root inner join, inner:UnionScan_11, outer key:test.t1.a, inner key:test.t2.a",
		"├─UnionScan_13 10000.00 root ",
		"│ └─TableReader_15 10000.00 root data:TableScan_14",
		"│   └─TableScan_14 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─UnionScan_11 10.00 root ",
		"  └─IndexLookUp_10 10.00 root ",
		"    ├─IndexScan_8 10.00 cop table:t2, index:a, range: decided by [test.t1.a], keep order:false, stats:pseudo",
		"    └─TableScan_9 10.00 cop table:t2, keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// IndexScan below UnionScan
	tk.MustQuery("explain select /*+ TIDB_INLJ(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"Projection_7 12500.00 root test.t1.a, test.t2.a",
		"└─IndexJoin_11 12500.00 root inner join, inner:UnionScan_10, outer key:test.t1.a, inner key:test.t2.a",
		"  ├─UnionScan_12 10000.00 root ",
		"  │ └─TableReader_14 10000.00 root data:TableScan_13",
		"  │   └─TableScan_13 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─UnionScan_10 10.00 root ",
		"    └─IndexReader_9 10.00 root index:IndexScan_8",
		"      └─IndexScan_8 10.00 cop table:t2, index:a, range: decided by [test.t1.a], keep order:false, stats:pseudo",
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
		"└─IndexJoin_24 12500.00 root inner join, inner:UnionScan_23, outer key:test.t1.a, inner key:test.t2.a",
		"  ├─UnionScan_25 10000.00 root ",
		"  │ └─TableReader_27 10000.00 root data:TableScan_26",
		"  │   └─TableScan_26 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─UnionScan_23 10.00 root ",
		"    └─IndexReader_22 10.00 root index:IndexScan_21",
		"      └─IndexScan_21 10.00 cop table:t2, index:a, range: decided by [test.t1.a], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"4",
	))
	tk.MustExec("rollback")
}
