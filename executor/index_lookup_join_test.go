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
	req := rs.NewChunk()
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
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	// IndexLookUp below UnionScan
	tk.MustQuery("select /*+ TIDB_INLJ(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// IndexScan below UnionScan
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
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ TIDB_INLJ(t1, t2) */ is inapplicable without column equal ON condition`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ TIDB_INLJ(t1, t2) */ is inapplicable`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint, b bigint, index idx_a(a));`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1) */ or /*+ TIDB_INLJ(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t2) */ or /*+ TIDB_INLJ(t2) */ is inapplicable`))
}

func (s *testSuite) TestIndexJoinOverflow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`insert into t1 values (-1)`)
	tk.MustExec(`create table t2(a int unsigned, index idx(a));`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Rows())
}

func (s *testSuite2) TestIssue11061(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(c varchar(30), index ix_c(c(10)))")
	tk.MustExec("insert into t1 (c) values('7_chars'), ('13_characters')")
	tk.MustQuery("SELECT /*+ TIDB_INLJ(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Rows("20"))
}

func (s *testSuite2) TestIndexJoinPartitionTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	tk.MustExec("insert into t values(1, 27, 2)")
	tk.MustQuery("SELECT /*+ TIDB_INLJ(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Rows("1"))
}
