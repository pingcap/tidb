// Copyright 2019 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testkit"
)

func (s *testSuite1) TestSingleTableRead(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by id").Check(testkit.Rows("1 1 1 1 1",
		"5 5 5 5 5"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ a from t1 where id < 2 or a > 4 order by a").Check(testkit.Rows("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ sum(a) from t1 where id < 2 or a > 4").Check(testkit.Rows("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ * from t1 where a < 2 or b > 4 order by a").Check(testkit.Rows("1 1 1 1 1",
		"5 5 5 5 5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ a from t1 where a < 2 or b > 4 order by a").Check(testkit.Rows("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(a) from t1 where a < 2 or b > 4").Check(testkit.Rows("6"))
}

func (s *testSuite1) TestJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("create table t2(id int primary key, a int)")
	tk.MustExec("create index t2a on t2(a)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustExec("insert into t2 values(1,1),(5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 4").Check(testkit.Rows("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 5").Check(testkit.Rows("1"))
}

func (s *testSuite1) TestIndexMergeReaderAndGeneratedColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
}
