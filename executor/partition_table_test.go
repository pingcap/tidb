// Copyright 2020 PingCAP, Inc.
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

func (s *partitionTableSuite) TestFourReader(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists pt")
	tk.MustExec(`create table pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("insert into pt values (0, 0), (2, 2), (4, 4), (6, 6), (7, 7), (9, 9), (null, null)")

	// Table reader
	tk.MustQuery("select * from pt").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	// Table reader: table dual
	tk.MustQuery("select * from pt where c > 10").Check(testkit.Rows())
	// Table reader: one partition
	tk.MustQuery("select * from pt where c > 8").Check(testkit.Rows("9 9"))
	// Table reader: more than one partition
	tk.MustQuery("select * from pt where c < 2 or c >= 9").Check(testkit.Rows("0 0", "9 9"))

	// Index reader
	tk.MustQuery("select c from pt").Sort().Check(testkit.Rows("0", "2", "4", "6", "7", "9", "<nil>"))
	tk.MustQuery("select c from pt where c > 10").Check(testkit.Rows())
	tk.MustQuery("select c from pt where c > 8").Check(testkit.Rows("9"))
	tk.MustQuery("select c from pt where c < 2 or c >= 9").Check(testkit.Rows("0", "9"))

	// Index lookup
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 4 and c > 10").Check(testkit.Rows())
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c > 8").Check(testkit.Rows("9 9"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c < 2 or c >= 9").Check(testkit.Rows("0 0", "9 9"))

	// Index Merge
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustQuery("select /*+ use_index(i_c, i_id) */ * from pt where id = 4 or c < 7").Check(testkit.Rows("0 0", "2 2", "4 4", "6 6"))
}

func (s *partitionTableSuite) TestPartitionIndexJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists p, t")
	tk.MustExec(`create table p (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("create table t (id int)")
	tk.MustExec("insert into p values (3,3), (4,4), (6,6), (9,9)")
	tk.MustExec("insert into t values (4), (9)")

	// Build indexLookUp in index join
	tk.MustQuery("select /*+ INL_JOIN(p) */ * from p, t where p.id = t.id").Sort().Check(testkit.Rows("4 4 4", "9 9 9"))
	// Build index reader in index join
	tk.MustQuery("select /*+ INL_JOIN(p) */ p.id from p, t where p.id = t.id").Check(testkit.Rows("4", "9"))
}

func (s *partitionTableSuite) TestPartitionUnionScanIndexJoin(c *C) {
	// For issue https://github.com/pingcap/tidb/issues/19152
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int)) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int, c_str)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (10, 'interesting neumann')")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("begin")
	tk.MustExec("insert into t2 values (11, 'hopeful hoover');")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Rows("10 interesting neumann 10 interesting neumann"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Rows("10 interesting neumann 10 interesting neumann"))
	tk.MustExec("commit")
}

func (s *partitionTableSuite) TestDAGTableID(c *C) {
	// This test checks the table ID in the DAG is changed to partition ID in the nextPartition function.
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table employees (id int,store_id int not null)partition by hash(store_id) partitions 4;")
	sql := "select * from test.employees"
	rs, err := tk.Exec(sql)
	c.Assert(err, IsNil)

	m := make(map[int64]struct{})
	ctx := context.WithValue(context.Background(), "nextPartitionUpdateDAGReq", m)
	tk.ResultSetToResultWithCtx(ctx, rs, Commentf("sql:%s, args:%v", sql))
	// Check table ID is changed to partition ID for each partition.
	c.Assert(m, HasLen, 4)
}
