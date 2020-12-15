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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
)

type testClusteredSuite struct{ *baseTestSuite }

func (s *testClusteredSuite) SetUpTest(c *C) {
}

func (s *testClusteredSuite) newTK(c *C) *testkit.TestKit {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("set @@tidb_enable_clustered_index = 1")
	return tk
}

func (s *testClusteredSuite) TestClusteredUnionScan(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b))")
	tk.MustExec("insert t (a, b) values (1, 1)")
	tk.MustExec("begin")
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("rollback")

	// cover old row format.
	tk = testkit.NewTestKitWithInit(c, s.store)
	tk.Se.GetSessionVars().RowEncoder.Enable = false
	tk.MustExec("begin")
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("rollback")
}

func (s *testClusteredSuite) TestClusteredUnionScanIndexLookup(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, pk char(10), c int, primary key(pk), key(a));")
	tk.MustExec("insert into t values (1, '111', 3);")

	tk.MustExec("begin")
	tk.MustExec("update t set a = a + 1, pk = '222' where a = 1;")
	sql := "select pk, c from t where a = 2;"
	tk.HasPlan(sql, "IndexLookUp")
	tk.MustQuery(sql).Check(testkit.Rows("222 3"))

	tk.MustExec("commit")
	tk.MustQuery(sql).Check(testkit.Rows("222 3"))
}

func (s *testClusteredSuite) TestClusteredIndexLookUp(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, primary key (a, b))")
	tk.MustExec("create index idx on t(c)")
	tk.MustExec("insert t values (1, 1, 1, 1)")
	tk.MustQuery("select d from t use index (idx)").Check(testkit.Rows("1"))
}

func (s *testClusteredSuite) TestClusteredIndexLookUp2(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists c3")
	createTable := `
CREATE TABLE c3 (
  c_id int(11) NOT NULL,
  c_d_id int(11) NOT NULL,
  c_w_id int(11) NOT NULL,
  c_first varchar(16) DEFAULT NULL,
  c_middle char(2) DEFAULT NULL,
  c_last varchar(16) DEFAULT NULL,
  c_balance decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id),
  KEY idx (c_w_id,c_d_id,c_last,c_first)
);`
	tk.MustExec(createTable)
	tk.MustExec("insert c3 values (772,1,1,'aaa','OE','CALL',0),(1905,1,1,'bbb','OE','CALL',0);")
	query := `
SELECT c_balance, c_first, c_middle, c_id FROM c3 use index (idx) WHERE c_w_id = 1 AND c_d_id = 1 and c_last = 'CALL' ORDER BY c_first
`
	tk.MustQuery(query).Check(testkit.Rows("0.00 aaa OE 772", "0.00 bbb OE 1905"))
}

func (s *testClusteredSuite) TestClusteredTopN(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists o3")
	createTables := `
	CREATE TABLE o3 (
	o_id int NOT NULL,
	o_d_id int,
	o_w_id int,
	o_c_id int,
	PRIMARY KEY (o_w_id,o_d_id,o_id),
	KEY idx_order (o_w_id,o_d_id,o_c_id,o_id)
);`
	tk.MustExec(createTables)
	tk.MustExec("insert o3 values (1, 6, 9, 3), (2, 6, 9, 5), (3, 6, 9, 7)")
	tk.MustQuery("SELECT max(o_id) max_order FROM o3 use index (idx_order)").Check(testkit.Rows("3"))
}

func (s *testClusteredSuite) TestClusteredHint(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists ht")
	tk.MustExec("create table ht (a varchar(64) primary key, b int)")
	tk.MustQuery("select * from ht use index (`PRIMARY`)")
}

func (s *testClusteredSuite) TestClusteredBatchPointGet(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int,b int,c int, PRIMARY KEY (a,b)) PARTITION BY HASH(a) PARTITIONS 3")
	tk.MustExec("insert t values (1, 1, 1), (3, 3, 3), (5, 5, 5)")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (3, 3), (5, 5))").Check(
		testkit.Rows("1 1 1", "3 3 3", "5 5 5"))
}

func (s *testClusteredSuite) TestClusteredInsertIgnoreBatchGetKeyCount(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(10) primary key, b int)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert ignore t values ('a', 1)")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	snapSize := tikv.SnapCacheSize(txn.GetSnapshot())
	c.Assert(snapSize, Equals, 1)
	tk.MustExec("rollback")
}

func (s *testClusteredSuite) TestClusteredPrefixingPrimaryKey(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, c int, primary key(name(2)), index idx(b));")
	tk.MustExec("insert into t(name, b) values('aaaaa', 1), ('bbbbb', 2);")
	tk.MustExec("admin check table t;")

	tk.MustGetErrCode("insert into t(name, b) values('aaa', 3);", errno.ErrDupEntry)
	sql := "select * from t use index(primary) where name = 'aaaaa';"
	tk.HasPlan(sql, "TableReader")
	tk.HasPlan(sql, "TableRangeScan")
	tk.MustQuery(sql).Check(testkit.Rows("aaaaa 1 <nil>"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, c char(10), primary key(c(2), name(2)), index idx(b));")
	tk.MustExec("insert into t values ('aaa', 1, 'aaa'), ('bbb', 1, 'bbb');")
	tk.MustExec("insert into t values ('aa', 1, 'bbb'), ('bbb', 1, 'ccc');")
	tk.MustGetErrCode("insert into t values ('aa', 1, 'aa');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('aac', 1, 'aac');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('bb', 1, 'bb');", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('bbc', 1, 'bbc');", errno.ErrDupEntry)
	tk.MustGetErrCode("update t set name = 'aa', c = 'aa' where c = 'ccc'", errno.ErrDupEntry)
	tk.MustExec("update t set name = 'ccc' where name = 'aa'")
	tk.MustQuery("select group_concat(name order by name separator '.') from t use index(idx);").
		Check(testkit.Rows("aaa.bbb.bbb.ccc"))
	tk.MustExec("admin check table t;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(name varchar(255), b int, primary key(name(2)), index idx(b));")
	tk.MustExec("insert into t values ('aaa', 1), ('bbb', 1);")
	tk.MustQuery("select group_concat(name order by name separator '.') from t use index(idx);").
		Check(testkit.Rows("aaa.bbb"))

	tk.MustGetErrCode("update t set name = 'aaaaa' where name = 'bbb'", errno.ErrDupEntry)
	tk.MustExec("update ignore t set name = 'aaaaa' where name = 'bbb'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry 'aaaaa' for key 'PRIMARY'"))
	tk.MustExec("admin check table t;")
}

func (s *testClusteredSuite) TestClusteredWithOldRowFormat(c *C) {
	tk := s.newTK(c)
	tk.Se.GetSessionVars().RowEncoder.Enable = false
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id varchar(255) primary key, a int, b int, unique index idx(b));")
	tk.MustExec("insert into t values ('b568004d-afad-11ea-8e4d-d651e3a981b7', 1, -1);")
	tk.MustQuery("select * from t use index(primary);").Check(testkit.Rows("b568004d-afad-11ea-8e4d-d651e3a981b7 1 -1"))

	// Test for issue https://github.com/pingcap/tidb/issues/21568
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key(c_str));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t (c_int, c_str) values (13, 'dazzling torvalds'), (3, 'happy rhodes');")
	tk.MustExec("delete from t where c_decimal <= 3.024 or (c_int, c_str) in ((5, 'happy saha'));")

	// Test for issue https://github.com/pingcap/tidb/issues/21502.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_double double, c_decimal decimal(12, 6), primary key(c_decimal, c_double), unique key(c_int));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (5, 55.068712, 8.256);")
	tk.MustExec("delete from t where c_int = 5;")

	// Test for issue https://github.com/pingcap/tidb/issues/21568#issuecomment-741601887
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c_int int, c_str varchar(40), c_timestamp timestamp, c_decimal decimal(12, 6), primary key(c_int, c_str), key(c_decimal));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (11, 'abc', null, null);")
	tk.MustExec("update t set c_str = upper(c_str) where c_decimal is null;")
	tk.MustQuery("select * from t where c_decimal is null;").Check(testkit.Rows("11 ABC <nil> <nil>"))
}

func (s *testClusteredSuite) TestIssue20002(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t ( c_int int, c_str varchar(40), c_datetime datetime, primary key(c_str), unique key(c_datetime));")
	tk.MustExec("insert into t values (1, 'laughing hertz', '2020-04-27 20:29:30'), (2, 'sharp yalow', '2020-04-01 05:53:36'), (3, 'pedantic hoover', '2020-03-10 11:49:00');")
	tk.MustExec("begin;")
	tk.MustExec("update t set c_str = 'amazing herschel' where c_int = 3;")
	tk.MustExec("select c_int, c_str, c_datetime from t where c_datetime between '2020-01-09 22:00:28' and '2020-04-08 15:12:37';")
	tk.MustExec("commit;")
	tk.MustExec("admin check index t `c_datetime`;")
}

// https://github.com/pingcap/tidb/issues/20727
func (s *testClusteredSuite) TestClusteredIndexSplitAndAddIndex(c *C) {
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255), b int, primary key(a));")
	tk.MustExec("insert into t values ('a', 1), ('b', 2), ('c', 3), ('u', 1);")
	tk.MustQuery("split table t between ('a') and ('z') regions 5;").Check(testkit.Rows("4 1"))
	tk.MustExec("create index idx on t (b);")
	tk.MustQuery("select a from t order by a;").Check(testkit.Rows("a", "b", "c", "u"))
	tk.MustQuery("select a from t use index (idx) order by a;").Check(testkit.Rows("a", "b", "c", "u"))
}
