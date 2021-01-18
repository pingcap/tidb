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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testkit"
)

type testClusteredSuite struct{ *baseTestSuite }
type testClusteredSerialSuite struct{ *testClusteredSuite }

func (s *testClusteredSuite) SetUpTest(c *C) {
}

func (s *testClusteredSuite) newTK(c *C) *testkit.TestKit {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.GetSessionVars().EnableClusteredIndex = true
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

// Test for union scan in prefixed clustered index table.
// See https://github.com/pingcap/tidb/issues/22069.
func (s *testClusteredSerialSuite) TestClusteredUnionScanOnPrefixingPrimaryKey(c *C) {
	originCollate := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(originCollate)
	tk := s.newTK(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (col_1 varchar(255), col_2 tinyint, primary key idx_1 (col_1(1)));")
	tk.MustExec("insert into t values ('aaaaa', -38);")
	tk.MustExec("insert into t values ('bbbbb', -48);")

	tk.MustExec("begin PESSIMISTIC;")
	tk.MustExec("update t set col_2 = 47 where col_1 in ('aaaaa') order by col_1,col_2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("aaaaa 47", "bbbbb -48"))
	tk.MustGetErrCode("insert into t values ('bb', 0);", errno.ErrDupEntry)
	tk.MustGetErrCode("insert into t values ('aa', 0);", errno.ErrDupEntry)
	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("aaaaa 47", "bbbbb -48"))
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

	// Test for issue https://github.com/pingcap/tidb/issues/22193
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (col_0 blob(20), col_1 int, primary key(col_0(1)), unique key idx(col_0(2)));")
	tk.MustExec("insert into t values('aaa', 1);")
	tk.MustExec("begin;")
	tk.MustExec("update t set col_0 = 'ccc';")
	tk.MustExec("update t set col_0 = 'ddd';")
	tk.MustExec("commit;")
	tk.MustQuery("select cast(col_0 as char(20)) from t use index (`primary`);").Check(testkit.Rows("ddd"))
	tk.MustQuery("select cast(col_0 as char(20)) from t use index (idx);").Check(testkit.Rows("ddd"))
	tk.MustExec("admin check table t")
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

func (s *testClusteredSerialSuite) TestClusteredIndexSyntax(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	const showPKType = `select tidb_pk_type from information_schema.tables where table_schema = 'test' and table_name = 't';`
	const nonClustered, intClustered, commonClustered = `NON-CLUSTERED`, `INT CLUSTERED`, `COMMON CLUSTERED`
	assertPkType := func(sql string, pkType string) {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(sql)
		tk.MustQuery(showPKType).Check(testkit.Rows(pkType))
	}

	defer config.RestoreFunc()
	for _, allowAlterPK := range []bool{true, false} {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.AlterPrimaryKey = allowAlterPK
		})
		// Test single integer column as the primary key.
		intPKDefault, commonPKDefault := intClustered, commonClustered
		if allowAlterPK {
			intPKDefault = nonClustered
			commonPKDefault = nonClustered
		}
		assertPkType("create table t (a int primary key, b int);", intPKDefault)
		assertPkType("create table t (a int, b int, primary key(a) clustered);", intClustered)
		assertPkType("create table t (a int, b int, primary key(a) nonclustered);", nonClustered)

		// Test for clustered index.
		tk.Se.GetSessionVars().EnableClusteredIndex = false
		assertPkType("create table t (a int, b varchar(255), primary key(b, a));", nonClustered)
		assertPkType("create table t (a int, b varchar(255), primary key(b, a) nonclustered);", nonClustered)
		assertPkType("create table t (a int, b varchar(255), primary key(b, a) clustered);", commonClustered)
		tk.Se.GetSessionVars().EnableClusteredIndex = true
		assertPkType("create table t (a int, b varchar(255), primary key(b, a));", commonPKDefault)
		assertPkType("create table t (a int, b varchar(255), primary key(b, a) nonclustered);", nonClustered)
		assertPkType("create table t (a int, b varchar(255), primary key(b, a) clustered);", commonClustered)
	}
}
