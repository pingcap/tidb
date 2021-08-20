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
	"flag"
	"fmt"
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testTableSampleSuite{})

type testTableSampleSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
}

func (s *testTableSampleSuite) SetUpSuite(c *C) {
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		store, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testTableSampleSuite) initSampleTest(c *C) *testkit.TestKit {
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_table_sample;")
	tk.MustExec("create database test_table_sample;")
	tk.MustExec("use test_table_sample;")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	return tk
}

func (s *testTableSampleSuite) TestTableSampleBasic(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int);")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows())

	tk.MustExec("insert into t values (0), (1000), (2000);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("0"))
	tk.MustExec("alter table t add column b varchar(255) not null default 'abc';")
	tk.MustQuery("select b from t tablesample regions();").Check(testkit.Rows("abc"))
	tk.MustExec("alter table t add column c int as (a + 1);")
	tk.MustQuery("select c from t tablesample regions();").Check(testkit.Rows("1"))
	tk.MustQuery("select c, _tidb_rowid from t tablesample regions();").Check(testkit.Rows("1 1"))
	c.Assert(tk.HasPlan("select * from t tablesample regions();", "TableSample"), IsTrue)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a BIGINT PRIMARY KEY AUTO_RANDOM(3), b int auto_increment, key(b)) pre_split_regions=8;")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows())
	for i := 0; i < 1000; i++ {
		tk.MustExec("insert into t values();")
	}
	tk.MustQuery("select count(*) from t tablesample regions();").Check(testkit.Rows("8"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a varchar(30) collate utf8mb4_general_ci primary key);")
	tk.MustQuery("split table t between ('a') and ('z') regions 100;").Check(testkit.Rows("99 1"))
	tk.MustExec("insert into t values ('a'), ('b'), ('c'), ('d'), ('e');")
	tk.MustQuery("select a from t tablesample regions() limit 2;").Check(testkit.Rows("a", "b"))
}

func (s *testTableSampleSuite) TestTableSampleMultiRegions(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?);", i)
	}
	rows := tk.MustQuery("select * from t tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 4)
	tk.MustQuery("select a from t tablesample regions() order by a limit 1;").Check(testkit.Rows("0"))
	tk.MustQuery("select a from t tablesample regions() where a = 0;").Check(testkit.Rows("0"))

	tk.MustExec("create table t2 (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t2 values (?);", i)
	}
	rows = tk.MustQuery("select * from t tablesample regions(), t2 tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 16)
	tk.MustQuery("select count(*) from t tablesample regions();").Check(testkit.Rows("4"))
	tk.MustExec("drop table t2;")
}

func (s *testTableSampleSuite) TestTableSampleNoSplitTable(c *C) {
	tk := s.initSampleTest(c)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1 (id int primary key);")
	tk.MustExec("create table t2 (id int primary key);")
	tk.MustExec("insert into t2 values(1);")
	rows := tk.MustQuery("select * from t1 tablesample regions();").Rows()
	rows2 := tk.MustQuery("select * from t2 tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 0)
	c.Assert(len(rows2), Equals, 1)
}

func (s *testTableSampleSuite) TestTableSamplePlan(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int default 10);")
	tk.MustExec("split table t between (0) and (100000) regions 4;")
	tk.MustExec("insert into t(a) values (1), (2), (3);")
	rows := tk.MustQuery("explain analyze select a from t tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 2)
	tableSample := fmt.Sprintf("%v", rows[1])
	c.Assert(tableSample, Matches, ".*TableSample.*")
}

func (s *testTableSampleSuite) TestTableSampleSchema(c *C) {
	tk := s.initSampleTest(c)
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	// Clustered index
	tk.MustExec("create table t (a varchar(255) primary key, b bigint);")
	tk.MustExec("insert into t values ('b', 100), ('y', 100);")
	tk.MustQuery("split table t between ('a') and ('z') regions 2;").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a from t tablesample regions();").Check(testkit.Rows("b", "y"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a varchar(255), b int, c decimal, primary key (a, b, c));")
	tk.MustQuery("split table t between ('a', 0, 0) and ('z', 100, 100) regions 2;").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into t values ('b', 10, 100), ('y', 100, 10);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("b 10 100", "y 100 10"))

	// PKIsHandle
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a bigint primary key, b int default 10);")
	tk.MustQuery("split table t between (1) and (100000) regions 4;").Check(testkit.Rows("3 1"))
	tk.MustExec("insert into t(a) values (200), (25600), (50300), (99900), (99901)")
	tk.MustQuery("select a from t tablesample regions();").Check(testkit.Rows("200", "25600", "50300", "99900"))

	// _tidb_rowid
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a bigint, b int default 10);")
	tk.MustQuery("split table t between (0) and (100000) regions 4;").Check(testkit.Rows("3 1"))
	tk.MustExec("insert into t(a) values (1), (2), (3);")
	tk.MustQuery("select a from t tablesample regions();").Check(testkit.Rows("1"))
}

func (s *testTableSampleSuite) TestTableSampleInvalid(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int, b varchar(255));")
	tk.MustExec("insert into t values (1, 'abc');")
	tk.MustExec("create view v as select * from t;")
	tk.MustGetErrCode("select * from v tablesample regions();", errno.ErrInvalidTableSample)
	tk.MustGetErrCode("select * from information_schema.tables tablesample regions();", errno.ErrInvalidTableSample)

	tk.MustGetErrCode("select a from t tablesample system();", errno.ErrInvalidTableSample)
	tk.MustGetErrCode("select a from t tablesample bernoulli(10 percent);", errno.ErrInvalidTableSample)
	tk.MustGetErrCode("select a from t as t1 tablesample regions(), t as t2 tablesample system();", errno.ErrInvalidTableSample)
	tk.MustGetErrCode("select a from t tablesample ();", errno.ErrInvalidTableSample)
}

func (s *testTableSampleSuite) TestTableSampleWithTiDBRowID(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int, b varchar(255));")
	tk.MustExec("insert into t values (1, 'abc');")
	tk.MustQuery("select _tidb_rowid from t tablesample regions();").Check(testkit.Rows("1"))
	tk.MustQuery("select a, _tidb_rowid from t tablesample regions();").Check(testkit.Rows("1 1"))
	tk.MustQuery("select _tidb_rowid, b from t tablesample regions();").Check(testkit.Rows("1 abc"))
	tk.MustQuery("select b, _tidb_rowid, a from t tablesample regions();").Check(testkit.Rows("abc 1 1"))
}

func (s *testTableSampleSuite) TestTableSampleWithPartition(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int, b varchar(255), primary key (a)) partition by hash(a) partitions 2;")
	tk.MustExec("insert into t values (1, '1'), (2, '2'), (3, '3');")
	rows := tk.MustQuery("select * from t tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 2)

	tk.MustExec("delete from t;")
	tk.MustExec("insert into t values (1, '1');")
	rows = tk.MustQuery("select * from t partition (p0) tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 0)
	rows = tk.MustQuery("select * from t partition (p1) tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 1)

	// Test https://github.com/pingcap/tidb/issues/27349.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (a int, b int, unique key idx(a)) partition by range (a) (
        partition p0 values less than (0),
        partition p1 values less than (10),
        partition p2 values less than (30),
        partition p3 values less than (maxvalue));`)
	tk.MustExec("insert into t values (2, 2), (31, 31), (12, 12);")
	tk.MustQuery("select _tidb_rowid from t tablesample regions() order by _tidb_rowid;").
		Check(testkit.Rows("1", "2", "3")) // The order of _tidb_rowid should be correct.
}

func (s *testTableSampleSuite) TestTableSampleGeneratedColumns(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int primary key, b int as (a + 1), c int as (b + 1), d int as (c + 1));")
	tk.MustQuery("split table t between (0) and (10000) regions 4;").Check(testkit.Rows("3 1"))
	tk.MustExec("insert into t(a) values (1), (2), (2999), (4999), (9999);")
	tk.MustQuery("select a from t tablesample regions()").Check(testkit.Rows("1", "2999", "9999"))
	tk.MustQuery("select c from t tablesample regions()").Check(testkit.Rows("3", "3001", "10001"))
	tk.MustQuery("select a, b from t tablesample regions()").Check(
		testkit.Rows("1 2", "2999 3000", "9999 10000"))
	tk.MustQuery("select d, c from t tablesample regions()").Check(
		testkit.Rows("4 3", "3002 3001", "10002 10001"))
	tk.MustQuery("select a, d from t tablesample regions()").Check(
		testkit.Rows("1 4", "2999 3002", "9999 10002"))
}

func (s *testTableSampleSuite) TestTableSampleUnionScanIgnorePendingKV(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int primary key);")
	tk.MustQuery("split table t between (0) and (40000) regions 4;").Check(testkit.Rows("3 1"))
	tk.MustExec("insert into t values (1), (1000), (10002);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002"))

	tk.MustExec("begin;")
	tk.MustExec("insert into t values (20006), (50000);")
	// The memory DB values in transactions are ignored.
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002"))
	tk.MustExec("delete from t where a = 1;")
	// The memory DB values in transactions are ignored.
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002"))
	tk.MustExec("commit;")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1000", "10002", "20006", "50000"))
}

func (s *testTableSampleSuite) TestTableSampleTransactionConsistency(c *C) {
	tk := s.initSampleTest(c)
	tk2 := s.initSampleTest(c)
	tk.MustExec("create table t (a int primary key);")
	tk.MustQuery("split table t between (0) and (40000) regions 4;").Check(testkit.Rows("3 1"))
	tk.MustExec("insert into t values (1), (1000), (10002);")

	tk.MustExec("begin;")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002"))
	tk2.MustExec("insert into t values (20006), (50000);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002"))
	tk.MustExec("commit;")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("1", "10002", "20006", "50000"))
}

func (s *testTableSampleSuite) TestTableSampleNotSupportedPlanWarning(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int primary key, b int, c varchar(255));")
	tk.MustQuery("split table t between (0) and (10000) regions 5;").Check(testkit.Rows("4 1"))
	tk.MustExec("insert into t values (1000, 1, '1'), (1001, 1, '1'), (2100, 2, '2'), (4500, 3, '3');")

	tk.MustExec("create index idx_0 on t (b);")
	tk.MustQuery("select a from t tablesample regions() order by a;").Check(
		testkit.Rows("1000", "2100", "4500"))
	tk.MustQuery("select a from t use index (idx_0) tablesample regions() order by a;").Check(
		testkit.Rows("1000", "1001", "2100", "4500"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 8128 Invalid TABLESAMPLE: plan not supported"))
}

func (s *testTableSampleSuite) TestMaxChunkSize(c *C) {
	tk := s.initSampleTest(c)
	tk.MustExec("create table t (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?);", i)
	}
	tk.Se.GetSessionVars().MaxChunkSize = 1
	rows := tk.MustQuery("select * from t tablesample regions();").Rows()
	c.Assert(len(rows), Equals, 4)
}
