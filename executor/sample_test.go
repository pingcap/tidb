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
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&testTableSampleSuite{})

type testTableSampleSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
	ctx     *mock.Context
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
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows())

	tk.MustExec("insert into t values (0), (1000), (2000);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("0"))
	tk.MustExec("alter table t add column b varchar(255) not null default 'abc';")
	tk.MustQuery("select b from t tablesample regions();").Check(testkit.Rows("abc"))
	tk.MustExec("alter table t add column c int as (a + 1);")
	tk.MustQuery("select c from t tablesample regions();").Check(testkit.Rows("1"))
	tk.MustQuery("select c, _tidb_rowid from t tablesample regions();").Check(testkit.Rows("1 1"))
	c.Assert(tk.HasPlan("select * from t tablesample regions();", "TableSample"), IsTrue)
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

func (s *testTableSampleSuite) TestTableSampleSchema(c *C) {
	tk := s.initSampleTest(c)
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
