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

package admin_test

import (
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testAdminSuite{})

type testAdminSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
}

func (s *testAdminSuite) SetUpSuite(c *C) {
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
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testAdminSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testAdminSuite) TestAdminCheckTable(c *C) {
	// test NULL value.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// test index column has pk-handle column
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b));")
	tk.MustExec("insert into t values(1, 1, 1)")
	tk.MustExec("admin check table t")

	// test for add index on the later added columns.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c1 int);")
	tk.MustExec("INSERT INTO t1 SET c1 = 1;")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN cc1 CHAR(36)    NULL DEFAULT '';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN cc2 VARCHAR(36) NULL DEFAULT ''")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx1 (cc1);")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (cc2);")
	tk.MustExec("admin check table t1;")

	// For add index on virtual column
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1 (
		a int             as (JSON_EXTRACT(k,'$.a')),
		c double          as (JSON_EXTRACT(k,'$.c')),
		d decimal(20,10)  as (JSON_EXTRACT(k,'$.d')),
		e char(10)        as (JSON_EXTRACT(k,'$.e')),
		f date            as (JSON_EXTRACT(k,'$.f')),
		g time            as (JSON_EXTRACT(k,'$.g')),
		h datetime        as (JSON_EXTRACT(k,'$.h')),
		i timestamp       as (JSON_EXTRACT(k,'$.i')),
		j year            as (JSON_EXTRACT(k,'$.j')),
		k json);`)

	tk.MustExec("insert into t1 set k='{\"a\": 100,\"c\":1.234,\"d\":1.2340000000,\"e\":\"abcdefg\",\"f\":\"2018-09-28\",\"g\":\"12:59:59\",\"h\":\"2018-09-28 12:59:59\",\"i\":\"2018-09-28 16:40:33\",\"j\":\"2018\"}';")
	tk.MustExec("alter table t1 add index idx_a(a);")
	tk.MustExec("alter table t1 add index idx_c(c);")
	tk.MustExec("alter table t1 add index idx_d(d);")
	tk.MustExec("alter table t1 add index idx_e(e);")
	tk.MustExec("alter table t1 add index idx_f(f);")
	tk.MustExec("alter table t1 add index idx_g(g);")
	tk.MustExec("alter table t1 add index idx_h(h);")
	tk.MustExec("alter table t1 add index idx_j(j);")
	tk.MustExec("alter table t1 add index idx_i(i);")
	tk.MustExec("alter table t1 add index idx_m(a,c,d,e,f,g,h,i,j);")
	tk.MustExec("admin check table t1;")
}

func (s *testAdminSuite) TestAdminCheckTableClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists admin_check_table_clustered_index;")
	tk.MustExec("create database admin_check_table_clustered_index;")
	tk.MustExec("use admin_check_table_clustered_index;")

	tk.MustExec("set @@tidb_enable_clustered_index = 1;")

	tk.MustExec("create table t (a bigint, b varchar(255), c int, primary key (a, b), index idx_0(a, b), index idx_1(b, c));")
	tk.MustExec("insert into t values (1, '1', 1);")
	tk.MustExec("insert into t values (2, '2', 2);")
	tk.MustExec("admin check table t;")
	for i := 3; i < 200; i++ {
		tk.MustExec("insert into t values (?, ?, ?);", i, strconv.Itoa(i), i)
	}
	tk.MustExec("admin check table t;")

	// Test back filled created index data.
	tk.MustExec("create index idx_2 on t (c);")
	tk.MustExec("admin check table t;")
	tk.MustExec("create index idx_3 on t (a,c);")
	tk.MustExec("admin check table t;")

	// Test newly created columns.
	tk.MustExec("alter table t add column e char(36);")
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t add column d char(36) NULL DEFAULT '';")
	tk.MustExec("admin check table t;")

	tk.MustExec("insert into t values (1000, '1000', 1000, '1000', '1000');")
	tk.MustExec("admin check table t;")
}
