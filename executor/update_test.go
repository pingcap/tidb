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
	"flag"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

type testUpdateSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testUpdateSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
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

func (s *testUpdateSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testUpdateSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testUpdateSuite) TestUpdateGenColInTxn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a bigint, b bigint as (a+1));`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t(a) values(1);`)
	err := tk.ExecToErr(`update t set b=6 where b=2;`)
	c.Assert(err.Error(), Equals, "[planner:3105]The value specified for generated column 'b' in table 't' is not allowed.")
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 2`))
}

func (s *testUpdateSuite) TestUpdateWithAutoidSchema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1(id int primary key auto_increment, n int);`)
	tk.MustExec(`create table t2(id int primary key, n float auto_increment, key I_n(n));`)
	tk.MustExec(`create table t3(id int primary key, n double auto_increment, key I_n(n));`)

	tests := []struct {
		exec   string
		query  string
		result [][]interface{}
	}{
		{
			`insert into t1 set n = 1`,
			`select * from t1 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`update t1 set id = id+1`,
			`select * from t1 where id = 2`,
			testkit.Rows(`2 1`),
		},
		{
			`insert into t1 set n = 2`,
			`select * from t1 where id = 3`,
			testkit.Rows(`3 2`),
		},
		{
			`update t1 set id = id + '1.1' where id = 3`,
			`select * from t1 where id = 4`,
			testkit.Rows(`4 2`),
		},
		{
			`insert into t1 set n = 3`,
			`select * from t1 where id = 5`,
			testkit.Rows(`5 3`),
		},
		{
			`update t1 set id = id + '0.5' where id = 5`,
			`select * from t1 where id = 6`,
			testkit.Rows(`6 3`),
		},
		{
			`insert into t1 set n = 4`,
			`select * from t1 where id = 7`,
			testkit.Rows(`7 4`),
		},
		{
			`insert into t2 set id = 1`,
			`select * from t2 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`update t2 set n = n+1`,
			`select * from t2 where id = 1`,
			testkit.Rows(`1 2`),
		},
		{
			`insert into t2 set id = 2`,
			`select * from t2 where id = 2`,
			testkit.Rows(`2 3`),
		},
		{
			`update t2 set n = n + '2.2'`,
			`select * from t2 where id = 2`,
			testkit.Rows(`2 5.2`),
		},
		{
			`insert into t2 set id = 3`,
			`select * from t2 where id = 3`,
			testkit.Rows(`3 6`),
		},
		{
			`update t2 set n = n + '0.5' where id = 3`,
			`select * from t2 where id = 3`,
			testkit.Rows(`3 6.5`),
		},
		{
			`insert into t2 set id = 4`,
			`select * from t2 where id = 4`,
			testkit.Rows(`4 7`),
		},
		{
			`insert into t3 set id = 1`,
			`select * from t3 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`update t3 set n = n+1`,
			`select * from t3 where id = 1`,
			testkit.Rows(`1 2`),
		},
		{
			`insert into t3 set id = 2`,
			`select * from t3 where id = 2`,
			testkit.Rows(`2 3`),
		},
		{
			`update t3 set n = n + '3.3'`,
			`select * from t3 where id = 2`,
			testkit.Rows(`2 6.3`),
		},
		{
			`insert into t3 set id = 3`,
			`select * from t3 where id = 3`,
			testkit.Rows(`3 7`),
		},
		{
			`update t3 set n = n + '0.5' where id = 3`,
			`select * from t3 where id = 3`,
			testkit.Rows(`3 7.5`),
		},
		{
			`insert into t3 set id = 4`,
			`select * from t3 where id = 4`,
			testkit.Rows(`4 8`),
		},
	}

	for _, tt := range tests {
		tk.MustExec(tt.exec)
		tk.MustQuery(tt.query).Check(tt.result)
	}
}

func (s *testUpdateSuite) TestUpdateSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a bigint, b bigint as (a+1));`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t(a) values(1);`)
	err := tk.ExecToErr(`update t set b=6 where b=2;`)
	c.Assert(err.Error(), Equals, "[planner:3105]The value specified for generated column 'b' in table 't' is not allowed.")
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 2`))
}

func (s *testUpdateSuite) TestUpdateMultiDatabaseTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists test2")
	tk.MustExec("create database test2")
	tk.MustExec("create table t(a int, b int generated always  as (a+1) virtual)")
	tk.MustExec("create table test2.t(a int, b int generated always  as (a+1) virtual)")
	tk.MustExec("update t, test2.t set test.t.a=1")
}

func (s *testUpdateSuite) TestUpdateClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(id varchar(200) primary key, v int)`)
	tk.MustExec(`insert into t(id, v) values ('abc', 233)`)
	tk.MustQuery(`select id, v from t where id = 'abc'`).Check(testkit.Rows("abc 233"))
	tk.MustExec(`update t set id = 'dfg' where id = 'abc'`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("dfg 233"))
	tk.MustExec(`update t set id = 'aaa', v = 333 where id = 'dfg'`)
	tk.MustQuery(`select * from t where id = 'aaa'`).Check(testkit.Rows("aaa 333"))
	tk.MustExec(`update t set v = 222 where id = 'aaa'`)
	tk.MustQuery(`select * from t where id = 'aaa'`).Check(testkit.Rows("aaa 222"))
	tk.MustExec(`insert into t(id, v) values ('bbb', 111)`)
	c.Assert(tk.ExecToErr(`update t set id = 'bbb' where id = 'aaa'`).Error(), Equals, `[kv:1062]Duplicate entry '{bbb}' for key 'PRIMARY'`)

	tk.MustExec(`drop table if exists t3pk`)
	tk.MustExec(`create table t3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustExec(`insert into t3pk(id1, id2, v, id3) values ('aaa', 'bbb', 233, 111)`)
	tk.MustQuery(`select id1, id2, id3, v from t3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Rows("aaa bbb 111 233"))
	tk.MustExec(`update t3pk set id1 = 'abc', id2 = 'bbb2', id3 = 222, v = 555 where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`)
	tk.MustQuery(`select id1, id2, id3, v from t3pk where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`).Check(testkit.Rows("abc bbb2 222 555"))
	tk.MustQuery(`select id1, id2, id3, v from t3pk`).Check(testkit.Rows("abc bbb2 222 555"))
	tk.MustExec(`update t3pk set v = 666 where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`)
	tk.MustQuery(`select id1, id2, id3, v from t3pk`).Check(testkit.Rows("abc bbb2 222 666"))
	tk.MustExec(`insert into t3pk(id1, id2, id3, v) values ('abc', 'bbb3', 222, 777)`)
	c.Assert(tk.ExecToErr(`update t3pk set id2 = 'bbb3' where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`).Error(), Equals, `[kv:1062]Duplicate entry '{abc, bbb3, 222}' for key 'PRIMARY'`)
}
