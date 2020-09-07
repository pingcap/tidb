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
	"github.com/pingcap/tidb/errno"
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

var _ = SerialSuites(&testSuite11{&baseTestSuite{}})

type testSuite11 struct {
	*baseTestSuite
}

func (s *testSuite11) TestUpdateClusterIndex(c *C) {
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
	tk.MustGetErrCode(`update t set id = 'bbb' where id = 'aaa'`, errno.ErrDupEntry)

	tk.MustExec(`drop table if exists ut3pk`)
	tk.MustExec(`create table ut3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustExec(`insert into ut3pk(id1, id2, v, id3) values ('aaa', 'bbb', 233, 111)`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Rows("aaa bbb 111 233"))
	tk.MustExec(`update ut3pk set id1 = 'abc', id2 = 'bbb2', id3 = 222, v = 555 where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`).Check(testkit.Rows("abc bbb2 222 555"))
	tk.MustQuery(`select id1, id2, id3, v from ut3pk`).Check(testkit.Rows("abc bbb2 222 555"))
	tk.MustExec(`update ut3pk set v = 666 where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk`).Check(testkit.Rows("abc bbb2 222 666"))
	tk.MustExec(`insert into ut3pk(id1, id2, id3, v) values ('abc', 'bbb3', 222, 777)`)
	tk.MustGetErrCode(`update ut3pk set id2 = 'bbb3' where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`, errno.ErrDupEntry)

	tk.MustExec(`drop table if exists ut1pku`)
	tk.MustExec(`create table ut1pku(id varchar(200) primary key, uk int, v int, unique key ukk(uk))`)
	tk.MustExec(`insert into ut1pku(id, uk, v) values('a', 1, 2), ('b', 2, 3)`)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Rows("a 1 2", "b 2 3"))
	tk.MustExec(`update ut1pku set uk = 3 where id = 'a'`)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Rows("a 3 2", "b 2 3"))
	tk.MustGetErrCode(`update ut1pku set uk = 2 where id = 'a'`, errno.ErrDupEntry)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Rows("a 3 2", "b 2 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) primary key, b char(10));")
	tk.MustExec("insert into t values('a', 'b');")
	tk.MustExec("update t set a='c' where t.a='a' and b='b';")
	tk.MustQuery("select * from t").Check(testkit.Rows("c b"))

	tk.MustExec("drop table if exists s")
	tk.MustExec("create table s (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert s values (3, 3, 3), (5, 5, 5)")
	tk.MustExec("update s set c = 10 where a = 3")
	tk.MustQuery("select * from s").Check(testkit.Rows("3 3 10", "5 5 5"))
}

func (s *testSuite11) TestDeleteClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(id varchar(200) primary key, v int)`)
	tk.MustExec(`insert into t(id, v) values ('abc', 233)`)
	tk.MustExec(`delete from t where id = 'abc'`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows())
	tk.MustQuery(`select * from t where id = 'abc'`).Check(testkit.Rows())

	tk.MustExec(`drop table if exists it3pk`)
	tk.MustExec(`create table it3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustExec(`insert into it3pk(id1, id2, v, id3) values ('aaa', 'bbb', 233, 111)`)
	tk.MustExec(`delete from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`)
	tk.MustQuery(`select * from it3pk`).Check(testkit.Rows())
	tk.MustQuery(`select * from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Rows())
	tk.MustExec(`insert into it3pk(id1, id2, v, id3) values ('aaa', 'bbb', 433, 111)`)
	tk.MustQuery(`select * from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Rows("aaa bbb 433 111"))

	tk.MustExec(`drop table if exists dt3pku`)
	tk.MustExec(`create table dt3pku(id varchar(200) primary key, uk int, v int, unique key uuk(uk))`)
	tk.MustExec(`insert into dt3pku(id, uk, v) values('a', 1, 2)`)
	tk.MustExec(`delete from dt3pku where id = 'a'`)
	tk.MustQuery(`select * from dt3pku`).Check(testkit.Rows())
	tk.MustExec(`insert into dt3pku(id, uk, v) values('a', 1, 2)`)

	tk.MustExec("drop table if exists s1")
	tk.MustExec("create table s1 (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert s1 values (3, 3, 3), (5, 5, 5)")
	tk.MustExec("delete from s1 where a = 3")
	tk.MustQuery("select * from s1").Check(testkit.Rows("5 5 5"))
}

func (s *testSuite11) TestReplaceClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists rt1pk`)
	tk.MustExec(`create table rt1pk(id varchar(200) primary key, v int)`)
	tk.MustExec(`replace into rt1pk(id, v) values('abc', 1)`)
	tk.MustQuery(`select * from rt1pk`).Check(testkit.Rows("abc 1"))
	tk.MustExec(`replace into rt1pk(id, v) values('bbb', 233), ('abc', 2)`)
	tk.MustQuery(`select * from rt1pk`).Check(testkit.Rows("abc 2", "bbb 233"))

	tk.MustExec(`drop table if exists rt3pk`)
	tk.MustExec(`create table rt3pk(id1 timestamp, id2 time, v int, id3 year, primary key(id1, id2, id3))`)
	tk.MustExec(`replace into rt3pk(id1, id2,id3, v) values('2018-01-01 11:11:11', '22:22:22', '2019', 1)`)
	tk.MustQuery(`select * from rt3pk`).Check(testkit.Rows("2018-01-01 11:11:11 22:22:22 1 2019"))
	tk.MustExec(`replace into rt3pk(id1, id2, id3, v) values('2018-01-01 11:11:11', '22:22:22', '2019', 2)`)
	tk.MustQuery(`select * from rt3pk`).Check(testkit.Rows("2018-01-01 11:11:11 22:22:22 2 2019"))

	tk.MustExec(`drop table if exists rt1pk1u`)
	tk.MustExec(`create table rt1pk1u(id varchar(200) primary key, uk int, v int, unique key uuk(uk))`)
	tk.MustExec(`replace into rt1pk1u(id, uk, v) values("abc", 2, 1)`)
	tk.MustQuery(`select * from rt1pk1u`).Check(testkit.Rows("abc 2 1"))
	tk.MustExec(`replace into rt1pk1u(id, uk, v) values("aaa", 2, 11)`)
	tk.MustQuery(`select * from rt1pk1u`).Check(testkit.Rows("aaa 2 11"))
}

func (s *testSuite11) TestPessimisticUpdatePKLazyCheck(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	s.testUpdatePKLazyCheck(c, tk, true)
	s.testUpdatePKLazyCheck(c, tk, false)
}

func (s *testSuite11) testUpdatePKLazyCheck(c *C, tk *testkit.TestKit, clusteredIndex bool) {
	tk.MustExec(fmt.Sprintf(`set @@tidb_enable_clustered_index=%v`, clusteredIndex))
	tk.MustExec(`drop table if exists upk`)
	tk.MustExec(`create table upk (a int, b int, c int, primary key (a, b))`)
	tk.MustExec(`insert upk values (1, 1, 1), (2, 2, 2), (3, 3, 3)`)
	tk.MustExec("begin pessimistic")
	tk.MustExec("update upk set b = b + 1 where a between 1 and 2")
	c.Assert(getPresumeExistsCount(c, tk.Se), Equals, 2)
	_, err := tk.Exec("update upk set a = 3, b = 3 where a between 1 and 2")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	tk.MustExec("commit")
}

func getPresumeExistsCount(c *C, se session.Session) int {
	txn, err := se.Txn(false)
	c.Assert(err, IsNil)
	buf := txn.GetMemBuffer()
	it, err := buf.Iter(nil, nil)
	c.Assert(err, IsNil)
	presumeNotExistsCnt := 0
	for it.Valid() {
		flags, err1 := buf.GetFlags(it.Key())
		c.Assert(err1, IsNil)
		err = it.Next()
		c.Assert(err, IsNil)
		if flags.HasPresumeKeyNotExists() {
			presumeNotExistsCnt++
		}
	}
	return presumeNotExistsCnt
}
