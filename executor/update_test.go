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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestUpdateGenColInTxn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a bigint, b bigint as (a+1));`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t(a) values(1);`)
	err := tk.ExecToErr(`update t set b=6 where b=2;`)
	require.Equal(t, "[planner:3105]The value specified for generated column 'b' in table 't' is not allowed.", err.Error())
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 2`))
}

func TestUpdateWithAutoidSchema(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestUpdateSchemaChange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a bigint, b bigint as (a+1));`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t(a) values(1);`)
	err := tk.ExecToErr(`update t set b=6 where b=2;`)
	require.Equal(t, "[planner:3105]The value specified for generated column 'b' in table 't' is not allowed.", err.Error())
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 2`))
}

func TestUpdateMultiDatabaseTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists test2")
	tk.MustExec("create database test2")
	tk.MustExec("create table t(a int, b int generated always  as (a+1) virtual)")
	tk.MustExec("create table test2.t(a int, b int generated always  as (a+1) virtual)")
	tk.MustExec("update t, test2.t set test.t.a=1")
}

func TestUpdateSwapColumnValues(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c_str varchar(40))")
	tk.MustExec("create table t2 (c_str varchar(40))")
	tk.MustExec("insert into t1 values ('Alice')")
	tk.MustExec("insert into t2 values ('Bob')")
	tk.MustQuery("select t1.c_str, t2.c_str from t1, t2 where t1.c_str <= t2.c_str").Check(testkit.Rows("Alice Bob"))
	tk.MustExec("update t1, t2 set t1.c_str = t2.c_str, t2.c_str = t1.c_str where t1.c_str <= t2.c_str")
	tk.MustQuery("select t1.c_str, t2.c_str from t1, t2 where t1.c_str <= t2.c_str").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("update t set a=b, b=a")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,3)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 3"))
	tk.MustExec("update t set a=b, b=a")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int as (-a) virtual, d int as (-b) stored)")
	tk.MustExec("insert into t(a, b) values (10, 11), (20, 22)")
	tk.MustQuery("select * from t").Check(testkit.Rows("10 11 -10 -11", "20 22 -20 -22"))
	tk.MustExec("update t set a=b, b=a")
	tk.MustQuery("select * from t").Check(testkit.Rows("11 10 -11 -10", "22 20 -22 -20"))
	tk.MustExec("update t set b=30, a=b")
	tk.MustQuery("select * from t").Check(testkit.Rows("10 30 -10 -30", "20 30 -20 -30"))
}

func TestMultiUpdateOnSameTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(x int, y int)")
	tk.MustExec("insert into t values()")
	tk.MustExec("update t t1, t t2 set t2.y=1, t1.x=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustExec("update t t1, t t2 set t1.x=t2.y, t2.y=t1.x")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))

	// Update generated columns
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(x int, y int, z int as (x+10) stored, w int as (y-10) virtual)")
	tk.MustExec("insert into t(x, y) values(1, 2), (3, 4)")
	tk.MustExec("update t t1, t t2 set t2.y=1, t1.x=2 where t1.x=1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1 12 -9", "3 1 13 -9"))

	tk.MustExec("update t t1, t t2 set t1.x=5, t2.y=t1.x where t1.x=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3 12 -7", "5 3 15 -7"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int as (a+b) stored)")
	tk.MustExec("insert into t(a, b) values (1, 2)")
	tk.MustExec("update t t1, t t2 set t2.a=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 2 5"))

	tk.MustExec("update t t1, t t2 set t1.a=4, t2.b=5")
	tk.MustQuery("select * from t").Check(testkit.Rows("4 5 9"))

	// Update primary keys
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustExec("insert into t values (1), (2)")
	tk.MustExec("update t set a=a+2")
	tk.MustQuery("select * from t").Check(testkit.Rows("3", "4"))
	tk.MustExec("update t m, t n set m.a = n.a+10 where m.a=n.a")
	tk.MustQuery("select * from t").Check(testkit.Rows("13", "14"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1,3), (2,4)")
	tk.MustGetErrMsg("update t m, t n set m.a = n.a+10, n.b = m.b+1 where m.a=n.a",
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, primary key(a, b))")
	tk.MustExec("insert into t values (1,3,5), (2,4,6)")
	tk.MustExec("update t m, t n set m.a = n.a+10, m.b = n.b+10 where m.a=n.a")
	tk.MustQuery("select * from t").Check(testkit.Rows("11 13 5", "12 14 6"))
	tk.MustExec("update t m, t n, t q set q.c=m.a+n.b, n.c = m.a+1, m.c = n.b+1 where m.b=n.b AND m.a=q.a")
	tk.MustQuery("select * from t").Check(testkit.Rows("11 13 24", "12 14 26"))
	tk.MustGetErrMsg("update t m, t n, t q set m.a = m.a+1, n.c = n.c-1, q.c = q.a+q.b where m.b=n.b and n.b=q.b",
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
}

func TestUpdateClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

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

func TestDeleteClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

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

func TestReplaceClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

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

func TestPessimisticUpdatePKLazyCheck(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testUpdatePKLazyCheck(t, tk, variable.ClusteredIndexDefModeOn)
	testUpdatePKLazyCheck(t, tk, variable.ClusteredIndexDefModeOff)
	testUpdatePKLazyCheck(t, tk, variable.ClusteredIndexDefModeIntOnly)
}

func testUpdatePKLazyCheck(t *testing.T, tk *testkit.TestKit, clusteredIndex variable.ClusteredIndexDefMode) {
	tk.Session().GetSessionVars().EnableClusteredIndex = clusteredIndex
	tk.MustExec(`drop table if exists upk`)
	tk.MustExec(`create table upk (a int, b int, c int, primary key (a, b))`)
	tk.MustExec(`insert upk values (1, 1, 1), (2, 2, 2), (3, 3, 3)`)
	tk.MustExec("begin pessimistic")
	tk.MustExec("update upk set b = b + 1 where a between 1 and 2")
	require.Equal(t, 2, getPresumeExistsCount(t, tk.Session()))
	_, err := tk.Exec("update upk set a = 3, b = 3 where a between 1 and 2")
	require.True(t, kv.ErrKeyExists.Equal(err))
	tk.MustExec("commit")
}

func getPresumeExistsCount(t *testing.T, se session.Session) int {
	txn, err := se.Txn(false)
	require.NoError(t, err)
	buf := txn.GetMemBuffer()
	it, err := buf.Iter(nil, nil)
	require.NoError(t, err)
	presumeNotExistsCnt := 0
	for it.Valid() {
		flags, err1 := buf.GetFlags(it.Key())
		require.Nil(t, err1)
		err = it.Next()
		require.NoError(t, err)
		if flags.HasPresumeKeyNotExists() {
			presumeNotExistsCnt++
		}
	}
	return presumeNotExistsCnt
}

func TestOutOfRangeWithUnsigned(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(ts int(10) unsigned NULL DEFAULT NULL)`)
	tk.MustExec(`insert into t values(1)`)
	_, err := tk.Exec("update t set ts = IF(ts < (0 - ts), 1,1) where ts>0")
	require.Equal(t, "[types:1690]BIGINT UNSIGNED value is out of range in '(0 - test.t.ts)'", err.Error())
}

func TestIssue21447(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, name varchar(40))")
	tk1.MustExec("insert into t1 values(1, 'abc')")

	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk2.MustExec("update t1 set name='xyz' where id=1")
	tk2.CheckExecResult(1, 0)
	tk2.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 xyz"))
	tk2.MustExec("commit")
	tk1.MustExec("update t1 set name='xyz' where id=1")
	tk1.CheckExecResult(0, 0)
	tk1.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 abc"))
	tk1.MustQuery("select * from t1 where id = 1 for update").Check(testkit.Rows("1 xyz"))
	tk1.MustQuery("select * from t1 where id in (1, 2)").Check(testkit.Rows("1 abc"))
	tk1.MustQuery("select * from t1 where id in (1, 2) for update").Check(testkit.Rows("1 xyz"))
	tk1.MustExec("commit")
}

func TestIssue23553(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists tt`)
	tk.MustExec(`create table tt (m0 varchar(64), status tinyint not null)`)
	tk.MustExec(`insert into tt values('1',0),('1',0),('1',0)`)
	tk.MustExec(`update tt a inner join (select m0 from tt where status!=1 group by m0 having count(*)>1) b on a.m0=b.m0 set a.status=1`)
}
