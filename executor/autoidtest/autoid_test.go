// Copyright 2022 PingCAP, Inc.
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

package autoid_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/autoid_service"
	ddltestutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/stretchr/testify/require"
)

// Test filter different kind of allocators.
// In special ddl type, for example:
// 1: ActionRenameTable             : it will abandon all the old allocators.
// 2: ActionRebaseAutoID            : it will drop row-id-type allocator.
// 3: ActionModifyTableAutoIdCache  : it will drop row-id-type allocator.
// 3: ActionRebaseAutoRandomBase    : it will drop auto-rand-type allocator.
func TestFilterDifferentAllocators(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")

	for _, str := range []string{"", " AUTO_ID_CACHE 1"} {
		tk.MustExec("create table t(a bigint auto_random(5) key, b int auto_increment unique)" + str)
		tk.MustExec("insert into t values()")
		tk.MustQuery("select b from t").Check(testkit.Rows("1"))
		allHandles, err := ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "t")
		require.NoError(t, err)
		require.Equal(t, 1, len(allHandles))
		orderedHandles := testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
		require.Equal(t, int64(1), orderedHandles[0])
		tk.MustExec("delete from t")

		// Test rebase auto_increment.
		tk.MustExec("alter table t auto_increment 3000000")
		tk.MustExec("insert into t values()")
		tk.MustQuery("select b from t").Check(testkit.Rows("3000000"))
		allHandles, err = ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "t")
		require.NoError(t, err)
		require.Equal(t, 1, len(allHandles))
		orderedHandles = testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
		require.Equal(t, int64(2), orderedHandles[0])
		tk.MustExec("delete from t")

		// Test rebase auto_random.
		tk.MustExec("alter table t auto_random_base 3000000")
		tk.MustExec("insert into t values()")
		tk.MustQuery("select b from t").Check(testkit.Rows("3000001"))
		allHandles, err = ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "t")
		require.NoError(t, err)
		require.Equal(t, 1, len(allHandles))
		orderedHandles = testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
		require.Equal(t, int64(3000000), orderedHandles[0])
		tk.MustExec("delete from t")

		// Test rename table.
		tk.MustExec("rename table t to t1")
		tk.MustExec("insert into t1 values()")
		res := tk.MustQuery("select b from t1")
		strInt64, err := strconv.ParseInt(res.Rows()[0][0].(string), 10, 64)
		require.NoError(t, err)
		require.GreaterOrEqual(t, strInt64, int64(3000002))
		allHandles, err = ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "t1")
		require.NoError(t, err)
		require.Equal(t, 1, len(allHandles))
		orderedHandles = testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
		require.Greater(t, orderedHandles[0], int64(3000001))

		tk.MustExec("drop table t1")
	}
}

func TestAutoIncrementInsertMinMax(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []struct {
		t      string
		s      string
		vals   []int64
		expect [][]interface{}
	}{
		{"tinyint", "signed", []int64{-128, 0, 127}, testkit.Rows("-128", "1", "2", "3", "127")},
		{"tinyint", "unsigned", []int64{0, 127, 255}, testkit.Rows("1", "2", "127", "128", "255")},
		{"smallint", "signed", []int64{-32768, 0, 32767}, testkit.Rows("-32768", "1", "2", "3", "32767")},
		{"smallint", "unsigned", []int64{0, 32767, 65535}, testkit.Rows("1", "2", "32767", "32768", "65535")},
		{"mediumint", "signed", []int64{-8388608, 0, 8388607}, testkit.Rows("-8388608", "1", "2", "3", "8388607")},
		{"mediumint", "unsigned", []int64{0, 8388607, 16777215}, testkit.Rows("1", "2", "8388607", "8388608", "16777215")},
		{"integer", "signed", []int64{-2147483648, 0, 2147483647}, testkit.Rows("-2147483648", "1", "2", "3", "2147483647")},
		{"integer", "unsigned", []int64{0, 2147483647, 4294967295}, testkit.Rows("1", "2", "2147483647", "2147483648", "4294967295")},
		{"bigint", "signed", []int64{-9223372036854775808, 0, 9223372036854775807}, testkit.Rows("-9223372036854775808", "1", "2", "3", "9223372036854775807")},
		{"bigint", "unsigned", []int64{0, 9223372036854775807}, testkit.Rows("1", "2", "9223372036854775807", "9223372036854775808")},
	}

	for _, option := range []string{"", "auto_id_cache 1", "auto_id_cache 100"} {
		for idx, c := range cases {
			sql := fmt.Sprintf("create table t%d (a %s %s key auto_increment) %s", idx, c.t, c.s, option)
			tk.MustExec(sql)

			for _, val := range c.vals {
				tk.MustExec(fmt.Sprintf("insert into t%d values (%d)", idx, val))
				tk.Exec(fmt.Sprintf("insert into t%d values ()", idx)) // ignore error
			}

			tk.MustQuery(fmt.Sprintf("select * from t%d order by a", idx)).Check(c.expect)

			tk.MustExec(fmt.Sprintf("drop table t%d", idx))
		}
	}

	tk.MustExec("create table t10 (a integer key auto_increment) auto_id_cache 1")
	err := tk.ExecToErr("insert into t10 values (2147483648)")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t10 values (-2147483649)")
	require.Error(t, err)
}

func TestInsertWithAutoidSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1(id int primary key auto_increment, n int);`)
	tk.MustExec(`create table t2(id int unsigned primary key auto_increment, n int);`)
	tk.MustExec(`create table t3(id tinyint primary key auto_increment, n int);`)
	tk.MustExec(`create table t4(id int primary key, n float auto_increment, key I_n(n));`)
	tk.MustExec(`create table t5(id int primary key, n float unsigned auto_increment, key I_n(n));`)
	tk.MustExec(`create table t6(id int primary key, n double auto_increment, key I_n(n));`)
	tk.MustExec(`create table t7(id int primary key, n double unsigned auto_increment, key I_n(n));`)
	// test for inserting multiple values
	tk.MustExec(`create table t8(id int primary key auto_increment, n int);`)

	testInsertWithAutoidSchema(t, tk)
}

func TestInsertWithAutoidSchemaCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1(id int primary key auto_increment, n int) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t2(id int unsigned primary key auto_increment, n int) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t3(id tinyint primary key auto_increment, n int) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t4(id int primary key, n float auto_increment, key I_n(n)) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t5(id int primary key, n float unsigned auto_increment, key I_n(n)) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t6(id int primary key, n double auto_increment, key I_n(n)) AUTO_ID_CACHE 1;`)
	tk.MustExec(`create table t7(id int primary key, n double unsigned auto_increment, key I_n(n)) AUTO_ID_CACHE 1;`)
	// test for inserting multiple values
	tk.MustExec(`create table t8(id int primary key auto_increment, n int);`)

	testInsertWithAutoidSchema(t, tk)
}

func testInsertWithAutoidSchema(t *testing.T, tk *testkit.TestKit) {
	tests := []struct {
		insert string
		query  string
		result [][]interface{}
	}{
		{
			`insert into t1(id, n) values(1, 1)`,
			`select * from t1 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t1(n) values(2)`,
			`select * from t1 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t1(n) values(3)`,
			`select * from t1 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t1(id, n) values(-1, 4)`,
			`select * from t1 where id = -1`,
			testkit.Rows(`-1 4`),
		},
		{
			`insert into t1(n) values(5)`,
			`select * from t1 where id = 4`,
			testkit.Rows(`4 5`),
		},
		{
			`insert into t1(id, n) values('5', 6)`,
			`select * from t1 where id = 5`,
			testkit.Rows(`5 6`),
		},
		{
			`insert into t1(n) values(7)`,
			`select * from t1 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t1(id, n) values(7.4, 8)`,
			`select * from t1 where id = 7`,
			testkit.Rows(`7 8`),
		},
		{
			`insert into t1(id, n) values(7.5, 9)`,
			`select * from t1 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t1(n) values(9)`,
			`select * from t1 where id = 9`,
			testkit.Rows(`9 9`),
		},
		// test last insert id
		{
			`insert into t1 values(3000, -1), (null, -2)`,
			`select * from t1 where id = 3000`,
			testkit.Rows(`3000 -1`),
		},
		{
			`;`,
			`select * from t1 where id = 3001`,
			testkit.Rows(`3001 -2`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`3001`),
		},
		{
			`insert into t2(id, n) values(1, 1)`,
			`select * from t2 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t2(n) values(2)`,
			`select * from t2 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t2(n) values(3)`,
			`select * from t2 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t3(id, n) values(1, 1)`,
			`select * from t3 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t3(n) values(2)`,
			`select * from t3 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t3(n) values(3)`,
			`select * from t3 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t3(id, n) values(-1, 4)`,
			`select * from t3 where id = -1`,
			testkit.Rows(`-1 4`),
		},
		{
			`insert into t3(n) values(5)`,
			`select * from t3 where id = 4`,
			testkit.Rows(`4 5`),
		},
		{
			`insert into t4(id, n) values(1, 1)`,
			`select * from t4 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t4(id) values(2)`,
			`select * from t4 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t4(id, n) values(3, -1)`,
			`select * from t4 where id = 3`,
			testkit.Rows(`3 -1`),
		},
		{
			`insert into t4(id) values(4)`,
			`select * from t4 where id = 4`,
			testkit.Rows(`4 3`),
		},
		{
			`insert into t4(id, n) values(5, 5.5)`,
			`select * from t4 where id = 5`,
			testkit.Rows(`5 5.5`),
		},
		{
			`insert into t4(id) values(6)`,
			`select * from t4 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t4(id, n) values(7, '7.7')`,
			`select * from t4 where id = 7`,
			testkit.Rows(`7 7.7`),
		},
		{
			`insert into t4(id) values(8)`,
			`select * from t4 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t4(id, n) values(9, 10.4)`,
			`select * from t4 where id = 9`,
			testkit.Rows(`9 10.4`),
		},
		{
			`insert into t4(id) values(10)`,
			`select * from t4 where id = 10`,
			testkit.Rows(`10 11`),
		},
		{
			`insert into t5(id, n) values(1, 1)`,
			`select * from t5 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t5(id) values(2)`,
			`select * from t5 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t5(id) values(3)`,
			`select * from t5 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t6(id, n) values(1, 1)`,
			`select * from t6 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t6(id) values(2)`,
			`select * from t6 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t6(id, n) values(3, -1)`,
			`select * from t6 where id = 3`,
			testkit.Rows(`3 -1`),
		},
		{
			`insert into t6(id) values(4)`,
			`select * from t6 where id = 4`,
			testkit.Rows(`4 3`),
		},
		{
			`insert into t6(id, n) values(5, 5.5)`,
			`select * from t6 where id = 5`,
			testkit.Rows(`5 5.5`),
		},
		{
			`insert into t6(id) values(6)`,
			`select * from t6 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t6(id, n) values(7, '7.7')`,
			`select * from t4 where id = 7`,
			testkit.Rows(`7 7.7`),
		},
		{
			`insert into t6(id) values(8)`,
			`select * from t4 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t6(id, n) values(9, 10.4)`,
			`select * from t6 where id = 9`,
			testkit.Rows(`9 10.4`),
		},
		{
			`insert into t6(id) values(10)`,
			`select * from t6 where id = 10`,
			testkit.Rows(`10 11`),
		},
		{
			`insert into t7(id, n) values(1, 1)`,
			`select * from t7 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t7(id) values(2)`,
			`select * from t7 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t7(id) values(3)`,
			`select * from t7 where id = 3`,
			testkit.Rows(`3 3`),
		},

		// the following is test for insert multiple values.
		{
			`insert into t8(n) values(1),(2)`,
			`select * from t8 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`;`,
			`select * from t8 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`;`,
			`select last_insert_id();`,
			testkit.Rows(`1`),
		},
		// test user rebase and auto alloc mixture.
		{
			`insert into t8 values(null, 3),(-1, -1),(null,4),(null, 5)`,
			`select * from t8 where id = 3`,
			testkit.Rows(`3 3`),
		},
		// -1 won't rebase allocator here cause -1 < base.
		{
			`;`,
			`select * from t8 where id = -1`,
			testkit.Rows(`-1 -1`),
		},
		{
			`;`,
			`select * from t8 where id = 4`,
			testkit.Rows(`4 4`),
		},
		{
			`;`,
			`select * from t8 where id = 5`,
			testkit.Rows(`5 5`),
		},
		{
			`;`,
			`select last_insert_id();`,
			testkit.Rows(`3`),
		},
		{
			`insert into t8 values(null, 6),(10, 7),(null, 8)`,
			`select * from t8 where id = 6`,
			testkit.Rows(`6 6`),
		},
		// 10 will rebase allocator here.
		{
			`;`,
			`select * from t8 where id = 10`,
			testkit.Rows(`10 7`),
		},
		{
			`;`,
			`select * from t8 where id = 11`,
			testkit.Rows(`11 8`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`6`),
		},
		// fix bug for last_insert_id should be first allocated id in insert rows (skip the rebase id).
		{
			`insert into t8 values(100, 9),(null,10),(null,11)`,
			`select * from t8 where id = 100`,
			testkit.Rows(`100 9`),
		},
		{
			`;`,
			`select * from t8 where id = 101`,
			testkit.Rows(`101 10`),
		},
		{
			`;`,
			`select * from t8 where id = 102`,
			testkit.Rows(`102 11`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`101`),
		},
		// test with sql_mode: NO_AUTO_VALUE_ON_ZERO.
		{
			`;`,
			`select @@sql_mode`,
			testkit.Rows(`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`),
		},
		{
			`;`,
			"set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,NO_AUTO_VALUE_ON_ZERO`",
			nil,
		},
		{
			`insert into t8 values (0, 12), (null, 13)`,
			`select * from t8 where id = 0`,
			testkit.Rows(`0 12`),
		},
		{
			`;`,
			`select * from t8 where id = 103`,
			testkit.Rows(`103 13`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`103`),
		},
		// test without sql_mode: NO_AUTO_VALUE_ON_ZERO.
		{
			`;`,
			"set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`",
			nil,
		},
		// value 0 will be substitute by autoid.
		{
			`insert into t8 values (0, 14), (null, 15)`,
			`select * from t8 where id = 104`,
			testkit.Rows(`104 14`),
		},
		{
			`;`,
			`select * from t8 where id = 105`,
			testkit.Rows(`105 15`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`104`),
		},
		// last test : auto increment allocation can find in retryInfo.
		{
			`retry : insert into t8 values (null, 16), (null, 17)`,
			`select * from t8 where id = 1000`,
			testkit.Rows(`1000 16`),
		},
		{
			`;`,
			`select * from t8 where id = 1001`,
			testkit.Rows(`1001 17`),
		},
		{
			`;`,
			`select last_insert_id()`,
			// this insert doesn't has the last_insert_id, should be same as the last insert case.
			testkit.Rows(`104`),
		},
	}

	for _, tt := range tests {
		if strings.HasPrefix(tt.insert, "retry : ") {
			// it's the last retry insert case, change the sessionVars.
			retryInfo := &variable.RetryInfo{Retrying: true}
			retryInfo.AddAutoIncrementID(1000)
			retryInfo.AddAutoIncrementID(1001)
			tk.Session().GetSessionVars().RetryInfo = retryInfo
			tk.MustExec(tt.insert[8:])
			tk.Session().GetSessionVars().RetryInfo = &variable.RetryInfo{}
		} else {
			tk.MustExec(tt.insert)
		}
		if tt.query == "set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,NO_AUTO_VALUE_ON_ZERO`" ||
			tt.query == "set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`" {
			tk.MustExec(tt.query)
		} else {
			tk.MustQuery(tt.query).Check(tt.result)
		}
	}
}

// TestAutoIDIncrementAndOffset There is a potential issue in MySQL: when the value of auto_increment_offset is greater
// than that of auto_increment_increment, the value of auto_increment_offset is ignored
// (https://dev.mysql.com/doc/refman/8.0/en/replication-options-master.html#sysvar_auto_increment_increment),
// This issue is a flaw of the implementation of MySQL and it doesn't exist in TiDB.
func TestAutoIDIncrementAndOffset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	// Test for offset is larger than increment.
	tk.Session().GetSessionVars().AutoIncrementIncrement = 5
	tk.Session().GetSessionVars().AutoIncrementOffset = 10

	for _, str := range []string{"", " AUTO_ID_CACHE 1"} {
		tk.MustExec(`create table io (a int key auto_increment)` + str)
		tk.MustExec(`insert into io values (null),(null),(null)`)
		tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "15", "20"))
		tk.MustExec(`drop table io`)
	}

	// Test handle is PK.
	for _, str := range []string{"", " AUTO_ID_CACHE 1"} {
		tk.MustExec(`create table io (a int key auto_increment)` + str)
		tk.Session().GetSessionVars().AutoIncrementOffset = 10
		tk.Session().GetSessionVars().AutoIncrementIncrement = 2
		tk.MustExec(`insert into io values (),(),()`)
		tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "12", "14"))
		tk.MustExec(`delete from io`)

		// Test reset the increment.
		tk.Session().GetSessionVars().AutoIncrementIncrement = 5
		tk.MustExec(`insert into io values (),(),()`)
		tk.MustQuery(`select * from io`).Check(testkit.Rows("15", "20", "25"))
		tk.MustExec(`delete from io`)

		tk.Session().GetSessionVars().AutoIncrementIncrement = 10
		tk.MustExec(`insert into io values (),(),()`)
		tk.MustQuery(`select * from io`).Check(testkit.Rows("30", "40", "50"))
		tk.MustExec(`delete from io`)

		tk.Session().GetSessionVars().AutoIncrementIncrement = 5
		tk.MustExec(`insert into io values (),(),()`)
		tk.MustQuery(`select * from io`).Check(testkit.Rows("55", "60", "65"))
		tk.MustExec(`drop table io`)
	}

	// Test handle is not PK.
	for _, str := range []string{"", " AUTO_ID_CACHE 1"} {
		tk.Session().GetSessionVars().AutoIncrementIncrement = 2
		tk.Session().GetSessionVars().AutoIncrementOffset = 10
		tk.MustExec(`create table io (a int, b int auto_increment, key(b))` + str)
		tk.MustExec(`insert into io(b) values (null),(null),(null)`)
		// AutoID allocation will take increment and offset into consideration.
		tk.MustQuery(`select b from io`).Check(testkit.Rows("10", "12", "14"))
		if str == "" {
			// HandleID allocation will ignore the increment and offset.
			tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("15", "16", "17"))
		} else {
			// Separate row id and auto inc id, increment and offset works on auto inc id
			tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("1", "2", "3"))
		}
		tk.MustExec(`delete from io`)

		tk.Session().GetSessionVars().AutoIncrementIncrement = 10
		tk.MustExec(`insert into io(b) values (null),(null),(null)`)
		tk.MustQuery(`select b from io`).Check(testkit.Rows("20", "30", "40"))
		if str == "" {
			tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("41", "42", "43"))
		} else {
			tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("4", "5", "6"))
		}

		// Test invalid value.
		tk.Session().GetSessionVars().AutoIncrementIncrement = -1
		tk.Session().GetSessionVars().AutoIncrementOffset = -2
		tk.MustGetErrMsg(`insert into io(b) values (null),(null),(null)`,
			"[autoid:8060]Invalid auto_increment settings: auto_increment_increment: -1, auto_increment_offset: -2, both of them must be in range [1..65535]")
		tk.MustExec(`delete from io`)

		tk.Session().GetSessionVars().AutoIncrementIncrement = 65536
		tk.Session().GetSessionVars().AutoIncrementOffset = 65536
		tk.MustGetErrMsg(`insert into io(b) values (null),(null),(null)`,
			"[autoid:8060]Invalid auto_increment settings: auto_increment_increment: 65536, auto_increment_offset: 65536, both of them must be in range [1..65535]")

		tk.MustExec(`drop table io`)
	}
}

func TestRenameTableForAutoIncrement(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t1, t2, t3;")
	tk.MustExec("create table t1 (id int key auto_increment);")
	tk.MustExec("insert into t1 values ()")
	tk.MustExec("rename table t1 to t11")
	tk.MustExec("insert into t11 values ()")
	// TODO(tiancaiamao): fix bug and uncomment here, rename table should not discard the cached AUTO_ID.
	// tk.MustQuery("select * from t11").Check(testkit.Rows("1", "2"))

	// auto_id_cache 1 use another implementation and do not have such bug.
	tk.MustExec("create table t2 (id int key auto_increment) auto_id_cache 1;")
	tk.MustExec("insert into t2 values ()")
	tk.MustExec("rename table t2 to t22")
	tk.MustExec("insert into t22 values ()")
	tk.MustQuery("select * from t22").Check(testkit.Rows("1", "2"))

	tk.MustExec("create table t3 (id int key auto_increment) auto_id_cache 100;")
	tk.MustExec("insert into t3 values ()")
	tk.MustExec("rename table t3 to t33")
	tk.MustExec("insert into t33 values ()")
	// TODO(tiancaiamao): fix bug and uncomment here, rename table should not discard the cached AUTO_ID.
	// tk.MustQuery("select * from t33").Check(testkit.Rows("1", "2"))
}

func TestAlterTableAutoIDCache(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t_473;")
	tk.MustExec("create table t_473 (id int key auto_increment)")
	tk.MustExec("insert into t_473 values ()")
	tk.MustQuery("select * from t_473").Check(testkit.Rows("1"))
	rs, err := tk.Exec("show table t_473 next_row_id")
	require.NoError(t, err)
	rows, err1 := session.ResultSetToStringSlice(context.Background(), tk.Session(), rs)
	require.NoError(t, err1)
	// "test t_473 id 1013608 AUTO_INCREMENT"
	val, err2 := strconv.ParseUint(rows[0][3], 10, 64)
	require.NoError(t, err2)

	tk.MustExec("alter table t_473 auto_id_cache = 100")
	tk.MustQuery("show table t_473 next_row_id").Check(testkit.Rows(
		fmt.Sprintf("test t_473 id %d _TIDB_ROWID", val),
		"test t_473 id 1 AUTO_INCREMENT",
	))
	tk.MustExec("insert into t_473 values ()")
	tk.MustQuery("select * from t_473").Check(testkit.Rows("1", fmt.Sprintf("%d", val)))
	tk.MustQuery("show table t_473 next_row_id").Check(testkit.Rows(
		fmt.Sprintf("test t_473 id %d _TIDB_ROWID", val+100),
		"test t_473 id 1 AUTO_INCREMENT",
	))

	// Note that auto_id_cache=1 use a different implementation, switch between them is not allowed.
	// TODO: relax this restriction and update the test case.
	tk.MustExecToErr("alter table t_473 auto_id_cache = 1")
}

func TestMockAutoIDServiceError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("create table t_mock_err (id int key auto_increment) auto_id_cache 1")

	failpoint.Enable("github.com/pingcap/tidb/autoid_service/mockErr", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/autoid_service/mockErr")
	// Cover a bug that the autoid client retry non-retryable errors forever cause dead loop.
	tk.MustExecToErr("insert into t_mock_err values (),()") // mock error, instead of dead loop
}

func TestIssue39528(t *testing.T) {
	// When AUTO_ID_CACHE is 1, it should not affect row id setting when autoid and rowid are separated.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table issue39528 (id int unsigned key nonclustered auto_increment) shard_row_id_bits=4 auto_id_cache 1;")
	tk.MustExec("insert into issue39528 values ()")
	tk.MustExec("insert into issue39528 values ()")

	ctx := context.Background()
	var codeRun bool
	ctx = context.WithValue(ctx, "testIssue39528", &codeRun)
	_, err := tk.ExecWithContext(ctx, "insert into issue39528 values ()")
	require.NoError(t, err)
	// Make sure the code does not visit tikv on allocate path.
	require.False(t, codeRun)
}
