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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/pkg/autoid_service"
	ddltestutil "github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
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
		require.GreaterOrEqual(t, orderedHandles[0], int64(3000001))

		tk.MustExec("drop table t1")
	}
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

	// test for auto_id_cache = 1
	tk.MustExec(`drop table if exists t1, t2, t3, t4, t5, t6, t7, t8`)
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
		result [][]any
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

func TestMockAutoIDServiceError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("create table t_mock_err (id int key auto_increment) auto_id_cache 1")

	failpoint.Enable("github.com/pingcap/tidb/pkg/autoid_service/mockErr", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/autoid_service/mockErr")
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

func TestIssue52622(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@auto_increment_increment = 66;`)
	tk.MustExec(`set @@auto_increment_offset = 9527;`)

	tk.MustQuery(`select @@auto_increment_increment;`).Check(testkit.Rows("66"))
	tk.MustQuery(`select @@auto_increment_offset;`).Check(testkit.Rows("9527"))

	for i := 0; i < 2; i++ {
		createTableSQL := "create table issue52622 (id int primary key auto_increment, k int)"
		if i == 0 {
			createTableSQL = createTableSQL + " AUTO_ID_CACHE 1"
		}

		tk.MustExec(createTableSQL)
		tk.MustExec("insert into issue52622 (k) values (1),(2),(3);")
		tk.MustQuery("select * from issue52622").Check(testkit.Rows("1 1", "67 2", "133 3"))
		if i == 0 {
			tk.MustQuery("show create table issue52622").CheckContain("134")
		}
		tk.MustExec("insert into issue52622 (k) values (4);")
		tk.MustQuery("select * from issue52622").Check(testkit.Rows("1 1", "67 2", "133 3", "199 4"))

		tk.MustExec("truncate table issue52622;")
		tk.MustExec("insert into issue52622 (k) values (1)")
		tk.MustExec("insert into issue52622 (k) values (2)")
		tk.MustExec("insert into issue52622 (k) values (3)")
		if i == 0 {
			tk.MustQuery("show create table issue52622").CheckContain("134")
		}
		tk.MustExec("insert into issue52622 (k) values (4);")
		tk.MustQuery("select * from issue52622").Check(testkit.Rows("1 1", "67 2", "133 3", "199 4"))

		tk.MustExec("drop table issue52622;")
	}
}
