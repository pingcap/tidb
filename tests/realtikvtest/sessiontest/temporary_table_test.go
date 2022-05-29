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

package sessiontest

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestLocalTemporaryTableInsert(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 (u, v) values(11, 101)")
	tk.MustExec("insert into tmp1 (u, v) values(12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 102)")

	checkRecordOneTwoThreeAndNonExist := func() {
		tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
		tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
		tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 102"))
		tk.MustQuery("select * from tmp1 where id=99").Check(testkit.Rows())
	}

	// inserted records exist
	checkRecordOneTwoThreeAndNonExist()

	// insert dup records out txn must be error
	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(1, 999, 9999)")))
	checkRecordOneTwoThreeAndNonExist()
	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(99, 11, 999)")))
	checkRecordOneTwoThreeAndNonExist()

	// insert dup records in txn must be error
	tk.MustExec("begin")

	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(1, 999, 9999)")))
	checkRecordOneTwoThreeAndNonExist()
	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(99, 11, 9999)")))
	checkRecordOneTwoThreeAndNonExist()

	tk.MustExec("insert into tmp1 values(4, 14, 104)")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(4, 999, 9999)")))
	require.True(t, kv.ErrKeyExists.Equal(tk.ExecToErr("insert into tmp1 values(99, 14, 9999)")))

	checkRecordOneTwoThreeAndNonExist()
	tk.MustExec("commit")

	// check committed insert works
	checkRecordOneTwoThreeAndNonExist()
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	// check rollback works
	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values(5, 15, 105)")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows())
}

func TestLocalTemporaryTableUpdate(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key, u int unique, v int)")

	idList := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	insertRecords := func(idList []int) {
		for _, id := range idList {
			tk.MustExec("insert into tmp1 values (?, ?, ?)", id, id+100, id+1000)
		}
	}

	checkNoChange := func() {
		expect := make([]string, 0)
		for _, id := range idList {
			expect = append(expect, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
		}
		tk.MustQuery("select * from tmp1").Check(testkit.Rows(expect...))
	}

	checkUpdatesAndDeletes := func(updates []string, deletes []int) {
		modifyMap := make(map[int]string)
		for _, m := range updates {
			parts := strings.Split(strings.TrimSpace(m), " ")
			require.NotZero(t, len(parts))
			id, err := strconv.Atoi(parts[0])
			require.NoError(t, err)
			modifyMap[id] = m
		}

		for _, d := range deletes {
			modifyMap[d] = ""
		}

		expect := make([]string, 0)
		for _, id := range idList {
			modify, exist := modifyMap[id]
			if !exist {
				expect = append(expect, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
				continue
			}

			if modify != "" {
				expect = append(expect, modify)
			}

			delete(modifyMap, id)
		}

		otherIds := make([]int, 0)
		for id := range modifyMap {
			otherIds = append(otherIds, id)
		}

		sort.Ints(otherIds)
		for _, id := range otherIds {
			modify, exist := modifyMap[id]
			require.True(t, exist)
			expect = append(expect, modify)
		}

		tk.MustQuery("select * from tmp1").Check(testkit.Rows(expect...))
	}

	type checkSuccess struct {
		update []string
		delete []int
	}

	type checkError struct {
		err error
	}

	cases := []struct {
		sql             string
		checkResult     interface{}
		additionalCheck func(error)
	}{
		// update with point get for primary key
		{"update tmp1 set v=999 where id=1", checkSuccess{[]string{"1 101 999"}, nil}, nil},
		{"update tmp1 set id=12 where id=1", checkSuccess{[]string{"12 101 1001"}, []int{1}}, nil},
		{"update tmp1 set id=1 where id=1", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=101 where id=1", checkSuccess{nil, nil}, nil},
		{"update tmp1 set v=999 where id=100", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=102 where id=100", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=21 where id=1", checkSuccess{[]string{"1 21 1001"}, nil}, func(_ error) {
			// check index deleted
			tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u=101").Check(testkit.Rows())
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}},
		{"update tmp1 set id=2 where id=1", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=102 where id=1", checkError{kv.ErrKeyExists}, nil},
		// update with batch point get for primary key
		{"update tmp1 set v=v+1000 where id in (1, 3, 5)", checkSuccess{[]string{"1 101 2001", "3 103 2003", "5 105 2005"}, nil}, nil},
		{"update tmp1 set u=u+1 where id in (9, 100)", checkSuccess{[]string{"9 110 1009"}, nil}, nil},
		{"update tmp1 set u=101 where id in (100, 101)", checkSuccess{nil, nil}, nil},
		{"update tmp1 set id=id+1 where id in (8, 9)", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=u+1 where id in (8, 9)", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set id=id+20 where id in (1, 3, 5)", checkSuccess{[]string{"21 101 1001", "23 103 1003", "25 105 1005"}, []int{1, 3, 5}}, nil},
		{"update tmp1 set u=u+100 where id in (1, 3, 5)", checkSuccess{[]string{"1 201 1001", "3 203 1003", "5 205 1005"}, nil}, func(_ error) {
			// check index deleted
			tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u in (101, 103, 105)").Check(testkit.Rows())
			tk.MustQuery("show warnings").Check(testkit.Rows())
		}},
		// update with point get for unique key
		{"update tmp1 set v=888 where u=101", checkSuccess{[]string{"1 101 888"}, nil}, nil},
		{"update tmp1 set id=21 where u=101", checkSuccess{[]string{"21 101 1001"}, []int{1}}, nil},
		{"update tmp1 set v=888 where u=201", checkSuccess{nil, nil}, nil},
		{"update tmp1 set u=201 where u=101", checkSuccess{[]string{"1 201 1001"}, nil}, nil},
		{"update tmp1 set id=2 where u=101", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=102 where u=101", checkError{kv.ErrKeyExists}, nil},
		// update with batch point get for unique key
		{"update tmp1 set v=v+1000 where u in (101, 103)", checkSuccess{[]string{"1 101 2001", "3 103 2003"}, nil}, nil},
		{"update tmp1 set v=v+1000 where u in (201, 203)", checkSuccess{nil, nil}, nil},
		{"update tmp1 set v=v+1000 where u in (101, 110)", checkSuccess{[]string{"1 101 2001"}, nil}, nil},
		{"update tmp1 set id=id+1 where u in (108, 109)", checkError{kv.ErrKeyExists}, nil},
		// update with table scan and index scan
		{"update tmp1 set v=v+1000 where id<3", checkSuccess{[]string{"1 101 2001", "2 102 2002"}, nil}, nil},
		{"update /*+ use_index(tmp1, u) */ tmp1 set v=v+1000 where u>107", checkSuccess{[]string{"8 108 2008", "9 109 2009"}, nil}, nil},
		{"update tmp1 set v=v+1000 where v>=1007 or v<=1002", checkSuccess{[]string{"1 101 2001", "2 102 2002", "7 107 2007", "8 108 2008", "9 109 2009"}, nil}, nil},
		{"update tmp1 set v=v+1000 where id>=10", checkSuccess{nil, nil}, nil},
		{"update tmp1 set id=id+1 where id>7", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set id=id+1 where id>8", checkSuccess{[]string{"10 109 1009"}, []int{9}}, nil},
		{"update tmp1 set u=u+1 where u>107", checkError{kv.ErrKeyExists}, nil},
		{"update tmp1 set u=u+1 where u>108", checkSuccess{[]string{"9 110 1009"}, nil}, nil},
		{"update /*+ use_index(tmp1, u) */ tmp1 set v=v+1000 where u>108 or u<102", checkSuccess{[]string{"1 101 2001", "9 109 2009"}, nil}, nil},
	}

	executeSQL := func(sql string, checkResult interface{}, additionalCheck func(error)) (err error) {
		switch check := checkResult.(type) {
		case checkSuccess:
			tk.MustExec(sql)
			tk.MustQuery("show warnings").Check(testkit.Rows())
			checkUpdatesAndDeletes(check.update, check.delete)
		case checkError:
			err = tk.ExecToErr(sql)
			require.Error(t, err)
			expectedErr, _ := check.err.(*terror.Error)
			require.True(t, expectedErr.Equal(err))
			checkNoChange()
		default:
			t.Fail()
		}

		if additionalCheck != nil {
			additionalCheck(err)
		}
		return
	}

	for _, sqlCase := range cases {
		// update records in txn and records are inserted in txn
		tk.MustExec("begin")
		insertRecords(idList)
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("rollback")
		tk.MustQuery("select * from tmp1").Check(testkit.Rows())

		// update records out of txn
		insertRecords(idList)
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("delete from tmp1")

		// update records in txn and rollback
		insertRecords(idList)
		tk.MustExec("begin")
		_ = executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("rollback")
		// rollback left records unmodified
		checkNoChange()

		// update records in txn and commit
		tk.MustExec("begin")
		err := executeSQL(sqlCase.sql, sqlCase.checkResult, sqlCase.additionalCheck)
		tk.MustExec("commit")
		if err != nil {
			checkNoChange()
		} else {
			r, _ := sqlCase.checkResult.(checkSuccess)
			checkUpdatesAndDeletes(r.update, r.delete)
		}
		if sqlCase.additionalCheck != nil {
			sqlCase.additionalCheck(err)
		}
		tk.MustExec("delete from tmp1")
		tk.MustQuery("select * from tmp1").Check(testkit.Rows())
	}
}

func TestTemporaryTableInterceptor(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create temporary table test.tmp1 (id int primary key)")
	tbl, err := tk.Session().GetInfoSchema().(infoschema.InfoSchema).TableByName(model.NewCIStr("test"), model.NewCIStr("tmp1"))
	require.NoError(t, err)
	require.Equal(t, model.TempTableLocal, tbl.Meta().TempTableType)
	tblID := tbl.Meta().ID

	// prepare a kv pair for temporary table
	k := append(tablecodec.EncodeTablePrefix(tblID), 1)
	require.NoError(t, tk.Session().GetSessionVars().TemporaryTableData.SetTableKey(tblID, k, []byte("v1")))

	initTxnFuncs := []func() error{
		func() error {
			tk.Session().PrepareTSFuture(context.Background())
			return nil
		},
		func() error {
			return sessiontxn.NewTxn(context.Background(), tk.Session())
		},
		func() error {
			return tk.Session().NewStaleTxnWithStartTS(context.Background(), 0)
		},
		func() error {
			return tk.Session().InitTxnWithStartTS(0)
		},
	}

	for _, initFunc := range initTxnFuncs {
		require.NoError(t, initFunc())

		txn, err := tk.Session().Txn(true)
		require.NoError(t, err)

		val, err := txn.Get(context.Background(), k)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), val)

		val, err = txn.GetSnapshot().Get(context.Background(), k)
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), val)

		tk.Session().RollbackTxn(context.Background())
	}

	// Also check GetSnapshotWithTS
	snap := tk.Session().GetSnapshotWithTS(0)
	val, err := snap.Get(context.Background(), k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

func TestTemporaryTableSize(t *testing.T) {
	// Test the @@tidb_tmp_table_max_size system variable.

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create global temporary table t (c1 int, c2 mediumtext) on commit delete rows")
	tk.MustExec("create temporary table tl (c1 int, c2 mediumtext)")

	tk.MustQuery("select @@global.tidb_tmp_table_max_size").Check(testkit.Rows(strconv.Itoa(variable.DefTiDBTmpTableMaxSize)))
	require.Equal(t, int64(variable.DefTiDBTmpTableMaxSize), tk.Session().GetSessionVars().TMPTableSize)

	// Min value 1M, so the result is change to 1M, with a warning.
	tk.MustExec("set @@global.tidb_tmp_table_max_size = 123")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tmp_table_max_size value: '123'"))

	// Change the session scope value to 2M.
	tk.MustExec("set @@session.tidb_tmp_table_max_size = 2097152")
	require.Equal(t, int64(2097152), tk.Session().GetSessionVars().TMPTableSize)

	// Check in another session, change session scope value does not affect the global scope.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery("select @@global.tidb_tmp_table_max_size").Check(testkit.Rows(strconv.Itoa(1 << 20)))

	// The value is now 1M, check the error when table size exceed it.
	tk.MustExec(fmt.Sprintf("set @@session.tidb_tmp_table_max_size = %d", 1<<20))
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, repeat('x', 512*1024))")
	tk.MustExec("insert into t values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into t values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")

	// Check local temporary table
	tk.MustExec("begin")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into tl values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")

	// Check local temporary table with some data in session
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustExec("begin")
	tk.MustExec("insert into tl values (1, repeat('x', 512*1024))")
	tk.MustGetErrCode("insert into tl values (1, repeat('x', 512*1024))", errno.ErrRecordFileFull)
	tk.MustExec("rollback")
}

func TestGlobalTemporaryTable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create global temporary table g_tmp (a int primary key, b int, c int, index i_b(b)) on commit delete rows")
	tk.MustExec("begin")
	tk.MustExec("insert into g_tmp values (3, 3, 3)")
	tk.MustExec("insert into g_tmp values (4, 7, 9)")

	// Cover table scan.
	tk.MustQuery("select * from g_tmp").Check(testkit.Rows("3 3 3", "4 7 9"))
	// Cover index reader.
	tk.MustQuery("select b from g_tmp where b > 3").Check(testkit.Rows("7"))
	// Cover index lookup.
	tk.MustQuery("select c from g_tmp where b = 3").Check(testkit.Rows("3"))
	// Cover point get.
	tk.MustQuery("select * from g_tmp where a = 3").Check(testkit.Rows("3 3 3"))
	// Cover batch point get.
	tk.MustQuery("select * from g_tmp where a in (2,3,4)").Check(testkit.Rows("3 3 3", "4 7 9"))
	tk.MustExec("commit")

	// The global temporary table data is discard after the transaction commit.
	tk.MustQuery("select * from g_tmp").Check(testkit.Rows())
}

func TestRetryGlobalTemporaryTable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists normal_table")
	tk.MustExec("create table normal_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists normal_table")
	tk.MustExec("drop table if exists temp_table")
	tk.MustExec("create global temporary table temp_table(a int primary key, b int) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_table")

	// insert select
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("insert normal_table value(100, 100)")
	tk.MustExec("set @@autocommit = 0")
	// used to make conflicts
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert temp_table value(1, 1)")
	tk.MustExec("insert normal_table select * from temp_table")
	require.Equal(t, 3, session.GetHistory(tk.Session()).Count())

	// try to conflict with tk
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 1", "100 102"))
	tk.MustQuery("select a, b from temp_table order by a").Check(testkit.Rows())

	// update multi-tables
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert temp_table value(1, 2)")
	// before update: normal_table=(1 1) (100 102), temp_table=(1 2)
	tk.MustExec("update normal_table, temp_table set normal_table.b=temp_table.b where normal_table.a=temp_table.a")
	require.Equal(t, 3, session.GetHistory(tk.Session()).Count())

	// try to conflict with tk
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 104"))
}

func TestRetryLocalTemporaryTable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists normal_table")
	tk.MustExec("create table normal_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists normal_table")
	tk.MustExec("drop table if exists temp_table")
	tk.MustExec("create temporary table l_temp_table(a int primary key, b int)")
	defer tk.MustExec("drop table if exists l_temp_table")

	// insert select
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("insert normal_table value(100, 100)")
	tk.MustExec("set @@autocommit = 0")
	// used to make conflicts
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert l_temp_table value(1, 2)")
	tk.MustExec("insert normal_table select * from l_temp_table")
	require.Equal(t, 3, session.GetHistory(tk.Session()).Count())

	// try to conflict with tk
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 102"))
	tk.MustQuery("select a, b from l_temp_table order by a").Check(testkit.Rows("1 2"))

	// update multi-tables
	tk.MustExec("update normal_table set b=b+1 where a=100")
	tk.MustExec("insert l_temp_table value(3, 4)")
	// before update: normal_table=(1 1) (100 102), temp_table=(1 2)
	tk.MustExec("update normal_table, l_temp_table set normal_table.b=l_temp_table.b where normal_table.a=l_temp_table.a")
	require.Equal(t, 3, session.GetHistory(tk.Session()).Count())

	// try to conflict with tk
	tk1.MustExec("update normal_table set b=b+1 where a=100")

	// It will retry internally.
	tk.MustExec("commit")
	tk.MustQuery("select a, b from normal_table order by a").Check(testkit.Rows("1 2", "100 104"))
}
