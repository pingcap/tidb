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

package temporarytabletest

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestLocalTemporaryTableInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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

func TestTemporaryTableSize(t *testing.T) {
	// Test the @@tidb_tmp_table_max_size system variable.

	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
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

func TestLocalTemporaryTableInsertOnDuplicateKeyUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'tmp1.u'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=202")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 202"))
	tk.MustExec("insert into tmp1 values(3, 13, 103) on duplicate key update v=203")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))

	// test in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'tmp1.u'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=302")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 302"))
	tk.MustExec("insert into tmp1 values(4, 14, 104) on duplicate key update v=204")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 202", "3 13 103"))

	// test commit
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'tmp1.u'"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=302")
	tk.MustExec("insert into tmp1 values(4, 14, 104) on duplicate key update v=204")
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 302", "3 13 103", "4 14 104"))
}

func TestLocalTemporaryTableReplace(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 103)")

	// out of transaction
	tk.MustExec("replace into tmp1 values(1, 12, 1000)")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 12 1000", "3 13 103"))
	tk.MustExec("replace into tmp1 values(4, 14, 104)")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows("4 14 104"))

	// in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("replace into tmp1 values(1, 13, 999)")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 13 999", "4 14 104"))
	tk.MustExec("replace into tmp1 values(5, 15, 105)")
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 12 1000", "3 13 103", "4 14 104"))

	// out of transaction
	tk.MustExec("begin")
	tk.MustExec("replace into tmp1 values(1, 13, 999)")
	tk.MustExec("replace into tmp1 values(5, 15, 105)")
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 13 999", "4 14 104", "5 15 105"))
}

func TestLocalTemporaryTableDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create temporary table tmp1 (id int primary key, u int unique, v int)")

	insertRecords := func(idList []int) {
		for _, id := range idList {
			tk.MustExec("insert into tmp1 values (?, ?, ?)", id, id+100, id+1000)
		}
	}

	checkAllExistRecords := func(idList []int) {
		sort.Ints(idList)
		expectedResult := make([]string, 0, len(idList))
		expectedIndexResult := make([]string, 0, len(idList))
		for _, id := range idList {
			expectedResult = append(expectedResult, fmt.Sprintf("%d %d %d", id, id+100, id+1000))
			expectedIndexResult = append(expectedIndexResult, fmt.Sprintf("%d", id+100))
		}
		tk.MustQuery("select * from tmp1 order by id").Check(testkit.Rows(expectedResult...))

		// check index deleted
		tk.MustQuery("select /*+ use_index(tmp1, u) */ u from tmp1 order by u").Check(testkit.Rows(expectedIndexResult...))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}

	assertDelete := func(sql string, deleted []int) {
		idList := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

		deletedMap := make(map[int]bool)
		for _, id := range deleted {
			deletedMap[id] = true
		}

		keepList := make([]int, 0)
		for _, id := range idList {
			if _, exist := deletedMap[id]; !exist {
				keepList = append(keepList, id)
			}
		}

		// delete records in txn and records are inserted in txn
		tk.MustExec("begin")
		insertRecords(idList)
		tk.MustExec(sql)
		tk.MustQuery("show warnings").Check(testkit.Rows())
		checkAllExistRecords(keepList)
		tk.MustExec("rollback")
		checkAllExistRecords([]int{})

		// delete records out of txn
		insertRecords(idList)
		tk.MustExec(sql)
		checkAllExistRecords(keepList)

		// delete records in txn
		insertRecords(deleted)
		tk.MustExec("begin")
		tk.MustExec(sql)
		checkAllExistRecords(keepList)

		// test rollback
		tk.MustExec("rollback")
		checkAllExistRecords(idList)

		// test commit
		tk.MustExec("begin")
		tk.MustExec(sql)
		tk.MustExec("commit")
		checkAllExistRecords(keepList)

		tk.MustExec("delete from tmp1")
		checkAllExistRecords([]int{})
	}

	assertDelete("delete from tmp1 where id=1", []int{1})
	assertDelete("delete from tmp1 where id in (1, 3, 5)", []int{1, 3, 5})
	assertDelete("delete from tmp1 where u=102", []int{2})
	assertDelete("delete from tmp1 where u in (103, 107, 108)", []int{3, 7, 8})
	assertDelete("delete from tmp1 where id=10", []int{})
	assertDelete("delete from tmp1 where id in (10, 12)", []int{})
	assertDelete("delete from tmp1 where u=110", []int{})
	assertDelete("delete from tmp1 where u in (111, 112)", []int{})
	assertDelete("delete from tmp1 where id in (1, 11, 5)", []int{1, 5})
	assertDelete("delete from tmp1 where u in (102, 121, 106)", []int{2, 6})
	assertDelete("delete from tmp1 where id<3", []int{1, 2})
	assertDelete("delete from tmp1 where u>107", []int{8, 9})
	assertDelete("delete /*+ use_index(tmp1, u) */ from tmp1 where u>105 and u<107", []int{6})
	assertDelete("delete from tmp1 where v>=1006 or v<=1002", []int{1, 2, 6, 7, 8, 9})
}

func TestLocalTemporaryTablePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(4, 14, 104)")

	// check point get out transaction
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from tmp1 where u=12").Check(testkit.Rows("2 12 102"))

	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u=11").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 102"))
	tk.MustQuery("select * from tmp1 where u=12").Check(testkit.Rows("2 12 102"))
	tk.MustExec("insert into tmp1 values(3, 13, 103)")
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustExec("update tmp1 set v=999 where id=2")
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 999"))
	tk.MustExec("delete from tmp1 where id=4")
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where u=14").Check(testkit.Rows())
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where u=13").Check(testkit.Rows("3 13 103"))
	tk.MustQuery("select * from tmp1 where id=2").Check(testkit.Rows("2 12 999"))
	tk.MustQuery("select * from tmp1 where id=4").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where u=14").Check(testkit.Rows())
}

func TestLocalTemporaryTableBatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")
	tk.MustExec("insert into tmp1 values(3, 13, 103)")
	tk.MustExec("insert into tmp1 values(4, 14, 104)")

	// check point get out transaction
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))

	// check point get in transaction
	tk.MustExec("begin")
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where id in (1, 3, 5)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 15)").Check(testkit.Rows("1 11 101", "3 13 103"))
	tk.MustExec("insert into tmp1 values(6, 16, 106)")
	tk.MustQuery("select * from tmp1 where id in (1, 6)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustQuery("select * from tmp1 where u in (11, 16)").Check(testkit.Rows("1 11 101", "6 16 106"))
	tk.MustExec("update tmp1 set v=999 where id=3")
	tk.MustQuery("select * from tmp1 where id in (1, 3)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustQuery("select * from tmp1 where u in (11, 13)").Check(testkit.Rows("1 11 101", "3 13 999"))
	tk.MustExec("delete from tmp1 where id=4")
	tk.MustQuery("select * from tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
	tk.MustExec("commit")

	// check point get after transaction
	tk.MustQuery("select * from tmp1 where id in (1, 3, 6)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from tmp1 where u in (11, 13, 16)").Check(testkit.Rows("1 11 101", "3 13 999", "6 16 106"))
	tk.MustQuery("select * from tmp1 where id in (1, 4)").Check(testkit.Rows("1 11 101"))
	tk.MustQuery("select * from tmp1 where u in (11, 14)").Check(testkit.Rows("1 11 101"))
}

func TestLocalTemporaryTableScan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values" +
		"(1, 101, 1001), (3, 113, 1003), (5, 105, 1005), (7, 117, 1007), (9, 109, 1009)," +
		"(10, 110, 1010), (12, 112, 1012), (14, 114, 1014), (16, 116, 1016), (18, 118, 1018)",
	)

	assertSelectAsUnModified := func() {
		// For TableReader
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"5 105 1005", "7 117 1007", "9 109 1009",
			"10 110 1010", "12 112 1012", "14 114 1014", "16 116 1016", "18 118 1018",
		))

		// For IndexLookUpReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"5 105 1005", "9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "5 105", "7 117", "9 109", "10 110",
			"12 112", "14 114", "16 116", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010",
			"12 112 1012", "3 113 1003", "14 114 1014", "16 116 1016", "7 117 1007", "18 118 1018",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."))
	}

	doModify := func() {
		tk.MustExec("insert into tmp1 values(2, 100, 1002)")
		tk.MustExec("insert into tmp1 values(4, 104, 1004)")
		tk.MustExec("insert into tmp1 values(11, 111, 1011)")
		tk.MustExec("update tmp1 set v=9999 where id=7")
		tk.MustExec("update tmp1 set u=132 where id=12")
		tk.MustExec("delete from tmp1 where id=16")
	}

	assertSelectAsModified := func() {
		// For TableReader
		tk.MustQuery("select * from tmp1 where id>3 order by id").Check(testkit.Rows(
			"4 104 1004", "5 105 1005", "7 117 9999", "9 109 1009",
			"10 110 1010", "11 111 1011", "12 132 1012", "14 114 1014", "18 118 1018",
		))

		// For IndexLookUpReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ * from tmp1 where u>101 order by u").Check(testkit.Rows(
			"4 104 1004", "5 105 1005", "9 109 1009", "10 110 1010", "11 111 1011",
			"3 113 1003", "14 114 1014", "7 117 9999", "18 118 1018", "12 132 1012",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexReader
		tk.MustQuery("select /*+ use_index(tmp1, u) */ id,u from tmp1 where u>101 order by id").Check(testkit.Rows(
			"3 113", "4 104", "5 105", "7 117", "9 109",
			"10 110", "11 111", "12 132", "14 114", "18 118",
		))
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// For IndexMerge, temporary table should not use index merge
		tk.MustQuery("select /*+ use_index_merge(tmp1, primary, u) */ * from tmp1 where id>5 or u>110 order by u").Check(testkit.Rows(
			"9 109 1009", "10 110 1010", "11 111 1011",
			"3 113 1003", "14 114 1014", "7 117 9999", "18 118 1018", "12 132 1012",
		))

		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."))
	}

	assertSelectAsUnModified()
	tk.MustExec("begin")
	assertSelectAsUnModified()
	doModify()
	tk.MustExec("rollback")
	assertSelectAsUnModified()
	tk.MustExec("begin")
	doModify()
	assertSelectAsModified()
	tk.MustExec("commit")
	assertSelectAsModified()
}

func TestSchemaCheckerTempTable(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk2.MustExec("use test")

	// create table
	tk1.MustExec(`drop table if exists normal_table`)
	tk1.MustExec(`create table normal_table (id int, c int);`)
	defer tk1.MustExec(`drop table if exists normal_table`)
	tk1.MustExec(`drop table if exists temp_table`)
	tk1.MustExec(`create global temporary table temp_table (id int primary key, c int) on commit delete rows;`)
	defer tk1.MustExec(`drop table if exists temp_table`)

	// The schema version is out of date in the first transaction, and the SQL can't be retried.
	atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&session.SchemaChangedWithoutRetry, 0)
	}()

	// It's fine to change the schema of temporary tables.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`alter table temp_table modify column c tinyint;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c smallint;`)
	tk1.MustExec(`insert into temp_table values(3, 4);`)
	tk1.MustQuery(`select * from temp_table for update;`).Check(testkit.Rows("3 4"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c smallint;`)
	tk1.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows("1 2"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustQuery(`select * from temp_table where id=1 for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustExec("insert into temp_table values (1, 2), (2, 3), (4, 5)")
	tk1.MustQuery(`select * from temp_table where id in (1, 2, 3) for update;`).Check(testkit.Rows("1 2", "2 3"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("insert into normal_table values(1, 2)")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table temp_table modify column c int;`)
	tk1.MustExec(`insert into temp_table values(1, 5);`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 5 1 2"))
	tk1.MustExec(`commit;`)

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table normal_table modify column c bigint;`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows())
	tk1.MustExec(`commit;`)

	// Truncate will modify table ID.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`truncate table temp_table;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`commit;`)

	// It reports error when also changing the schema of a normal table.
	tk1.MustExec(`begin;`)
	tk2.MustExec(`alter table normal_table modify column c bigint;`)
	tk1.MustExec(`insert into temp_table values(3, 3);`)
	tk1.MustExec(`insert into normal_table values(3, 3);`)
	err := tk1.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))

	tk1.MustExec("begin pessimistic")
	tk2.MustExec(`alter table normal_table modify column c int;`)
	tk1.MustExec(`insert into temp_table values(1, 6);`)
	tk1.MustQuery(`select * from temp_table, normal_table where temp_table.id = normal_table.id for update;`).Check(testkit.Rows("1 6 1 2"))
	err = tk1.ExecToErr(`commit;`)
	require.True(t, terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), fmt.Sprintf("err %v", err))
}

func TestSameNameObjectWithLocalTemporaryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop sequence if exists s1")
	tk.MustExec("drop view if exists v1")

	// prepare
	tk.MustExec("create table t1 (a int)")
	defer tk.MustExec("drop table if exists t1")
	tk.MustQuery("show create table t1").Check(testkit.Rows(
		"t1 CREATE TABLE `t1` (\n" +
			"  `a` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create view v1 as select 1")
	defer tk.MustExec("drop view if exists v1")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))
	tk.MustQuery("show create table v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))

	tk.MustExec("create sequence s1")
	defer tk.MustExec("drop sequence if exists s1")
	tk.MustQuery("show create sequence s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	tk.MustQuery("show create table s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// temp table
	tk.MustExec("create temporary table t1 (ct1 int)")
	tk.MustQuery("show create table t1").Check(testkit.Rows(
		"t1 CREATE TEMPORARY TABLE `t1` (\n" +
			"  `ct1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create temporary table v1 (cv1 int)")
	tk.MustQuery("show create view v1").Check(testkit.Rows("v1 CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v1` (`1`) AS SELECT 1 AS `1` utf8mb4 utf8mb4_bin"))
	tk.MustQuery("show create table v1").Check(testkit.Rows(
		"v1 CREATE TEMPORARY TABLE `v1` (\n" +
			"  `cv1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("create temporary table s1 (cs1 int)")
	tk.MustQuery("show create sequence s1").Check(testkit.Rows("s1 CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))
	tk.MustQuery("show create table s1").Check(testkit.Rows(
		"s1 CREATE TEMPORARY TABLE `s1` (\n" +
			"  `cs1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// drop
	tk.MustExec("drop view v1")
	tk.MustGetErrMsg("show create view v1", "[schema:1146]Table 'test.v1' doesn't exist")
	tk.MustQuery("show create table v1").Check(testkit.Rows(
		"v1 CREATE TEMPORARY TABLE `v1` (\n" +
			"  `cv1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop sequence s1")
	tk.MustGetErrMsg("show create sequence s1", "[schema:1146]Table 'test.s1' doesn't exist")
	tk.MustQuery("show create table s1").Check(testkit.Rows(
		"s1 CREATE TEMPORARY TABLE `s1` (\n" +
			"  `cs1` int(11) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestLocalTemporaryTableInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'tmp1.PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(5, 15, 105)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))

	// test in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'tmp1.PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'tmp1.PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "5 15 105"))

	// test commit
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'tmp1.PRIMARY'"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'tmp1.PRIMARY'"))
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "3 13 103", "5 15 105"))
}
