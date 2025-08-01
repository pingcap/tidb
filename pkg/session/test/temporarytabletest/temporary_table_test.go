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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
		checkResult     any
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

	executeSQL := func(sql string, checkResult any, additionalCheck func(error)) (err error) {
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
