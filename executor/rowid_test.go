// Copyright 2018 PingCAP, Inc.
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

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestExportRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().AllowWriteRowID = true
	defer func() {
		tk.Session().GetSessionVars().AllowWriteRowID = false
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (1, 7), (1, 8), (1, 9)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 8 2", "1 9 3"))
	tk.MustExec("update t set a = 2 where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 8 2", "1 9 3"))

	tk.MustExec("delete from t where _tidb_rowid = 2")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "1 9 3"))

	tk.MustExec("insert t (a, b, _tidb_rowid) values (2, 2, 2), (5, 5, 5)")
	tk.MustQuery("select *, _tidb_rowid from t").
		Check(testkit.Rows("1 7 1", "2 2 2", "1 9 3", "5 5 5"))

	// If PK is handle, _tidb_rowid is unknown column.
	tk.MustExec("create table s (a int primary key)")
	tk.MustExec("insert s values (1)")
	_, err := tk.Exec("insert s (a, _tidb_rowid) values (1, 2)")
	require.Error(t, err)
	err = tk.ExecToErr("select _tidb_rowid from s")
	require.Error(t, err)
	_, err = tk.Exec("update s set a = 2 where _tidb_rowid = 1")
	require.Error(t, err)
	_, err = tk.Exec("delete from s where _tidb_rowid = 1")
	require.Error(t, err)

	// Make sure "AllowWriteRowID" is a session variable.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	_, err = tk1.Exec("insert into t (a, _tidb_rowid) values(10, 1);")
	require.EqualError(t, err, "insert, update and replace statements for _tidb_rowid are not supported")
}

func TestNotAllowWriteRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table tt(id binary(10), c int, primary key(id));")
	tk.MustExec("insert tt values (1, 10);")
	// select statement
	tk.MustQuery("select *, _tidb_rowid from tt").
		Check(testkit.Rows("1\x00\x00\x00\x00\x00\x00\x00\x00\x00 10 1"))
	// insert statement
	_, err := tk.Exec("insert into tt (id, c, _tidb_rowid) values(30000,10,1);")
	require.EqualError(t, err, "insert, update and replace statements for _tidb_rowid are not supported")
	// replace statement
	_, err = tk.Exec("replace into tt (id, c, _tidb_rowid) values(30000,10,1);")
	require.EqualError(t, err, "insert, update and replace statements for _tidb_rowid are not supported")
	// update statement
	_, err = tk.Exec("update tt set id = 2, _tidb_rowid = 1 where _tidb_rowid = 1")
	require.EqualError(t, err, "insert, update and replace statements for _tidb_rowid are not supported")
	tk.MustExec("update tt set id = 2 where _tidb_rowid = 1")
	tk.MustExec("admin check table tt;")
	tk.MustExec("drop table tt")
	// There is currently no real support for inserting, updating, and replacing _tidb_rowid statements.
	// After we support it, the following operations must be passed.
	//	tk.MustExec("insert into tt (id, c, _tidb_rowid) values(30000,10,1);")
	//	tk.MustExec("admin check table tt;")
}

// Test for https://github.com/pingcap/tidb/issues/22029.
func TestExplicitInsertRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_explicit_insert_rowid;")
	tk.MustExec("create database test_explicit_insert_rowid;")
	tk.MustExec("use test_explicit_insert_rowid;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("set @@tidb_opt_write_row_id = true;")

	// Check that _tidb_rowid insertion success.
	tk.MustExec("insert into t (_tidb_rowid, a) values (1, 1), (2, 2);")
	tk.MustQuery("select *, _tidb_rowid from t;").Check(testkit.Rows("1 1", "2 2"))
	// Test that explicitly insert _tidb_rowid will trigger rebase.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id varchar(10), c int);")
	tk.MustExec("insert t values('one', 101), ('two', 102);")
	tk.MustQuery("select *, _tidb_rowid from t;").
		Check(testkit.Rows("one 101 1", "two 102 2"))
	// check that _tidb_rowid takes the values from the insert statement
	tk.MustExec("insert t (id, c, _tidb_rowid) values ('three', 103, 9), ('four', 104, 16), ('five', 105, 5);")
	tk.MustQuery("select *, _tidb_rowid from t where c > 102;").
		Check(testkit.Rows("five 105 5", "three 103 9", "four 104 16"))
	// check that the new _tidb_rowid is rebased
	tk.MustExec("insert t values ('six', 106), ('seven', 107);")
	tk.MustQuery("select *, _tidb_rowid from t where c > 105;").
		Check(testkit.Rows("six 106 17", "seven 107 18"))

	// Check that shard_row_id_bits are ignored during rebase.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int) shard_row_id_bits = 5;")
	tk.MustExec("insert into t values (1);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into t (a, _tidb_rowid) values (2, (1<<62)+5);")
	tk.MustExec("insert into t values (3);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("1 1", "2 5", "3 6"))
	tk.MustExec("insert into t (a, _tidb_rowid) values (4, null);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("1 1", "2 5", "3 6", "4 7"))

	tk.MustExec("delete from t;")
	tk.MustExec("SET sql_mode=(SELECT CONCAT(@@sql_mode,',NO_AUTO_VALUE_ON_ZERO'));")
	tk.MustExec("insert into t (a, _tidb_rowid) values (5, 0);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("5 0"))
	tk.MustExec("SET sql_mode=(SELECT REPLACE(@@sql_mode,'NO_AUTO_VALUE_ON_ZERO',''));")
	tk.MustExec("insert into t (a, _tidb_rowid) values (6, 0);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("5 0", "6 8"))
	tk.MustExec("insert into t (_tidb_rowid, a) values (0, 7);")
	tk.MustQuery("select *, ((1 << (64-5-1)) - 1) & _tidb_rowid from t order by a;").Check(testkit.Rows("5 0", "6 8", "7 9"))
}
