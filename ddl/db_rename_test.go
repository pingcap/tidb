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

package ddl_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestRenameIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (pk int primary key, c int default 1, c1 int default 1, unique key k1(c), key k2(c1))")

	// Test rename success
	tk.MustExec("alter table t rename index k1 to k3")
	tk.MustExec("admin check index t k3")

	// Test rename to the same name
	tk.MustExec("alter table t rename index k3 to k3")
	tk.MustExec("admin check index t k3")

	// Test rename on non-exists keys
	tk.MustGetErrCode("alter table t rename index x to x", errno.ErrKeyDoesNotExist)

	// Test rename on already-exists keys
	tk.MustGetErrCode("alter table t rename index k3 to k2", errno.ErrDupKeyName)

	tk.MustExec("alter table t rename index k2 to K2")
	tk.MustGetErrCode("alter table t rename key k3 to K2", errno.ErrDupKeyName)
}

// See issue: https://github.com/pingcap/tidb/issues/29752
// Ref https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
func TestRenameTableWithLocked(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database renamedb")
	tk.MustExec("create database renamedb2")
	tk.MustExec("use renamedb")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (a int);")

	tk.MustExec("LOCK TABLES t1 WRITE;")
	tk.MustGetErrCode("drop database renamedb2;", errno.ErrLockOrActiveTransaction)
	tk.MustExec("RENAME TABLE t1 TO t2;")
	tk.MustQuery("select * from renamedb.t2").Check(testkit.Rows())
	tk.MustExec("UNLOCK TABLES")
	tk.MustExec("RENAME TABLE t2 TO t1;")
	tk.MustQuery("select * from renamedb.t1").Check(testkit.Rows())

	tk.MustExec("LOCK TABLES t1 READ;")
	tk.MustGetErrCode("RENAME TABLE t1 TO t2;", errno.ErrTableNotLockedForWrite)
	tk.MustExec("UNLOCK TABLES")

	tk.MustExec("drop database renamedb")
}

func TestRenameTable2(t *testing.T) {
	isAlterTable := false
	renameTableTest(t, "rename table %s to %s", isAlterTable)
}

func TestAlterTableRenameTable(t *testing.T) {
	isAlterTable := true
	renameTableTest(t, "alter table %s rename to %s", isAlterTable)
}

func renameTableTest(t *testing.T, sql string, isAlterTable bool) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("rename table tb1 to tb2;", errno.ErrNoSuchTable)
	// for different databases
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	oldTblID := oldTblInfo.Meta().ID
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "test.t", "test1.t1"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID, newTblInfo.Meta().ID)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustExec("use test")

	// Make sure t doesn't exist.
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("drop table t")

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "t1", "t2"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID, newTblInfo.Meta().ID)
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	require.False(t, isExist)
	tk.MustQuery("show tables").Check(testkit.Rows("t2"))

	// for failure case
	failSQL := fmt.Sprintf(sql, "test_not_exist.t", "test_not_exist.t")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test.test_not_exist", "test.test_not_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test_not_exist.t")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}
	failSQL = fmt.Sprintf(sql, "test1.t2", "test_not_exist.t")
	tk.MustGetErrCode(failSQL, errno.ErrErrorOnRename)

	tk.MustExec("use test1")
	tk.MustExec("create table if not exists t_exist (c1 int, c2 int)")
	failSQL = fmt.Sprintf(sql, "test1.t2", "test1.t_exist")
	tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test1.t_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	}
	failSQL = fmt.Sprintf(sql, "test_not_exist.t", "test1.t_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrTableExists)
	}
	failSQL = fmt.Sprintf(sql, "test_not_exist.t", "test1.t_not_exist")
	if isAlterTable {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	} else {
		tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	}

	// for the same table name
	tk.MustExec("use test1")
	tk.MustExec("create table if not exists t (c1 int, c2 int)")
	tk.MustExec("create table if not exists t1 (c1 int, c2 int)")
	if isAlterTable {
		tk.MustExec(fmt.Sprintf(sql, "test1.t", "t"))
		tk.MustExec(fmt.Sprintf(sql, "test1.t1", "test1.T1"))
	} else {
		tk.MustGetErrCode(fmt.Sprintf(sql, "test1.t", "t"), errno.ErrTableExists)
		tk.MustGetErrCode(fmt.Sprintf(sql, "test1.t1", "test1.T1"), errno.ErrTableExists)
	}

	// Test rename table name too long.
	tk.MustGetErrCode("rename table test1.t1 to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)
	tk.MustGetErrCode("alter  table test1.t1 rename to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)

	tk.MustExec("drop database test1")
}

func TestRenameMultiTables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	sql := "rename table t1 to t3, t2 to t4"
	_, err := tk.Exec(sql)
	require.NoError(t, err)

	tk.MustExec("drop table t3, t4")

	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("insert t2 values (1, 1), (2, 2)")
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	oldTblID1 := oldTblInfo1.Meta().ID
	oldTblInfo2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	oldTblID2 := oldTblInfo2.Meta().ID
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec("rename table test.t1 to test1.t1, test.t2 to test1.t2")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))

	// Make sure t1,t2 doesn't exist.
	isExist := is.TableExists(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.False(t, isExist)
	isExist = is.TableExists(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.False(t, isExist)

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec("rename table test1.t1 to test1.t3, test1.t2 to test1.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t4"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t1"))
	require.False(t, isExist)
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(model.NewCIStr("test1"), model.NewCIStr("t2"))
	require.False(t, isExist)
	tk.MustQuery("show tables").Check(testkit.Rows("t3", "t4"))

	// for multi tables same database
	tk.MustExec("create table t5 (c1 int, c2 int)")
	tk.MustExec("insert t5 values (1, 1), (2, 2)")
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo3, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t5"))
	require.NoError(t, err)
	oldTblID3 := oldTblInfo3.Meta().ID
	tk.MustExec("rename table test1.t3 to test1.t1, test1.t4 to test1.t2, test1.t5 to test1.t3")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	newTblInfo3, err := is.TableByName(model.NewCIStr("test1"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID3, newTblInfo3.Meta().ID)
	tk.MustQuery("show tables").Check(testkit.Rows("t1", "t2", "t3"))

	// for multi tables different databases
	tk.MustExec("use test")
	tk.MustExec("rename table test1.t1 to test.t2, test1.t2 to test.t3, test1.t3 to test.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	newTblInfo3, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	require.Equal(t, oldTblID3, newTblInfo3.Meta().ID)
	tk.MustQuery("show tables").Check(testkit.Rows("t2", "t3", "t4"))

	// for failure case
	failSQL := "rename table test_not_exist.t to test_not_exist.t, test_not_exist.t to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test.test_not_exist to test.test_not_exist, test.test_not_exist to test.test_not_exist"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test.t_not_exist to test_not_exist.t, test.t_not_exist to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = "rename table test1.t2 to test_not_exist.t, test1.t2 to test_not_exist.t"
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)

	tk.MustExec("drop database test1")
	tk.MustExec("drop database test")
}
