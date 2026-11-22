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
	"context"
	"fmt"
	"testing"
	gotime "time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// See issue: https://github.com/pingcap/tidb/issues/29752
// Ref https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
func TestRenameTableWithLocked(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})

	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("rename table tb1 to tb2;", errno.ErrNoSuchTable)
	// for different databases
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2)")
	ctx := tk.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	oldTblInfo, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	oldTblID := oldTblInfo.Meta().ID
	oldDBID := oldTblInfo.Meta().DBID
	require.NotEqual(t, oldDBID, 0)

	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "test.t", "test1.t1"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID, newTblInfo.Meta().ID)
	require.NotEqual(t, newTblInfo.Meta().DBID, oldDBID)
	require.NotEqual(t, newTblInfo.Meta().DBID, 0)
	oldDBID = newTblInfo.Meta().DBID
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustExec("use test")

	// Make sure t doesn't exist.
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("drop table t")

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec(fmt.Sprintf(sql, "t1", "t2"))
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID, newTblInfo.Meta().ID)
	require.Equal(t, oldDBID, newTblInfo.Meta().DBID)
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.TableExists(ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.False(t, isExist)
	tk.MustQuery("show tables").Check(testkit.Rows("t2"))

	// for failure case
	failSQL := fmt.Sprintf(sql, "test_not_exist.t", "test_not_exist.t")
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = fmt.Sprintf(sql, "test.test_not_exist", "test.test_not_exist")
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
	failSQL = fmt.Sprintf(sql, "test.t_not_exist", "test_not_exist.t")
	tk.MustGetErrCode(failSQL, errno.ErrNoSuchTable)
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
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

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
	oldTblInfo1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	oldTblID1 := oldTblInfo1.Meta().ID
	oldTblInfo2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	oldTblID2 := oldTblInfo2.Meta().ID
	tk.MustExec("create database test1")
	tk.MustExec("use test1")
	tk.MustExec("rename table test.t1 to test1.t1, test.t2 to test1.t2")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))

	// Make sure t1,t2 doesn't exist.
	isExist := is.TableExists(ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.False(t, isExist)
	isExist = is.TableExists(ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.False(t, isExist)

	// for the same database
	tk.MustExec("use test1")
	tk.MustExec("rename table test1.t1 to test1.t3, test1.t2 to test1.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t4"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.False(t, isExist)
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 1", "2 2"))
	isExist = is.TableExists(ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.False(t, isExist)
	tk.MustQuery("show tables").Check(testkit.Rows("t3", "t4"))

	// for multi tables same database
	tk.MustExec("create table t5 (c1 int, c2 int)")
	tk.MustExec("insert t5 values (1, 1), (2, 2)")
	is = domain.GetDomain(ctx).InfoSchema()
	oldTblInfo3, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t5"))
	require.NoError(t, err)
	oldTblID3 := oldTblInfo3.Meta().ID
	tk.MustExec("rename table test1.t3 to test1.t1, test1.t4 to test1.t2, test1.t5 to test1.t3")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	newTblInfo3, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID3, newTblInfo3.Meta().ID)
	tk.MustQuery("show tables").Check(testkit.Rows("t1", "t2", "t3"))

	// for multi tables different databases
	tk.MustExec("use test")
	tk.MustExec("rename table test1.t1 to test.t2, test1.t2 to test.t3, test1.t3 to test.t4")
	is = domain.GetDomain(ctx).InfoSchema()
	newTblInfo1, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	require.Equal(t, oldTblID1, newTblInfo1.Meta().ID)
	newTblInfo2, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)
	require.Equal(t, oldTblID2, newTblInfo2.Meta().ID)
	newTblInfo3, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t4"))
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

func TestRenameMultiTablesIssue47064(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("create database test1")
	tk.MustExec("rename table test.t1 to test1.t1, test.t2 to test1.t2")
	tk.MustQuery("select column_name from information_schema.columns where table_name = 't1'").Check(testkit.Rows("a"))
}

func TestRenameConcurrentAutoID(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec(`use test`)
	tk3.MustExec(`use test`)
	// Use first client session, tidb1
	tk1.MustExec(`create schema if not exists test1`)
	tk1.MustExec(`create schema if not exists test2`)
	tk1.MustExec(`drop table if exists test1.t1, test2.t2`)
	tk1.MustExec(`CREATE TABLE test1.t1 (a int auto_increment primary key nonclustered, b varchar(255), key (b)) auto_id_cache 5`)
	tk1.MustExec(`begin`)
	tk1.MustExec(`insert into test1.t1 values (null, "t1 first null")`)
	tk1.MustQuery(`select _tidb_rowid, a, b from test1.t1`).Sort().Check(testkit.Rows("2 1 t1 first null"))

	ctx := tk1.Session()
	is := domain.GetDomain(ctx).InfoSchema()
	tblInfo, err := is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, int64(0), tblInfo.Meta().AutoIDSchemaID)
	origAllocs := tblInfo.Allocators(nil)
	require.Equal(t, int64(5), origAllocs.Allocs[0].End())

	// Switch to a new client (tidb2)
	alterChan := make(chan error)
	go func() {
		// will wait for tidb1
		alterChan <- tk2.ExecToErr(`rename table test1.t1 to test2.t2`)
	}()
	waitFor := func(tableName, s string, pos int) {
		for {
			select {
			case alterErr := <-alterChan:
				require.Fail(t, "Alter completed unexpectedly", "With error %v", alterErr)
			default:
				// Alter still running
			}
			res := tk3.MustQuery(`admin show ddl jobs where table_name = '` + tableName + `' and job_type = 'rename table'`).Rows()
			if len(res) == 1 && res[0][pos] == s {
				logutil.DDLLogger().Info("Got state", zap.String("State", s))
				break
			}
			gotime.Sleep(50 * gotime.Millisecond)
		}
		// Sleep 50ms to wait load InforSchema finish, issue #46815.
		gotime.Sleep(50 * gotime.Millisecond)
	}

	// Switch to new client (tidb3)
	waitFor("t2", "public", 4)
	tk3.MustExec(`begin`)
	tk3.MustExec(`insert into test2.t2 values (null, "t2 first null")`)
	tk3.MustQuery(`select _tidb_rowid, a, b from test2.t2`).Sort().Check(testkit.Rows("4 3 t2 first null"))

	// Switch back to tidb1
	// instead of generating 30k inserts with null
	tk1.MustExec(`insert into test1.t1 values (null, "t1 second null")`)
	// Bug was that this gave:
	// ERROR 1146 (42S02): table doesn't exist
	// Due to AutoID does no-longer exists.
	tk1.MustExec(`insert into test1.t1 values (null, "t1 third null")`)
	tk1.MustExec(`commit`)
	tk3.MustExec(`insert into test2.t2 values (null, "t2 second null")`)
	tk3.MustExec(`insert into test2.t2 values (null, "t2 third null")`)
	tk3.MustExec(`commit`)
	require.NoError(t, <-alterChan)
	tk1.MustQuery(`select _tidb_rowid, a, b from test2.t2`).Sort().Check(testkit.Rows(""+
		"10 9 t2 second null",
		"12 11 t2 third null",
		"2 1 t1 first null",
		"4 3 t2 first null",
		"6 5 t1 second null",
		"8 7 t1 third null"))

	// Unit test part for checking what happens when you rename back to the old schema (it should reset the 'AutoIDSchemaID' variable)
	// and if you rename multiple time (so it does not lose the autoID).
	ctx = tk1.Session()
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(context.Background(), ast.NewCIStr("test2"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	originalSchemaID := tblInfo.Meta().AutoIDSchemaID
	require.NotEqual(t, int64(0), originalSchemaID)
	origAllocs = tblInfo.Allocators(nil)
	require.Equal(t, int64(15), origAllocs.Allocs[0].End())

	// Plan:
	// - Rename to new table name in same Schema
	// - Rename to new table name in new Schema
	// - Rename to new table name in original Schema
	// - Rename to new table name in new Schema
	// - Drop original schema (verify that it does not clean up AutoIDs or hides them!)
	// - Recreate original schema (by name) (Original Schema ID will not be used by anything else, ever!)
	// - Rename to new table name in original Schema (should keep its AutoIDSchemaID)

	tk1.MustExec(`use test`)
	tk1.MustExec(`rename table test2.t2 to test2.t1`)
	tk1.MustExec(`insert into test2.t1 values (null, "Now t1 again")`)
	tk1.MustQuery(`select _tidb_rowid, a, b from test2.t1`).Sort().Check(testkit.Rows(""+
		"10 9 t2 second null",
		"12 11 t2 third null",
		"14 13 Now t1 again",
		"2 1 t1 first null",
		"4 3 t2 first null",
		"6 5 t1 second null",
		"8 7 t1 third null"))

	ctx = tk1.Session()
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(context.Background(), ast.NewCIStr("test2"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.Equal(t, originalSchemaID, tblInfo.Meta().AutoIDSchemaID)
	origAllocs = tblInfo.Allocators(nil)
	require.Equal(t, int64(15), origAllocs.Allocs[0].End())

	tk1.MustExec(`insert into test2.t1 values (15, "Now t1, Explicit 15")`)
	tk1.MustExec(`insert into test2.t1 values (null, "Is it 17?")`)
	tk1.MustQuery(`select _tidb_rowid, a, b from test2.t1`).Sort().Check(testkit.Rows(""+
		"10 9 t2 second null",
		"12 11 t2 third null",
		"14 13 Now t1 again",
		"16 15 Now t1, Explicit 15",
		"18 17 Is it 17?",
		"2 1 t1 first null",
		"4 3 t2 first null",
		"6 5 t1 second null",
		"8 7 t1 third null"))

	tk1.MustExec(`rename table test2.t1 to test1.t1`)

	tk1.MustExec(`insert into test1.t1 values (null, "Is it 19?")`)
	tk1.MustExec(`insert into test1.t1 values (22, "Now test1, Explicit 22")`)
	tk1.MustExec(`insert into test1.t1 values (null, "Is it 24?")`)
	tk1.MustQuery(`select _tidb_rowid, a, b from test1.t1`).Sort().Check(testkit.Rows(""+
		"10 9 t2 second null",
		"12 11 t2 third null",
		"14 13 Now t1 again",
		"16 15 Now t1, Explicit 15",
		"18 17 Is it 17?",
		"2 1 t1 first null",
		"20 19 Is it 19?",
		"23 22 Now test1, Explicit 22",
		"25 24 Is it 24?",
		"4 3 t2 first null",
		"6 5 t1 second null",
		"8 7 t1 third null"))

	ctx = tk1.Session()
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	// Should be cleared when moved back to the original SchemaID
	require.Equal(t, int64(0), tblInfo.Meta().AutoIDSchemaID)

	tk1.MustExec(`rename table test1.t1 to test2.t2`)
	tk1.MustExec(`drop schema test1`)
	tk1.MustExec(`insert into test2.t2 values (30, "Now test2 again, Explicit 30")`)
	tk1.MustExec(`insert into test2.t2 values (null, "Is it 32?")`)
	tk1.MustExec(`rename table test2.t2 to test2.t1`)
	tk1.MustExec(`insert into test2.t1 values (35, "Now t1 again, Explicit 35")`)
	tk1.MustExec(`insert into test2.t1 values (null, "Is it 37?")`)
	tk1.MustExec(`create schema test1`)
	tk1.MustExec(`rename table test2.t1 to test1.t1`)

	ctx = tk1.Session()
	is = domain.GetDomain(ctx).InfoSchema()
	tblInfo, err = is.TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	require.NotEqual(t, int64(0), tblInfo.Meta().AutoIDSchemaID)
	origAllocs = tblInfo.Allocators(nil)
	require.Equal(t, int64(40), origAllocs.Allocs[0].End())

	tk1.MustExec(`insert into test1.t1 values (null, "Is it 39?")`)

	tk1.MustQuery(`select _tidb_rowid, a, b from test1.t1`).Sort().Check(testkit.Rows(""+
		"10 9 t2 second null",
		"12 11 t2 third null",
		"14 13 Now t1 again",
		"16 15 Now t1, Explicit 15",
		"18 17 Is it 17?",
		"2 1 t1 first null",
		"20 19 Is it 19?",
		"23 22 Now test1, Explicit 22",
		"25 24 Is it 24?",
		"31 30 Now test2 again, Explicit 30",
		"33 32 Is it 32?",
		"36 35 Now t1 again, Explicit 35",
		"38 37 Is it 37?",
		"4 3 t2 first null",
		"40 39 Is it 39?",
		"6 5 t1 second null",
		"8 7 t1 third null"))
}

func TestShowRunningRenameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create database test2")
	tk1.MustExec("create table t1 (a int, b int)")

	tk2 := testkit.NewTestKit(t, store)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
		if job.State != model.JobStateSynced && job.Type == model.ActionRenameTable {
			rs := tk2.MustQuery("admin show ddl jobs where state != 'synced'").Rows()
			require.Len(t, rs, 1)
			require.Equal(t, "test2", rs[0][1])
			require.Equal(t, "t2", rs[0][2])

			rs = tk2.MustQuery("select db_name, table_name from information_schema.ddl_jobs where job_type = 'rename table'").Rows()
			require.Len(t, rs, 1)
			require.Equal(t, "test2", rs[0][0])
			require.Equal(t, "t2", rs[0][1])
		}
	})

	tk1.MustExec("rename table test.t1 to test2.t2")
}

// TestRenameTableRevision covers renames that preserve table IDs but change the
// name-to-ID bindings used by cached plans.
func TestRenameTableRevision(t *testing.T) {
	for _, enableFK := range []bool{true, false} {
		t.Run(fmt.Sprintf("foreign_keys=%t", enableFK), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec(fmt.Sprintf("set global tidb_enable_foreign_key = %t", enableFK))
			t.Cleanup(func() { tk.MustExec("set global tidb_enable_foreign_key = on") })
			tk.MustExec("create database rename_revision")
			tk.MustExec("use test")
			getInfo := func(db, name string) *model.TableInfo {
				tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(
					context.Background(), ast.NewCIStr(db), ast.NewCIStr(name))
				require.NoError(t, err)
				return tbl.Meta()
			}
			checkRename := func(sql, oldDB, oldName, newDB, newName string) {
				before := getInfo(oldDB, oldName)
				tk.MustExec(sql)
				after := getInfo(newDB, newName)
				require.Equal(t, before.ID, after.ID)
				require.Greater(t, after.Revision, before.Revision, sql)
			}
			tk.MustExec("create table t1(a int primary key)")
			tk.MustExec("create table t2(a int primary key)")
			tk.MustExec("insert into t1 values(1)")
			tk.MustExec("insert into t2 values(2)")
			checkRename("rename table t1 to tmp", "test", "t1", "test", "tmp")
			checkRename("rename table t2 to t1", "test", "t2", "test", "t1")
			checkRename("rename table tmp to t2", "test", "tmp", "test", "t2")
			tk.MustQuery("select t1.a,t2.a from t1 join t2").Check(testkit.Rows("2 1"))
			checkRename("alter table t1 rename to tmp", "test", "t1", "test", "tmp")
			checkRename("rename table tmp to rename_revision.tmp", "test", "tmp", "rename_revision", "tmp")
			checkRename("rename table rename_revision.tmp to test.t1", "rename_revision", "tmp", "test", "t1")
			before1, before2 := getInfo("test", "t1"), getInfo("test", "t2")
			tk.MustExec("rename table t1 to tmp, t2 to t1, tmp to t2")
			after1, after2 := getInfo("test", "t2"), getInfo("test", "t1")
			require.Equal(t, before1.ID, after1.ID)
			require.Equal(t, before2.ID, after2.ID)
			require.Greater(t, after1.Revision, before1.Revision)
			require.Greater(t, after2.Revision, before2.Revision)
			tk.MustQuery("select t1.a,t2.a from t1 join t2").Check(testkit.Rows("1 2"))

			// The first rename has already written an increased revision when the
			// second rename fails. Neither change may survive the failed job.
			before1, before2 = getInfo("test", "t1"), getInfo("test", "t2")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/renameTableErr", `return("fail_name")`)
			err := tk.ExecToErr("rename table t1 to tmp, t2 to fail_name")
			require.ErrorContains(t, err, "occur an error after renaming table")
			testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/renameTableErr")
			require.Equal(t, before1, getInfo("test", "t1"))
			require.Equal(t, before2, getInfo("test", "t2"))
			tk.MustQuery("select t1.a,t2.a from t1 join t2").Check(testkit.Rows("1 2"))
			// A successful retry must read the original durable revision.
			checkRename("rename table t1 to tmp", "test", "t1", "test", "tmp")
			require.Equal(t, before1.Revision+1, getInfo("test", "tmp").Revision)
		})
	}
}

func TestRenameTableRevisionWithForeignKeys(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table parent(a int primary key, b int, foreign key(b) references parent(a))")
	tk.MustExec("create table child(a int primary key, b int, foreign key(b) references parent(a))")
	getInfo := func(name string) *model.TableInfo {
		tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	parent, child := getInfo("parent"), getInfo("child")
	tk.MustExec("rename table parent to renamed_parent")
	newParent, newChild := getInfo("renamed_parent"), getInfo("child")
	require.Equal(t, parent.ID, newParent.ID)
	require.Greater(t, newParent.Revision, parent.Revision)
	require.Greater(t, newChild.Revision, child.Revision)
	require.Equal(t, "renamed_parent", newParent.ForeignKeys[0].RefTable.L)
	require.Equal(t, "renamed_parent", newChild.ForeignKeys[0].RefTable.L)
	parent, child = newParent, newChild
	tk.MustExec("rename table child to renamed_child, renamed_parent to parent")
	newParent, newChild = getInfo("parent"), getInfo("renamed_child")
	require.Equal(t, parent.ID, newParent.ID)
	require.Equal(t, child.ID, newChild.ID)
	require.Greater(t, newParent.Revision, parent.Revision)
	require.Greater(t, newChild.Revision, child.Revision)
	require.Equal(t, "parent", newParent.ForeignKeys[0].RefTable.L)
	require.Equal(t, "parent", newChild.ForeignKeys[0].RefTable.L)
}

func TestRenameTableRevisionRetry(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table revision_retry(a int)")
	getInfo := func(name string) *model.TableInfo {
		tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	before := getInfo("revision_retry")
	injected := make(chan struct{}, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, err *error) {
		if job.Type != model.ActionRenameTable || job.TableID != before.ID || job.SchemaState != model.StatePublic || *err != nil {
			return
		}
		select {
		case injected <- struct{}{}:
			*err = fmt.Errorf("retry rename before committing metadata")
		default:
		}
	})
	// The new name must already carry the new revision after the first schema
	// synchronization, before the separate step that marks the job done.
	observed := make(chan uint64, 1)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.Type == model.ActionRenameTable && job.TableID == before.ID && job.SchemaState == model.StatePublic && job.State == model.JobStateRunning {
			tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("revision_renamed"))
			if err == nil {
				select {
				case observed <- tbl.Meta().Revision:
				default:
				}
			}
		}
	})
	tk.MustExec("rename table revision_retry to revision_renamed")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced")
	require.Len(t, injected, 1)
	require.Len(t, observed, 1)
	require.Equal(t, before.Revision+1, <-observed)
	after := getInfo("revision_renamed")
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, before.Revision+1, after.Revision)
}
