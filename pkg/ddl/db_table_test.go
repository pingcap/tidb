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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	testddlutil "github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestAddNotNullColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// for different databases
	tk.MustExec("create table tnn (c1 int primary key auto_increment, c2 int)")
	tk.MustExec("insert tnn (c2) values (0)" + strings.Repeat(",(0)", 99))
	done := make(chan error, 1)
	testddlutil.SessionExecInGoroutine(store, "test", "alter table tnn add column c3 int not null default 3", done)
	updateCnt := 0
out:
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			break out
		default:
			// Close issue #14636
			// Because add column action is not amendable now, it causes an error when the schema is changed
			// in the process of an insert statement.
			_, err := tk.Exec("update tnn set c2 = c2 + 1 where c1 = 99")
			if err == nil {
				updateCnt++
			}
		}
	}
	expected := fmt.Sprintf("%d %d", updateCnt, 3)
	tk.MustQuery("select c2, c3 from tnn where c1 = 99").Check(testkit.Rows(expected))
	tk.MustExec("drop table tnn")
}

func TestAddNotNullColumnWhileInsertOnDupUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	closeCh := make(chan bool)
	var wg util.WaitGroupWrapper
	tk1.MustExec("create table nn (a int primary key, b int)")
	tk1.MustExec("insert nn values (1, 1)")
	wg.Run(func() {
		for {
			select {
			case <-closeCh:
				return
			default:
			}
			tk2.MustExec("insert nn (a, b) values (1, 1) on duplicate key update a = 1, b = values(b) + 1")
		}
	})
	tk1.MustExec("alter table nn add column c int not null default 3 after a")
	close(closeCh)
	wg.Wait()
	tk1.MustQuery("select * from nn").Check(testkit.Rows("1 3 2"))
}

func TestTransactionOnAddDropColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_max_delta_schema_count= 4096")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int);")
	tk.MustExec("create table t2 (a int, b int);")
	tk.MustExec("insert into t2 values (2,0)")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set b=1 where a=1",
			"commit",
		},
		{
			"begin",
			"insert into t1 select a,b from t2",
			"update t1 set b=2 where a=2",
			"commit",
		},
	}

	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly, model.StateWriteReorganization, model.StateDeleteOnly, model.StateDeleteReorganization:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				if _, checkErr = tk.Exec(sql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, sql: %s, job schema state: %s", checkErr.Error(), sql, job.SchemaState)
					return
				}
			}
		}
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "test", "alter table t1 add column c int not null after a", done)
	err := <-done
	require.NoError(t, err)
	require.Nil(t, checkErr)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
	tk.MustExec("delete from t1")

	// test transaction on drop column.
	go backgroundExec(store, "test", "alter table t1 drop column c", done)
	err = <-done
	require.NoError(t, err)
	require.Nil(t, checkErr)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
}

func TestCreateTableWithSetCol(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_set (a int, b set('e') default '');")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` set('e') DEFAULT ''\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('a', 'b', 'c', 'd') default 'a,c,c');")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('a','b','c','d') DEFAULT 'a,c'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// It's for failure cases.
	// The type of default value is string.
	tk.MustExec("drop table t_set")
	failedSQL := "create table t_set (a set('1', '4', '10') default '3');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_set (a set('1', '4', '10') default '1,4,11');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	// Success when the new collation is enabled.
	tk.MustExec("create table t_set (a set('1', '4', '10') default '1 ,4');")
	// The type of default value is int.
	failedSQL = "create table t_set (a set('1', '4', '10') default 0);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_set (a set('1', '4', '10') default 8);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)

	// The type of default value is int.
	// It's for successful cases
	tk.MustExec("drop table if exists t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 1);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 2);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 3);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_set")
	tk.MustExec("create table t_set (a set('1', '4', '10', '21') default 15);")
	tk.MustQuery("show create table t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4,10,21'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("insert into t_set value()")
	tk.MustQuery("select * from t_set").Check(testkit.Rows("1,4,10,21"))
}

func TestCreateTableWithEnumCol(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// It's for failure cases.
	// The type of default value is string.
	tk.MustExec("drop table if exists t_enum")
	failedSQL := "create table t_enum (a enum('1', '4', '10') default '3');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_enum (a enum('1', '4', '10') default '');"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	// The type of default value is int.
	failedSQL = "create table t_enum (a enum('1', '4', '10') default 0);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t_enum (a enum('1', '4', '10') default 8);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)

	// The type of default value is int.
	// It's for successful cases
	tk.MustExec("drop table if exists t_enum")
	tk.MustExec("create table t_enum (a enum('2', '3', '4') default 2);")
	ret := tk.MustQuery("show create table t_enum").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` enum('2','3','4') DEFAULT '3'"))
	tk.MustExec("drop table t_enum")
	tk.MustExec("create table t_enum (a enum('a', 'c', 'd') default 2);")
	ret = tk.MustQuery("show create table t_enum").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` enum('a','c','d') DEFAULT 'c'"))
	tk.MustExec("insert into t_enum value()")
	tk.MustQuery("select * from t_enum").Check(testkit.Rows("c"))
}

func TestCreateTableWithIntegerColWithDefault(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// It's for failure cases.
	tk.MustExec("drop table if exists t1")
	failedSQL := "create table t1 (a tinyint unsigned default -1.25);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)
	failedSQL = "create table t1 (a tinyint default 999999999);"
	tk.MustGetErrCode(failedSQL, errno.ErrInvalidDefault)

	// It's for successful cases
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a tinyint unsigned default 1.25);")
	ret := tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` tinyint(3) unsigned DEFAULT '1'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a smallint default -1.25);")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` smallint(6) DEFAULT '-1'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a mediumint default 2.8);")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` mediumint(9) DEFAULT '3'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int default -2.8);")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` int(11) DEFAULT '-3'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a bigint unsigned default 0.0);")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` bigint(20) unsigned DEFAULT '0'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a float default '0012.43');")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` float DEFAULT '12.43'"))

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a double default '12.4300');")
	ret = tk.MustQuery("show create table t1").Rows()[0][1]
	require.True(t, strings.Contains(ret.(string), "`a` double DEFAULT '12.43'"))
}

func TestCreateTableWithInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().SetValue(sessionctx.QueryString, "skip")

	d := dom.DDL()
	require.NotNil(t, d)
	info := []*model.TableInfo{{
		ID:   42042, // Note, we must ensure the table ID is globally unique!
		Name: model.NewCIStr("t"),
	}}

	require.NoError(t, d.BatchCreateTableWithInfo(tk.Session(), model.NewCIStr("test"), info, ddl.OnExistError, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return false
	})))
	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 't'").Check(testkit.Rows("42042"))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)

	var id int64
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		id, err = m.GenGlobalID()
		return err
	})

	require.NoError(t, err)
	info = []*model.TableInfo{{
		ID:   42,
		Name: model.NewCIStr("tt"),
	}}
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.BatchCreateTableWithInfo(tk.Session(), model.NewCIStr("test"), info, ddl.OnExistError, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return true
	})))
	idGen, ok := tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'tt'").Rows()[0][0].(string)
	require.True(t, ok)
	idGenNum, err := strconv.ParseInt(idGen, 10, 64)
	require.NoError(t, err)
	require.Greater(t, idGenNum, id)
}

func TestBatchCreateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tables_1")
	tk.MustExec("drop table if exists tables_2")
	tk.MustExec("drop table if exists tables_3")

	d := dom.DDL()
	infos := []*model.TableInfo{}
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_1"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_2"),
	})
	infos = append(infos, &model.TableInfo{
		Name: model.NewCIStr("tables_3"),
	})

	// correct name
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err := d.BatchCreateTableWithInfo(tk.Session(), model.NewCIStr("test"), infos, ddl.OnExistError)
	require.NoError(t, err)

	tk.MustQuery("show tables like '%tables_%'").Check(testkit.Rows("tables_1", "tables_2", "tables_3"))
	job := tk.MustQuery("admin show ddl jobs").Rows()[0]
	require.Equal(t, "test", job[1])
	require.Equal(t, "tables_1,tables_2,tables_3", job[2])
	require.Equal(t, "create tables", job[3])
	require.Equal(t, "public", job[4])
	// FIXME: we must change column type to give multiple id
	// c.Assert(job[6], Matches, "[^,]+,[^,]+,[^,]+")

	// duplicated name
	infos[1].Name = model.NewCIStr("tables_1")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = d.BatchCreateTableWithInfo(tk.Session(), model.NewCIStr("test"), infos, ddl.OnExistError)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableExists))

	newinfo := &model.TableInfo{
		Name: model.NewCIStr("tables_4"),
	}
	{
		colNum := 2
		cols := make([]*model.ColumnInfo, colNum)
		viewCols := make([]model.CIStr, colNum)
		var stmtBuffer bytes.Buffer
		stmtBuffer.WriteString("SELECT ")
		for i := range cols {
			col := &model.ColumnInfo{
				Name:   model.NewCIStr(fmt.Sprintf("c%d", i+1)),
				Offset: i,
				State:  model.StatePublic,
			}
			cols[i] = col
			viewCols[i] = col.Name
			stmtBuffer.WriteString(cols[i].Name.L + ",")
		}
		stmtBuffer.WriteString("1 FROM t")
		newinfo.Columns = cols
		newinfo.View = &model.ViewInfo{Cols: viewCols, Security: model.SecurityDefiner, Algorithm: model.AlgorithmMerge, SelectStmt: stmtBuffer.String(), CheckOption: model.CheckOptionCascaded, Definer: &auth.UserIdentity{CurrentUser: true}}
	}

	tk.Session().SetValue(sessionctx.QueryString, "skip")
	tk.Session().SetValue(sessionctx.QueryString, "skip")
	err = d.BatchCreateTableWithInfo(tk.Session(), model.NewCIStr("test"), []*model.TableInfo{newinfo}, ddl.OnExistError)
	require.NoError(t, err)
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/mysql-test/t/tablelock.test
func TestTableLock(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")

	/* Test of lock tables */
	tk.MustExec("create table t1 ( n int auto_increment primary key)")
	tk.MustExec("lock tables t1 write")
	tk.MustExec("insert into t1 values(NULL)")
	tk.MustExec("unlock tables")
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)

	tk.MustExec("lock tables t1 write")
	tk.MustExec("insert into t1 values(NULL)")
	tk.MustExec("unlock tables")
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)

	tk.MustExec("drop table if exists t1")

	/* Test of locking and delete of files */
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t1,t2")

	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t2,t1")
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/mysql-test/t/lock_tables_lost_commit.test
func TestTableLocksLostCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1(a INT)")
	tk.MustExec("LOCK TABLES t1 WRITE")
	tk.MustExec("INSERT INTO t1 VALUES(10)")

	err := tk2.ExecToErr("SELECT * FROM t1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))

	tk.Session().Close()

	tk2.MustExec("SELECT * FROM t1")
	tk2.MustExec("DROP TABLE t1")

	tk.MustExec("unlock tables")
}

func checkTableLock(t *testing.T, tk *testkit.TestKit, dbName, tableName string, lockTp model.TableLockType) {
	tb := external.GetTableByName(t, tk, dbName, tableName)
	dom := domain.GetDomain(tk.Session())
	err := dom.Reload()
	require.NoError(t, err)
	if lockTp != model.TableLockNone {
		require.NotNil(t, tb.Meta().Lock)
		require.Equal(t, lockTp, tb.Meta().Lock.Tp)
		require.Equal(t, model.TableLockStatePublic, tb.Meta().Lock.State)
		require.True(t, len(tb.Meta().Lock.Sessions) == 1)
		require.Equal(t, dom.DDL().GetID(), tb.Meta().Lock.Sessions[0].ServerID)
		require.Equal(t, tk.Session().GetSessionVars().ConnectionID, tb.Meta().Lock.Sessions[0].SessionID)
	} else {
		require.Nil(t, tb.Meta().Lock)
	}
}

// test write local lock
func TestWriteLocal(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 ( n int auto_increment primary key)")

	// Test: allow read
	tk.MustExec("lock tables t1 write local")
	tk.MustExec("insert into t1 values(NULL)")
	tk2.MustQuery("select count(*) from t1")
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test: forbid write
	tk.MustExec("lock tables t1 write local")
	err := tk2.ExecToErr("insert into t1 values(NULL)")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write local first
	tk.MustExec("lock tables t1 write local")
	err = tk2.ExecToErr("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	err = tk2.ExecToErr("lock tables t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	err = tk2.ExecToErr("lock tables t1 read")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write first
	tk.MustExec("lock tables t1 write")
	err = tk2.ExecToErr("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock read first
	tk.MustExec("lock tables t1 read")
	err = tk2.ExecToErr("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func TestLockTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	setTxnTk.MustExec("set global tidb_enable_metadata_lock=0")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")

	// Test lock 1 table.
	tk.MustExec("lock tables t1 write")
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)
	tk.MustExec("lock tables t1 read")
	checkTableLock(t, tk, "test", "t1", model.TableLockRead)
	tk.MustExec("lock tables t1 write")
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)

	// Test lock multi tables.
	tk.MustExec("lock tables t1 write, t2 read")
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)
	checkTableLock(t, tk, "test", "t2", model.TableLockRead)
	tk.MustExec("lock tables t1 read, t2 write")
	checkTableLock(t, tk, "test", "t1", model.TableLockRead)
	checkTableLock(t, tk, "test", "t2", model.TableLockWrite)
	tk.MustExec("lock tables t2 write")
	checkTableLock(t, tk, "test", "t2", model.TableLockWrite)
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)
	tk.MustExec("lock tables t1 write")
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)
	checkTableLock(t, tk, "test", "t2", model.TableLockNone)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// Test read lock.
	tk.MustExec("lock tables t1 read")
	tk.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	tk.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableNotLockedForWrite)
	tk.MustGetDBError("update t1 set a=1", infoschema.ErrTableNotLockedForWrite)
	tk.MustGetDBError("delete from t1", infoschema.ErrTableNotLockedForWrite)

	tk2.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("update t1 set a=1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("delete from t1", infoschema.ErrTableLocked)
	tk2.MustExec("lock tables t1 read")
	tk2.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableNotLockedForWrite)

	// Test write lock.
	tk.MustGetDBError("lock tables t1 write", infoschema.ErrTableLocked)
	tk2.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	tk2.MustGetDBError("select * from t1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("lock tables t1 write", infoschema.ErrTableLocked)

	// Test write local lock.
	tk.MustExec("lock tables t1 write local")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	tk2.MustQuery("select * from t1")
	tk2.MustGetDBError("delete from t1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableLocked)
	tk2.MustGetDBError("lock tables t1 write", infoschema.ErrTableLocked)
	tk2.MustGetDBError("lock tables t1 read", infoschema.ErrTableLocked)

	// Test none unique table.
	tk.MustGetDBError("lock tables t1 read, t1 write", infoschema.ErrNonuniqTable)

	// Test lock table by other session in transaction and commit without retry.
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("set @@session.tidb_disable_txn_auto_retry=1")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustExec("lock tables t1 write")
	tk.MustGetErrMsg("commit",
		"previous statement: insert into t1 set a=1: [domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")

	// Test lock tables and drop tables
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 write")
	tk.MustExec("drop table t1")
	tk2.MustExec("create table t1 (a int)")
	tk.MustExec("lock tables t1 write, t2 read")

	// Test lock tables and drop database.
	tk.MustExec("unlock tables")
	tk.MustExec("create database test_lock")
	tk.MustExec("create table test_lock.t3 (a int)")
	tk.MustExec("lock tables t1 write, test_lock.t3 write")
	tk2.MustExec("create table t3 (a int)")
	tk.MustExec("lock tables t1 write, t3 write")
	tk.MustExec("drop table t3")

	// Test lock tables and truncate tables.
	tk.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 read")
	tk.MustExec("truncate table t1")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustGetDBError("insert into t1 set a=1", infoschema.ErrTableLocked)

	// Test for lock unsupported schema tables.
	tk2.MustGetDBError("lock tables performance_schema.global_status write", infoschema.ErrAccessDenied)
	tk2.MustGetDBError("lock tables information_schema.tables write", infoschema.ErrAccessDenied)
	tk2.MustGetDBError("lock tables mysql.db write", infoschema.ErrAccessDenied)

	// Test create table/view when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 read")
	tk.MustGetDBError("create table t3 (a int)", infoschema.ErrTableNotLocked)
	tk.MustGetDBError("create view v1 as select * from t1;", infoschema.ErrTableNotLocked)

	// Test for locking view was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create view v1 as select * from t1;")
	tk.MustGetDBError("lock tables v1 read", table.ErrUnsupportedOp)

	// Test for locking sequence was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create sequence seq")
	tk.MustGetDBError("lock tables seq read", table.ErrUnsupportedOp)
	tk.MustExec("drop sequence seq")

	// Test for create/drop/alter database when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write")
	tk.MustGetDBError("drop database test", table.ErrLockOrActiveTransaction)
	tk.MustGetDBError("create database test_lock", table.ErrLockOrActiveTransaction)
	tk.MustGetDBError("alter database test charset='utf8mb4'", table.ErrLockOrActiveTransaction)
	// Test alter/drop database when other session is holding the table locks of the database.
	tk2.MustExec("create database test_lock2")
	tk2.MustGetDBError("drop database test", infoschema.ErrTableLocked)
	tk2.MustGetDBError("alter database test charset='utf8mb4'", infoschema.ErrTableLocked)

	// Test for admin cleanup table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write, t2 write")
	tk2.MustGetDBError("lock tables t1 write, t2 read", infoschema.ErrTableLocked)
	tk2.MustExec("admin cleanup table lock t1,t2")
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)
	checkTableLock(t, tk, "test", "t2", model.TableLockNone)
	// cleanup unlocked table.
	tk2.MustExec("admin cleanup table lock t1,t2")
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)
	checkTableLock(t, tk, "test", "t2", model.TableLockNone)
	tk2.MustExec("lock tables t1 write, t2 read")
	checkTableLock(t, tk2, "test", "t1", model.TableLockWrite)
	checkTableLock(t, tk2, "test", "t2", model.TableLockRead)

	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func TestTablesLockDelayClean(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	defer tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")

	tk.MustExec("lock tables t1 write")
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DelayCleanTableLock = 100
	})
	var wg util.WaitGroupWrapper
	var startTime time.Time
	wg.Run(func() {
		startTime = time.Now()
		tk.Session().Close()
	})
	time.Sleep(50 * time.Millisecond)
	checkTableLock(t, tk, "test", "t1", model.TableLockWrite)
	wg.Wait()
	require.True(t, time.Since(startTime).Seconds() > 0.1)
	checkTableLock(t, tk, "test", "t1", model.TableLockNone)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DelayCleanTableLock = 0
	})
}

func TestAddColumn2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int key, b int);")
	defer tk.MustExec("drop table if exists t1, t2")

	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &callback.TestDDLCallback{Do: dom}
	var writeOnlyTable table.Table
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			writeOnlyTable, _ = dom.InfoSchema().TableByID(job.TableID)
		}
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "test", "alter table t1 add column c int not null", done)
	err := <-done
	require.NoError(t, err)

	tk.MustExec("insert into t1 values (1,1,1)")
	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 1 1"))

	// mock for outdated tidb update record.
	require.NotNil(t, writeOnlyTable)
	ctx := context.Background()
	err = sessiontxn.NewTxn(ctx, tk.Session())
	require.NoError(t, err)
	oldRow, err := tables.RowWithCols(writeOnlyTable, tk.Session(), kv.IntHandle(1), writeOnlyTable.WritableCols())
	require.NoError(t, err)
	require.Equal(t, 3, len(oldRow))
	err = writeOnlyTable.RemoveRecord(tk.Session().GetTableCtx(), kv.IntHandle(1), oldRow)
	require.NoError(t, err)
	_, err = writeOnlyTable.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()), table.IsUpdate)
	require.NoError(t, err)
	tk.Session().StmtCommit(ctx)
	err = tk.Session().CommitTxn(ctx)
	require.NoError(t, err)

	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 2 1"))

	// Test for _tidb_rowid
	var re *testkit.Result
	tk.MustExec("create table t2 (a int);")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		// allow write _tidb_rowid first
		tk.MustExec("set @@tidb_opt_write_row_id=1")
		tk.MustExec("begin")
		tk.MustExec("insert into t2 (a,_tidb_rowid) values (1,2);")
		re = tk.MustQuery(" select a,_tidb_rowid from t2;")
		tk.MustExec("commit")
	}
	dom.DDL().SetHook(hook)

	go backgroundExec(store, "test", "alter table t2 add column b int not null default 3", done)
	err = <-done
	require.NoError(t, err)
	re.Check(testkit.Rows("1 2"))
	tk.MustQuery("select a,b,_tidb_rowid from t2").Check(testkit.Rows("1 3 2"))
}

func TestDropTables(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")

	failedSQL := "drop table t1;"
	tk.MustGetErrCode(failedSQL, errno.ErrBadTable)
	failedSQL = "drop table test2.t1;"
	tk.MustGetErrCode(failedSQL, errno.ErrBadTable)

	tk.MustExec("create table t1 (a int);")
	tk.MustExec("drop table if exists t1, t2;")

	tk.MustExec("create table t1 (a int);")
	tk.MustExec("drop table if exists t2, t1;")

	// Without IF EXISTS, the statement drops all named tables that do exist, and returns an error indicating which
	// nonexisting tables it was unable to drop.
	// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
	tk.MustExec("create table t1 (a int);")
	failedSQL = "drop table t1, t2;"
	tk.MustGetErrCode(failedSQL, errno.ErrBadTable)

	tk.MustExec("create table t1 (a int);")
	failedSQL = "drop table t2, t1;"
	tk.MustGetErrCode(failedSQL, errno.ErrBadTable)

	failedSQL = "show create table t1;"
	tk.MustGetErrCode(failedSQL, errno.ErrNoSuchTable)
}

func TestCreateConstraintForTable(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1, t2")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("CREATE TABLE t1 (id INT PRIMARY KEY, CONSTRAINT c1 CHECK (id<50))")
	failedSQL := "CREATE TABLE t2 (id INT PRIMARY KEY, CONSTRAINT c1 CHECK (id<50))"
	tk.MustGetErrCode(failedSQL, errno.ErrCheckConstraintDupName)

	tk.MustExec("CREATE TABLE t2 (id INT PRIMARY KEY)")
	failedSQL = "ALTER TABLE t2 ADD CONSTRAINT c1 CHECK (id<50)"
	tk.MustGetErrCode(failedSQL, errno.ErrCheckConstraintDupName)

	tk.MustExec("DROP DATABASE IF EXISTS test2")
	tk.MustExec("CREATE DATABASE test2")
	tk.MustExec("CREATE TABLE test2.t1 (id INT PRIMARY KEY, CONSTRAINT c1 CHECK (id<50))")
	rs, err := tk.Exec("SHOW TABLES FROM test2 LIKE 't1'")
	require.NoError(t, err)
	require.Equal(t, tk.ResultSetToResult(rs, "").Rows()[0][0], "t1")
}
