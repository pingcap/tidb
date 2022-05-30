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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestTableForeignKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int);")
	// test create table with foreign key.
	failSQL := "create table t2 (c int, foreign key (a) references t1(a));"
	tk.MustGetErrCode(failSQL, errno.ErrKeyColumnDoesNotExits)
	// test add foreign key.
	tk.MustExec("create table t3 (a int, b int);")
	failSQL = "alter table t1 add foreign key (c) REFERENCES t3(a);"
	tk.MustGetErrCode(failSQL, errno.ErrKeyColumnDoesNotExits)
	// test origin key not match error
	failSQL = "alter table t1 add foreign key (a) REFERENCES t3(a, b);"
	tk.MustGetErrCode(failSQL, errno.ErrWrongFkDef)
	// Test drop column with foreign key.
	tk.MustExec("create table t4 (c int,d int,foreign key (d) references t1 (b));")
	failSQL = "alter table t4 drop column d"
	tk.MustGetErrCode(failSQL, errno.ErrFkColumnCannotDrop)
	// Test change column with foreign key.
	failSQL = "alter table t4 change column d e bigint;"
	tk.MustGetErrCode(failSQL, errno.ErrFKIncompatibleColumns)
	// Test modify column with foreign key.
	failSQL = "alter table t4 modify column d bigint;"
	tk.MustGetErrCode(failSQL, errno.ErrFKIncompatibleColumns)
	tk.MustQuery("select count(*) from information_schema.KEY_COLUMN_USAGE;")
	tk.MustExec("alter table t4 drop foreign key d")
	tk.MustExec("alter table t4 modify column d bigint;")
	tk.MustExec("drop table if exists t1,t2,t3,t4;")
}

func TestAddNotNullColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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

func TestCharacterSetInColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database varchar_test;")
	defer tk.MustExec("drop database varchar_test;")
	tk.MustExec("use varchar_test")
	tk.MustExec("create table t (c1 int, s1 varchar(10), s2 text)")
	tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name != 'utf8mb4'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'varchar_test' and character_set_name = 'utf8mb4'").Check(testkit.Rows("2"))

	tk.MustExec("create table t1(id int) charset=UTF8;")
	tk.MustExec("create table t2(id int) charset=BINARY;")
	tk.MustExec("create table t3(id int) charset=LATIN1;")
	tk.MustExec("create table t4(id int) charset=ASCII;")
	tk.MustExec("create table t5(id int) charset=UTF8MB4;")

	tk.MustExec("create table t11(id int) charset=utf8;")
	tk.MustExec("create table t12(id int) charset=binary;")
	tk.MustExec("create table t13(id int) charset=latin1;")
	tk.MustExec("create table t14(id int) charset=ascii;")
	tk.MustExec("create table t15(id int) charset=utf8mb4;")
}

func TestAddNotNullColumnWhileInsertOnDupUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, time.Microsecond*500)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	hook := &ddl.TestDDLCallback{Do: dom}
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
	go backgroundExec(store, "alter table t1 add column c int not null after a", done)
	err := <-done
	require.NoError(t, err)
	require.Nil(t, checkErr)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
	tk.MustExec("delete from t1")

	// test transaction on drop column.
	go backgroundExec(store, "alter table t1 drop column c", done)
	err = <-done
	require.NoError(t, err)
	require.Nil(t, checkErr)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
}

func TestCreateTableWithSetCol(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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

func TestAlterTableWithValidation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")

	tk.MustExec("create table t1 (c1 int, c2 int as (c1 + 1));")

	// Test for alter table with validation.
	tk.MustExec("alter table t1 with validation")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8200|ALTER TABLE WITH VALIDATION is currently unsupported"))

	// Test for alter table without validation.
	tk.MustExec("alter table t1 without validation")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8200|ALTER TABLE WITHOUT VALIDATION is currently unsupported"))
}

func TestBatchCreateTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, time.Microsecond*500)
	defer clean()
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
// https://github.com/mysql/mysql-server/blob/124c7ab1d6f914637521fd4463a993aa73403513/mysql-test/t/lock.test
func TestLock(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	/* Testing of table locking */
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 (  `id` int(11) NOT NULL default '0', `id2` int(11) NOT NULL default '0', `id3` int(11) NOT NULL default '0', `dummy1` char(30) default NULL, PRIMARY KEY  (`id`,`id2`), KEY `index_id3` (`id3`))")
	tk.MustExec("insert into t1 (id,id2) values (1,1),(1,2),(1,3)")
	tk.MustExec("LOCK TABLE t1 WRITE")
	tk.MustExec("select dummy1,count(distinct id) from t1 group by dummy1")
	tk.MustExec("update t1 set id=-1 where id=1")
	tk.MustExec("LOCK TABLE t1 READ")
	_, err := tk.Exec("update t1 set id=1 where id=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite))
	tk.MustExec("unlock tables")
	tk.MustExec("update t1 set id=1 where id=-1")
	tk.MustExec("drop table t1")
}

// port from mysql
// https://github.com/mysql/mysql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/mysql-test/t/tablelock.test
func TestTableLock(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1(a INT)")
	tk.MustExec("LOCK TABLES t1 WRITE")
	tk.MustExec("INSERT INTO t1 VALUES(10)")

	_, err := tk2.Exec("SELECT * FROM t1")
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	_, err := tk2.Exec("insert into t1 values(NULL)")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write local first
	tk.MustExec("lock tables t1 write local")
	_, err = tk2.Exec("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("lock tables t1 read")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock write first
	tk.MustExec("lock tables t1 write")
	_, err = tk2.Exec("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test mutex: lock read first
	tk.MustExec("lock tables t1 read")
	_, err = tk2.Exec("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func TestLockTables(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, time.Microsecond*500)
	defer clean()
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
	_, err := tk.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite))
	_, err = tk.Exec("update t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite))
	_, err = tk.Exec("delete from t1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite))

	_, err = tk2.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("update t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("delete from t1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk2.MustExec("lock tables t1 read")
	_, err = tk2.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLockedForWrite))

	// Test write lock.
	_, err = tk.Exec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	tk2.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	_, err = tk2.Exec("select * from t1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))

	// Test write local lock.
	tk.MustExec("lock tables t1 write local")
	tk.MustQuery("select * from t1")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 set a=1")

	tk2.MustQuery("select * from t1")
	_, err = tk2.Exec("delete from t1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("lock tables t1 read")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))

	// Test none unique table.
	_, err = tk.Exec("lock tables t1 read, t1 write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrNonuniqTable))

	// Test lock table by other session in transaction and commit without retry.
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("set @@session.tidb_disable_txn_auto_retry=1")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustExec("lock tables t1 write")
	_, err = tk.Exec("commit")
	require.Error(t, err)
	require.Equal(t, "previous statement: insert into t1 set a=1: [domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]", err.Error())

	// Test lock table by other session in transaction and commit with retry.
	tk.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
	tk.MustExec("set @@session.tidb_disable_txn_auto_retry=0")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 set a=1")
	tk2.MustExec("lock tables t1 write")
	_, err = tk.Exec("commit")
	require.Truef(t, terror.ErrorEqual(err, infoschema.ErrTableLocked), "err: %v\n", err)

	// Test for lock the same table multiple times.
	tk2.MustExec("lock tables t1 write")
	tk2.MustExec("lock tables t1 write, t2 read")

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
	_, err = tk2.Exec("insert into t1 set a=1")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))

	// Test for lock unsupported schema tables.
	_, err = tk2.Exec("lock tables performance_schema.global_status write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrAccessDenied))
	_, err = tk2.Exec("lock tables information_schema.tables write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrAccessDenied))
	_, err = tk2.Exec("lock tables mysql.db write")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrAccessDenied))

	// Test create table/view when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock tables t1 write, t2 read")
	_, err = tk.Exec("create table t3 (a int)")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLocked))
	_, err = tk.Exec("create view v1 as select * from t1;")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableNotLocked))

	// Test for locking view was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create view v1 as select * from t1;")
	_, err = tk.Exec("lock tables v1 read")
	require.True(t, terror.ErrorEqual(err, table.ErrUnsupportedOp))

	// Test for locking sequence was not supported.
	tk.MustExec("unlock tables")
	tk.MustExec("create sequence seq")
	_, err = tk.Exec("lock tables seq read")
	require.True(t, terror.ErrorEqual(err, table.ErrUnsupportedOp))
	tk.MustExec("drop sequence seq")

	// Test for create/drop/alter database when session is holding the table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write")
	_, err = tk.Exec("drop database test")
	require.True(t, terror.ErrorEqual(err, table.ErrLockOrActiveTransaction))
	_, err = tk.Exec("create database test_lock")
	require.True(t, terror.ErrorEqual(err, table.ErrLockOrActiveTransaction))
	_, err = tk.Exec("alter database test charset='utf8mb4'")
	require.True(t, terror.ErrorEqual(err, table.ErrLockOrActiveTransaction))
	// Test alter/drop database when other session is holding the table locks of the database.
	tk2.MustExec("create database test_lock2")
	_, err = tk2.Exec("drop database test")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
	_, err = tk2.Exec("alter database test charset='utf8mb4'")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))

	// Test for admin cleanup table locks.
	tk.MustExec("unlock tables")
	tk.MustExec("lock table t1 write, t2 write")
	_, err = tk2.Exec("lock tables t1 write, t2 read")
	require.True(t, terror.ErrorEqual(err, infoschema.ErrTableLocked))
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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

func TestDDLWithInvalidTableInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	// Test create with invalid expression.
	_, err := tk.Exec(`CREATE TABLE t (
		c0 int(11) ,
  		c1 int(11),
    	c2 decimal(16,4) GENERATED ALWAYS AS ((case when (c0 = 0) then 0when (c0 > 0) then (c1 / c0) end))
	);`)
	require.Error(t, err)
	require.Equal(t, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 4 column 88 near \"then (c1 / c0) end))\n\t);\" ", err.Error())

	tk.MustExec("create table t (a bigint, b int, c int generated always as (b+1)) partition by hash(a) partitions 4;")
	// Test drop partition column.
	_, err = tk.Exec("alter table t drop column a;")
	require.Error(t, err)
	// TODO: refine the error message to compatible with MySQL
	require.Equal(t, "[planner:1054]Unknown column 'a' in 'expression'", err.Error())
	// Test modify column with invalid expression.
	_, err = tk.Exec("alter table t modify column c int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	require.Error(t, err)
	require.Equal(t, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 97 near \"then (b / a) end));\" ", err.Error())
	// Test add column with invalid expression.
	_, err = tk.Exec("alter table t add column d int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	require.Error(t, err)
	require.Equal(t, "[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 94 near \"then (b / a) end));\" ", err.Error())
}

func TestAddColumn2(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, time.Microsecond*500)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int key, b int);")
	defer tk.MustExec("drop table if exists t1, t2")

	originHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originHook)
	hook := &ddl.TestDDLCallback{Do: dom}
	var writeOnlyTable table.Table
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			writeOnlyTable, _ = dom.InfoSchema().TableByID(job.TableID)
		}
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "alter table t1 add column c int not null", done)
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
	err = writeOnlyTable.RemoveRecord(tk.Session(), kv.IntHandle(1), oldRow)
	require.NoError(t, err)
	_, err = writeOnlyTable.AddRecord(tk.Session(), types.MakeDatums(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()), table.IsUpdate)
	require.NoError(t, err)
	tk.Session().StmtCommit()
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

	go backgroundExec(store, "alter table t2 add column b int not null default 3", done)
	err = <-done
	require.NoError(t, err)
	re.Check(testkit.Rows("1 2"))
	tk.MustQuery("select a,b,_tidb_rowid from t2").Check(testkit.Rows("1 3 2"))
}
