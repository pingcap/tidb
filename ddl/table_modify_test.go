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
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/israce"
	"github.com/stretchr/testify/require"
)

const tableModifyLease = 600 * time.Millisecond

func TestCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, tableModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	cols := tbl.Cols()
	require.Len(t, cols, 1)
	col := cols[0]
	require.Equal(t, "a", col.Name.L)
	d, ok := col.DefaultValue.(string)
	require.True(t, ok)
	require.Equal(t, "2.0", d)

	tk.MustExec("drop table t")
	tk.MustGetErrCode("CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", errno.ErrUnknownCharacterSet)

	tk.MustExec("CREATE TABLE `collateTest` (`a` int, `b` varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")
	expects := "collateTest CREATE TABLE `collateTest` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table collateTest").Check(testkit.Rows(expects))

	tk.MustGetErrCode("CREATE TABLE `collateTest2` (`a` int) CHARSET utf8 COLLATE utf8mb4_unicode_ci", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("CREATE TABLE `collateTest3` (`a` int) COLLATE utf8mb4_unicode_ci CHARSET utf8", errno.ErrConflictingDeclarations)

	tk.MustExec("CREATE TABLE `collateTest4` (`a` int) COLLATE utf8_uniCOde_ci")
	expects = "collateTest4 CREATE TABLE `collateTest4` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	tk.MustQuery("show create table collateTest4").Check(testkit.Rows(expects))

	tk.MustExec("create database test2 default charset utf8 collate utf8_general_ci")
	tk.MustExec("use test2")
	tk.MustExec("create table dbCollateTest (a varchar(10))")
	expects = "dbCollateTest CREATE TABLE `dbCollateTest` (\n  `a` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table dbCollateTest").Check(testkit.Rows(expects))

	// test for enum column
	tk.MustExec("use test")
	tk.MustGetErrCode("create table t_enum (a enum('e','e'));", errno.ErrDuplicatedValueInType)

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('abc','Abc')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	// test for set column
	tk.MustGetErrCode("create table t_enum (a set('e','e'));", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('e','E')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('abc','Abc')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrMsg("create table t_enum (a enum('B','b')) charset=utf8 collate=utf8_general_ci;", "[types:1291]Column 'a' has duplicated value 'b' in ENUM")
	tk.MustGetErrCode("create table t_enum (a set('e','E')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrMsg("create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", "[types:1291]Column 'a' has duplicated value 'ß' in ENUM")

	// test for table option "union" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	tk.MustGetErrCode("CREATE TABLE z (a INT) ENGINE = MERGE UNION = (x, y);", errno.ErrTableOptionUnionUnsupported)
	tk.MustGetErrCode("ALTER TABLE x UNION = (y);", errno.ErrTableOptionUnionUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")

	// test for table option "insert method" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	tk.MustGetErrCode("CREATE TABLE z (a INT) ENGINE = MERGE INSERT_METHOD=LAST;", errno.ErrTableOptionInsertMethodUnsupported)
	tk.MustGetErrCode("ALTER TABLE x INSERT_METHOD=LAST;", errno.ErrTableOptionInsertMethodUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")
}

func TestLockTableReadOnly(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("skip race test")
	}
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, tableModifyLease)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1,t2")
	defer func() {
		tk1.MustExec("alter table t1 read write")
		tk1.MustExec("alter table t2 read write")
		tk1.MustExec("drop table if exists t1,t2")
	}()
	tk1.MustExec("create table t1 (a int key, b int)")
	tk1.MustExec("create table t2 (a int key)")

	tk1.MustExec("alter table t1 read only")
	tk1.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("update t1 set a=1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("delete from t1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("update t1 set a=1"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("delete from t1"), infoschema.ErrTableLocked))

	tk2.MustExec("alter table t1 read only")
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("insert into t1 set a=1, b=2"), infoschema.ErrTableLocked))

	tk1.MustExec("alter table t1 read write")
	tk1.MustExec("lock tables t1 read")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("lock tables t1 write")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("lock tables t1 write local")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("alter table t1 read only"), infoschema.ErrTableLocked))
	tk1.MustExec("unlock tables")

	tk1.MustExec("alter table t1 read only")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 read"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked))
	require.True(t, terror.ErrorEqual(tk2.ExecToErr("lock tables t1 write local"), infoschema.ErrTableLocked))
	tk1.MustExec("admin cleanup table lock t1")
	tk2.MustExec("insert into t1 set a=1, b=2")

	tk1.MustExec("set tidb_enable_amend_pessimistic_txn = 1")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk2.MustExec("update t1 set b = 3")
	tk2.MustExec("alter table t1 read only")
	tk2.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 3"))
	tk1.MustQuery("select * from t1 where a = 1").Check(testkit.Rows("1 2"))
	tk1.MustExec("update t1 set b = 4")
	require.True(t, terror.ErrorEqual(tk1.ExecToErr("commit"), domain.ErrInfoSchemaChanged))
	tk2.MustExec("alter table t1 read write")
}

// TestConcurrentLockTables test concurrent lock/unlock tables.
func TestConcurrentLockTables(t *testing.T) {
	if israce.RaceEnabled {
		t.Skip("skip race test")
	}
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, tableModifyLease)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table t1 (a int)")

	// Test concurrent lock tables read.
	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.NoError(t, err2)
	})
	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write.
	sql1 = "lock tables t1 write"
	sql2 = "lock tables t1 write"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.True(t, terror.ErrorEqual(err2, infoschema.ErrTableLocked))
	})
	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")

	// Test concurrent lock tables write local.
	sql1 = "lock tables t1 write local"
	sql2 = "lock tables t1 write local"
	testParallelExecSQL(t, store, dom, sql1, sql2, tk1.Session(), tk2.Session(), func(t *testing.T, err1, err2 error) {
		require.NoError(t, err1)
		require.True(t, terror.ErrorEqual(err2, infoschema.ErrTableLocked))
	})

	tk1.MustExec("unlock tables")
	tk2.MustExec("unlock tables")
}

func testParallelExecSQL(t *testing.T, store kv.Storage, dom *domain.Domain, sql1, sql2 string, se1, se2 session.Session, f func(t *testing.T, err1, err2 error)) {
	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobRunBeforeExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err1 := admin.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			require.NoError(t, err)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	d.SetHook(callback)

	var wg util.WaitGroupWrapper
	var err1 error
	var err2 error
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err3 := admin.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			require.NoError(t, err)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Run(func() {
		_, err1 = se1.Execute(context.Background(), sql1)
	})
	wg.Run(func() {
		<-ch
		_, err2 = se2.Execute(context.Background(), sql2)
	})

	wg.Wait()
	f(t, err1, err2)
}

func TestUnsupportedAlterTableOption(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, tableModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a char(10) not null,b char(20)) shard_row_id_bits=6")
	tk.MustGetErrCode("alter table t pre_split_regions=6", errno.ErrUnsupportedDDLOperation)
}
