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

package executor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
)

func TestRecoverTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")

	timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	// if GC safe point is not exists in mysql.tidb
	tk.MustGetErrMsg("recover table t_recover", "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Should recover, and we can drop it straight away.
	tk.MustExec("recover table t_recover")
	tk.MustExec("drop table t_recover")

	require.NoError(t, gcutil.EnableGC(tk.Session()))

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	tk.MustContainErrMsg("recover table t_recover", "Can't find dropped/truncated table 't_recover' in GC safe point")

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int);")
	tk.MustGetErrMsg("recover table t_recover", infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then recover table.
	tk.MustExec("rename table t_recover to t_recover2")

	// do recover table.
	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_recover;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// recover table by none exits job.
	err := tk.ExecToErr(fmt.Sprintf("recover table by job %d", 10000000))
	require.Error(t, err)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	require.NoError(t, gcutil.DisableGC(tk.Session()))

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")

	tk.MustExec("recover table t_recover")

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Recover truncate table.
	tk.MustExec("truncate table t_recover")
	tk.MustExec("rename table t_recover to t_recover_new")
	tk.MustExec("recover table t_recover")
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	// Test for recover one table multiple time.
	tk.MustExec("drop table t_recover")
	tk.MustExec("flashback table t_recover to t_recover_tmp")
	err = tk.ExecToErr("recover table t_recover")
	require.True(t, infoschema.ErrTableExists.Equal(err))

	gcEnable, err := gcutil.CheckGCEnable(tk.Session())
	require.NoError(t, err)
	require.False(t, gcEnable)
}

func TestFlashbackTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_flashback")
	tk.MustExec("use test_flashback")
	tk.MustExec("drop table if exists t_flashback")
	tk.MustExec("create table t_flashback (a int);")

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()

	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// Set GC enable.
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	tk.MustExec("insert into t_flashback values (1),(2),(3)")
	tk.MustExec("drop table t_flashback")

	// Test flash table with not_exist_table_name name.
	tk.MustGetErrMsg("flashback table t_not_exists", "Can't find localTemporary/dropped/truncated table: t_not_exists in DDL history jobs")

	// Test flashback table failed by there is already a new table with the same name.
	// If there is a new table with the same name, should return failed.
	tk.MustExec("create table t_flashback (a int);")
	tk.MustGetErrMsg("flashback table t_flashback", infoschema.ErrTableExists.GenWithStackByArgs("t_flashback").Error())

	// Drop the new table with the same name, then flashback table.
	tk.MustExec("rename table t_flashback to t_flashback_tmp")

	// Test for flashback table.
	tk.MustExec("flashback table t_flashback")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback values (4),(5),(6)")
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// Test for flashback to new table.
	tk.MustExec("drop table t_flashback")
	tk.MustExec("create table t_flashback (a int);")
	tk.MustExec("flashback table t_flashback to t_flashback2")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback2 values (7),(8),(9)")
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback2;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003"))

	// Test for flashback one table multiple time.
	err := tk.ExecToErr("flashback table t_flashback to t_flashback4")
	require.True(t, infoschema.ErrTableExists.Equal(err))

	// Test for flashback truncated table to new table.
	tk.MustExec("truncate table t_flashback2")
	tk.MustExec("flashback table t_flashback2 to t_flashback3")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_flashback3 values (10),(11)")
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"))
	// Check rebase auto id.
	tk.MustQuery("select a,_tidb_rowid from t_flashback3;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003", "10 15001", "11 15002"))

	// Test for flashback drop partition table.
	tk.MustExec("drop table if exists t_p_flashback")
	tk.MustExec("create table t_p_flashback (a int) partition by hash(a) partitions 4;")
	tk.MustExec("insert into t_p_flashback values (1),(2),(3)")
	tk.MustExec("drop table t_p_flashback")
	tk.MustExec("flashback table t_p_flashback")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_p_flashback order by a;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_p_flashback values (4),(5)")
	tk.MustQuery("select a,_tidb_rowid from t_p_flashback order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002"))

	// Test for flashback truncate partition table.
	tk.MustExec("truncate table t_p_flashback")
	tk.MustExec("flashback table t_p_flashback to t_p_flashback1")
	// Check flashback table meta and data record.
	tk.MustQuery("select * from t_p_flashback1 order by a;").Check(testkit.Rows("1", "2", "3", "4", "5"))
	// Check flashback table autoID.
	tk.MustExec("insert into t_p_flashback1 values (6)")
	tk.MustQuery("select a,_tidb_rowid from t_p_flashback1 order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 10001"))

	tk.MustExec("drop database if exists Test2")
	tk.MustExec("create database Test2")
	tk.MustExec("use Test2")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1),(2)")
	tk.MustExec("drop table t")
	tk.MustExec("flashback table t")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table t")
	tk.MustExec("drop database if exists Test3")
	tk.MustExec("create database Test3")
	tk.MustExec("use Test3")
	tk.MustExec("create table t (a int);")
	tk.MustExec("drop table t")
	tk.MustExec("drop database Test3")
	tk.MustExec("use Test2")
	tk.MustExec("flashback table t")
	tk.MustExec("insert into t values (3)")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2", "3"))
}

func TestRecoverTempTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create global temporary table t_recover (a int) on commit delete rows;")

	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists tmp2_recover")
	tk.MustExec("create temporary table tmp2_recover (a int);")

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()
	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	tk.MustExec("drop table t_recover")
	tk.MustGetErrCode("recover table t_recover;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("flashback table t_recover;", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("drop table tmp2_recover")
	tk.MustGetErrMsg("recover table tmp2_recover;", "Can't find localTemporary/dropped/truncated table: tmp2_recover in DDL history jobs")
	tk.MustGetErrMsg("flashback table tmp2_recover;", "Can't find localTemporary/dropped/truncated table: tmp2_recover in DDL history jobs")
}

func TestRecoverTableMeetError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@GLOBAL.tidb_ddl_error_count_limit=3")
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int);")

	timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
	defer resetGC()

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	//set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Should recover, and we can drop it straight away.
	tk.MustExec("recover table t_recover")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table t_recover")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr", `return(1)`))
	tk.MustContainErrMsg("recover table t_recover", "mock update version and tableInfo error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr"))
	tk.MustContainErrMsg("select * from t_recover", "Table 'test_recover.t_recover' doesn't exist")
}

// MockGC is used to make GC work in the test environment.
func MockGC(tk *testkit.TestKit) (string, string, string, func()) {
	originGC := ddlutil.IsEmulatorGCEnable()
	resetGC := func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddlutil.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	return timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC
}
