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
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	parsertypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

const defaultBatchSize = 1024

const dbTestLease = 600 * time.Millisecond

func TestGetTimeZone(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	systemTimeZone := timeutil.SystemLocation().String()

	testCases := []struct {
		tzSQL  string
		tzStr  string
		tzName string
		offset int
		err    string
	}{
		{"set time_zone = '+00:00'", "", "", 0, ""},
		{"set time_zone = '-00:00'", "", "", 0, ""},
		{"set time_zone = 'UTC'", "UTC", "UTC", 0, ""},
		{"set time_zone = '+05:00'", "", "", 18000, ""},
		{"set time_zone = '-08:00'", "", "", -28800, ""},
		{"set time_zone = '+08:00'", "", "", 28800, ""},
		{"set time_zone = 'Asia/Shanghai'", "Asia/Shanghai", "Asia/Shanghai", 0, ""},
		{"set time_zone = 'SYSTEM'", systemTimeZone, systemTimeZone, 0, ""},
		{"set time_zone = DEFAULT", systemTimeZone, systemTimeZone, 0, ""},
		{"set time_zone = 'GMT'", "GMT", "GMT", 0, ""},
		{"set time_zone = 'GMT+1'", "GMT", "GMT", 0, "[variable:1298]Unknown or incorrect time zone: 'GMT+1'"},
		{"set time_zone = 'Etc/GMT+12'", "Etc/GMT+12", "Etc/GMT+12", 0, ""},
		{"set time_zone = 'Etc/GMT-12'", "Etc/GMT-12", "Etc/GMT-12", 0, ""},
		{"set time_zone = 'EST'", "EST", "EST", 0, ""},
		{"set time_zone = 'Australia/Lord_Howe'", "Australia/Lord_Howe", "Australia/Lord_Howe", 0, ""},
	}
	for _, tc := range testCases {
		if tc.err != "" {
			tk.MustGetErrMsg(tc.tzSQL, tc.err)
		} else {
			tk.MustExec(tc.tzSQL)
		}
		require.Equal(t, tc.tzStr, tk.Session().GetSessionVars().TimeZone.String(), fmt.Sprintf("sql: %s", tc.tzSQL))
		tz, offset := ddlutil.GetTimeZone(tk.Session())
		require.Equal(t, tz, tc.tzName, fmt.Sprintf("sql: %s, offset: %d", tc.tzSQL, offset))
		require.Equal(t, offset, tc.offset, fmt.Sprintf("sql: %s", tc.tzSQL))
	}
}

func TestIssue22819(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk1.MustExec("use test;")
	tk1.MustExec("create table t1 (v int) partition by hash (v) partitions 2")
	tk1.MustExec("insert into t1 values (1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test;")
	tk1.MustExec("begin")
	tk1.MustExec("update t1 set v = 2 where v = 1")

	tk2.MustExec("alter table t1 truncate partition p0")

	err := tk1.ExecToErr("commit")
	require.Error(t, err)
	require.Regexp(t, ".*8028.*Information schema is changed during the execution of the statement.*", err.Error())
}

func TestIssue22307(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1);")

	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr1, checkErr2 error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		_, checkErr1 = tk.Exec("update t set a = 3 where b = 1;")
		_, checkErr2 = tk.Exec("update t set a = 3 order by b;")
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "test", "alter table t drop column b;", done)
	err := <-done
	require.NoError(t, err)
	require.EqualError(t, checkErr1, "[planner:1054]Unknown column 'b' in 'where clause'")
	require.EqualError(t, checkErr2, "[planner:1054]Unknown column 'b' in 'order clause'")
}

func TestAddExpressionIndexRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
	tk.MustExec("insert into t1 values (20, 20, 20), (40, 40, 40), (80, 80, 80), (160, 160, 160);")

	var checkErr error
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	d := dom.DDL()
	hook := &callback.TestDDLCallback{Do: dom}
	var currJob *model.Job
	ctx := mock.NewContext()
	ctx.Store = store
	times := 0
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (6, 3, 3) on duplicate key update c1 = 10")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 7 where c2=6;")
			}
			if checkErr == nil {
				_, checkErr = tk1.Exec("delete from t1 where c1 = 40;")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2, 2, 2)")
			if checkErr == nil {
				_, checkErr = tk1.Exec("update t1 set c1 = 3 where c2 = 80")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (4, 4, 4)")
				if checkErr != nil {
					return
				}
				_, checkErr = tk1.Exec("update t1 set c1 = 5 where c2 = 80")
				if checkErr != nil {
					return
				}
				currJob = job
				times++
			}
		}
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(hook)

	tk.MustGetErrMsg("alter table t1 add index expr_idx ((pow(c1, c2)));", "[types:1690]DOUBLE value is out of range in 'pow(160, 160)'")
	require.NoError(t, checkErr)
	tk.MustQuery("select * from t1 order by c1;").Check(testkit.Rows("2 2 2", "4 4 4", "5 80 80", "10 3 3", "20 20 20", "160 160 160"))

	// Check whether the reorg information is cleaned up.
	err := sessiontxn.NewTxn(context.Background(), ctx)
	require.NoError(t, err)
	element, start, end, physicalID, err := ddl.NewReorgHandlerForTest(testkit.NewTestKit(t, store).Session()).GetDDLReorgHandle(currJob)
	require.True(t, meta.ErrDDLReorgElementNotExist.Equal(err))
	require.Nil(t, element)
	require.Nil(t, start)
	require.Nil(t, end)
	require.Equal(t, int64(0), physicalID)
}

func TestDropTableOnTiKVDiskFull(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test_disk_full_drop_table(a int);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcTiKVAllowedOnAlmostFull"))
	}()
	tk.MustExec("drop table test_disk_full_drop_table;")
}

func TestRebaseAutoID(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1"))
	tk.MustExec("alter table tidb.test auto_increment = 6000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1"))
	tk.MustExec("alter table tidb.test auto_increment = 5;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1"))

	// Current range for table test is [11000, 15999].
	// Though it does not have a tuple "a = 15999", its global next auto increment id should be 16000.
	// Anyway it is not compatible with MySQL.
	tk.MustExec("alter table tidb.test auto_increment = 12000;")
	tk.MustExec("insert tidb.test values (null, 1);")
	tk.MustQuery("select * from tidb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1", "16000 1"))

	tk.MustExec("create table tidb.test2 (a int);")
	tk.MustGetErrCode("alter table tidb.test2 add column b int auto_increment key, auto_increment=10;", errno.ErrUnsupportedDDLOperation)
}

func TestProcessColumnFlags(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// check `processColumnFlags()`
	tk.MustExec("create table t(a year(4) comment 'xxx', b year, c bit)")
	defer tk.MustExec("drop table t;")

	check := func(n string, f func(uint) bool) {
		tbl := external.GetTableByName(t, tk, "test", "t")
		for _, col := range tbl.Cols() {
			if strings.EqualFold(col.Name.L, n) {
				require.True(t, f(col.GetFlag()))
				break
			}
		}
	}

	yearcheck := func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && mysql.HasZerofillFlag(f) && !mysql.HasBinaryFlag(f)
	}

	tk.MustExec("alter table t modify a year(4)")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) unsigned")
	check("a", yearcheck)

	tk.MustExec("alter table t modify a year(4) zerofill")

	tk.MustExec("alter table t modify b year")
	check("b", yearcheck)

	tk.MustExec("alter table t modify c bit")
	check("c", func(f uint) bool {
		return mysql.HasUnsignedFlag(f) && !mysql.HasBinaryFlag(f)
	})
}

func TestForbidCacheTableForSystemTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	sysTables := make([]string, 0, 24)
	memOrSysDB := []string{"MySQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "METRICS_SCHEMA", "SYS"}
	for _, db := range memOrSysDB {
		tk.MustExec("use " + db)
		tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
		rows := tk.MustQuery("show tables").Rows()
		for i := 0; i < len(rows); i++ {
			sysTables = append(sysTables, rows[i][0].(string))
		}
		for _, one := range sysTables {
			err := tk.ExecToErr(fmt.Sprintf("alter table `%s` cache", one))
			if db == "MySQL" || db == "SYS" {
				tbl, err1 := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(one))
				require.NoError(t, err1)
				if tbl.Meta().View != nil {
					require.ErrorIs(t, err, dbterror.ErrWrongObject)
				} else {
					require.EqualError(t, err, "[ddl:8200]ALTER table cache for tables in system database is currently unsupported")
				}
			} else {
				require.EqualError(t, err, fmt.Sprintf("[planner:1142]ALTER command denied to user 'root'@'%%' for table '%s'", strings.ToLower(one)))
			}
		}
		sysTables = sysTables[:0]
	}
}

func TestAlterShardRowIDBits(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange"))
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	// Test alter shard_row_id_bits
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 5")
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustExec("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	tk.MustGetErrMsg("alter table t1 SHARD_ROW_ID_BITS = 10;", "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID 72057594037932936 overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := external.GetTableByName(t, tk, "test", "t1")
		require.True(t, tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits)
		require.True(t, tbl.Meta().ShardRowIDBits == shardRowIDBits)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 10")
	tk.MustExec("alter table t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustExec(fmt.Sprintf("alter table t1 auto_increment = %d;", 1<<56))
	tk.MustGetErrMsg("insert into t1 set a=1;", "[autoid:1467]Failed to read auto-increment value from storage engine")
}

func TestDDLJobErrorCount(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ddl_error_table, new_ddl_error_table")
	tk.MustExec("create table ddl_error_table(a int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockErrEntrySizeTooLarge", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockErrEntrySizeTooLarge"))
	}()

	var jobID int64
	hook := &callback.TestDDLCallback{}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		jobID = job.ID
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(hook)
	defer dom.DDL().SetHook(originHook)

	tk.MustGetErrCode("rename table ddl_error_table to new_ddl_error_table", errno.ErrEntryTooLarge)

	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.NotNil(t, historyJob)
	require.Equal(t, int64(1), historyJob.ErrorCount)
	require.True(t, kv.ErrEntryTooLarge.Equal(historyJob.Error))
	tk.MustQuery("select * from ddl_error_table;").Check(testkit.Rows())
}

// TestAddIndexFailOnCaseWhenCanExit is used to close #19325.
func TestAddIndexFailOnCaseWhenCanExit(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockCaseWhenParseFailure", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockCaseWhenParseFailure"))
	}()
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	originalVal := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", originalVal))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustGetErrMsg("alter table t add index idx(b)", "[ddl:-1]job.ErrCount:0, mock unknown type: ast.whenClause.")
	tk.MustExec("drop table if exists t")
}

func TestCreateTableWithIntegerLengthWarning(t *testing.T) {
	// Inject the strict-integer-display-width variable in parser directly.
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() { parsertypes.TiDBStrictIntegerDisplayWidth = false }()
	store := testkit.CreateMockStoreWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a tinyint(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a smallint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a mediumint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a integer(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int1(1))") // Note that int1(1) is tinyint(1) which is boolean-ish
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int2(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int3(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int4(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int8(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 Integer display width is deprecated and will be removed in a future release."))

	tk.MustExec("drop table if exists t")
}
func TestShowCountWarningsOrErrors(t *testing.T) {
	// Inject the strict-integer-display-width variable in parser directly.
	parsertypes.TiDBStrictIntegerDisplayWidth = true
	defer func() { parsertypes.TiDBStrictIntegerDisplayWidth = false }()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test sql run work
	tk.MustExec("show count(*) warnings")
	tk.MustExec("show count(*) errors")

	// test count warnings
	tk.MustExec("drop table if exists t1,t2,t3")
	// Warning: Integer display width is deprecated and will be removed in a future release.
	tk.MustExec("create table t(a int8(2));" +
		"create table t1(a int4(2));" +
		"create table t2(a int4(2));")
	tk.MustQuery("show count(*) warnings").Check(tk.MustQuery("select @@session.warning_count").Rows())

	// test count errors
	tk.MustExec("drop table if exists show_errors")
	tk.MustExec("create table show_errors (a int)")
	// Error: Table exist
	_, _ = tk.Exec("create table show_errors (a int)")
	tk.MustQuery("show count(*) errors").Check(tk.MustQuery("select @@session.error_count").Rows())
}

// Close issue #24172.
// See https://github.com/pingcap/tidb/issues/24172
func TestCancelJobWriteConflict(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")

	tk1.MustExec("create table t(id int)")

	var cancelErr error
	var rs []sqlexec.RecordSet
	hook := &callback.TestDDLCallback{Do: dom}
	d := dom.DDL()
	originalHook := d.GetHook()
	d.SetHook(hook)
	defer d.SetHook(originalHook)

	// Test when cancelling cannot be retried and adding index succeeds.
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn", `return("no_retry")`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn"))
			}()
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockFailedCommandOnConcurencyDDL", `return(true)`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockFailedCommandOnConcurencyDDL"))
			}()

			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			rs, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustExec("alter table t add index (id)")
	require.EqualError(t, cancelErr, "mock failed admin command on ddl jobs")

	// Test when cancelling is retried only once and adding index is cancelled in the end.
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization {
			jobID = job.ID
			stmt := fmt.Sprintf("admin cancel ddl jobs %d", job.ID)
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn", `return("retry_once")`))
			defer func() {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn"))
			}()
			rs, cancelErr = tk2.Session().Execute(context.Background(), stmt)
		}
	}
	tk1.MustGetErrCode("alter table t add index (id)", errno.ErrCancelledDDLJob)
	require.NoError(t, cancelErr)
	result := tk2.ResultSetToResultWithCtx(context.Background(), rs[0], "cancel ddl job fails")
	result.Check(testkit.Rows(fmt.Sprintf("%d successful", jobID)))
}

func TestTxnSavepointWithDDL(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set global tidb_enable_metadata_lock=0")
	tk2.MustExec("use test;")

	prepareFn := func() {
		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1 (c1 int primary key, c2 int)")
		tk.MustExec("create table t2 (c1 int primary key, c2 int)")
	}
	prepareFn()

	tk.MustExec("begin pessimistic")
	tk.MustExec("savepoint s1")
	tk.MustExec("insert t1 values (1, 11)")
	tk.MustExec("rollback to s1")
	tk2.MustExec("alter table t1 add index idx2(c2)")
	tk.MustExec("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("admin check table t1")

	tk.MustExec("begin pessimistic")
	tk.MustExec("savepoint s1")
	tk.MustExec("insert t1 values (1, 11)")
	tk.MustExec("savepoint s2")
	tk.MustExec("insert t2 values (1, 11)")
	tk.MustExec("rollback to s2")
	tk2.MustExec("alter table t2 add index idx2(c2)")
	tk.MustExec("commit")
	tk.MustQuery("select * from t2").Check(testkit.Rows())
	tk.MustExec("admin check table t1, t2")

	prepareFn()
	tk.MustExec("truncate table t1")
	tk.MustExec("begin pessimistic")
	tk.MustExec("savepoint s1")
	tk.MustExec("insert t1 values (1, 11)")
	tk.MustExec("savepoint s2")
	tk.MustExec("insert t2 values (1, 11)")
	tk.MustExec("rollback to s2")
	tk2.MustExec("alter table t1 add index idx2(c2)")
	tk2.MustExec("alter table t2 add index idx2(c2)")
	err := tk.ExecToErr("commit")
	require.Error(t, err)
	require.Regexp(t, ".*8028.*Information schema is changed during the execution of the statement.*", err.Error())
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustExec("admin check table t1, t2")
}

func TestSnapshotVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk := testkit.NewTestKit(t, store)

	dd := dom.DDL()
	ddl.DisableTiFlashPoll(dd)
	require.Equal(t, dbTestLease, dd.GetLease())

	snapTS := oracle.GoTimeToTS(time.Now())
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(a int)")

	is := dom.InfoSchema()
	require.NotNil(t, is)

	// For updating the self schema version.
	goCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, 0, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	snapIs, err := dom.GetSnapshotInfoSchema(snapTS)
	require.NotNil(t, snapIs)
	require.NoError(t, err)

	// Make sure that the self schema version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, 0, is.SchemaMetaVersion())
	cancel()
	require.NoError(t, err)

	// for GetSnapshotInfoSchema
	currSnapTS := oracle.GoTimeToTS(time.Now())
	currSnapIs, err := dom.GetSnapshotInfoSchema(currSnapTS)
	require.NoError(t, err)
	require.NotNil(t, currSnapTS)
	require.Equal(t, is.SchemaMetaVersion(), currSnapIs.SchemaMetaVersion())

	// for GetSnapshotMeta
	dbInfo, ok := currSnapIs.SchemaByName(model.NewCIStr("test2"))
	require.True(t, ok)

	tbl, err := currSnapIs.TableByName(model.NewCIStr("test2"), model.NewCIStr("t"))
	require.NoError(t, err)

	m, err := dom.GetSnapshotMeta(snapTS)
	require.NoError(t, err)

	tblInfo1, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.True(t, meta.ErrDBNotExists.Equal(err))
	require.Nil(t, tblInfo1)

	m, err = dom.GetSnapshotMeta(currSnapTS)
	require.NoError(t, err)

	tblInfo2, err := m.GetTable(dbInfo.ID, tbl.Meta().ID)
	require.NoError(t, err)
	require.Equal(t, tblInfo2, tbl.Meta())
}

func TestSchemaValidator(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	tk := testkit.NewTestKit(t, store)

	dd := dom.DDL()
	ddl.DisableTiFlashPoll(dd)
	require.Equal(t, dbTestLease, dd.GetLease())

	tk.MustExec("create table test.t(a int)")

	err := dom.Reload()
	require.NoError(t, err)
	schemaVer := dom.InfoSchema().SchemaMetaVersion()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)

	ts := ver.Ver
	_, res := dom.SchemaValidator.Check(ts, schemaVer, nil, true)
	require.Equal(t, domain.ResultSucc, res)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed", `return(true)`))

	err = dom.Reload()
	require.Error(t, err)
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil, true)
	require.Equal(t, domain.ResultSucc, res)
	time.Sleep(dbTestLease)

	ver, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	ts = ver.Ver
	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil, true)
	require.Equal(t, domain.ResultUnknown, res)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/ErrorMockReloadFailed"))
	err = dom.Reload()
	require.NoError(t, err)

	_, res = dom.SchemaValidator.Check(ts, schemaVer, nil, true)
	require.Equal(t, domain.ResultSucc, res)

	// For schema check, it tests for getting the result of "ResultUnknown".
	is := dom.InfoSchema()
	schemaChecker := domain.NewSchemaChecker(dom, is.SchemaMetaVersion(), nil, true)
	// Make sure it will retry one time and doesn't take a long time.
	domain.SchemaOutOfDateRetryTimes.Store(1)
	domain.SchemaOutOfDateRetryInterval.Store(time.Millisecond * 1)
	dom.SchemaValidator.Stop()
	_, err = schemaChecker.Check(uint64(123456))
	require.EqualError(t, err, domain.ErrInfoSchemaExpired.Error())
}

func TestLogAndShowSlowLog(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "bbb", Duration: 3 * time.Second, SessAlias: "alias1"})
	dom.LogSlowQuery(&domain.SlowQueryInfo{SQL: "ccc", Duration: 2 * time.Second})
	// Collecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	result := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, "alias1", result[0].SessAlias)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)
	require.Empty(t, result[1].SessAlias)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, Kind: ast.ShowSlowKindInternal})
	require.Len(t, result, 1)
	require.Equal(t, "aaa", result[0].SQL)
	require.Equal(t, time.Second, result[0].Duration)
	require.True(t, result[0].Internal)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, Kind: ast.ShowSlowKindAll})
	require.Len(t, result, 3)
	require.Equal(t, "bbb", result[0].SQL)
	require.Equal(t, 3*time.Second, result[0].Duration)
	require.Equal(t, "alias1", result[0].SessAlias)
	require.Equal(t, "ccc", result[1].SQL)
	require.Equal(t, 2*time.Second, result[1].Duration)
	require.Empty(t, result[1].SessAlias)
	require.Equal(t, "aaa", result[2].SQL)
	require.Equal(t, time.Second, result[2].Duration)
	require.True(t, result[2].Internal)
	require.Empty(t, result[2].SessAlias)

	result = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	require.Len(t, result, 2)
	require.Equal(t, "ccc", result[0].SQL)
	require.Equal(t, 2*time.Second, result[0].Duration)
	require.Empty(t, result[0].SessAlias)
	require.Equal(t, "bbb", result[1].SQL)
	require.Equal(t, 3*time.Second, result[1].Duration)
	require.Equal(t, "alias1", result[1].SessAlias)
}

func TestReportingMinStartTimestamp(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, dbTestLease)

	infoSyncer := dom.InfoSyncer()
	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	infoSyncer.SetSessionManager(sm)
	beforeTS := oracle.GoTimeToTS(time.Now())
	infoSyncer.ReportMinStartTS(dom.Store())
	afterTS := oracle.GoTimeToTS(time.Now())
	require.False(t, infoSyncer.GetMinStartTS() > beforeTS && infoSyncer.GetMinStartTS() < afterTS)

	now := time.Now()
	validTS := oracle.GoTimeToLowerLimitStartTS(now.Add(time.Minute), tikv.MaxTxnTimeUse)
	lowerLimit := oracle.GoTimeToLowerLimitStartTS(now, tikv.MaxTxnTimeUse)
	sm.PS = []*util.ProcessInfo{
		{CurTxnStartTS: 0},
		{CurTxnStartTS: math.MaxUint64},
		{CurTxnStartTS: lowerLimit},
		{CurTxnStartTS: validTS},
	}
	infoSyncer.SetSessionManager(sm)
	infoSyncer.ReportMinStartTS(dom.Store())
	require.Equal(t, validTS, infoSyncer.GetMinStartTS())
}

// for issue #34931
func TestBuildMaxLengthIndexWithNonRestrictedSqlMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	maxIndexLength := config.GetGlobalConfig().MaxIndexLength

	tt := []struct {
		ColType           string
		SpecifiedColLen   bool
		SpecifiedIndexLen bool
	}{
		{
			"text",
			false,
			true,
		},
		{
			"blob",
			false,
			true,
		},
		{
			"varchar",
			true,
			false,
		},
		{
			"varbinary",
			true,
			false,
		},
	}

	sqlTemplate := "create table %s (id int, name %s, age int, %s index(name%s%s)) charset=%s;"
	// test character strings for varchar and text
	for _, tc := range tt {
		for _, cs := range charset.CharacterSetInfos {
			tableName := fmt.Sprintf("t_%s", cs.Name)
			tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
			tk.MustExec("set @@sql_mode=default")

			// test in strict sql mode
			maxLen := cs.Maxlen
			if tc.ColType == "varbinary" || tc.ColType == "blob" {
				maxLen = 1
			}
			expectKeyLength := maxIndexLength / maxLen
			length := 2 * expectKeyLength

			indexLen := ""
			// specify index length for text type
			if tc.SpecifiedIndexLen {
				indexLen = fmt.Sprintf("(%d)", length)
			}

			col := tc.ColType
			// specify column length for varchar type
			if tc.SpecifiedColLen {
				col += fmt.Sprintf("(%d)", length)
			}
			sql := fmt.Sprintf(sqlTemplate,
				tableName, col, "", indexLen, "", cs.Name)
			tk.MustGetErrCode(sql, errno.ErrTooLongKey)

			tk.MustExec("set @@sql_mode=''")

			err := tk.ExecToErr(sql)
			require.NoErrorf(t, err, "exec sql '%s' failed", sql)

			require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())

			warnErr := tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err
			tErr := errors.Cause(warnErr).(*terror.Error)
			sqlErr := terror.ToSQLError(tErr)
			require.Equal(t, errno.ErrTooLongKey, int(sqlErr.Code))

			if cs.Name == charset.CharsetBin {
				if tc.ColType == "varchar" || tc.ColType == "varbinary" {
					col = fmt.Sprintf("varbinary(%d)", length)
				} else {
					col = "blob"
				}
			}
			rows := fmt.Sprintf("%s CREATE TABLE `%s` (\n  `id` int(11) DEFAULT NULL,\n  `name` %s DEFAULT NULL,\n  `age` int(11) DEFAULT NULL,\n  KEY `name` (`name`(%d))\n) ENGINE=InnoDB DEFAULT CHARSET=%s",
				tableName, tableName, col, expectKeyLength, cs.Name)
			// add collation for binary charset
			if cs.Name != charset.CharsetBin {
				rows += fmt.Sprintf(" COLLATE=%s", cs.DefaultCollation)
			}

			tk.MustQuery(fmt.Sprintf("show create table %s", tableName)).Check(testkit.Rows(rows))

			ukTable := fmt.Sprintf("t_%s_uk", cs.Name)
			mkTable := fmt.Sprintf("t_%s_mk", cs.Name)
			tk.MustExec(fmt.Sprintf("drop table if exists %s", ukTable))
			tk.MustExec(fmt.Sprintf("drop table if exists %s", mkTable))

			// For a unique index, an error occurs regardless of SQL mode because reducing
			//the index length might enable insertion of non-unique entries that do not meet
			//the specified uniqueness requirement.
			sql = fmt.Sprintf(sqlTemplate, ukTable, col, "unique", indexLen, "", cs.Name)
			tk.MustGetErrCode(sql, errno.ErrTooLongKey)

			// The multiple column index in which the length sum exceeds the maximum size
			// will return an error instead produce a warning in strict sql mode.
			indexLen = fmt.Sprintf("(%d)", expectKeyLength)
			sql = fmt.Sprintf(sqlTemplate, mkTable, col, "", indexLen, ", age", cs.Name)
			tk.MustGetErrCode(sql, errno.ErrTooLongKey)
		}
	}
}

func TestTiDBDownBeforeUpdateGlobalVersion(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDownBeforeUpdateGlobalVersion", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/checkDownBeforeUpdateGlobalVersion", `return(true)`))
	tk.MustExec("alter table t add column b int")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDownBeforeUpdateGlobalVersion"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/checkDownBeforeUpdateGlobalVersion"))
}

func TestDDLBlockedCreateView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")

	hook := &callback.TestDDLCallback{Do: dom}
	first := true
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		if !first {
			return
		}
		first = false
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("create view v as select * from t")
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column a char(10)")
}

func TestHashPartitionAddColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int) partition by hash(a) partitions 4")

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateWriteOnly {
			return
		}
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("delete from t")
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t add column c int")
}

func TestSetInvalidDefaultValueAfterModifyColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")

	var wg sync.WaitGroup
	var checkErr error
	one := false
	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.SchemaState != model.StateDeleteOnly {
			return
		}
		if one {
			return
		}
		one = true
		wg.Add(1)
		go func() {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			_, checkErr = tk2.Exec("alter table t alter column a set default 1")
			wg.Done()
		}()
	}
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column a text(100)")
	wg.Wait()
	require.EqualError(t, checkErr, "[ddl:1101]BLOB/TEXT/JSON column 'a' can't have a default value")
}

func TestMDLTruncateTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")
	tk.MustExec("begin")
	tk.MustExec("select * from t for update")

	var wg sync.WaitGroup

	hook := &callback.TestDDLCallback{Do: dom}
	wg.Add(2)
	var timetk2 time.Time
	var timetk3 time.Time

	one := false
	f := func(job *model.Job) {
		if one {
			return
		}
		one = true
		go func() {
			tk3.MustExec("truncate table test.t")
			timetk3 = time.Now()
			wg.Done()
		}()
	}

	hook.OnJobUpdatedExported.Store(&f)
	dom.DDL().SetHook(hook)

	go func() {
		tk2.MustExec("truncate table test.t")
		timetk2 = time.Now()
		wg.Done()
	}()

	time.Sleep(2 * time.Second)
	timeMain := time.Now()
	tk.MustExec("commit")
	wg.Wait()
	require.True(t, timetk2.After(timeMain))
	require.True(t, timetk3.After(timeMain))
}

func TestTruncateTableAndSchemaDependence(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int);")

	var wg sync.WaitGroup
	wg.Add(2)

	var timetk2 time.Time
	var timetk3 time.Time

	first := false
	f := func(job *model.Job) {
		if first || job.Type != model.ActionTruncateTable {
			return
		}
		first = true
		go func() {
			tk3.MustExec("drop database test")
			timetk3 = time.Now()
			wg.Done()
		}()
		time.Sleep(3 * time.Second)
	}

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobUpdatedExported.Store(&f)
	dom.DDL().SetHook(hook)

	go func() {
		tk2.MustExec("truncate table test.t")
		timetk2 = time.Now()
		wg.Done()
	}()

	wg.Wait()
	require.True(t, timetk3.After(timetk2))
}

func TestInsertIgnore(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a smallint(6) DEFAULT '-13202', b varchar(221) NOT NULL DEFAULT 'duplicatevalue', " +
		"c tinyint(1) NOT NULL DEFAULT '0', PRIMARY KEY (c, b));")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	d := dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	callback := &callback.TestDDLCallback{}

	onJobUpdatedExportedFunc := func(job *model.Job) {
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, err := tk1.Exec("INSERT INTO t VALUES (-18585,'aaa',1), (-18585,'0',1), (-18585,'1',1), (-18585,'duplicatevalue',1);")
			assert.NoError(t, err)
		case model.StateWriteReorganization:
			idx := testutil.FindIdxInfo(dom, "test", "t", "idx")
			if idx.BackfillState == model.BackfillStateReadyToMerge {
				_, err := tk1.Exec("insert ignore into `t`  values ( 234,'duplicatevalue',-2028 );")
				assert.NoError(t, err)
				return
			}
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(callback)

	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
}

func TestDDLJobErrEntrySizeTooLarge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockErrEntrySizeTooLarge", `1*return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockErrEntrySizeTooLarge"))
	})

	tk.MustGetErrCode("rename table t to t1;", errno.ErrEntryTooLarge)
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("alter table t add column b int;") // Should not block.
}
