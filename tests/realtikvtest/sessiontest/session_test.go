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

package sessiontest

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSysdateIsNow(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now OFF"))
	require.False(t, tk.Session().GetSessionVars().SysdateIsNow)
	tk.MustExec("set @@tidb_sysdate_is_now=true")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now ON"))
	require.True(t, tk.Session().GetSessionVars().SysdateIsNow)
}

func TestEnableLegacyInstanceScope(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// enable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 1")
	tk.MustExec("set tidb_general_log = 1")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_general_log will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
	require.True(t, tk.Session().GetSessionVars().EnableLegacyInstanceScope)

	// disable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 0")
	tk.MustGetErrCode("set tidb_general_log = 1", errno.ErrGlobalVariable)
	require.False(t, tk.Session().GetSessionVars().EnableLegacyInstanceScope)
}

func TestSetPDClientDynamicOption(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 0.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	require.Error(t, tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -0.1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-0.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10.1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '10.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 11;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '11'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))

	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = on;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = off;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;"))
}

func TestSameNameObjectWithLocalTemporaryTable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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

func TestWriteOnMultipleCachedTable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ct1, ct2")
	tk.MustExec("create table ct1 (id int, c int)")
	tk.MustExec("create table ct2 (id int, c int)")
	tk.MustExec("alter table ct1 cache")
	tk.MustExec("alter table ct2 cache")
	tk.MustQuery("select * from ct1").Check(testkit.Rows())
	tk.MustQuery("select * from ct2").Check(testkit.Rows())

	lastReadFromCache := func(tk *testkit.TestKit) bool {
		return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	}

	cached := false
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from ct1")
		if lastReadFromCache(tk) {
			cached = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, cached)

	tk.MustExec("begin")
	tk.MustExec("insert into ct1 values (3, 4)")
	tk.MustExec("insert into ct2 values (5, 6)")
	tk.MustExec("commit")

	tk.MustQuery("select * from ct1").Check(testkit.Rows("3 4"))
	tk.MustQuery("select * from ct2").Check(testkit.Rows("5 6"))

	// cleanup
	tk.MustExec("alter table ct1 nocache")
	tk.MustExec("alter table ct2 nocache")
}

func TestForbidSettingBothTSVariable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	// For mock tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set tidb_snapshot and assert tidb_read_staleness
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
	tk.MustGetErrMsg("set @@tidb_read_staleness='-5'", "tidb_snapshot should be clear before setting tidb_read_staleness")
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustExec("set @@tidb_read_staleness='-5'")

	// Set tidb_read_staleness and assert tidb_snapshot
	tk.MustExec("set @@tidb_read_staleness='-5'")
	tk.MustGetErrMsg("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'", "tidb_read_staleness should be clear before setting tidb_snapshot")
	tk.MustExec("set @@tidb_read_staleness = ''")
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
}

func TestTiDBReadStaleness(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_read_staleness='-5'")
	tk.MustExec("set @@tidb_read_staleness='-100'")
	err := tk.ExecToErr("set @@tidb_read_staleness='-5s'")
	require.Error(t, err)
	err = tk.ExecToErr("set @@tidb_read_staleness='foo'")
	require.Error(t, err)
	tk.MustExec("set @@tidb_read_staleness=''")
	tk.MustExec("set @@tidb_read_staleness='0'")
}

func TestFixSetTiDBSnapshotTS(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("create database t123")
	time.Sleep(time.Second)
	ts := time.Now().Format("2006-1-2 15:04:05")
	time.Sleep(time.Second)
	tk.MustExec("drop database t123")
	tk.MustMatchErrMsg("use t123", ".*Unknown database.*")
	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot='%s'", ts))
	tk.MustExec("use t123")
	// update any session variable and assert whether infoschema is changed
	tk.MustExec("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';")
	tk.MustExec("use t123")
}

func TestSetVarHint(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_mode", mysql.DefaultSQLMode))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_mode=ALLOW_INVALID_DATES) */ @@sql_mode;").Check(testkit.Rows("ALLOW_INVALID_DATES"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_mode;").Check(testkit.Rows(mysql.DefaultSQLMode))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tmp_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=1024) */ @@tmp_table_size;").Check(testkit.Rows("1024"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("range_alloc_block_size", "4096"))
	tk.MustQuery("SELECT /*+ SET_VAR(range_alloc_block_size=4294967295) */ @@range_alloc_block_size;").Check(testkit.Rows("4294967295"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@range_alloc_block_size;").Check(testkit.Rows("4096"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_execution_time", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_execution_time=1) */ @@max_execution_time;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_execution_time;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("time_zone", "SYSTEM"))
	tk.MustQuery("SELECT /*+ SET_VAR(time_zone='+12:00') */ @@time_zone;").Check(testkit.Rows("+12:00"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@time_zone;").Check(testkit.Rows("SYSTEM"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("join_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(join_buffer_size=128) */ @@join_buffer_size;").Check(testkit.Rows("128"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@join_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_length_for_sort_data", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_length_for_sort_data=4) */ @@max_length_for_sort_data;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_length_for_sort_data;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_error_count", "64"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_error_count=0) */ @@max_error_count;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_error_count;").Check(testkit.Rows("64"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_buffer_result", "OFF"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_buffer_result=ON) */ @@sql_buffer_result;").Check(testkit.Rows("ON"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_buffer_result;").Check(testkit.Rows("OFF"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_heap_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_heap_table_size=16384) */ @@max_heap_table_size;").Check(testkit.Rows("16384"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_heap_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tmp_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=16384) */ @@tmp_table_size;").Check(testkit.Rows("16384"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("div_precision_increment", "4"))
	tk.MustQuery("SELECT /*+ SET_VAR(div_precision_increment=0) */ @@div_precision_increment;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@div_precision_increment;").Check(testkit.Rows("4"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_auto_is_null", "OFF"))
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_noop_functions", "ON"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_noop_functions", "OFF"))
	tk.MustQuery("SELECT @@sql_auto_is_null;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sort_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(sort_buffer_size=32768) */ @@sort_buffer_size;").Check(testkit.Rows("32768"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sort_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_join_size", "18446744073709551615"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_join_size=1) */ @@max_join_size;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_join_size;").Check(testkit.Rows("18446744073709551615"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_seeks_for_key", "18446744073709551615"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_seeks_for_key=1) */ @@max_seeks_for_key;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_seeks_for_key;").Check(testkit.Rows("18446744073709551615"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_sort_length", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_sort_length=4) */ @@max_sort_length;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_sort_length;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("bulk_insert_buffer_size", "8388608"))
	tk.MustQuery("SELECT /*+ SET_VAR(bulk_insert_buffer_size=0) */ @@bulk_insert_buffer_size;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@bulk_insert_buffer_size;").Check(testkit.Rows("8388608"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_big_selects", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_big_selects=0) */ @@sql_big_selects;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_big_selects;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("read_rnd_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(read_rnd_buffer_size=1) */ @@read_rnd_buffer_size;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@read_rnd_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("unique_checks", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(unique_checks=0) */ @@unique_checks;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@unique_checks;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("read_buffer_size", "131072"))
	tk.MustQuery("SELECT /*+ SET_VAR(read_buffer_size=8192) */ @@read_buffer_size;").Check(testkit.Rows("8192"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@read_buffer_size;").Check(testkit.Rows("131072"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("default_tmp_storage_engine", "InnoDB"))
	tk.MustQuery("SELECT /*+ SET_VAR(default_tmp_storage_engine='CSV') */ @@default_tmp_storage_engine;").Check(testkit.Rows("CSV"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@default_tmp_storage_engine;").Check(testkit.Rows("InnoDB"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("optimizer_search_depth", "62"))
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_search_depth=1) */ @@optimizer_search_depth;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@optimizer_search_depth;").Check(testkit.Rows("62"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_points_in_geometry", "65536"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_points_in_geometry=3) */ @@max_points_in_geometry;").Check(testkit.Rows("3"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_points_in_geometry;").Check(testkit.Rows("65536"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("updatable_views_with_limit", "YES"))
	tk.MustQuery("SELECT /*+ SET_VAR(updatable_views_with_limit=0) */ @@updatable_views_with_limit;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@updatable_views_with_limit;").Check(testkit.Rows("YES"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("optimizer_prune_level", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_prune_level=0) */ @@optimizer_prune_level;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@optimizer_prune_level;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("group_concat_max_len", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(group_concat_max_len=4) */ @@group_concat_max_len;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@group_concat_max_len;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("eq_range_index_dive_limit", "200"))
	tk.MustQuery("SELECT /*+ SET_VAR(eq_range_index_dive_limit=0) */ @@eq_range_index_dive_limit;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@eq_range_index_dive_limit;").Check(testkit.Rows("200"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_safe_updates", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_safe_updates;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("end_markers_in_json", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(end_markers_in_json=1) */ @@end_markers_in_json;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@end_markers_in_json;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("windowing_use_high_precision", "ON"))
	tk.MustQuery("SELECT /*+ SET_VAR(windowing_use_high_precision=OFF) */ @@windowing_use_high_precision;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@windowing_use_high_precision;").Check(testkit.Rows("1"))

	tk.MustExec("SELECT /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(max_heap_table_size = 1G) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)

	tk.MustExec("SELECT /*+ SET_VAR(collation_server = 'utf8') */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3637]Variable 'collation_server' cannot be set using SET_VAR hint.")

	tk.MustExec("SELECT /*+ SET_VAR(max_size = 1G) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3128]Unresolved name 'max_size' for SET_VAR hint")

	tk.MustExec("SELECT /*+ SET_VAR(group_concat_max_len = 1024) SET_VAR(group_concat_max_len = 2048) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3126]Hint SET_VAR(group_concat_max_len=2048) is ignored as conflicting/duplicated.")
}

func TestPrepareZero(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v timestamp)")
	tk.MustExec("prepare s1 from 'insert into t (v) values (?)'")
	tk.MustExec("set @v1='0'")
	require.Error(t, tk.ExecToErr("execute s1 using @v1"))
	tk.MustExec("set @v2='" + types.ZeroDatetimeStr + "'")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustExec("execute s1 using @v2")
	tk.MustQuery("select v from t").Check(testkit.Rows("0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")
}

func TestPrimaryKeyAutoIncrement(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	tk.MustExec("insert t (name) values (?)", "abc")
	id := tk.Session().LastInsertID()
	require.NotZero(t, id)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc <nil>", id)))

	tk.MustExec("update t set name = 'abc', status = 1 where id = ?", id)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc 1", id)))

	// Check for pass bool param to tidb prepared statement
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id tinyint)")
	tk.MustExec("insert t values (?)", true)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

// TestSetGroupConcatMaxLen is for issue #7034
func TestSetGroupConcatMaxLen(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Normal case
	tk.MustExec("set global group_concat_max_len = 100")
	tk.MustExec("set @@session.group_concat_max_len = 50")
	result := tk.MustQuery("show global variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 100"))

	result = tk.MustQuery("show session variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 50"))

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	tk.MustExec("set @@group_concat_max_len = 1024")

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	// Test value out of range
	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	_, err := tk.Exec("set @@group_concat_max_len = 18446744073709551616")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	// Test illegal type
	_, err = tk.Exec("set @@group_concat_max_len='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))
}

func TestLocalTemporaryTableInsertIgnore(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(5, 15, 105)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=5").Check(testkit.Rows("5 15 105"))

	// test in transaction and rollback
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=1").Check(testkit.Rows("1 11 101"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'PRIMARY'"))
	tk.MustQuery("select * from tmp1 where id=3").Check(testkit.Rows("3 13 103"))
	tk.MustExec("rollback")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "5 15 105"))

	// test commit
	tk.MustExec("begin")
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustExec("insert ignore into tmp1 values(3, 13, 103)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert ignore into tmp1 values(3, 100, 1000)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '3' for key 'PRIMARY'"))
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 102", "3 13 103", "5 15 105"))
}

func TestLocalTemporaryTableInsertOnDuplicateKeyUpdate(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create temporary table tmp1 (id int primary key auto_increment, u int unique, v int)")
	tk.MustExec("insert into tmp1 values(1, 11, 101)")
	tk.MustExec("insert into tmp1 values(2, 12, 102)")

	// test outside transaction
	tk.MustExec("insert ignore into tmp1 values(1, 100, 1000) on duplicate key update u=12")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '12' for key 'u'"))
	tk.MustExec("insert into tmp1 values(2, 100, 1000) on duplicate key update v=302")
	tk.MustExec("insert into tmp1 values(4, 14, 104) on duplicate key update v=204")
	tk.MustExec("commit")
	tk.MustQuery("select * from tmp1").Check(testkit.Rows("1 11 101", "2 12 302", "3 13 103", "4 14 104"))
}

func TestLocalTemporaryTableReplace(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

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

func TestRetryForCurrentTxn(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table history (a int)")
	tk.MustExec("insert history values (1)")

	// Firstly, enable retry.
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("update history set a = 2")
	// Disable retry now.
	tk.MustExec("set tidb_disable_txn_auto_retry = 1")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("update history set a = 3")

	tk.MustExec("commit")
	tk.MustQuery("select * from history").Check(testkit.Rows("2"))
}

// TestTruncateAlloc tests that the auto_increment ID does not reuse the old table's allocator.
func TestTruncateAlloc(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table truncate_id (a int primary key auto_increment)")
	tk.MustExec("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustExec("truncate table truncate_id")
	tk.MustExec("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustQuery("select a from truncate_id where a > 11").Check(testkit.Rows())
}

func TestString(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	t.Log(tk.Session().String())
}

func TestDatabase(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test database.
	tk.MustExec("create database xxx")
	tk.MustExec("drop database xxx")

	tk.MustExec("drop database if exists xxx")
	tk.MustExec("create database xxx")
	tk.MustExec("create database if not exists xxx")
	tk.MustExec("drop database if exists xxx")

	// Test schema.
	tk.MustExec("create schema xxx")
	tk.MustExec("drop schema xxx")

	tk.MustExec("drop schema if exists xxx")
	tk.MustExec("create schema xxx")
	tk.MustExec("create schema if not exists xxx")
	tk.MustExec("drop schema if exists xxx")
}

func TestSkipWithGrant(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	save2 := privileges.SkipWithGrant

	privileges.SkipWithGrant = false
	require.False(t, tk.Session().Auth(&auth.UserIdentity{Username: "user_not_exist"}, []byte("yyy"), []byte("zzz")))

	privileges.SkipWithGrant = true
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "xxx", Hostname: `%`}, []byte("yyy"), []byte("zzz")))
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, []byte(""), []byte("")))
	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create role r_1")
	tk.MustExec("grant r_1 to root")
	tk.MustExec("set role all")
	tk.MustExec("show grants for root")
	privileges.SkipWithGrant = save2
}

func TestParseWithParams(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	exec := se.(sqlexec.RestrictedSQLExecutor)

	// test compatibility with ExcuteInternal
	_, err := exec.ParseWithParams(context.TODO(), "SELECT 4")
	require.NoError(t, err)

	// test charset attack
	stmt, err := exec.ParseWithParams(context.TODO(), "SELECT * FROM test WHERE name = %? LIMIT 1", "\xbf\x27 OR 1=1 /*")
	require.NoError(t, err)

	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.RestoreStringDoubleQuotes, &sb)
	err = stmt.Restore(ctx)
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM test WHERE name=_utf8mb4\"\xbf' OR 1=1 /*\" LIMIT 1", sb.String())

	// test invalid sql
	_, err = exec.ParseWithParams(context.TODO(), "SELECT")
	require.Regexp(t, ".*You have an error in your SQL syntax.*", err)

	// test invalid arguments to escape
	_, err = exec.ParseWithParams(context.TODO(), "SELECT %?, %?", 3)
	require.Regexp(t, "missing arguments.*", err)

	// test noescape
	stmt, err = exec.ParseWithParams(context.TODO(), "SELECT 3")
	require.NoError(t, err)

	sb.Reset()
	ctx = format.NewRestoreCtx(0, &sb)
	err = stmt.Restore(ctx)
	require.NoError(t, err)
	require.Equal(t, "SELECT 3", sb.String())
}

func TestStatementCountLimit(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table stmt_count_limit (id int)")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	_, err := tk.Exec("insert into stmt_count_limit values (3)")
	require.Error(t, err)

	// begin is counted into history but this one is not.
	tk.MustExec("SET SESSION autocommit = false")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	tk.MustExec("insert into stmt_count_limit values (3)")
	_, err = tk.Exec("insert into stmt_count_limit values (4)")
	require.Error(t, err)
}

func TestBatchCommit(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_batch_commit = 1")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("create table t (id int)")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk.MustExec("SET SESSION autocommit = 1")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (2)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("rollback")
	tk1.MustQuery("select * from t").Check(testkit.Rows())

	// The above rollback will not make the session in transaction.
	tk.MustExec("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (5)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (6)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values (7)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))

	// The session is still in transaction.
	tk.MustExec("insert into t values (8)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("insert into t values (9)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("insert into t values (10)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustExec("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10"))

	// The above commit will not make the session in transaction.
	tk.MustExec("insert into t values (11)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10", "11"))

	tk.MustExec("delete from t")
	tk.MustExec("SET SESSION autocommit = 0")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("insert into t values (3)")
	tk.MustExec("rollback")
	tk1.MustExec("insert into t values (4)")
	tk1.MustExec("insert into t values (5)")
	tk.MustQuery("select * from t").Check(testkit.Rows("4", "5"))
}

func TestKVVars(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_backoff_lock_fast = 1")
	tk.MustExec("set @@tidb_backoff_weight = 100")
	tk.MustExec("create table if not exists kvvars (a int key)")
	tk.MustExec("insert into kvvars values (1)")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	vars := txn.GetVars().(*tikv.Variables)
	require.Equal(t, 1, vars.BackoffLockFast)
	require.Equal(t, 100, vars.BackOffWeight)
	tk.MustExec("rollback")
	tk.MustExec("set @@tidb_backoff_weight = 50")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("select * from kvvars")
	require.True(t, tk.Session().GetSessionVars().InTxn())
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	vars = txn.GetVars().(*tikv.Variables)
	require.Equal(t, 50, vars.BackOffWeight)

	tk.MustExec("set @@autocommit = 1")
	require.Nil(t, failpoint.Enable("tikvclient/probeSetVars", `return(true)`))
	tk.MustExec("select * from kvvars where a = 1")
	require.Nil(t, failpoint.Disable("tikvclient/probeSetVars"))
	require.True(t, transaction.SetSuccess)
	transaction.SetSuccess = false
}

func TestTxnRetryErrMsg(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("begin")
	tk2.MustExec("use test")
	tk2.MustExec("update no_retry set id = id + 1")
	tk1.MustExec("update no_retry set id = id + 1")
	require.NoError(t, failpoint.Enable("tikvclient/mockRetryableErrorResp", `return(true)`))
	_, err := tk1.Session().Execute(context.Background(), "commit")
	require.NoError(t, failpoint.Disable("tikvclient/mockRetryableErrorResp"))
	require.Error(t, err)
	require.True(t, kv.ErrTxnRetryable.Equal(err), "error: %s", err)
	require.True(t, strings.Contains(err.Error(), "mock retryable error"), "error: %s", err)
	require.True(t, strings.Contains(err.Error(), kv.TxnRetryableMark), "error: %s", err)
}

func TestSetTxnScope(t *testing.T) {
	// Check the default value of @@tidb_enable_local_txn and @@txn_scope whitout configuring the zone label.
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Check the default value of @@tidb_enable_local_txn and @@txn_scope with configuring the zone label.
	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`))
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))

	// @@tidb_enable_local_txn is off without configuring the zone label.
	tk = testkit.NewTestKit(t, store)
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to local.
	err := tk.ExecToErr("set @@txn_scope = 'local';")
	require.Error(t, err)
	require.Regexp(t, `.*txn_scope can not be set to local when tidb_enable_local_txn is off.*`, err)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())

	// @@tidb_enable_local_txn is off with configuring the zone label.
	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`))
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@global.tidb_enable_local_txn;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to local.
	err = tk.ExecToErr("set @@txn_scope = 'local';")
	require.Error(t, err)
	require.Regexp(t, `.*txn_scope can not be set to local when tidb_enable_local_txn is off.*`, err)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))

	// @@tidb_enable_local_txn is on without configuring the zone label.
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to local.
	err = tk.ExecToErr("set @@txn_scope = 'local';")
	require.Error(t, err)
	require.Regexp(t, `.*txn_scope can not be set to local when zone label is empty or "global".*`, err)
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())

	// @@tidb_enable_local_txn is on with configuring the zone label.
	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`))
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_local_txn = on;")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.LocalTxnScope))
	require.Equal(t, "bj", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to global.
	tk.MustExec("set @@txn_scope = 'global';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.GlobalTxnScope))
	require.Equal(t, kv.GlobalTxnScope, tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Set @@txn_scope to local.
	tk.MustExec("set @@txn_scope = 'local';")
	tk.MustQuery("select @@txn_scope;").Check(testkit.Rows(kv.LocalTxnScope))
	require.Equal(t, "bj", tk.Session().GetSessionVars().CheckAndGetTxnScope())
	// Try to set @@txn_scope to an invalid value.
	err = tk.ExecToErr("set @@txn_scope='foo'")
	require.Error(t, err)
	require.Regexp(t, `.*txn_scope value should be global or local.*`, err)
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
}

func TestDoDDLJobQuit(t *testing.T) {
	// test https://github.com/pingcap/tidb/issues/18714, imitate DM's use environment
	// use isolated store, because in below failpoint we will cancel its context
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	se, err := session.CreateSession(store)
	require.NoError(t, err)
	defer se.Close()

	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/ddl/storeCloseInLoop", `return`))
	defer func() { require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/storeCloseInLoop")) }()

	// this DDL call will enter deadloop before this fix
	err = dom.DDL().CreateSchema(se, model.NewCIStr("testschema"), nil, nil)
	require.Equal(t, "context canceled", err.Error())
}

func TestCoprocessorOOMAction(t *testing.T) {
	// Assert Coprocessor OOMAction
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_wait_split_region_finish=1`)
	// create table for non keep-order case
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(id int)")
	tk.MustQuery(`split table t5 between (0) and (10000) regions 10`).Check(testkit.Rows("9 1"))
	// create table for keep-order case
	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6(id int, index(id))")
	tk.MustQuery(`split table t6 between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	tk.MustQuery("split table t6 INDEX id between (0) and (10000) regions 10;").Check(testkit.Rows("10 1"))
	count := 10
	for i := 0; i < count; i++ {
		tk.MustExec(fmt.Sprintf("insert into t5 (id) values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t6 (id) values (%v)", i))
	}

	testcases := []struct {
		name string
		sql  string
	}{
		{
			name: "keep Order",
			sql:  "select id from t6 order by id",
		},
		{
			name: "non keep Order",
			sql:  "select id from t5",
		},
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert"))
	}()

	enableOOM := func(tk *testkit.TestKit, name, sql string) {
		t.Logf("enable OOM, testcase: %v", name)
		// larger than 4 copResponse, smaller than 5 copResponse
		quota := 5*copr.MockResponseSizeForTest - 100
		defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
		tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		var expect []string
		for i := 0; i < count; i++ {
			expect = append(expect, fmt.Sprintf("%v", i))
		}
		tk.MustQuery(sql).Sort().Check(testkit.Rows(expect...))
		// assert oom action worked by max consumed > memory quota
		require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(quota))
	}

	disableOOM := func(tk *testkit.TestKit, name, sql string) {
		t.Logf("disable OOM, testcase: %v", name)
		quota := 5*copr.MockResponseSizeForTest - 100
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		err := tk.QueryToErr(sql)
		require.Error(t, err)
		require.Regexp(t, "Out Of Memory Quota.*", err)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax", `return(true)`))
	// assert oom action and switch
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		enableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 0")
		disableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 1")
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	globaltk := testkit.NewTestKit(t, store)
	globaltk.MustExec("use test")
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 0")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		disableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 1")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax"))

	// assert oom fallback
	for _, testcase := range testcases {
		t.Log(testcase.name)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		tk.MustExec("use test")
		tk.MustExec("set tidb_distsql_scan_concurrency = 1")
		tk.MustExec("set @@tidb_mem_quota_query=1;")
		err = tk.QueryToErr(testcase.sql)
		require.Error(t, err)
		require.Regexp(t, "Out Of Memory Quota.*", err)
		se.Close()
	}
}

// TestDefaultWeekFormat checks for issue #21510.
func TestDefaultWeekFormat(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@global.default_week_format = 4;")
	defer tk1.MustExec("set @@global.default_week_format = default;")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustQuery("select week('2020-02-02'), @@default_week_format, week('2020-02-02');").Check(testkit.Rows("6 4 6"))
}

func TestIssue21944(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	_, err := tk1.Exec("set @@tidb_current_ts=1;")
	require.Equal(t, "[variable:1238]Variable 'tidb_current_ts' is a read only variable", err.Error())
}

func TestIssue21943(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	_, err := tk.Exec("set @@last_plan_from_binding='123';")
	require.Equal(t, "[variable:1238]Variable 'last_plan_from_binding' is a read only variable", err.Error())

	_, err = tk.Exec("set @@last_plan_from_cache='123';")
	require.Equal(t, "[variable:1238]Variable 'last_plan_from_cache' is a read only variable", err.Error())
}

func TestRemovedSysVars(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "bogus_var", Value: "acdc"})
	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows("bogus_var acdc"))
	result = tk.MustQuery("SELECT @@GLOBAL.bogus_var")
	result.Check(testkit.Rows("acdc"))
	tk.MustExec("SET GLOBAL bogus_var = 'newvalue'")

	// unregister
	variable.UnregisterSysVar("bogus_var")

	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows()) // empty
	_, err := tk.Exec("SET GLOBAL bogus_var = 'newvalue'")
	require.Equal(t, "[variable:1193]Unknown system variable 'bogus_var'", err.Error())
	_, err = tk.Exec("SELECT @@GLOBAL.bogus_var")
	require.Equal(t, "[variable:1193]Unknown system variable 'bogus_var'", err.Error())
}

func TestCorrectScopeError(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeNone, Name: "sv_none", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal, Name: "sv_global", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeSession, Name: "sv_session", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "sv_both", Value: "acdc"})

	// check set behavior

	// none
	_, err := tk.Exec("SET sv_none='acdc'")
	require.Equal(t, "[variable:1238]Variable 'sv_none' is a read only variable", err.Error())
	_, err = tk.Exec("SET GLOBAL sv_none='acdc'")
	require.Equal(t, "[variable:1238]Variable 'sv_none' is a read only variable", err.Error())

	// global
	tk.MustExec("SET GLOBAL sv_global='acdc'")
	_, err = tk.Exec("SET sv_global='acdc'")
	require.Equal(t, "[variable:1229]Variable 'sv_global' is a GLOBAL variable and should be set with SET GLOBAL", err.Error())

	// session
	_, err = tk.Exec("SET GLOBAL sv_session='acdc'")
	require.Equal(t, "[variable:1228]Variable 'sv_session' is a SESSION variable and can't be used with SET GLOBAL", err.Error())
	tk.MustExec("SET sv_session='acdc'")

	// both
	tk.MustExec("SET GLOBAL sv_both='acdc'")
	tk.MustExec("SET sv_both='acdc'")

	// unregister
	variable.UnregisterSysVar("sv_none")
	variable.UnregisterSysVar("sv_global")
	variable.UnregisterSysVar("sv_session")
	variable.UnregisterSysVar("sv_both")
}

func TestTiKVSystemVars(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_enable'") // default is on from the sysvar
	result.Check(testkit.Rows("tidb_gc_enable ON"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows()) // but no value in the table (yet) because the value has not been set and the GC has never been run

	// update will set a value in the table
	tk.MustExec("SET GLOBAL tidb_gc_enable = 1")
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows("true"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'false' WHERE variable_name='tikv_gc_enable'")
	result = tk.MustQuery("SELECT @@tidb_gc_enable;")
	result.Check(testkit.Rows("0")) // reads from mysql.tidb value and changes to false

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = -1") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("-1"))

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = 5") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("false"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("5"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'true' WHERE variable_name='tikv_gc_auto_concurrency'")
	result = tk.MustQuery("SELECT @@tidb_gc_concurrency;")
	result.Check(testkit.Rows("-1")) // because auto_concurrency is turned on it takes precedence

	tk.MustExec("REPLACE INTO mysql.tidb (variable_value, variable_name) VALUES ('15m', 'tikv_gc_run_interval')")
	result = tk.MustQuery("SELECT @@GLOBAL.tidb_gc_run_interval;")
	result.Check(testkit.Rows("15m0s"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 15m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '9m'") // too small
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_gc_run_interval value: '9m'"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 10m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '700000000000ns'") // specified in ns, also valid

	_, err := tk.Exec("SET GLOBAL tidb_gc_run_interval = '11mins'")
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'tidb_gc_run_interval'", err.Error())
}

func TestGlobalVarCollationServer(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.collation_server=utf8mb4_general_ci")
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk = testkit.NewTestKit(t, store)
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk.MustQuery("show variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
}

func TestProcessInfoIssue22068(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		tk.MustQuery("select 1 from t where a = (select sleep(5));").Check(testkit.Rows())
	})
	time.Sleep(2 * time.Second)
	pi := tk.Session().ShowProcess()
	require.NotNil(t, pi)
	require.Equal(t, "select 1 from t where a = (select sleep(5));", pi.Info)
	require.Nil(t, pi.Plan)
	wg.Wait()
}

func TestIssue19127(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists issue19127")
	tk.MustExec("create table issue19127 (c_int int, c_str varchar(40), primary key (c_int, c_str) ) partition by hash (c_int) partitions 4;")
	tk.MustExec("insert into issue19127 values (9, 'angry williams'), (10, 'thirsty hugle');")
	_, _ = tk.Exec("update issue19127 set c_int = c_int + 10, c_str = 'adoring stonebraker' where c_int in (10, 9);")
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
}

func TestMemoryUsageAlarmVariable(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0.7")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0.7"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1.1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '1.1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=-1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '-1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))

	tk.MustExec("set @@session.tidb_memory_usage_alarm_ratio=0.8")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_memory_usage_alarm_ratio will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
}

func TestSelectLockInShare(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t_sel_in_share")
	tk.MustExec("CREATE TABLE t_sel_in_share (id int DEFAULT NULL)")
	tk.MustExec("insert into t_sel_in_share values (11)")
	require.Error(t, tk.ExecToErr("select * from t_sel_in_share lock in share mode"))
	tk.MustExec("set @@tidb_enable_noop_functions = 1")
	tk.MustQuery("select * from t_sel_in_share lock in share mode").Check(testkit.Rows("11"))
	tk.MustExec("DROP TABLE t_sel_in_share")
}

func TestReadDMLBatchSize(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_dml_batch_size=1000")
	se, err := session.CreateSession(store)
	require.NoError(t, err)

	// `select 1` to load the global variables.
	_, _ = se.Execute(context.TODO(), "select 1")
	require.Equal(t, 1000, se.GetSessionVars().DMLBatchSize)
}

func TestPerStmtTaskID(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table task_id (v int)")

	tk.MustExec("begin")
	tk.MustExec("select * from task_id where v > 10")
	taskID1 := tk.Session().GetSessionVars().StmtCtx.TaskID
	tk.MustExec("select * from task_id where v < 5")
	taskID2 := tk.Session().GetSessionVars().StmtCtx.TaskID
	tk.MustExec("commit")

	require.NotEqual(t, taskID1, taskID2)
}

func TestSetEnableRateLimitAction(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// assert default value
	result := tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("use test")
	tk.MustExec("create table tmp123(id int)")
	tk.MustQuery("select * from tmp123;")
	haveRateLimitAction := false
	action := tk.Session().GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	require.True(t, haveRateLimitAction)

	// assert set sys variable
	tk.MustExec("set global tidb_enable_rate_limit_action= '0';")
	tk.Session().Close()

	tk.RefreshSession()
	result = tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("0"))

	haveRateLimitAction = false
	action = tk.Session().GetSessionVars().StmtCtx.MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	require.False(t, haveRateLimitAction)
}

func TestStmtHints(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test MEMORY_QUOTA hint
	tk.MustExec("select /*+ MEMORY_QUOTA(1 MB) */ 1;")
	val := int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB) */ 1;")
	val = int64(1) * 1024 * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(1 GB), MEMORY_QUOTA(1 MB) */ 1;")
	val = int64(1) * 1024 * 1024
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))
	tk.MustExec("select /*+ MEMORY_QUOTA(0 GB) */ 1;")
	val = int64(0)
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "Setting the MEMORY_QUOTA to 0 means no memory limit")

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */ into t1 (a) values (1);")
	val = int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))

	tk.MustExec("insert /*+ MEMORY_QUOTA(1 MB) */  into t1 select /*+ MEMORY_QUOTA(3 MB) */ * from t1;")
	val = int64(1) * 1024 * 1024
	require.True(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.CheckBytesLimit(val))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[util:3126]Hint MEMORY_QUOTA(`3145728`) is ignored as conflicting/duplicated.")

	// Test NO_INDEX_MERGE hint
	tk.Session().GetSessionVars().SetEnableIndexMerge(true)
	tk.MustExec("select /*+ NO_INDEX_MERGE() */ 1;")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.NoIndexMergeHint)
	tk.MustExec("select /*+ NO_INDEX_MERGE(), NO_INDEX_MERGE() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().GetEnableIndexMerge())

	// Test STRAIGHT_JOIN hint
	tk.MustExec("select /*+ straight_join() */ 1;")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.StraightJoinOrder)
	tk.MustExec("select /*+ straight_join(), straight_join() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)

	// Test USE_TOJA hint
	tk.Session().GetSessionVars().SetAllowInSubqToJoinAndAgg(true)
	tk.MustExec("select /*+ USE_TOJA(false) */ 1;")
	require.False(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())
	tk.Session().GetSessionVars().SetAllowInSubqToJoinAndAgg(false)
	tk.MustExec("select /*+ USE_TOJA(true) */ 1;")
	require.True(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())
	tk.MustExec("select /*+ USE_TOJA(false), USE_TOJA(true) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.True(t, tk.Session().GetSessionVars().GetAllowInSubqToJoinAndAgg())

	// Test USE_CASCADES hint
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	tk.MustExec("select /*+ USE_CASCADES(false) */ 1;")
	require.False(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	tk.MustExec("select /*+ USE_CASCADES(true) */ 1;")
	require.True(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())
	tk.MustExec("select /*+ USE_CASCADES(false), USE_CASCADES(true) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(true)")
	require.True(t, tk.Session().GetSessionVars().GetEnableCascadesPlanner())

	// Test READ_CONSISTENT_REPLICA hint
	tk.Session().GetSessionVars().SetReplicaRead(kv.ReplicaReadLeader)
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA() */ 1;")
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("select /*+ READ_CONSISTENT_REPLICA(), READ_CONSISTENT_REPLICA() */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
}

func TestMaxExecutionTime(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("use test")
	tk.MustExec("create table MaxExecTime( id int,name varchar(128),age int);")
	tk.MustExec("begin")
	tk.MustExec("insert into MaxExecTime (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18);")

	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) MAX_EXECUTION_TIME(500) */ * FROM MaxExecTime;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(500)")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.HasMaxExecutionTime)
	require.Equal(t, uint64(500), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)

	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxExecTime;")

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 300;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustExec("set @@MAX_EXECUTION_TIME = 150;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("300"))
	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("150"))

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 0;")
	tk.MustExec("set @@MAX_EXECUTION_TIME = 0;")
	tk.MustExec("commit")
	tk.MustExec("drop table if exists MaxExecTime;")
}

func TestGrantViewRelated(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tkRoot := testkit.NewTestKit(t, store)
	tkUser := testkit.NewTestKit(t, store)
	tkRoot.MustExec("use test")
	tkUser.MustExec("use test")

	tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustExec("create table if not exists t (a int)")
	tkRoot.MustExec("create view v_version29 as select * from t")
	tkRoot.MustExec("create user 'u_version29'@'%'")
	tkRoot.MustExec("grant select on t to u_version29@'%'")

	tkUser.Session().Auth(&auth.UserIdentity{Username: "u_version29", Hostname: "localhost", CurrentUser: true, AuthUsername: "u_version29", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	require.Error(t, tkUser.ExecToErr("select * from test.v_version29;"))
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	require.Error(t, tkUser.ExecToErr("create view v_version29_c as select * from t;"))

	tkRoot.MustExec(`grant show view, select on v_version29 to 'u_version29'@'%'`)
	tkRoot.MustQuery("select table_priv from mysql.tables_priv where host='%' and db='test' and user='u_version29' and table_name='v_version29'").Check(testkit.Rows("Select,Show View"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustQuery("show create view v_version29;")
	require.Error(t, tkUser.ExecToErr("create view v_version29_c as select * from v_version29;"))

	tkRoot.MustExec("create view v_version29_c as select * from v_version29;")
	tkRoot.MustExec(`grant create view on v_version29_c to 'u_version29'@'%'`) // Can't grant privilege on a non-exist table/view.
	tkRoot.MustQuery("select table_priv from mysql.tables_priv where host='%' and db='test' and user='u_version29' and table_name='v_version29_c'").Check(testkit.Rows("Create View"))
	tkRoot.MustExec("drop view v_version29_c")

	tkRoot.MustExec(`grant select on v_version29 to 'u_version29'@'%'`)
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustExec("create view v_version29_c as select * from v_version29;")
}

func TestLoadClientInteractive(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.RefreshSession()
	tk.Session().GetSessionVars().ClientCapability = tk.Session().GetSessionVars().ClientCapability | mysql.ClientInteractive
	tk.MustQuery("select @@wait_timeout").Check(testkit.Rows("28800"))
}

func TestReplicaRead(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	require.Equal(t, kv.ReplicaReadLeader, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("set @@tidb_replica_read = 'follower';")
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("set @@tidb_replica_read = 'leader';")
	require.Equal(t, kv.ReplicaReadLeader, tk.Session().GetSessionVars().GetReplicaRead())
}

func TestIsolationRead(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	require.Len(t, tk.Session().GetSessionVars().GetIsolationReadEngines(), 3)
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")
	engines := tk.Session().GetSessionVars().GetIsolationReadEngines()
	require.Len(t, engines, 1)
	_, hasTiFlash := engines[kv.TiFlash]
	_, hasTiKV := engines[kv.TiKV]
	require.True(t, hasTiFlash)
	require.False(t, hasTiKV)
}

func TestUpdatePrivilege(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (id int);")
	tk.MustExec("create table t2 (id int);")
	tk.MustExec("insert into t1 values (1);")
	tk.MustExec("insert into t2 values (2);")
	tk.MustExec("create user xxx;")
	tk.MustExec("grant all on test.t1 to xxx;")
	tk.MustExec("grant select on test.t2 to xxx;")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"}, []byte(""), []byte("")))

	tk1.MustMatchErrMsg("update t2 set id = 666 where id = 1;", "privilege check.*")

	// Cover a bug that t1 and t2 both require update privilege.
	// In fact, the privlege check for t1 should be update, and for t2 should be select.
	tk1.MustExec("update t1,t2 set t1.id = t2.id;")

	// Fix issue 8911
	tk.MustExec("create database weperk")
	tk.MustExec("use weperk")
	tk.MustExec("create table tb_wehub_server (id int, active_count int, used_count int)")
	tk.MustExec("create user 'weperk'")
	tk.MustExec("grant all privileges on weperk.* to 'weperk'@'%'")
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "weperk", Hostname: "%"}, []byte(""), []byte("")))
	tk1.MustExec("use weperk")
	tk1.MustExec("update tb_wehub_server a set a.active_count=a.active_count+1,a.used_count=a.used_count+1 where id=1")

	tk.MustExec("create database service")
	tk.MustExec("create database report")
	tk.MustExec(`CREATE TABLE service.t1 (
  id int(11) DEFAULT NULL,
  a bigint(20) NOT NULL,
  b text DEFAULT NULL,
  PRIMARY KEY (a)
)`)
	tk.MustExec(`CREATE TABLE report.t2 (
  a bigint(20) DEFAULT NULL,
  c bigint(20) NOT NULL
)`)
	tk.MustExec("grant all privileges on service.* to weperk")
	tk.MustExec("grant all privileges on report.* to weperk")
	tk1.Session().GetSessionVars().CurrentDB = ""
	tk1.MustExec(`update service.t1 s,
report.t2 t
set s.a = t.a
WHERE
s.a = t.a
and t.c >=  1 and t.c <= 10000
and s.b !='xx';`)

	// Fix issue 10028
	tk.MustExec("create database ap")
	tk.MustExec("create database tp")
	tk.MustExec("grant all privileges on ap.* to xxx")
	tk.MustExec("grant select on tp.* to xxx")
	tk.MustExec("create table tp.record( id int,name varchar(128),age int)")
	tk.MustExec("insert into tp.record (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18)")
	tk.MustExec("create table ap.record( id int,name varchar(128),age int)")
	tk.MustExec("insert into ap.record(id) values(1)")
	require.True(t, tk1.Session().Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"}, []byte(""), []byte("")))
	tk1.MustExec("update ap.record t inner join tp.record tt on t.id=tt.id  set t.name=tt.name")
}

func TestDBUserNameLength(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists t (a int)")
	// Test username length can be longer than 16.
	tk.MustExec(`CREATE USER 'abcddfjakldfjaldddds'@'%' identified by ''`)
	tk.MustExec(`grant all privileges on test.* to 'abcddfjakldfjaldddds'@'%'`)
	tk.MustExec(`grant all privileges on test.t to 'abcddfjakldfjaldddds'@'%'`)
}

func TestHostLengthMax(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	host1 := strings.Repeat("a", 65)
	host2 := strings.Repeat("a", 256)

	tk.MustExec(fmt.Sprintf(`CREATE USER 'abcddfjakldfjaldddds'@'%s'`, host1))
	tk.MustGetErrMsg(fmt.Sprintf(`CREATE USER 'abcddfjakldfjaldddds'@'%s'`, host2), "[ddl:1470]String 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' is too long for host name (should be no longer than 255)")
}

func TestCommitRetryCount(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("create table no_retry (id int)")
	tk1.MustExec("insert into no_retry values (1)")
	tk1.MustExec("set @@tidb_retry_limit = 0")

	tk1.MustExec("begin")
	tk1.MustExec("update no_retry set id = 2")

	tk2.MustExec("begin")
	tk2.MustExec("update no_retry set id = 3")
	tk2.MustExec("commit")

	// No auto retry because retry limit is set to 0.
	require.Error(t, tk1.ExecToErr("commit"))
}

func TestEnablePartition(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_table_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))

	tk.MustExec("set global tidb_enable_table_partition = on")

	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))
	tk.MustQuery("show global variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set tidb_enable_list_partition=1")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=on")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set global tidb_enable_list_partition=off")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))
	tk1.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
}

func TestRollbackOnCompileError(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert t values (1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("rename table t to t2")
	var meetErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err != nil {
			meetErr = true
			break
		}
	}
	require.True(t, meetErr)

	tk.MustExec("rename table t2 to t")
	var recoverErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.Exec("insert t values (1)")
		if err == nil {
			recoverErr = true
			break
		}
	}
	require.True(t, recoverErr)
}

func TestCastTimeToDate(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '-8:00'")
	date := time.Now().In(time.FixedZone("", -8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))

	tk.MustExec("set time_zone = '+08:00'")
	date = time.Now().In(time.FixedZone("", 8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))
}

func TestSetGlobalTZ(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk.MustExec("set global time_zone = '+00:00'")

	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +00:00"))
}

func TestErrorRollback(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_rollback")
	tk.MustExec("create table t_rollback (c1 int, c2 int, primary key(c1))")
	tk.MustExec("insert into t_rollback values (0, 0)")

	var wg sync.WaitGroup
	cnt := 4
	wg.Add(cnt)
	num := 20

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@session.tidb_retry_limit = 100")
			for j := 0; j < num; j++ {
				_, _ = tk.Exec("insert into t_rollback values (1, 1)")
				tk.MustExec("update t_rollback set c2 = c2 + 1 where c1 = 0")
			}
		}()
	}

	wg.Wait()
	tk.MustQuery("select c2 from t_rollback where c1 = 0").Check(testkit.Rows(fmt.Sprint(cnt * num)))
}

func TestDeletePanic(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("delete from `t` where `c` = ?", 1)
	tk.MustExec("delete from `t` where `c` = ?", 2)
}

func TestInformationSchemaCreateTime(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c int)")
	tk.MustExec(`set @@time_zone = 'Asia/Shanghai'`)
	ret := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	// Make sure t1 is greater than t.
	time.Sleep(time.Second)
	tk.MustExec("alter table t modify c int default 11")
	ret1 := tk.MustQuery("select create_time from information_schema.tables where table_name='t';")
	ret2 := tk.MustQuery("show table status like 't'")
	require.Equal(t, ret2.Rows()[0][11].(string), ret1.Rows()[0][0].(string))
	typ1, err := types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	require.NoError(t, err)
	typ2, err := types.ParseDatetime(nil, ret1.Rows()[0][0].(string))
	require.NoError(t, err)
	r := typ2.Compare(typ1)
	require.Equal(t, 1, r)
	// Check that time_zone changes makes the create_time different
	tk.MustExec(`set @@time_zone = 'Europe/Amsterdam'`)
	ret = tk.MustQuery(`select create_time from information_schema.tables where table_name='t'`)
	ret2 = tk.MustQuery(`show table status like 't'`)
	require.Equal(t, ret2.Rows()[0][11].(string), ret.Rows()[0][0].(string))
	typ3, err := types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	require.NoError(t, err)
	// Asia/Shanghai 2022-02-17 17:40:05 > Europe/Amsterdam 2022-02-17 10:40:05
	r = typ2.Compare(typ3)
	require.Equal(t, 1, r)
}

func TestPrepare(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("id");`)
	id, ps, _, err := tk.Session().PrepareStmt("select id+? from t")
	ctx := context.Background()
	require.NoError(t, err)
	require.Equal(t, uint32(1), id)
	require.Equal(t, 1, ps)
	tk.MustExec(`set @a=1`)
	rs, err := tk.Session().ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum("1")})
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	err = tk.Session().DropPreparedStmt(id)
	require.NoError(t, err)

	tk.MustExec("prepare stmt from 'select 1+?'")
	tk.MustExec("set @v1=100")
	tk.MustQuery("execute stmt using @v1").Check(testkit.Rows("101"))

	tk.MustExec("set @v2=200")
	tk.MustQuery("execute stmt using @v2").Check(testkit.Rows("201"))

	tk.MustExec("set @v3=300")
	tk.MustQuery("execute stmt using @v3").Check(testkit.Rows("301"))
	tk.MustExec("deallocate prepare stmt")

	// Execute prepared statements for more than one time.
	tk.MustExec("create table multiexec (a int, b int)")
	tk.MustExec("insert multiexec values (1, 1), (2, 2)")
	id, _, _, err = tk.Session().PrepareStmt("select a from multiexec where b = ? order by b")
	require.NoError(t, err)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	rs, err = tk.Session().ExecutePreparedStmt(ctx, id, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	require.NoError(t, rs.Close())
}

func TestSpecifyIndexPrefixLength(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	_, err := tk.Exec("create table t (c1 char, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	_, err = tk.Exec("create table t (c1 int, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	_, err = tk.Exec("create table t (c1 bit(10), index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	tk.MustExec("create table t (c1 char, c2 int, c3 bit(10));")

	_, err = tk.Exec("create index idx_c1 on t (c1(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	_, err = tk.Exec("create index idx_c1 on t (c2(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	_, err = tk.Exec("create index idx_c1 on t (c3(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	tk.MustExec("drop table if exists t;")

	_, err = tk.Exec("create table t (c1 int, c2 blob, c3 varchar(64), index(c2));")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	require.Error(t, err)

	tk.MustExec("create table t (c1 int, c2 blob, c3 varchar(64));")
	_, err = tk.Exec("create index idx_c1 on t (c2);")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	require.Error(t, err)

	_, err = tk.Exec("create index idx_c1 on t (c2(555555));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	require.Error(t, err)

	_, err = tk.Exec("create index idx_c1 on t (c1(5))")
	// ERROR 1089 (HY000): Incorrect prefix key;
	// the used key part isn't a string, the used length is longer than the key part,
	// or the storage engine doesn't support unique prefix keys
	require.Error(t, err)

	tk.MustExec("create index idx_c1 on t (c1);")
	tk.MustExec("create index idx_c2 on t (c2(3));")
	tk.MustExec("create unique index idx_c3 on t (c3(5));")

	tk.MustExec("insert into t values (3, 'abc', 'def');")
	tk.MustQuery("select c2 from t where c2 = 'abc';").Check(testkit.Rows("abc"))

	tk.MustExec("insert into t values (4, 'abcd', 'xxx');")
	tk.MustExec("insert into t values (4, 'abcf', 'yyy');")
	tk.MustQuery("select c2 from t where c2 = 'abcf';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 = 'abcd';").Check(testkit.Rows("abcd"))

	tk.MustExec("insert into t values (4, 'ignore', 'abcdeXXX');")
	_, err = tk.Exec("insert into t values (5, 'ignore', 'abcdeYYY');")
	// ERROR 1062 (23000): Duplicate entry 'abcde' for key 'idx_c3'
	require.Error(t, err)
	tk.MustQuery("select c3 from t where c3 = 'abcde';").Check(testkit.Rows())

	tk.MustExec("delete from t where c3 = 'abcdeXXX';")
	tk.MustExec("delete from t where c2 = 'abc';")

	tk.MustQuery("select c2 from t where c2 > 'abcd';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 < 'abcf';").Check(testkit.Rows("abcd"))
	tk.MustQuery("select c2 from t where c2 >= 'abcd';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 <= 'abcf';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abc';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abcd';").Check(testkit.Rows("abcf"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b char(255), key(a, b(20)));")
	tk.MustExec("insert into t1 values (0, '1');")
	tk.MustExec("update t1 set b = b + 1 where a = 0;")
	tk.MustQuery("select b from t1 where a = 0;").Check(testkit.Rows("2"))

	// test union index.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a text, b text, c int, index (a(3), b(3), c));")
	tk.MustExec("insert into t values ('abc', 'abcd', 1);")
	tk.MustExec("insert into t values ('abcx', 'abcf', 2);")
	tk.MustExec("insert into t values ('abcy', 'abcf', 3);")
	tk.MustExec("insert into t values ('bbc', 'abcd', 4);")
	tk.MustExec("insert into t values ('bbcz', 'abcd', 5);")
	tk.MustExec("insert into t values ('cbck', 'abd', 6);")
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abc';").Check(testkit.Rows())
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abd';").Check(testkit.Rows("1"))
	tk.MustQuery("select c from t where a < 'cbc' and b > 'abcd';").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select c from t where a <= 'abd' and b > 'abc';").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select c from t where a < 'bbcc' and b = 'abcd';").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select c from t where a > 'bbcf';").Check(testkit.Rows("5", "6"))
}

func TestResultField(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")

	tk.MustExec(`INSERT INTO t VALUES (1);`)
	tk.MustExec(`INSERT INTO t VALUES (2);`)
	r, err := tk.Exec(`SELECT count(*) from t;`)
	require.NoError(t, err)
	fields := r.Fields()
	require.NoError(t, err)
	require.Len(t, fields, 1)
	field := fields[0].Column
	require.Equal(t, mysql.TypeLonglong, field.GetType())
	require.Equal(t, 21, field.GetFlen())
}

// Testcase for https://github.com/pingcap/tidb/issues/325
func TestResultType(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	rs, err := tk.Exec(`select cast(null as char(30))`)
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(context.Background(), req)
	require.NoError(t, err)
	require.True(t, req.GetRow(0).IsNull(0))
	require.Equal(t, mysql.TypeVarString, rs.Fields()[0].Column.FieldType.GetType())
}

func TestFieldText(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tests := []struct {
		sql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
		{"select 1 /*!32301 +1 */;", "1  +1 "},
		{"select /*!32301 1  +1 */;", "1  +1 "},
		{"/*!32301 select 1  +1 */;", "1  +1 "},
		{"select 1 + /*!32301 1 +1 */;", "1 +  1 +1 "},
		{"select 1 /*!32301 + 1, 1 */;", "1  + 1"},
		{"select /*!32301 1, 1 +1 */;", "1"},
		{"select /*!32301 1 + 1, */ +1;", "1 + 1"},
	}
	for _, tt := range tests {
		result, err := tk.Exec(tt.sql)
		require.NoError(t, err)
		require.Equal(t, tt.field, result.Fields()[0].ColumnAsName.O)
	}
}

func TestIndexMaxLength(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_index_max_length")
	tk.MustExec("use test_index_max_length")

	// create simple index at table creation
	tk.MustGetErrCode("create table t (c1 varchar(3073), index(c1)) charset = ascii;", mysql.ErrTooLongKey)

	// create simple index after table creation
	tk.MustExec("create table t (c1 varchar(3073)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1 on t(c1) ", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	// create compound index at table creation
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 varchar(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 char(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 char, index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3072), c2 date, index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)
	tk.MustGetErrCode("create table t (c1 varchar(3069), c2 timestamp(1), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)

	tk.MustExec("create table t (c1 varchar(3068), c2 bit(26), index(c1, c2)) charset = ascii;") // 26 bit = 4 bytes
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (c1 varchar(3068), c2 bit(32), index(c1, c2)) charset = ascii;") // 32 bit = 4 bytes
	tk.MustExec("drop table t;")
	tk.MustGetErrCode("create table t (c1 varchar(3068), c2 bit(33), index(c1, c2)) charset = ascii;", mysql.ErrTooLongKey)

	// create compound index after table creation
	tk.MustExec("create table t (c1 varchar(3072), c2 varchar(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 char(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 char) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3072), c2 date) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (c1 varchar(3069), c2 timestamp(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", mysql.ErrTooLongKey)
	tk.MustExec("drop table t;")

	// Test charsets other than `ascii`.
	assertCharsetLimit := func(charset string, bytesPerChar int) {
		base := 3072 / bytesPerChar
		tk.MustGetErrCode(fmt.Sprintf("create table t (a varchar(%d) primary key) charset=%s", base+1, charset), mysql.ErrTooLongKey)
		tk.MustExec(fmt.Sprintf("create table t (a varchar(%d) primary key) charset=%s", base, charset))
		tk.MustExec("drop table if exists t")
	}
	assertCharsetLimit("binary", 1)
	assertCharsetLimit("latin1", 1)
	assertCharsetLimit("utf8", 3)
	assertCharsetLimit("utf8mb4", 4)

	// Test types bit length limit.
	assertTypeLimit := func(tp string, limitBitLength int) {
		base := 3072 - limitBitLength
		tk.MustGetErrCode(fmt.Sprintf("create table t (a blob(10000), b %s, index idx(a(%d), b))", tp, base+1), mysql.ErrTooLongKey)
		tk.MustExec(fmt.Sprintf("create table t (a blob(10000), b %s, index idx(a(%d), b))", tp, base))
		tk.MustExec("drop table if exists t")
	}

	assertTypeLimit("tinyint", 1)
	assertTypeLimit("smallint", 2)
	assertTypeLimit("mediumint", 3)
	assertTypeLimit("int", 4)
	assertTypeLimit("integer", 4)
	assertTypeLimit("bigint", 8)
	assertTypeLimit("float", 4)
	assertTypeLimit("float(24)", 4)
	assertTypeLimit("float(25)", 8)
	assertTypeLimit("decimal(9)", 4)
	assertTypeLimit("decimal(10)", 5)
	assertTypeLimit("decimal(17)", 8)
	assertTypeLimit("year", 1)
	assertTypeLimit("date", 3)
	assertTypeLimit("time", 3)
	assertTypeLimit("datetime", 8)
	assertTypeLimit("timestamp", 4)
}

func TestIndexColumnLength(t *testing.T) {
	store, dom, clean := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 blob);")
	tk.MustExec("create index idx_c1 on t(c1);")
	tk.MustExec("create index idx_c2 on t(c2(6));")

	is := dom.InfoSchema()
	tab, err2 := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err2)

	idxC1Cols := tables.FindIndexByColName(tab, "c1").Meta().Columns
	require.Equal(t, types.UnspecifiedLength, idxC1Cols[0].Length)

	idxC2Cols := tables.FindIndexByColName(tab, "c2").Meta().Columns
	require.Equal(t, 6, idxC2Cols[0].Length)
}

func TestIgnoreForeignKey(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	tk.MustExec(sqlText)
}

// TestISColumns tests information_schema.columns.
func TestISColumns(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("select ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS;")
	tk.MustQuery("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'").Check(testkit.Rows("utf8mb4"))
}

func TestMultiStmts(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1; create table t1(id int ); insert into t1 values (1);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1"))
}

func TestLastExecuteDDLFlag(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int)")
	require.NotNil(t, tk.Session().Value(sessionctx.LastExecuteDDL))
	tk.MustExec("insert into t1 values (1)")
	require.Nil(t, tk.Session().Value(sessionctx.LastExecuteDDL))
}

func TestDecimal(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a decimal unique);")
	tk.MustExec("insert t values ('100');")
	_, err := tk.Exec("insert t values ('1e2');")
	require.NotNil(t, err)
}

func TestParser(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test for https://github.com/pingcap/tidb/pull/177
	tk.MustExec("CREATE TABLE `t1` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustExec("CREATE TABLE `t2` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustExec(`INSERT INTO t1 VALUES (1,1,1);`)
	tk.MustExec(`INSERT INTO t2 VALUES (1,1,1);`)
	tk.MustExec(`PREPARE my_stmt FROM "SELECT t1.b, count(*) FROM t1 group by t1.b having count(*) > ALL (SELECT COUNT(*) FROM t2 WHERE t2.a=1 GROUP By t2.b)";`)
	tk.MustExec(`EXECUTE my_stmt;`)
	tk.MustExec(`EXECUTE my_stmt;`)
	tk.MustExec(`deallocate prepare my_stmt;`)
	tk.MustExec(`drop table t1,t2;`)
}

func TestOnDuplicate(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test for https://github.com/pingcap/tidb/pull/454
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int);")
	tk.MustExec("insert into t1 set c1=1, c2=2, c3=1;")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustExec("insert into t set c1=1, c2=4;")
	tk.MustExec("insert into t select * from t1 limit 1 on duplicate key update c3=3333;")
}

func TestReplace(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test for https://github.com/pingcap/tidb/pull/456
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int);")
	tk.MustExec("replace into t1 set c1=1, c2=2, c3=1;")
	tk.MustExec("create table t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustExec("replace into t set c1=1, c2=4;")
	tk.MustExec("replace into t select * from t1 limit 1;")
}

func TestDelete(t *testing.T) {
	// test for https://github.com/pingcap/tidb/pull/1135

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database test1")
	tk1.MustExec("use test1")
	tk1.MustExec("create table t (F1 VARCHAR(30));")
	tk1.MustExec("insert into t (F1) values ('1'), ('4');")

	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m2,t m1 where m1.F1>1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m1,t m2 where true and m1.F1<2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1 from t m1,t m2 where false;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete m1, m2 from t m1,t m2 where m1.F1>m2.F1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (F1 VARCHAR(30));")
	tk.MustExec("insert into t (F1) values ('1'), ('2');")
	tk.MustExec("delete test1.t from test1.t inner join test.t where test1.t.F1 > test.t.F1")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func TestResetCtx(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("create table t (i int auto_increment not null key);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (10);")
	tk.MustExec("update t set i = i + row_count();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2", "11"))

	tk1.MustExec("update t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "11"))

	tk.MustExec("delete from t where i = 11;")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values ();")
	tk.MustExec("update t set i = i + last_insert_id() + 1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("14", "25"))

	tk1.MustExec("update t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("13", "25"))
}

// test for https://github.com/pingcap/tidb/pull/461
func TestUnique(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk.MustExec(`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(1, 1);")
	tk1.MustExec("begin;")
	tk1.MustExec("insert into test(id, val) values(2, 2);")
	tk2.MustExec("begin;")
	tk2.MustExec("insert into test(id, val) values(1, 2);")
	tk2.MustExec("commit;")
	_, err := tk.Exec("commit")
	require.Error(t, err)
	// Check error type and error message
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists), fmt.Sprintf("err %v", err))
	require.Equal(t, "previous statement: insert into test(id, val) values(1, 1);: [kv:1062]Duplicate entry '1' for key 'PRIMARY'", err.Error())

	_, err = tk1.Exec("commit")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, kv.ErrKeyExists), fmt.Sprintf("err %v", err))
	require.Equal(t, "previous statement: insert into test(id, val) values(2, 2);: [kv:1062]Duplicate entry '2' for key 'val'", err.Error())

	// Test for https://github.com/pingcap/tidb/issues/463
	tk.MustExec("drop table test;")
	tk.MustExec(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustExec("insert into test(id, val) values(1, 1);")
	_, err = tk.Exec("insert into test(id, val) values(2, 1);")
	require.Error(t, err)
	tk.MustExec("insert into test(id, val) values(2, 2);")

	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(3, 3);")
	_, err = tk.Exec("insert into test(id, val) values(4, 3);")
	require.Error(t, err)
	tk.MustExec("insert into test(id, val) values(4, 4);")
	tk.MustExec("commit;")

	tk1.MustExec("begin;")
	tk1.MustExec("insert into test(id, val) values(5, 6);")
	tk.MustExec("begin;")
	tk.MustExec("insert into test(id, val) values(20, 6);")
	tk.MustExec("commit;")
	_, _ = tk1.Exec("commit")
	tk1.MustExec("insert into test(id, val) values(5, 5);")

	tk.MustExec("drop table test;")
	tk.MustExec(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val1 int UNIQUE,
			val2 int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustExec("insert into test(id, val1, val2) values(1, 1, 1);")
	tk.MustExec("insert into test(id, val1, val2) values(2, 2, 2);")
	_, _ = tk.Exec("update test set val1 = 3, val2 = 2 where id = 1;")
	tk.MustExec("insert into test(id, val1, val2) values(3, 3, 3);")
}

// Test for https://github.com/pingcap/tidb/issues/1114
func TestSet(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @tmp = 0")
	tk.MustExec("set @tmp := @tmp + 1")
	tk.MustQuery("select @tmp").Check(testkit.Rows("1"))
	tk.MustQuery("select @tmp1 = 1, @tmp2 := 2").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select @tmp1 := 11, @tmp2").Check(testkit.Rows("11 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int);")
	tk.MustExec("insert into t values (1),(2);")
	tk.MustExec("update t set c = 3 WHERE c = @var:= 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("3", "2"))
	tk.MustQuery("select @tmp := count(*) from t").Check(testkit.Rows("2"))
	tk.MustQuery("select @tmp := c-2 from t where c=3").Check(testkit.Rows("1"))
}

func TestMySQLTypes(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery(`select 0x01 + 1, x'4D7953514C' = "MySQL"`).Check(testkit.Rows("2 1"))
	tk.MustQuery(`select 0b01 + 1, 0b01000001 = "A"`).Check(testkit.Rows("2 1"))
}

func TestIssue986(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sqlText := `CREATE TABLE address (
 		id bigint(20) NOT NULL AUTO_INCREMENT,
 		PRIMARY KEY (id));`
	tk.MustExec(sqlText)
	tk.MustExec(`insert into address values ('10')`)
}

func TestCast(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select cast(0.5 as unsigned)")
	tk.MustQuery("select cast(-0.5 as signed)")
	tk.MustQuery("select hex(cast(0x10 as binary(2)))").Check(testkit.Rows("1000"))
}

func TestTableInfoMeta(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	checkResult := func(affectedRows uint64, insertID uint64) {
		gotRows := tk.Session().AffectedRows()
		require.Equal(t, affectedRows, gotRows)

		gotID := tk.Session().LastInsertID()
		require.Equal(t, insertID, gotID)
	}

	// create table
	tk.MustExec("CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// insert data
	tk.MustExec(`INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(1, 0)

	tk.MustExec(`UPDATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(1, 0)

	tk.MustExec(`DELETE from tbl_test where id = 2;`)
	checkResult(1, 0)

	// select data
	tk.MustQuery("select * from tbl_test").Check(testkit.Rows("1 hello"))
}

func TestCaseInsensitive(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table T (a text, B int)")
	tk.MustExec("insert t (A, b) values ('aaa', 1)")
	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	fields := rs.Fields()
	require.Equal(t, "a", fields[0].ColumnAsName.O)
	require.Equal(t, "B", fields[1].ColumnAsName.O)
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select A, b from t")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "A", fields[0].ColumnAsName.O)
	require.Equal(t, "b", fields[1].ColumnAsName.O)
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select a as A from t where A > 0")
	require.NoError(t, err)
	fields = rs.Fields()
	require.Equal(t, "A", fields[0].ColumnAsName.O)
	require.NoError(t, rs.Close())

	tk.MustExec("update T set b = B + 1")
	tk.MustExec("update T set B = b + 1")
	tk.MustQuery("select b from T").Check(testkit.Rows("3"))
}

func TestLastMessage(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")

	// Insert
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t VALUES ("b"), ("c");`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	// Update
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Rows matched: 0  Changed: 0  Warnings: 0")

	// Replace
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t VALUES (1,1)`)
	tk.MustExec(`REPLACE INTO t VALUES (2,2)`)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT INTO t1 VALUES (1,10), (3,30);`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`REPLACE INTO t SELECT * from t1`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")

	// Check insert with CLIENT_FOUND_ROWS is set
	tk.Session().SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec(`drop table if exists t, t1;
        create table t (c1 int PRIMARY KEY, c2 int);
        create table t1 (a1 int, a2 int);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 2), (3, 30);`)
	tk.MustExec(`INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);`)
	tk.MustExec(`INSERT INTO t SELECT * FROM t1 ON DUPLICATE KEY UPDATE c2=a2;`)
	tk.CheckLastMessage("Records: 6  Duplicates: 3  Warnings: 0")
}

func TestQueryString(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table mutil1 (a int);create table multi2 (a int)")
	queryStr := tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "create table multi2 (a int)", queryStr)

	// Test execution of DDL through the "ExecutePreparedStmt" interface.
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id bigint PRIMARY KEY, age int)")
	tk.MustExec("show create table t")
	id, _, _, err := tk.Session().PrepareStmt("CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
	require.NoError(t, err)
	var params []types.Datum
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), id, params)
	require.NoError(t, err)
	qs := tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)", qs.(string))

	// Test execution of DDL through the "Execute" interface.
	tk.MustExec("use test")
	tk.MustExec("drop table t2")
	tk.MustExec("prepare stmt from 'CREATE TABLE t2(id bigint PRIMARY KEY, age int)'")
	tk.MustExec("execute stmt")
	qs = tk.Session().Value(sessionctx.QueryString)
	require.Equal(t, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)", qs.(string))
}

func TestAffectedRows(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id TEXT)")
	tk.MustExec(`INSERT INTO t VALUES ("a");`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`INSERT INTO t VALUES ("b");`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`UPDATE t set id = 'c' where id = 'a';`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(`UPDATE t set id = 'a' where id = 'a';`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))
	tk.MustQuery(`SELECT * from t`).Check(testkit.Rows("c", "b"))
	require.Equal(t, 0, int(tk.Session().AffectedRows()))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 timestamp);")
	tk.MustExec(`insert t(id) values(1);`)
	tk.MustExec(`UPDATE t set id = 1 where id = 1;`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))

	// With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted as a new row,
	// 2 if an existing row is updated, and 0 if an existing row is set to its current values.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int PRIMARY KEY, c2 int);")
	tk.MustExec(`insert t values(1, 1);`)
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
	tk.MustExec(`insert into t values (1, 1) on duplicate key update c2=2;`)
	require.Equal(t, 0, int(tk.Session().AffectedRows()))
	tk.MustExec("drop table if exists test")
	createSQL := `CREATE TABLE test (
	  id        VARCHAR(36) PRIMARY KEY NOT NULL,
	  factor    INTEGER                 NOT NULL                   DEFAULT 2);`
	tk.MustExec(createSQL)
	insertSQL := `INSERT INTO test(id) VALUES('id') ON DUPLICATE KEY UPDATE factor=factor+3;`
	tk.MustExec(insertSQL)
	require.Equal(t, 1, int(tk.Session().AffectedRows()))
	tk.MustExec(insertSQL)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
	tk.MustExec(insertSQL)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))

	tk.Session().SetClientCapability(mysql.ClientFoundRows)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, data int)")
	tk.MustExec(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustExec(`UPDATE t set id = 1 where data = 0;`)
	require.Equal(t, 2, int(tk.Session().AffectedRows()))
}

// TestRowLock . See http://dev.mysql.com/doc/refman/5.7/en/commit.html.
func TestRowLock(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t")
	txn, err := tk.Session().Txn(true)
	require.True(t, kv.ErrInvalidTxn.Equal(err))
	require.False(t, txn.Valid())
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("begin")
	tk1.MustExec("update t set c2=21 where c1=11")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	// tk1 will retry and the final value is 21
	tk1.MustExec("commit")

	// Check the result is correct
	tk.MustQuery("select c2 from t where c1=11").Check(testkit.Rows("21"))

	tk1.MustExec("begin")
	tk1.MustExec("update t set c2=21 where c1=11")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")
}

// TestAutocommit . See https://dev.mysql.com/doc/internals/en/status-flags.html
func TestAutocommit(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t;")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("insert t values ()")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("begin")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("insert t values ()")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("drop table if exists t")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)

	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)
	tk.MustExec("set autocommit=0")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("insert t values ()")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("commit")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("drop table if exists t")
	require.Equal(t, 0, int(tk.Session().Status()&mysql.ServerStatusAutocommit))
	tk.MustExec("set autocommit='On'")
	require.Greater(t, int(tk.Session().Status()&mysql.ServerStatusAutocommit), 0)

	// When autocommit is 0, transaction start ts should be the first *valid*
	// statement, rather than *any* statement.
	tk.MustExec("create table t (id int)")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("rollback")
	tk.MustExec("set @@autocommit = 0")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t select 1")

	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	// TODO: MySQL compatibility for setting global variable.
	// tk.MustExec("begin")
	// tk.MustExec("insert into t values (42)")
	// tk.MustExec("set @@global.autocommit = 1")
	// tk.MustExec("rollback")
	// tk.MustQuery("select count(*) from t where id = 42").Check(testkit.Rows("0"))
	// Even the transaction is rollbacked, the set statement succeed.
	// tk.MustQuery("select @@global.autocommit").Rows("1")
}

// TestTxnLazyInitialize tests that when autocommit = 0, not all statement starts
// a new transaction.
func TestTxnLazyInitialize(t *testing.T) {
	testTxnLazyInitialize(t, false)
	testTxnLazyInitialize(t, true)
}

func testTxnLazyInitialize(t *testing.T, isPessimistic bool) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")
	if isPessimistic {
		tk.MustExec("set tidb_txn_mode = 'pessimistic'")
	}

	tk.MustExec("set @@autocommit = 0")
	_, err := tk.Session().Txn(true)
	require.True(t, kv.ErrInvalidTxn.Equal(err))
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Those statements should not start a new transaction automatically.
	tk.MustQuery("select 1")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_general_log = 0")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	tk.MustQuery("explain select * from t")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	// Begin statement should start a new transaction.
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")

	tk.MustExec("select * from t")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")

	tk.MustExec("insert into t values (1)")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
}

func TestGlobalVarAccessor(t *testing.T) {
	varName := "max_allowed_packet"
	varValue := strconv.FormatUint(variable.DefMaxAllowedPacket, 10) // This is the default value for max_allowed_packet

	// The value of max_allowed_packet should be a multiple of 1024,
	// so the setting of varValue1 and varValue2 would be truncated to varValue0
	varValue0 := "4194304"
	varValue1 := "4194305"
	varValue2 := "4194306"

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	se := tk.Session().(variable.GlobalVarAccessor)
	// Get globalSysVar twice and get the same value
	v, err := se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue, v)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue, v)
	// Set global var to another value
	err = se.SetGlobalSysVar(varName, varValue1)
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	require.NoError(t, tk.Session().CommitTxn(context.TODO()))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	se1 := tk1.Session().(variable.GlobalVarAccessor)
	v, err = se1.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	err = se1.SetGlobalSysVar(varName, varValue2)
	require.NoError(t, err)
	v, err = se1.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	require.NoError(t, tk1.Session().CommitTxn(context.TODO()))

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)

	// For issue 10955, make sure the new session load `max_execution_time` into sessionVars.
	tk1.MustExec("set @@global.max_execution_time = 100")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	require.Equal(t, uint64(100), tk2.Session().GetSessionVars().MaxExecutionTime)
	tk1.MustExec("set @@global.max_execution_time = 0")

	result := tk.MustQuery("show global variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	tk.MustExec("set session sql_select_limit=100000000000;")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 100000000000"))
	tk.MustExec("set @@global.sql_select_limit = 1")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 1"))
	tk.MustExec("set @@global.sql_select_limit = default")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))

	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit = 0;")
	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit=1")

	err = tk.ExecToErr("set global time_zone = 'timezone'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, variable.ErrUnknownTimeZone))
}

func TestUpgradeSysvars(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session().(variable.GlobalVarAccessor)

	// Set the global var to a non-canonical form of the value
	// i.e. implying that it was set from an earlier version of TiDB.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', '0')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache() // update cache
	v, err := se.GetGlobalSysVar("tidb_enable_noop_functions")
	require.NoError(t, err)
	require.Equal(t, "OFF", v)

	// Set the global var to ""  which is the invalid version of this from TiDB 4.0.16
	// the err is quashed by the GetGlobalSysVar, and the default value is restored.
	// This helps callers of GetGlobalSysVar(), which can't individually be expected
	// to handle upgrade/downgrade issues correctly.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('rpl_semi_sync_slave_enabled', '')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("rpl_semi_sync_slave_enabled")
	require.NoError(t, err)
	require.Equal(t, "OFF", v) // the default value is restored.
	result := tk.MustQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_slave_enabled'")
	result.Check(testkit.Rows("rpl_semi_sync_slave_enabled OFF"))

	// Ensure variable out of range is converted to in range after upgrade.
	// This further helps for https://github.com/pingcap/tidb/pull/28842

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_executor_concurrency', '999')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("tidb_executor_concurrency")
	require.NoError(t, err)
	require.Equal(t, "256", v) // the max value is restored.

	// Handle the case of a completely bogus value from an earlier version of TiDB.
	// This could be the case if an ENUM sysvar removes a value.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', 'SOMEVAL')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache() // update cache
	v, err = se.GetGlobalSysVar("tidb_enable_noop_functions")
	require.NoError(t, err)
	require.Equal(t, "OFF", v) // the default value is restored.
}

func TestSetInstanceSysvarBySetGlobalSysVar(t *testing.T) {
	varName := "tidb_general_log"
	defaultValue := "OFF" // This is the default value for tidb_general_log

	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session().(variable.GlobalVarAccessor)

	// Get globalSysVar twice and get the same default value
	v, err := se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)

	// session.GetGlobalSysVar would not get the value which session.SetGlobalSysVar writes,
	// because SetGlobalSysVar calls SetGlobalFromHook, which uses TiDBGeneralLog's SetGlobal,
	// but GetGlobalSysVar could not access TiDBGeneralLog's GetGlobal.

	// set to "1"
	err = se.SetGlobalSysVar(varName, "ON")
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("1"))
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)

	// set back to "0"
	err = se.SetGlobalSysVar(varName, defaultValue)
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("0"))
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)
}

func TestMatchIdentity(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER `useridentity`@`%`")
	tk.MustExec("CREATE USER `useridentity`@`localhost`")
	tk.MustExec("CREATE USER `useridentity`@`192.168.1.1`")
	tk.MustExec("CREATE USER `useridentity`@`example.com`")

	// The MySQL matching rule is most specific to least specific.
	// So if I log in from 192.168.1.1 I should match that entry always.
	identity, err := tk.Session().MatchIdentity("useridentity", "192.168.1.1")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "192.168.1.1", identity.Hostname)

	// If I log in from localhost, I should match localhost
	identity, err = tk.Session().MatchIdentity("useridentity", "localhost")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "localhost", identity.Hostname)

	// If I log in from 192.168.1.2 I should match wildcard.
	identity, err = tk.Session().MatchIdentity("useridentity", "192.168.1.2")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "%", identity.Hostname)

	identity, err = tk.Session().MatchIdentity("useridentity", "127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	require.Equal(t, "localhost", identity.Hostname)

	// This uses the lookup of example.com to get an IP address.
	// We then login with that IP address, but expect it to match the example.com
	// entry in the privileges table (by reverse lookup).
	ips, err := net.LookupHost("example.com")
	require.NoError(t, err)
	identity, err = tk.Session().MatchIdentity("useridentity", ips[0])
	require.NoError(t, err)
	require.Equal(t, "useridentity", identity.Username)
	// FIXME: we *should* match example.com instead
	// as long as skip-name-resolve is not set (DEFAULT)
	require.Equal(t, "%", identity.Hostname)
}

func TestGetSysVariables(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test ScopeSession
	tk.MustExec("select @@warning_count")
	tk.MustExec("select @@session.warning_count")
	tk.MustExec("select @@local.warning_count")
	err := tk.ExecToErr("select @@global.warning_count")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err %v", err))

	// Test ScopeGlobal
	tk.MustExec("select @@max_connections")
	tk.MustExec("select @@global.max_connections")
	tk.MustGetErrMsg("select @@session.max_connections", "[variable:1238]Variable 'max_connections' is a GLOBAL variable")
	tk.MustGetErrMsg("select @@local.max_connections", "[variable:1238]Variable 'max_connections' is a GLOBAL variable")

	// Test ScopeNone
	tk.MustExec("select @@performance_schema_max_mutex_classes")
	tk.MustExec("select @@global.performance_schema_max_mutex_classes")
	// For issue 19524, test
	tk.MustExec("select @@session.performance_schema_max_mutex_classes")
	tk.MustExec("select @@local.performance_schema_max_mutex_classes")
	tk.MustGetErrMsg("select @@global.last_insert_id", "[variable:1238]Variable 'last_insert_id' is a SESSION variable")
}

// TestInTrans . See https://dev.mysql.com/doc/internals/en/status-flags.html
func TestInTrans(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("insert t values ()")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("drop table if exists t;")
	require.False(t, txn.Valid())
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	require.False(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.False(t, txn.Valid())
	tk.MustExec("commit")
	tk.MustExec("insert t values ()")

	tk.MustExec("set autocommit=0")
	tk.MustExec("begin")
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	require.False(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("commit")
	require.False(t, txn.Valid())

	tk.MustExec("set autocommit=1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustExec("begin")
	require.True(t, txn.Valid())
	tk.MustExec("insert t values ()")
	require.True(t, txn.Valid())
	tk.MustExec("rollback")
	require.False(t, txn.Valid())
}

func TestSession(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("ROLLBACK;")
	tk.Session().Close()
}

func TestSessionAuth(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.False(t, tk.Session().Auth(&auth.UserIdentity{Username: "Any not exist username with zero password!", Hostname: "anyhost"}, []byte(""), []byte("")))
}

func TestLastInsertID(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// insert
	tk.MustExec("create table t (c1 int not null auto_increment, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("insert into t set c2 = 11")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("1"))

	tk.MustExec("insert into t (c2) values (22), (33), (44)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	tk.MustExec("insert into t (c1, c2) values (10, 55)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	// replace
	tk.MustExec("replace t (c2) values(66)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 11", "2 22", "3 33", "4 44", "10 55", "11 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("11"))

	// update
	tk.MustExec("update t set c1=last_insert_id(c1 + 100)")
	tk.MustQuery("select * from t").Check(testkit.Rows("101 11", "102 22", "103 33", "104 44", "110 55", "111 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("111"))
	tk.MustExec("insert into t (c2) values (77)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	// drop
	tk.MustExec("drop table t")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	tk.MustExec("create table t (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustExec("insert into t set c2 = 30")

	// insert values
	lastInsertID := tk.Session().LastInsertID()
	tk.MustExec("prepare stmt1 from 'insert into t (c2) values (?)'")
	tk.MustExec("set @v1=10")
	tk.MustExec("set @v2=20")
	tk.MustExec("execute stmt1 using @v1")
	tk.MustExec("execute stmt1 using @v2")
	tk.MustExec("deallocate prepare stmt1")
	currLastInsertID := tk.Session().GetSessionVars().StmtCtx.PrevLastInsertID
	tk.MustQuery("select c1 from t where c2 = 20").Check(testkit.Rows(fmt.Sprint(currLastInsertID)))
	require.Equal(t, currLastInsertID, lastInsertID+2)
}

func TestBinaryReadOnly(t *testing.T) {
	store, clean := realtikvtest.CreateMockStoreAndSetup(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key)")
	id, _, _, err := tk.Session().PrepareStmt("select i from t where i = ?")
	require.NoError(t, err)
	id2, _, _, err := tk.Session().PrepareStmt("insert into t values (?)")
	require.NoError(t, err)
	tk.MustExec("set autocommit = 0")
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), id, []types.Datum{types.NewDatum(1)})
	require.NoError(t, err)
	require.Equal(t, 0, session.GetHistory(tk.Session()).Count())
	tk.MustExec("insert into t values (1)")
	require.Equal(t, 1, session.GetHistory(tk.Session()).Count())
	_, err = tk.Session().ExecutePreparedStmt(context.Background(), id2, []types.Datum{types.NewDatum(2)})
	require.NoError(t, err)
	require.Equal(t, 2, session.GetHistory(tk.Session()).Count())
	tk.MustExec("commit")
}
