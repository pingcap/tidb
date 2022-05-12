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

package realtikvtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestSysdateIsNow(t *testing.T) {
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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

func TestLocalTemporaryTablePointGet(t *testing.T) {
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
	store, clean := createMockStoreAndSetup(t)
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
