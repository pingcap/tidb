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

package session_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func clearTiKVStorage(store kv.Storage) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		if err := txn.Delete(iter.Key()); err != nil {
			return errors.Trace(err)
		}
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

func clearEtcdStorage(ebd kv.EtcdBackend) error {
	endpoints, err := ebd.EtcdAddrs()
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
		TLS: ebd.TLSConfig(),
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	resp, err := cli.Get(context.Background(), "/tidb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	for _, kv := range resp.Kvs {
		if kv.Lease != 0 {
			if _, err := cli.Revoke(context.Background(), clientv3.LeaseID(kv.Lease)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	_, err = cli.Delete(context.Background(), "/tidb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func createMockStoreAndSetup(t *testing.T) (kv.Storage, func()) {
	store, _, clean := createMockStoreAndDomainAndSetup(t)
	return store, clean
}

func createMockStoreAndDomainAndSetup(t *testing.T) (kv.Storage, *domain.Domain, func()) {
	if *withTiKV {
		var d driver.TiKVDriver
		config.UpdateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		store, err := d.Open("tikv://127.0.0.1:2379?disableGC=true")
		require.NoError(t, err)
		require.NoError(t, clearTiKVStorage(store))
		require.NoError(t, clearEtcdStorage(store.(kv.EtcdBackend)))
		session.ResetStoreForWithTiKVTest(store)
		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)

		return store, dom, func() {
			dom.Close()
			require.NoError(t, store.Close())
		}
	}
	return testkit.CreateMockStoreAndDomain(t)
}

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
