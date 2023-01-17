// Copyright 2021 PingCAP, Inc.
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

package telemetry_test

import (
	"fmt"
	"testing"

	_ "github.com/pingcap/tidb/autoid_service"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestTxnUsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	t.Run("Used", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBEnableAsyncCommit))
		tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBEnable1PC))

		txnUsage := telemetry.GetTxnUsageInfo(tk.Session())
		require.False(t, txnUsage.AsyncCommitUsed)
		require.False(t, txnUsage.OnePCUsed)

		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBEnableAsyncCommit))
		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBEnable1PC))

		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.AsyncCommitUsed)
		require.True(t, txnUsage.OnePCUsed)

		tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBEnableMutationChecker))
		tk.MustExec(fmt.Sprintf("set global %s = off", variable.TiDBTxnAssertionLevel))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.False(t, txnUsage.MutationCheckerUsed)
		require.Equal(t, "OFF", txnUsage.AssertionLevel)

		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBEnableMutationChecker))
		tk.MustExec(fmt.Sprintf("set global %s = strict", variable.TiDBTxnAssertionLevel))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.MutationCheckerUsed)
		require.Equal(t, "STRICT", txnUsage.AssertionLevel)

		tk.MustExec(fmt.Sprintf("set global %s = fast", variable.TiDBTxnAssertionLevel))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.Equal(t, "FAST", txnUsage.AssertionLevel)

		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBRCReadCheckTS))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.RcCheckTS)

		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBRCWriteCheckTs))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.RCWriteCheckTS)
	})

	t.Run("Count", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists txn_usage_info")
		tk.MustExec("create table txn_usage_info (a int)")
		tk.MustExec(fmt.Sprintf("set %s = 1", variable.TiDBEnableAsyncCommit))
		tk.MustExec(fmt.Sprintf("set %s = 1", variable.TiDBEnable1PC))
		tk.MustExec("insert into txn_usage_info values (1)")
		tk.MustExec(fmt.Sprintf("set %s = 0", variable.TiDBEnable1PC))
		tk.MustExec("insert into txn_usage_info values (2)")
		tk.MustExec(fmt.Sprintf("set %s = 0", variable.TiDBEnableAsyncCommit))
		tk.MustExec("insert into txn_usage_info values (3)")

		txnUsage := telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.AsyncCommitUsed)
		require.True(t, txnUsage.OnePCUsed)
		require.Greater(t, txnUsage.TxnCommitCounter.AsyncCommit, int64(0))
		require.Greater(t, txnUsage.TxnCommitCounter.OnePC, int64(0))
		require.Greater(t, txnUsage.TxnCommitCounter.TwoPC, int64(0))
	})
}

func TestTemporaryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.TemporaryTable)

	tk.MustExec("create global temporary table t (id int) on commit delete rows")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.TemporaryTable)
}

func TestCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.CachedTable)
	tk.MustExec("drop table if exists tele_cache_t")
	tk.MustExec("create table tele_cache_t (id int)")
	tk.MustExec("alter table tele_cache_t cache")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.CachedTable)
	tk.MustExec("alter table tele_cache_t nocache")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.CachedTable)
}

func TestAutoIDNoCache(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.CachedTable)
	tk.MustExec("drop table if exists tele_autoid")
	tk.MustExec("create table tele_autoid (id int) auto_id_cache 1")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.AutoIDNoCache)
	tk.MustExec("drop table tele_autoid")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.AutoIDNoCache)
}

func TestAccountLock(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.AccountLock.LockUser)
	require.Equal(t, int64(0), usage.AccountLock.UnlockUser)
	require.Equal(t, int64(0), usage.AccountLock.CreateOrAlterUser)

	tk.MustExec("drop user if exists testUser")
	tk.MustExec("create user testUser account lock")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.AccountLock.LockUser)
	require.Equal(t, int64(0), usage.AccountLock.UnlockUser)
	require.Equal(t, int64(1), usage.AccountLock.CreateOrAlterUser)
	tk.MustExec("alter user testUser account unlock")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.AccountLock.LockUser)
	require.Equal(t, int64(1), usage.AccountLock.UnlockUser)
	require.Equal(t, int64(2), usage.AccountLock.CreateOrAlterUser)
}

func TestMultiSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.MultiSchemaChange.MultiSchemaChangeUsed)

	tk.MustExec("drop table if exists tele_multi_t")
	tk.MustExec("create table tele_multi_t(id int)")
	tk.MustExec("alter table tele_multi_t add column b int")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.MultiSchemaChange.MultiSchemaChangeUsed)

	tk.MustExec("alter table tele_multi_t add column c int, drop column b")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.MultiSchemaChange.MultiSchemaChangeUsed)

	tk.MustExec("alter table tele_multi_t add column b int, drop column c")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.MultiSchemaChange.MultiSchemaChangeUsed)

	tk.MustExec("alter table tele_multi_t drop column b")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.MultiSchemaChange.MultiSchemaChangeUsed)
}

func TestTablePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionMaxPartitionsCnt)

	tk.MustExec("drop table if exists pt")
	tk.MustExec("create table pt (a int,b int) partition by hash(a) partitions 4")

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionHashCnt)
	require.Equal(t, int64(4), usage.TablePartition.TablePartitionMaxPartitionsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeColumnsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeColumnsGt1Cnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeColumnsGt2Cnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeColumnsGt3Cnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListColumnsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionCreateIntervalPartitionsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionAddIntervalPartitionsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionDropIntervalPartitionsCnt)

	telemetry.PostReportTelemetryDataForTest()
	tk.MustExec("drop table if exists pt1")
	tk.MustExec("create table pt1 (a int,b int) partition by range(a) (" +
		"partition p0 values less than (3)," +
		"partition p1 values less than (6), " +
		"partition p2 values less than (9)," +
		"partition p3 values less than (12)," +
		"partition p4 values less than (15))")
	tk.MustExec("alter table pt1 first partition less than (9)")
	tk.MustExec("alter table pt1 last partition less than (21)")
	tk.MustExec("drop table if exists pt1")
	tk.MustExec("create table pt1 (d datetime primary key, v varchar(255)) partition by range columns(d)" +
		" interval (1 day) first partition less than ('2022-01-01') last partition less than ('2022-02-22')")
	tk.MustExec("create table pt2 (d datetime, v varchar(255)) partition by range columns(d,v)" +
		" (partition p0 values less than ('2022-01-01', ''), partition p1 values less than ('2023-01-01','ZZZ'))")
	tk.MustExec("create table pt3 (d datetime, v varchar(255), i int) partition by range columns(d,v,i)" +
		" (partition p0 values less than ('2022-01-01', '', 0), partition p1 values less than ('2023-01-01','ZZZ', 1))")
	tk.MustExec("create table pt4 (d datetime, v varchar(255), s bigint unsigned, u tinyint) partition by range columns(d,v,s,u)" +
		" (partition p0 values less than ('2022-01-01', '', 1, -3), partition p1 values less than ('2023-01-01','ZZZ', 1, 3))")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(5), usage.TablePartition.TablePartitionCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionHashCnt)
	require.Equal(t, int64(11), usage.TablePartition.TablePartitionMaxPartitionsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionRangeCnt)
	require.Equal(t, int64(4), usage.TablePartition.TablePartitionRangeColumnsCnt)
	require.Equal(t, int64(3), usage.TablePartition.TablePartitionRangeColumnsGt1Cnt)
	require.Equal(t, int64(2), usage.TablePartition.TablePartitionRangeColumnsGt2Cnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionRangeColumnsGt3Cnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListColumnsCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionCreateIntervalPartitionsCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionAddIntervalPartitionsCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionDropIntervalPartitionsCnt)

	tk.MustExec("drop table if exists pt2")
	tk.MustExec("create table pt2 (a int,b int) partition by range(a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (20))")
	tk.MustExec("drop table if exists nt")
	tk.MustExec("create table nt (a int,b int)")
	require.Equal(t, int64(0), usage.ExchangePartition.ExchangePartitionCnt)
	tk.MustExec(`alter table pt2 exchange partition p1 with table nt`)
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.ExchangePartition.ExchangePartitionCnt)

	require.Equal(t, int64(0), usage.TablePartition.TablePartitionComactCnt)
	tk.MustExec(`alter table pt2 compact partition p0 tiflash replica;`)
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionComactCnt)
}

func TestPlacementPolicies(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(0), usage.PlacementPolicyUsage.NumPlacementPolicies)
	require.Equal(t, uint64(0), usage.PlacementPolicyUsage.NumDBWithPolicies)
	require.Equal(t, uint64(0), usage.PlacementPolicyUsage.NumTableWithPolicies)
	require.Equal(t, uint64(0), usage.PlacementPolicyUsage.NumPartitionWithExplicitPolicies)

	tk.MustExec("create placement policy p1 followers=4;")
	tk.MustExec(`create placement policy p2 primary_region="cn-east-1" regions="cn-east-1,cn-east"`)
	tk.MustExec(`create placement policy p3 followers=3`)
	tk.MustExec("alter database test placement policy=p1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("create table t2(a int) placement policy=p2;")
	tk.MustExec("create table t3(id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100) placement policy p3," +
		"PARTITION p1 VALUES LESS THAN (1000))")

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(3), usage.PlacementPolicyUsage.NumPlacementPolicies)
	require.Equal(t, uint64(1), usage.PlacementPolicyUsage.NumDBWithPolicies)
	require.Equal(t, uint64(3), usage.PlacementPolicyUsage.NumTableWithPolicies)
	require.Equal(t, uint64(1), usage.PlacementPolicyUsage.NumPartitionWithExplicitPolicies)

	tk.MustExec("drop table t2;")
	tk.MustExec("drop placement policy p2;")
	tk.MustExec("alter table t3 placement policy=default")

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(2), usage.PlacementPolicyUsage.NumPlacementPolicies)
	require.Equal(t, uint64(1), usage.PlacementPolicyUsage.NumDBWithPolicies)
	require.Equal(t, uint64(1), usage.PlacementPolicyUsage.NumTableWithPolicies)
	require.Equal(t, uint64(1), usage.PlacementPolicyUsage.NumPartitionWithExplicitPolicies)
}

func TestAutoCapture(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.AutoCapture)

	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off")
	}()
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.AutoCapture)
}

func TestClusterIndexUsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int key clustered);")
	tk.MustExec("create table t2(a int);")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, usage.ClusterIndex)
	require.Equal(t, uint64(1), usage.NewClusterIndex.NumClusteredTables)
	require.Equal(t, uint64(2), usage.NewClusterIndex.NumTotalTables)
}

func TestNonTransactionalUsage(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.NonTransactionalUsage.DeleteCount)
	require.Equal(t, int64(0), usage.NonTransactionalUsage.UpdateCount)
	require.Equal(t, int64(0), usage.NonTransactionalUsage.InsertCount)

	tk.MustExec("create table t(a int);")
	tk.MustExec("batch limit 1 delete from t")
	tk.MustExec("batch limit 1 update t set a = 1")
	tk.MustExec("batch limit 1 insert into t select * from t")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.NonTransactionalUsage.DeleteCount)
	require.Equal(t, int64(1), usage.NonTransactionalUsage.UpdateCount)
	require.Equal(t, int64(1), usage.NonTransactionalUsage.InsertCount)
}

func TestGlobalKillUsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.GlobalKill)

	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = false
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.GlobalKill)
}

func TestPagingUsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.EnablePaging == variable.DefTiDBEnablePaging)

	tk.Session().GetSessionVars().EnablePaging = false
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.EnablePaging)
}

func TestCostModelVer2UsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.EnableCostModelVer2, variable.DefTiDBCostModelVer == 2)

	tk.Session().GetSessionVars().CostModelVersion = 2
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.EnableCostModelVer2)
}

func TestTxnSavepointUsageInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("savepoint sp1")
	tk.MustExec("savepoint sp2")
	txnUsage := telemetry.GetTxnUsageInfo(tk.Session())
	require.Equal(t, int64(2), txnUsage.SavepointCounter)

	tk.MustExec("savepoint sp3")
	txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
	require.Equal(t, int64(3), txnUsage.SavepointCounter)

	telemetry.PostSavepointCount()
	tk.MustExec("savepoint sp1")
	txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
	require.Equal(t, int64(1), txnUsage.SavepointCounter)
}

func TestLazyPessimisticUniqueCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	usage := telemetry.GetTxnUsageInfo(tk.Session())
	require.Equal(t, int64(0), usage.LazyUniqueCheckSetCounter)

	tk2.MustExec("set @@tidb_constraint_check_in_place_pessimistic = 0")
	tk2.MustExec("set @@tidb_constraint_check_in_place_pessimistic = 0")
	usage = telemetry.GetTxnUsageInfo(tk.Session())
	require.Equal(t, int64(2), usage.LazyUniqueCheckSetCounter)
}

func TestFlashbackCluster(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.Equal(t, int64(0), usage.DDLUsageCounter.FlashbackClusterUsed)
	require.NoError(t, err)

	tk.MustExecToErr("flashback cluster to timestamp '2011-12-21 00:00:00'")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.Equal(t, int64(1), usage.DDLUsageCounter.FlashbackClusterUsed)
	require.NoError(t, err)

	tk1.MustExec("use test")
	tk1.MustExec("create table t(a int)")
	usage, err = telemetry.GetFeatureUsage(tk1.Session())
	require.Equal(t, int64(1), usage.DDLUsageCounter.FlashbackClusterUsed)
	require.NoError(t, err)
}

func TestAddIndexAccelerationAndMDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.Equal(t, int64(0), usage.DDLUsageCounter.AddIndexIngestUsed)
	require.NoError(t, err)

	allow := ddl.IsEnableFastReorg()
	require.Equal(t, true, allow)
	tk.MustExec("set global tidb_enable_metadata_lock = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tele_t")
	tk.MustExec("create table tele_t(id int, b int)")
	tk.MustExec("insert into tele_t values(1,1),(2,2);")
	tk.MustExec("alter table tele_t add index idx_org(b)")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.DDLUsageCounter.AddIndexIngestUsed)
	require.Equal(t, false, usage.DDLUsageCounter.MetadataLockUsed)

	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on")
	tk.MustExec("set global tidb_enable_metadata_lock = 1")
	allow = ddl.IsEnableFastReorg()
	require.Equal(t, true, allow)
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.DDLUsageCounter.AddIndexIngestUsed)
	tk.MustExec("alter table tele_t add index idx_new(b)")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.DDLUsageCounter.AddIndexIngestUsed)
	require.Equal(t, true, usage.DDLUsageCounter.MetadataLockUsed)
}

func TestGlobalMemoryControl(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.EnableGlobalMemoryControl == (variable.DefTiDBServerMemoryLimit != "0"))

	tk.MustExec("set global tidb_server_memory_limit = 5 << 30")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.EnableGlobalMemoryControl)

	tk.MustExec("set global tidb_server_memory_limit = 0")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.False(t, usage.EnableGlobalMemoryControl)
}

func TestIndexMergeUsage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(c1 int, c2 int, index idx1(c1), index idx2(c2))")
	res := tk.MustQuery("explain select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 and c2 = 1").Rows()
	require.Contains(t, res[0][0], "IndexMerge")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.IndexMergeUsageCounter.IndexMergeUsed, int64(0))

	tk.MustExec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 and c2 = 1")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.IndexMergeUsageCounter.IndexMergeUsed)

	tk.MustExec("select /*+ use_index_merge(t1, idx1, idx2) */ * from t1 where c1 = 1 or c2 = 1")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.IndexMergeUsageCounter.IndexMergeUsed)

	tk.MustExec("select /*+ no_index_merge() */ * from t1 where c1 = 1 or c2 = 1")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.IndexMergeUsageCounter.IndexMergeUsed)
}
