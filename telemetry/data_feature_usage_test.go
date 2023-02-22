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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/autoid_service"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/model"
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

		tk.MustExec(fmt.Sprintf("set global %s = 0", variable.TiDBPessimisticTransactionAggressiveLocking))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.False(t, txnUsage.AggressiveLocking)

		tk.MustExec(fmt.Sprintf("set global %s = 1", variable.TiDBPessimisticTransactionAggressiveLocking))
		txnUsage = telemetry.GetTxnUsageInfo(tk.Session())
		require.True(t, txnUsage.AggressiveLocking)
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
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionReorganizePartitionCnt)

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
	tk.MustExec("alter table pt1 reorganize partition p4 into (partition p4 values less than (13), partition p5 values less than (15))")
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
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionReorganizePartitionCnt)

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

func TestResourceGroups(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(0), usage.ResourceControlUsage.NumResourceGroups)
	require.Equal(t, false, usage.ResourceControlUsage.Enabled)

	tk.MustExec("set global tidb_enable_resource_control = 'ON'")
	tk.MustExec("create resource group x ru_per_sec=100")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, true, usage.ResourceControlUsage.Enabled)
	require.Equal(t, uint64(1), usage.ResourceControlUsage.NumResourceGroups)

	tk.MustExec("create resource group y ru_per_sec=100")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(2), usage.ResourceControlUsage.NumResourceGroups)

	tk.MustExec("alter resource group y ru_per_sec=200")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(2), usage.ResourceControlUsage.NumResourceGroups)

	tk.MustExec("drop resource group y")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.ResourceControlUsage.NumResourceGroups)

	tk.MustExec("set global tidb_enable_resource_control = 'OFF'")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.ResourceControlUsage.NumResourceGroups)
	require.Equal(t, false, usage.ResourceControlUsage.Enabled)
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

func TestDistReorgUsage(t *testing.T) {
	t.Skip("skip in order to pass the test quickly")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	initCount := usage.DDLUsageCounter.DistReorgUsed

	tk.MustExec("set @@global.tidb_ddl_distribute_reorg = off")
	allow := variable.DDLEnableDistributeReorg.Load()
	require.Equal(t, false, allow)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tele_t")
	tk.MustExec("create table tele_t(id int, b int)")
	tk.MustExec("insert into tele_t values(1,1),(2,2);")
	tk.MustExec("alter table tele_t add index idx_org(b)")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, initCount, usage.DDLUsageCounter.DistReorgUsed)

	tk.MustExec("set @@global.tidb_ddl_distribute_reorg = on")
	allow = variable.DDLEnableDistributeReorg.Load()
	require.Equal(t, true, allow)
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, initCount, usage.DDLUsageCounter.DistReorgUsed)
	tk.MustExec("alter table tele_t add index idx_new(b)")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, initCount+1, usage.DDLUsageCounter.DistReorgUsed)
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

func TestTTLTelemetry(t *testing.T) {
	timeFormat := "2006-01-02 15:04:05"
	dateFormat := "2006-01-02"

	now := time.Now()
	curDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if interval := curDate.Add(time.Hour * 24).Sub(now); interval > 0 && interval < 5*time.Minute {
		// make sure testing is not running at the end of one day
		time.Sleep(interval)
	}

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_ttl_job_enable=0")

	getTTLTable := func(name string) *model.TableInfo {
		tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(name))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().TTLInfo)
		return tbl.Meta()
	}

	jobIDIdx := 1
	insertTTLHistory := func(tblName string, partitionName string, createTime, finishTime, ttlExpire time.Time, scanError string, totalRows, errorRows int64, status string) {
		defer func() {
			jobIDIdx++
		}()

		tbl := getTTLTable(tblName)
		tblID := tbl.ID
		partitionID := tbl.ID
		if partitionName != "" {
			for _, def := range tbl.Partition.Definitions {
				if def.Name.L == strings.ToLower(partitionName) {
					partitionID = def.ID
				}
			}
			require.NotEqual(t, tblID, partitionID)
		}

		summary := make(map[string]interface{})
		summary["total_rows"] = totalRows
		summary["success_rows"] = totalRows - errorRows
		summary["error_rows"] = errorRows
		summary["total_scan_task"] = 1
		summary["scheduled_scan_task"] = 1
		summary["finished_scan_task"] = 1
		if scanError != "" {
			summary["scan_task_err"] = scanError
		}

		summaryText, err := json.Marshal(summary)
		require.NoError(t, err)

		tk.MustExec("insert into "+
			"mysql.tidb_ttl_job_history ("+
			"	job_id, table_id, parent_table_id, table_schema, table_name, partition_name, "+
			"	create_time, finish_time, ttl_expire, summary_text, "+
			"	expired_rows, deleted_rows, error_delete_rows, status) "+
			"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			jobIDIdx, partitionID, tblID, "test", tblName, partitionName,
			createTime.Format(timeFormat), finishTime.Format(timeFormat), ttlExpire.Format(timeFormat), summaryText,
			totalRows, totalRows-errorRows, errorRows, status,
		)

		if status == "running" {
			tk.MustExec("update mysql.tidb_ttl_job_history set finish_time=FROM_UNIXTIME(1), summary_text=NULL, expired_rows=NULL, deleted_rows=NULL, error_delete_rows=NULL where job_id=?", strconv.Itoa(jobIDIdx))
		}
	}

	oneDayAgoDate := curDate.Add(-24 * time.Hour)
	// start today, end today
	times11 := []time.Time{curDate.Add(time.Hour), curDate.Add(2 * time.Hour), curDate}
	// start yesterday, end today
	times21 := []time.Time{curDate.Add(-2 * time.Hour), curDate, curDate.Add(-3 * time.Hour)}
	// start yesterday, end yesterday
	times31 := []time.Time{oneDayAgoDate, oneDayAgoDate.Add(time.Hour), oneDayAgoDate.Add(-time.Hour)}
	times32 := []time.Time{oneDayAgoDate.Add(2 * time.Hour), oneDayAgoDate.Add(3 * time.Hour), oneDayAgoDate.Add(time.Hour)}
	times33 := []time.Time{oneDayAgoDate.Add(4 * time.Hour), oneDayAgoDate.Add(5 * time.Hour), oneDayAgoDate.Add(3 * time.Hour)}
	// start 2 days ago, end yesterday
	times41 := []time.Time{oneDayAgoDate.Add(-2 * time.Hour), oneDayAgoDate.Add(time.Hour), oneDayAgoDate.Add(-3 * time.Hour)}
	// start two days ago, end two days ago
	times51 := []time.Time{oneDayAgoDate.Add(-5 * time.Hour), oneDayAgoDate.Add(-4 * time.Hour), oneDayAgoDate.Add(-6 * time.Hour)}

	tk.MustExec("create table t1 (t timestamp) TTL=`t` + interval 1 hour")
	insertTTLHistory("t1", "", times11[0], times11[1], times11[2], "", 100000000, 0, "finished")
	insertTTLHistory("t1", "", times21[0], times21[1], times21[2], "", 100000000, 0, "finished")
	insertTTLHistory("t1", "", times31[0], times31[1], times31[2], "err1", 112600, 110000, "finished")
	insertTTLHistory("t1", "", times32[0], times32[1], times32[2], "", 2600, 0, "timeout")
	insertTTLHistory("t1", "", times33[0], times33[1], times33[2], "", 2600, 0, "finished")
	insertTTLHistory("t1", "", times31[0], times31[1], times31[2], "", 100000000, 0, "running")
	insertTTLHistory("t1", "", times41[0], times41[1], times41[2], "", 2600, 0, "finished")
	insertTTLHistory("t1", "", times51[0], times51[1], times51[2], "", 100000000, 1, "finished")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	checkTableHistWithDeleteRows := func(vals ...int64) {
		require.Equal(t, 5, len(vals))
		require.Equal(t, 5, len(usage.TTLUsage.TableHistWithDeleteRows))
		require.Equal(t, int64(10*1000), *usage.TTLUsage.TableHistWithDeleteRows[0].LessThan)
		require.Equal(t, vals[0], usage.TTLUsage.TableHistWithDeleteRows[0].Count)
		require.Equal(t, int64(100*1000), *usage.TTLUsage.TableHistWithDeleteRows[1].LessThan)
		require.Equal(t, vals[1], usage.TTLUsage.TableHistWithDeleteRows[1].Count)
		require.Equal(t, int64(1000*1000), *usage.TTLUsage.TableHistWithDeleteRows[2].LessThan)
		require.Equal(t, vals[2], usage.TTLUsage.TableHistWithDeleteRows[2].Count)
		require.Equal(t, int64(10*1000*1000), *usage.TTLUsage.TableHistWithDeleteRows[3].LessThan)
		require.Equal(t, vals[3], usage.TTLUsage.TableHistWithDeleteRows[3].Count)
		require.True(t, usage.TTLUsage.TableHistWithDeleteRows[4].LessThanMax)
		require.Nil(t, usage.TTLUsage.TableHistWithDeleteRows[4].LessThan)
		require.Equal(t, vals[4], usage.TTLUsage.TableHistWithDeleteRows[4].Count)
	}

	checkTableHistWithDelay := func(vals ...int64) {
		require.Equal(t, 5, len(vals))
		require.Equal(t, 5, len(usage.TTLUsage.TableHistWithDelayTime))
		require.Equal(t, int64(1), *usage.TTLUsage.TableHistWithDelayTime[0].LessThan)
		require.Equal(t, vals[0], usage.TTLUsage.TableHistWithDelayTime[0].Count)
		require.Equal(t, int64(6), *usage.TTLUsage.TableHistWithDelayTime[1].LessThan)
		require.Equal(t, vals[1], usage.TTLUsage.TableHistWithDelayTime[1].Count)
		require.Equal(t, int64(24), *usage.TTLUsage.TableHistWithDelayTime[2].LessThan)
		require.Equal(t, vals[2], usage.TTLUsage.TableHistWithDelayTime[2].Count)
		require.Equal(t, int64(72), *usage.TTLUsage.TableHistWithDelayTime[3].LessThan)
		require.Equal(t, vals[3], usage.TTLUsage.TableHistWithDelayTime[3].Count)
		require.True(t, usage.TTLUsage.TableHistWithDelayTime[4].LessThanMax)
		require.Nil(t, usage.TTLUsage.TableHistWithDelayTime[4].LessThan)
		require.Equal(t, vals[4], usage.TTLUsage.TableHistWithDelayTime[4].Count)
	}

	require.False(t, usage.TTLUsage.TTLJobEnabled)
	require.Equal(t, int64(1), usage.TTLUsage.TTLTables)
	require.Equal(t, int64(1), usage.TTLUsage.TTLJobEnabledTables)
	require.Equal(t, oneDayAgoDate.Format(dateFormat), usage.TTLUsage.TTLHistDate)
	checkTableHistWithDeleteRows(0, 1, 0, 0, 0)
	checkTableHistWithDelay(0, 0, 1, 0, 0)

	tk.MustExec("create table t2 (t timestamp) TTL=`t` + interval 20 hour")
	tk.MustExec("set @@global.tidb_ttl_job_enable=1")
	insertTTLHistory("t2", "", times31[0], times31[1], times31[2], "", 9999, 0, "finished")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.TTLUsage.TTLJobEnabled)
	require.Equal(t, int64(2), usage.TTLUsage.TTLTables)
	require.Equal(t, int64(2), usage.TTLUsage.TTLJobEnabledTables)
	require.Equal(t, oneDayAgoDate.Format(dateFormat), usage.TTLUsage.TTLHistDate)
	checkTableHistWithDeleteRows(1, 1, 0, 0, 0)
	checkTableHistWithDelay(0, 1, 1, 0, 0)

	tk.MustExec("create table t3 (t timestamp) TTL=`t` + interval 1 hour TTL_ENABLE='OFF'")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.True(t, usage.TTLUsage.TTLJobEnabled)
	require.Equal(t, int64(3), usage.TTLUsage.TTLTables)
	require.Equal(t, int64(2), usage.TTLUsage.TTLJobEnabledTables)
	require.Equal(t, oneDayAgoDate.Format(dateFormat), usage.TTLUsage.TTLHistDate)
	checkTableHistWithDeleteRows(1, 1, 0, 0, 0)
	checkTableHistWithDelay(0, 1, 1, 0, 1)
}

func TestStoreBatchCopr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	init, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, init.StoreBatchCoprUsage.BatchSize, 4)

	tk.MustExec("drop table if exists tele_batch_t")
	tk.MustExec("create table tele_batch_t (id int primary key, c int, k int, index i(k))")
	tk.MustExec("select * from tele_batch_t force index(i) where k between 1 and 10 and k % 2 != 0")
	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.StoreBatchCoprUsage.BatchSize, 4)
	diff := usage.StoreBatchCoprUsage.Sub(*init.StoreBatchCoprUsage)
	require.Equal(t, diff.BatchedQuery, int64(1))
	require.Equal(t, diff.BatchedQueryTask, int64(0))
	require.Equal(t, diff.BatchedCount, int64(0))
	require.Equal(t, diff.BatchedFallbackCount, int64(0))

	tk.MustExec("insert into tele_batch_t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (5, 5, 5), (7, 7, 7)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/setRangesPerTask", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/setRangesPerTask"))
	}()
	tk.MustQuery("select * from tele_batch_t force index(i) where k between 1 and 3 and k % 2 != 0").Sort().
		Check(testkit.Rows("1 1 1", "3 3 3"))
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.StoreBatchCoprUsage.BatchSize, 4)
	diff = usage.StoreBatchCoprUsage.Sub(*init.StoreBatchCoprUsage)
	require.Equal(t, diff.BatchedQuery, int64(2))
	require.Equal(t, diff.BatchedQueryTask, int64(2))
	require.Equal(t, diff.BatchedCount, int64(1))
	require.Equal(t, diff.BatchedFallbackCount, int64(0))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/batchCopRegionError", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/batchCopRegionError"))
	}()
	tk.MustQuery("select * from tele_batch_t force index(i) where k between 1 and 3 and k % 2 != 0").Sort().
		Check(testkit.Rows("1 1 1", "3 3 3"))
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.StoreBatchCoprUsage.BatchSize, 4)
	diff = usage.StoreBatchCoprUsage.Sub(*init.StoreBatchCoprUsage)
	require.Equal(t, diff.BatchedQuery, int64(3))
	require.Equal(t, diff.BatchedQueryTask, int64(4))
	require.Equal(t, diff.BatchedCount, int64(1))
	require.Equal(t, diff.BatchedFallbackCount, int64(1))

	tk.MustExec("set global tidb_store_batch_size = 0")
	tk.MustExec("set session tidb_store_batch_size = 0")
	tk.MustQuery("select * from tele_batch_t force index(i) where k between 1 and 3 and k % 2 != 0").Sort().
		Check(testkit.Rows("1 1 1", "3 3 3"))
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, usage.StoreBatchCoprUsage.BatchSize, 0)
	diff = usage.StoreBatchCoprUsage.Sub(*init.StoreBatchCoprUsage)
	require.Equal(t, diff.BatchedQuery, int64(3))
	require.Equal(t, diff.BatchedQueryTask, int64(4))
	require.Equal(t, diff.BatchedCount, int64(1))
	require.Equal(t, diff.BatchedFallbackCount, int64(1))
}

func TestAggressiveLockingUsage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	usage, err := telemetry.GetFeatureUsage(tk2.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingUsed)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingEffective)

	tk.MustExec("set @@tidb_pessimistic_txn_aggressive_locking = 1")

	tk.MustExec("begin pessimistic")
	tk.MustExec("update t set v = v + 1 where id = 1")
	usage, err = telemetry.GetFeatureUsage(tk2.Session())
	// Not counted before transaction committing.
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingUsed)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingEffective)

	tk.MustExec("commit")
	usage, err = telemetry.GetFeatureUsage(tk2.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingUsed)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingEffective)

	// Counted by transaction instead of by statement.
	tk.MustExec("begin pessimistic")
	tk.MustExec("update t set v = v + 1 where id = 1")
	tk.MustExec("update t set v = v + 1 where id = 2")
	tk.MustExec("commit")
	usage, err = telemetry.GetFeatureUsage(tk2.Session())
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingUsed)
	require.Equal(t, int64(0), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingEffective)

	// Effective only when LockedWithConflict occurs.
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk.MustExec("begin pessimistic")
	tk3.MustExec("begin pessimistic")
	tk3.MustExec("update t set v = v + 1 where id = 1")
	ch := make(chan interface{})
	go func() {
		tk.MustExec("update t set v = v + 1 where id = 1")
		ch <- nil
	}()
	select {
	case <-ch:
		require.Fail(t, "expected statement to be blocked but finished")
	case <-time.After(time.Millisecond * 100):
	}
	tk3.MustExec("commit")
	select {
	case <-time.After(time.Second):
		require.Fail(t, "expected statement to be resumed but still blocked")
	case <-ch:
	}
	tk.MustExec("commit")
	usage, err = telemetry.GetFeatureUsage(tk2.Session())
	require.NoError(t, err)
	require.Equal(t, int64(3), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingUsed)
	require.Equal(t, int64(1), usage.Txn.AggressiveLockingUsageCounter.TxnAggressiveLockingEffective)
}
