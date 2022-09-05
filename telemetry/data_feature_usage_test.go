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

	"github.com/pingcap/tidb/config"
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
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListColumnsCnt)

	telemetry.PostReportTelemetryDataForTest()
	tk.MustExec("drop table if exists pt1")
	tk.MustExec("create table pt1 (a int,b int) partition by range(a) (" +
		"partition p0 values less than (3)," +
		"partition p1 values less than (6), " +
		"partition p2 values less than (9)," +
		"partition p3 values less than (12)," +
		"partition p4 values less than (15))")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionHashCnt)
	require.Equal(t, int64(5), usage.TablePartition.TablePartitionMaxPartitionsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListCnt)
	require.Equal(t, int64(1), usage.TablePartition.TablePartitionRangeCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionRangeColumnsCnt)
	require.Equal(t, int64(0), usage.TablePartition.TablePartitionListColumnsCnt)
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

	tk.MustExec("create table t(a int);")
	tk.MustExec("batch limit 1 delete from t")
	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.NonTransactionalUsage.DeleteCount)
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
	require.False(t, usage.EnableCostModelVer2)

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
