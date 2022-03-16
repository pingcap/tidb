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

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestTxnUsageInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestPlacementPolicies(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
