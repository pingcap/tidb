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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
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

func TestMultiSchemaChange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

func TestClusterIndexUsageInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()

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

type tiflashContext struct {
	store   kv.Storage
	dom     *domain.Domain
	tiflash *infosync.MockTiFlash
	cluster *unistore.Cluster
}

func createTiFlashContext(t *testing.T) (*tiflashContext, func()) {
	s := &tiflashContext{}
	var err error

	ddl.PollTiFlashInterval = 1000 * time.Millisecond
	ddl.PullTiFlashPdTick.Store(60)
	s.tiflash = infosync.NewMockTiFlash()
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
			s.cluster = mockCluster
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)

	require.NoError(t, err)
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	infosync.SetMockTiFlash(s.tiflash)
	require.NoError(t, err)
	s.dom.SetStatsUpdating(true)

	tearDown := func() {
		s.tiflash.Lock()
		s.tiflash.StatusServer.Close()
		s.tiflash.Unlock()
		s.dom.Close()
		require.NoError(t, s.store.Close())
		ddl.PollTiFlashInterval = 2 * time.Second
	}
	return s, tearDown
}

func TestTiFlashModeStatistics(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")

	usage, err := telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), usage.TiFlashModeStatistics.FastModeTableCount)
	require.Equal(t, int64(0), usage.TiFlashModeStatistics.NormalModeTableCount)

	tk.MustExec(`create table t1(a int);`)
	tk.MustExec(`alter table t1 set tiflash replica 1;`)
	tk.MustExec(`alter table t1 set tiflash mode fast;`)

	tk.MustExec(`create table t2(a int);`)
	tk.MustExec(`alter table t2 set tiflash replica 1;`)
	tk.MustExec(`alter table t2 set tiflash mode normal;`)

	tk.MustExec(`create table t3(a int);`)
	tk.MustExec(`alter table t3 set tiflash replica 1;`)

	tk.MustExec(`create table t4(a int);`)

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.TiFlashModeStatistics.FastModeTableCount)
	require.Equal(t, int64(2), usage.TiFlashModeStatistics.NormalModeTableCount)

	tk.MustExec("drop table t1;")
	tk.MustExec(`alter table t2 set tiflash mode fast;`)

	usage, err = telemetry.GetFeatureUsage(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.TiFlashModeStatistics.FastModeTableCount)
	require.Equal(t, int64(1), usage.TiFlashModeStatistics.NormalModeTableCount)
}
