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
