// Copyright 2026 PingCAP, Inc.
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

package domain_test

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
)

func TestMaskingPolicyPersistAfterRestart(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create table and masking policy
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key auto_increment, c char(120))")
	tk.MustExec("create masking policy simple_mask on t(c) as c enable")

	// Verify policy shows in show create table
	tk.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is in system table
	tk.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))

	// Simulate restart by creating a new test kit
	// This should trigger a fresh info schema load
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	// BUG: Policy disappears after restart!
	// This test should FAIL until the bug is fixed
	tk2.MustQuery("show create table t").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `c` char(120) DEFAULT NULL /* MASKING POLICY `simple_mask` ENABLED */,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Verify policy is still in system table
	tk2.MustQuery("select policy_name, db_name, table_name, column_name, status from mysql.tidb_masking_policy where policy_name = 'simple_mask'").
		Check(testkit.Rows("simple_mask test t c ENABLED"))
}

func TestMaskingPolicyColdSnapshot(t *testing.T) {
	for _, change := range []string{
		"alter table t_cold disable masking policy p",
		"alter table t_cold drop masking policy p",
		"truncate table t_cold",
	} {
		t.Run(change, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_cold(c varchar(20))")
			tk.MustExec("insert into t_cold values ('secret')")
			tk.MustExec("create masking policy p on t_cold(c) as mask_full(c)")
			ctx := context.Background()
			tbl, err := dom.InfoSchema().TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("t_cold"))
			require.NoError(t, err)
			snapshot, err := store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			tk.MustExec(change)
			latest := dom.InfoSchema()
			current, err := store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			// Evict historical schemas so the next read cannot reuse a warmed policy map.
			dom.InfoCache().Reset(16)
			dom.InfoCache().Insert(latest, current.Ver)
			historical, err := dom.GetSnapshotInfoSchema(snapshot.Ver)
			require.NoError(t, err)
			var intercepted atomic.Int32
			loadCtx := interceptor.WithRPCInterceptor(ctx, interceptor.NewRPCInterceptor("masking-policy-user-context", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
				return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
					intercepted.Add(1)
					return next(target, req)
				}
			}))
			policy, ok, err := historical.MaskingPolicyByTableColumn(loadCtx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
			require.NoError(t, err)
			require.Zero(t, intercepted.Load(), "policy SQL must not inherit the user's RPC interceptor")
			require.True(t, ok)
			require.Equal(t, model.MaskingPolicyStatusEnable, policy.Status)
			require.Equal(t, tbl.Meta().ID, policy.TableID)
		})
	}
}

func TestMaskingPolicyReloadWarmsCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_warm_policy(c varchar(20))")
	for _, tc := range []struct {
		sql         string
		hasPolicies bool
	}{
		{"", false},
		{"create masking policy p on test.t_warm_policy(c) as mask_full(c)", true},
		{"truncate table test.t_warm_policy", true},
		{"alter table test.t_warm_policy drop masking policy p", false},
	} {
		if tc.sql != "" {
			tk.MustExec(tc.sql)
		}
		require.NoError(t, dom.Reload())
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
		hasPolicies, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
		require.NoError(t, err, "Reload must warm the cache before the first user query")
		require.Equal(t, tc.hasPolicies, hasPolicies)
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError")
	}
}

func TestMaskingPolicyPreloadFailureRetry(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_retry_policy(c varchar(20))")
	tk.MustExec("create masking policy p on test.t_retry_policy(c) as mask_full(c)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies")

	// Publishing the schema must still renew validation and synchronize versions.
	require.NoError(t, dom.Reload())
	_, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
	require.ErrorContains(t, err, "mock masking policy load error")
	require.ErrorContains(t, tk.ExecToErr("select c from test.t_retry_policy"), "mock masking policy load error")

	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError")
	require.NoError(t, dom.Reload())
	hasPolicies, err := dom.InfoSchema().HasMaskingPolicies(context.Background())
	require.NoError(t, err)
	require.True(t, hasPolicies)
}

func TestMaskingPolicyLatestSnapshot(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t_latest_policy(c varchar(20))")
	tk.MustExec("create masking policy p on test.t_latest_policy(c) as mask_full(c)")
	ctx := context.Background()
	is, err := dom.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	tbl, err := is.TableByName(ctx, pmodel.NewCIStr("test"), pmodel.NewCIStr("t_latest_policy"))
	require.NoError(t, err)
	policy, ok, err := is.MaskingPolicyByTableColumn(ctx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.MaskingPolicyStatusEnable, policy.Status)
}
