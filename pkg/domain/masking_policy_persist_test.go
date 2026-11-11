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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
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
			policy, ok, err := historical.MaskingPolicyByTableColumn(ctx, tbl.Meta().ID, tbl.Meta().Columns[0].ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, model.MaskingPolicyStatusEnable, policy.Status)
			require.Equal(t, tbl.Meta().ID, policy.TableID)
		})
	}
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
