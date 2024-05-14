// Copyright 2023 PingCAP, Inc.
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

package internal

import (
	"context"
	"os"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// PrepareSlowLogfile prepares a slow log file for test.
func PrepareSlowLogfile(t *testing.T, slowLogFileName string) {
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteString(`# Time: 2019-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Conn_ID: 6
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 4.895492
# Parse_time: 0.4
# Compile_time: 0.2
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# LockKeys_time: 1.71 Request_count: 1 Prewrite_time: 0.19 Wait_prewrite_binlog_time: 0.21 Commit_time: 0.01 Commit_backoff_time: 0.18 Backoff_types: [txnLock] Resolve_lock_time: 0.03 Write_keys: 15 Write_size: 480 Prewrite_region: 1 Txn_retry: 8
# Cop_time: 0.3824278 Process_time: 0.161 Request_count: 1 Total_keys: 100001 Process_keys: 100000
# Rocksdb_delete_skipped_count: 100 Rocksdb_key_skipped_count: 10 Rocksdb_block_cache_hit_count: 10 Rocksdb_block_read_count: 10 Rocksdb_block_read_byte: 100
# Wait_time: 0.101
# Backoff_time: 0.092
# DB: test
# Is_internal: false
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Result_rows: 10
# Succ: true
# Plan: abcd
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 2;
# Resource_group: default
select * from t_slim;
# Time: 2021-09-08T14:39:54.506967433+08:00
# Txn_start_ts: 427578666238083075
# User@Host: root[root] @ 172.16.0.0 [172.16.0.0]
# Conn_ID: 40507
# Session_alias: alias123
# Query_time: 25.571605962
# Parse_time: 0.002923536
# Compile_time: 0.006800973
# Rewrite_time: 0.002100764
# Optimize_time: 0
# Wait_TS: 0.000015801
# Prewrite_time: 25.542014572 Commit_time: 0.002294647 Get_commit_ts_time: 0.000605473 Commit_backoff_time: 12.483 Backoff_types: [tikvRPC regionMiss tikvRPC regionMiss regionMiss] Write_keys: 624 Write_size: 172064 Prewrite_region: 60
# DB: rtdb
# Is_internal: false
# Digest: 124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc
# Num_cop_tasks: 0
# Mem_max: 856544
# Prepared: false
# Plan_from_cache: false
# Plan_from_binding: false
# Has_more_results: false
# KV_total: 86.635049185
# PD_total: 0.015486658
# Backoff_total: 100.054
# Write_sql_response_total: 0
# Succ: true
# Resource_group: rg1
# Request_unit_read: 96.66703066666668
# Request_unit_write: 3182.424414062492
INSERT INTO ...;
`)
	require.NoError(t, f.Close())
	require.NoError(t, err)
}

type mockAutoIDRequirement struct {
	store  kv.Storage
	client *autoid.ClientDiscover
}

func (mr *mockAutoIDRequirement) Store() kv.Storage {
	return mr.store
}

func (mr *mockAutoIDRequirement) AutoIDClient() *autoid.ClientDiscover {
	return mr.client
}

// CreateAutoIDRequirement create autoid requirement for testing.
func CreateAutoIDRequirement(t testing.TB, opts ...mockstore.MockTiKVStoreOption) autoid.Requirement {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	return &mockAutoIDRequirement{
		store:  store,
		client: nil,
	}
}

// CreateAutoIDRequirementWithStore create autoid requirement with storage for testing.
func CreateAutoIDRequirementWithStore(t testing.TB, store kv.Storage) autoid.Requirement {
	return &mockAutoIDRequirement{
		store:  store,
		client: nil,
	}
}

// GenGlobalID generates next id globally for testing.
func GenGlobalID(store kv.Storage) (int64, error) {
	var globalID int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		var err error
		globalID, err = meta.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})
	return globalID + 100, errors.Trace(err)
}

// MockDBInfo mock DBInfo for testing.
func MockDBInfo(t testing.TB, store kv.Storage, DBName string) *model.DBInfo {
	id, err := GenGlobalID(store)
	require.NoError(t, err)
	return &model.DBInfo{
		ID:     id,
		Name:   model.NewCIStr(DBName),
		Tables: []*model.TableInfo{},
		State:  model.StatePublic,
	}
}

// MockTableInfo mock TableInfo for testing.
func MockTableInfo(t testing.TB, store kv.Storage, tblName string) *model.TableInfo {
	colID, err := GenGlobalID(store)
	require.NoError(t, err)
	colInfo := &model.ColumnInfo{
		ID:        colID,
		Name:      model.NewCIStr("a"),
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}

	tblID, err := GenGlobalID(store)
	require.NoError(t, err)

	return &model.TableInfo{
		ID:      tblID,
		Name:    model.NewCIStr(tblName),
		Columns: []*model.ColumnInfo{colInfo},
		State:   model.StatePublic,
	}
}

// MockTable mock table for testing.
func MockTable(t *testing.T, store kv.Storage, tblInfo *model.TableInfo) table.Table {
	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	require.NoError(t, err)
	return tbl
}

// MockResourceGroupInfo mock resource group for testing.
func MockResourceGroupInfo(t *testing.T, store kv.Storage, groupName string) *model.ResourceGroupInfo {
	id, err := GenGlobalID(store)
	require.NoError(t, err)
	return &model.ResourceGroupInfo{
		ID:   id,
		Name: model.NewCIStr(groupName),
	}
}

// MockPolicyInfo mock policy for testing.
func MockPolicyInfo(t *testing.T, store kv.Storage, policyName string) *model.PolicyInfo {
	id, err := GenGlobalID(store)
	require.NoError(t, err)
	return &model.PolicyInfo{
		ID:   id,
		Name: model.NewCIStr(policyName),
	}
}

// MockPolicyRefInfo mock policy ref info for testing.
func MockPolicyRefInfo(t *testing.T, store kv.Storage, policyName string) *model.PolicyRefInfo {
	id, err := GenGlobalID(store)
	require.NoError(t, err)
	return &model.PolicyRefInfo{
		ID:   id,
		Name: model.NewCIStr(policyName),
	}
}

// AddTable add mock table for testing.
func AddTable(t testing.TB, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateTableOrView(dbInfo.ID, dbInfo.Name.O, tblInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// UpdateTable update mock table for testing.
func UpdateTable(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).UpdateTable(dbInfo.ID, tblInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// DropTable drop mock table for testing.
func DropTable(t testing.TB, store kv.Storage, dbInfo *model.DBInfo, tblID int64, tblName string) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).DropTableOrView(dbInfo.ID, dbInfo.Name.O, tblID, tblName)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// AddDB add mock db for testing.
func AddDB(t testing.TB, store kv.Storage, dbInfo *model.DBInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// DropDB drop mock db for testing.
func DropDB(t testing.TB, store kv.Storage, dbInfo *model.DBInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).DropDatabase(dbInfo.ID, dbInfo.Name.O)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// UpdateDB update mock db for testing.
func UpdateDB(t testing.TB, store kv.Storage, dbInfo *model.DBInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).UpdateDatabase(dbInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// AddResourceGroup add mock resource group for testing.
func AddResourceGroup(t *testing.T, store kv.Storage, group *model.ResourceGroupInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).AddResourceGroup(group)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// UpdateResourceGroup update mock resource group for testing.
func UpdateResourceGroup(t *testing.T, store kv.Storage, group *model.ResourceGroupInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).UpdateResourceGroup(group)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// DropResourceGroup drop mock resource group for testing.
func DropResourceGroup(t *testing.T, store kv.Storage, group *model.ResourceGroupInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).DropResourceGroup(group.ID)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// CreatePolicy create mock policy for testing.
func CreatePolicy(t *testing.T, store kv.Storage, policy *model.PolicyInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreatePolicy(policy)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// UpdatePolicy update mock policy for testing.
func UpdatePolicy(t *testing.T, store kv.Storage, policy *model.PolicyInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).UpdatePolicy(policy)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}

// DropPolicy drop mock policy for testing.
func DropPolicy(t *testing.T, store kv.Storage, policy *model.PolicyInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).DropPolicy(policy.ID)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)
}
