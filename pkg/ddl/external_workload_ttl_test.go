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

package ddl_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

type recordingExternalWorkloadManager struct {
	role config.ExternalWorkloadRole

	mu               sync.Mutex
	registeredTables []int64
	deletedTables    []int64
}

func (*recordingExternalWorkloadManager) Close() error { return nil }

func (m *recordingExternalWorkloadManager) Role() config.ExternalWorkloadRole {
	return m.role
}

func (*recordingExternalWorkloadManager) Meta() *keyspacepb.KeyspaceMeta { return nil }

func (*recordingExternalWorkloadManager) InitializeGCV2(context.Context, time.Duration) error {
	return nil
}

func (*recordingExternalWorkloadManager) AbortGCV2(context.Context) error { return nil }

func (*recordingExternalWorkloadManager) RegisterGCV2(context.Context, uint64, time.Duration) error {
	return nil
}

func (*recordingExternalWorkloadManager) RecycleGCV2(context.Context, uint64) error {
	return nil
}

func (*recordingExternalWorkloadManager) UpdateGCLifeTime(context.Context, time.Duration) error {
	return nil
}

func (m *recordingExternalWorkloadManager) RegisterTTLTask(_ context.Context, tableID int64, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registeredTables = append(m.registeredTables, tableID)
	return nil
}

func (m *recordingExternalWorkloadManager) DeleteTTLTableInfo(_ context.Context, tableID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedTables = append(m.deletedTables, tableID)
	return nil
}

func (*recordingExternalWorkloadManager) RecycleTTLTask(context.Context, uint64) error {
	return nil
}

func (*recordingExternalWorkloadManager) UpdateTTLJobEnable(context.Context, bool) error {
	return nil
}

func (*recordingExternalWorkloadManager) RegisterAutoAnalyze(context.Context, uint64) error {
	return nil
}

func (*recordingExternalWorkloadManager) RecycleAutoAnalyze(context.Context, uint64) error {
	return nil
}

func (m *recordingExternalWorkloadManager) registeredTTLTables() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.registeredTables...)
}

func (m *recordingExternalWorkloadManager) deletedTTLTables() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.deletedTables...)
}

func createTTLExternalWorkloadTestKit(t *testing.T, mgr *recordingExternalWorkloadManager) (*testkit.TestKit, kv.Storage) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	vardef.SetSchemaLease(500 * time.Millisecond)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()

	dom, err := session.BootstrapSessionWithExternalWorkloadManager(store, mgr)
	require.NoError(t, err)
	dom.SetStatsUpdating(true)

	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	return tk, store
}

func TestExternalWorkloadTTLDDLIntegration(t *testing.T) {
	t.Run("create table with foreign key registers ttl", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec("create table parent(id int primary key)")
		tk.MustExec(`create table child(
			id int primary key,
			parent_id int,
			created_at datetime,
			index idx_parent(parent_id),
			foreign key (parent_id) references parent(id)
		) TTL = created_at + interval 1 day`)

		childTbl := external.GetTableByName(t, tk, "test", "child")
		require.Equal(t, []int64{childTbl.Meta().ID}, mgr.registeredTTLTables())
		require.Empty(t, mgr.deletedTTLTables())
	})

	t.Run("drop table deletes ttl metadata", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(`create table t(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)

		tbl := external.GetTableByName(t, tk, "test", "t")
		tk.MustExec("drop table t")

		require.Equal(t, []int64{tbl.Meta().ID}, mgr.deletedTTLTables())
	})

	t.Run("truncate table refreshes ttl metadata", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(`create table t(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)

		oldTbl := external.GetTableByName(t, tk, "test", "t")
		tk.MustExec("truncate table t")
		newTbl := external.GetTableByName(t, tk, "test", "t")

		require.NotEqual(t, oldTbl.Meta().ID, newTbl.Meta().ID)
		require.Equal(t, []int64{oldTbl.Meta().ID}, mgr.deletedTTLTables())
		require.Equal(t, []int64{oldTbl.Meta().ID, newTbl.Meta().ID}, mgr.registeredTTLTables())
	})

	t.Run("ddl syncs ttl metadata from ttl worker role", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleTTLTaskWorker}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(`create table t(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)

		tbl := external.GetTableByName(t, tk, "test", "t")
		require.Equal(t, []int64{tbl.Meta().ID}, mgr.registeredTTLTables())

		tk.MustExec("drop table t")
		require.Equal(t, []int64{tbl.Meta().ID}, mgr.deletedTTLTables())
	})
}
