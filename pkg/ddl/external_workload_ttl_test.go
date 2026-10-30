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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const ttlTableSQL = `create table t(
	id int primary key,
	created_at datetime
) TTL = created_at + interval 1 day`

type recordingExternalWorkloadManager struct {
	role config.ExternalWorkloadRole

	mu               sync.Mutex
	registeredTables []int64
	deletedTables    []int64
	activeTables     map[int64]struct{}
	registerErrFn    func(int64) error
	deleteErrFn      func(int64) error
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

func (m *recordingExternalWorkloadManager) RegisterTTLTableInfo(_ context.Context, tableID int64, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registerErrFn != nil {
		if err := m.registerErrFn(tableID); err != nil {
			return err
		}
	}
	if m.activeTables == nil {
		m.activeTables = make(map[int64]struct{})
	}
	m.activeTables[tableID] = struct{}{}
	m.registeredTables = append(m.registeredTables, tableID)
	return nil
}

func (m *recordingExternalWorkloadManager) DeleteTTLTableInfo(_ context.Context, tableID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErrFn != nil {
		if err := m.deleteErrFn(tableID); err != nil {
			return err
		}
	}
	delete(m.activeTables, tableID)
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

func (m *recordingExternalWorkloadManager) activeTTLTableIDs() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]int64, 0, len(m.activeTables))
	for tableID := range m.activeTables {
		ids = append(ids, tableID)
	}
	return ids
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
	t.Run("create table registration failure aborts ddl", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{
			role:          config.RoleMaster,
			registerErrFn: func(int64) error { return context.DeadlineExceeded },
		}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		err := tk.ExecToErr(ttlTableSQL)
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())
		tk.MustQuery("show tables like 't'").Check(testkit.Rows())
		require.Empty(t, mgr.registeredTTLTables())
	})

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

		tk.MustExec(ttlTableSQL)

		tbl := external.GetTableByName(t, tk, "test", "t")
		tk.MustExec("drop table t")

		require.Equal(t, []int64{tbl.Meta().ID}, mgr.deletedTTLTables())
	})

	t.Run("drop table delete failure aborts ddl", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(ttlTableSQL)

		tbl := external.GetTableByName(t, tk, "test", "t")
		mgr.deleteErrFn = func(tableID int64) error {
			if tableID == tbl.Meta().ID {
				return context.DeadlineExceeded
			}
			return nil
		}

		err := tk.ExecToErr("drop table t")
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())

		currentTbl := external.GetTableByName(t, tk, "test", "t")
		require.Equal(t, tbl.Meta().ID, currentTbl.Meta().ID)
		require.Empty(t, mgr.deletedTTLTables())
	})

	t.Run("drop and flashback database sync ttl metadata", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		const dbName = "test_drop_schema_ttl"
		tk.MustExec("create database " + dbName)
		tk.MustExec(`create table ` + dbName + `.t_enabled_1(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)
		tk.MustExec(`create table ` + dbName + `.t_enabled_2(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)
		tk.MustExec(`create table ` + dbName + `.t_disabled(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day TTL_ENABLE='OFF'`)

		enabledTbl1 := external.GetTableByName(t, tk, dbName, "t_enabled_1")
		enabledTbl2 := external.GetTableByName(t, tk, dbName, "t_enabled_2")
		disabledTbl := external.GetTableByName(t, tk, dbName, "t_disabled")

		require.Len(t, mgr.registeredTTLTables(), 2)
		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.registeredTTLTables())
		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.activeTTLTableIDs())

		tk.MustExec("drop database " + dbName)

		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID, disabledTbl.Meta().ID}, mgr.deletedTTLTables())
		require.Empty(t, mgr.activeTTLTableIDs())

		safePoint := time.Now().Add(-48 * time.Hour).Format("20060102-15:04:05 -0700 MST")
		tk.MustExec("delete from mysql.tidb where variable_name in ('tikv_gc_safe_point', 'tikv_gc_enable')")
		tk.MustExec("insert into mysql.tidb values ('tikv_gc_safe_point', '" + safePoint + "', '') on duplicate key update variable_value = '" + safePoint + "'")
		tk.MustExec("insert into mysql.tidb values ('tikv_gc_enable', 'true', '') on duplicate key update variable_value = 'true'")

		tk.MustExec("flashback database " + dbName)

		require.Len(t, mgr.registeredTTLTables(), 2)
		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.registeredTTLTables())
		require.Empty(t, mgr.activeTTLTableIDs())

		for _, tblName := range []string{"t_enabled_1", "t_enabled_2", "t_disabled"} {
			rows := tk.MustQuery("show create table " + dbName + "." + tblName).Rows()
			require.Len(t, rows, 1)
			require.Len(t, rows[0], 2)
			require.Contains(t, rows[0][1], "TTL_ENABLE='OFF'")
		}
	})

	t.Run("drop database delete failure restores ttl registrations", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		const dbName = "test_drop_schema_ttl_fail"
		tk.MustExec("create database " + dbName)
		tk.MustExec(`create table ` + dbName + `.t_enabled_1(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)
		tk.MustExec(`create table ` + dbName + `.t_disabled(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day TTL_ENABLE='OFF'`)
		tk.MustExec(`create table ` + dbName + `.t_enabled_2(
			id int primary key,
			created_at datetime
		) TTL = created_at + interval 1 day`)

		enabledTbl1 := external.GetTableByName(t, tk, dbName, "t_enabled_1")
		disabledTbl := external.GetTableByName(t, tk, dbName, "t_disabled")
		enabledTbl2 := external.GetTableByName(t, tk, dbName, "t_enabled_2")

		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.activeTTLTableIDs())

		mgr.deleteErrFn = func(tableID int64) error {
			if tableID == enabledTbl2.Meta().ID {
				return context.DeadlineExceeded
			}
			return nil
		}

		err := tk.ExecToErr("drop database " + dbName)
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())

		require.Len(t, mgr.registeredTTLTables(), 3)
		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.registeredTTLTables())
		require.ElementsMatch(t, []int64{enabledTbl1.Meta().ID, enabledTbl2.Meta().ID}, mgr.activeTTLTableIDs())

		currentEnabledTbl1 := external.GetTableByName(t, tk, dbName, "t_enabled_1")
		currentDisabledTbl := external.GetTableByName(t, tk, dbName, "t_disabled")
		currentEnabledTbl2 := external.GetTableByName(t, tk, dbName, "t_enabled_2")
		require.Equal(t, enabledTbl1.Meta().ID, currentEnabledTbl1.Meta().ID)
		require.Equal(t, disabledTbl.Meta().ID, currentDisabledTbl.Meta().ID)
		require.Equal(t, enabledTbl2.Meta().ID, currentEnabledTbl2.Meta().ID)
	})

	t.Run("truncate table refreshes ttl metadata", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(ttlTableSQL)

		oldTbl := external.GetTableByName(t, tk, "test", "t")
		tk.MustExec("truncate table t")
		newTbl := external.GetTableByName(t, tk, "test", "t")

		require.NotEqual(t, oldTbl.Meta().ID, newTbl.Meta().ID)
		require.Equal(t, []int64{oldTbl.Meta().ID}, mgr.deletedTTLTables())
		require.Equal(t, []int64{oldTbl.Meta().ID, newTbl.Meta().ID}, mgr.registeredTTLTables())
	})

	t.Run("truncate table register failure restores old ttl registration", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(ttlTableSQL)

		oldTbl := external.GetTableByName(t, tk, "test", "t")
		mgr.registerErrFn = func(tableID int64) error {
			if tableID != oldTbl.Meta().ID {
				return context.DeadlineExceeded
			}
			return nil
		}

		err := tk.ExecToErr("truncate table t")
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())

		currentTbl := external.GetTableByName(t, tk, "test", "t")
		require.Equal(t, oldTbl.Meta().ID, currentTbl.Meta().ID)
		require.Equal(t, []int64{oldTbl.Meta().ID}, mgr.deletedTTLTables())
		require.Equal(t, []int64{oldTbl.Meta().ID, oldTbl.Meta().ID}, mgr.registeredTTLTables())
	})

	t.Run("truncate table compensation failure logs keyword", func(t *testing.T) {
		core, recorded := observer.New(zap.WarnLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
		defer restoreLog()

		mgr := &recordingExternalWorkloadManager{role: config.RoleMaster}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(ttlTableSQL)

		oldTbl := external.GetTableByName(t, tk, "test", "t")
		mgr.registerErrFn = func(int64) error { return context.DeadlineExceeded }

		err := tk.ExecToErr("truncate table t")
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())

		found := false
		for _, entry := range recorded.All() {
			if entry.Message != "truncate TTL external workload compensation failed" {
				continue
			}
			fields := entry.ContextMap()
			require.Equal(t, "truncate_ttl_restore_old_registration_failed", fields["keyword"])
			require.EqualValues(t, oldTbl.Meta().ID, fields["oldTableID"])
			found = true
			break
		}
		require.True(t, found)
	})

	t.Run("ddl syncs ttl metadata from ttl worker role", func(t *testing.T) {
		mgr := &recordingExternalWorkloadManager{role: config.RoleTTLTaskWorker}
		tk, _ := createTTLExternalWorkloadTestKit(t, mgr)

		tk.MustExec(ttlTableSQL)

		tbl := external.GetTableByName(t, tk, "test", "t")
		require.Equal(t, []int64{tbl.Meta().ID}, mgr.registeredTTLTables())

		tk.MustExec("drop table t")
		require.Equal(t, []int64{tbl.Meta().ID}, mgr.deletedTTLTables())
	})
}
