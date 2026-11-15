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

package meta_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBootstrapDurableAdmissionAndTerminalFences(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	target := meta.SPFreshBootstrapTarget{TableID: 20, IndexIDs: []int64{2, 1}, ClusterID: 7, KeyspaceID: ^uint32(0), WorkerEndpoints: []string{"http://one", "http://two"}, Owner: "http://one", BaseSnapshotTS: 100}
	write := func(fn func(*meta.Mutator)) {
		txn, err := store.Begin()
		require.NoError(t, err)
		fn(meta.NewMutator(txn))
		require.NoError(t, txn.Commit(ctx))
	}
	read := func() []*meta.SPFreshBootstrapJob {
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		records, err := meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 0, 100)
		require.NoError(t, err)
		return records
	}
	write(func(m *meta.Mutator) {
		got, err := m.RegisterSPFreshBootstrap(10, target)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, got.IndexIDs)
	})
	require.False(t, read()[0].Finished)
	changed := target
	changed.Owner, changed.WorkerEndpoints, changed.BaseSnapshotTS = "http://new", []string{"http://new"}, 200
	write(func(m *meta.Mutator) {
		got, err := m.RegisterSPFreshBootstrap(10, changed)
		require.NoError(t, err)
		require.Equal(t, target.Owner, got.Owner)
		require.Equal(t, target.BaseSnapshotTS, got.BaseSnapshotTS)
	})
	// A later table in a batch can be admitted; a schema identity change cannot.
	second := target
	second.TableID++
	write(func(m *meta.Mutator) { _, err := m.RegisterSPFreshBootstrap(10, second); require.NoError(t, err) })
	require.Len(t, read()[0].Targets, 2)
	txn, err := store.Begin()
	require.NoError(t, err)
	changed = target
	changed.IndexIDs = []int64{9}
	_, err = meta.NewMutator(txn).RegisterSPFreshBootstrap(10, changed)
	require.ErrorContains(t, err, "identity changed")
	require.NoError(t, txn.Rollback())
	// Rollback of the DDL terminal transaction must not enable cleanup.
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishSPFreshBootstrap(10, false, true))
	require.NoError(t, txn.Rollback())
	require.False(t, read()[0].Finished)
	// A replacement owner commits failure and durably retries cleanup.
	write(func(m *meta.Mutator) { require.NoError(t, m.FinishSPFreshBootstrap(10, false, true)) })
	write(func(m *meta.Mutator) { require.NoError(t, m.ScheduleSPFreshBootstrapCleanup(10, 999)) })
	require.True(t, read()[0].Finished)
	require.False(t, read()[0].Succeeded)
	require.Equal(t, int64(999), read()[0].NextSweepAtUnixMilli)
	write(func(m *meta.Mutator) {
		require.NoError(t, m.FinishSPFreshBootstrap(10, false, true))
		_, err := m.RegisterSPFreshBootstrap(10, target)
		require.ErrorContains(t, err, "already finished")
	})
	// Cancellation before admission also closes the late-owner race.
	write(func(m *meta.Mutator) { require.NoError(t, m.FinishSPFreshBootstrap(11, false, true)) })
	write(func(m *meta.Mutator) {
		_, err := m.RegisterSPFreshBootstrap(11, target)
		require.ErrorContains(t, err, "already finished")
	})
	// Success keeps the fence, but must never become eligible for cleanup.
	write(func(m *meta.Mutator) {
		_, err := m.RegisterSPFreshBootstrap(12, target)
		require.NoError(t, err)
		require.NoError(t, m.FinishSPFreshBootstrap(12, true, true))
	})
	write(func(m *meta.Mutator) {
		require.Error(t, m.ScheduleSPFreshBootstrapCleanup(12, 999))
		require.Error(t, m.FinishSPFreshBootstrap(12, false, true))
	})
	require.True(t, read()[2].Succeeded)
	// Ordinary creates without prior admission add no metadata.
	write(func(m *meta.Mutator) { require.NoError(t, m.FinishSPFreshBootstrap(13, true, false)) })
	require.Len(t, read(), 3)
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	records, err := meta.ReadSPFreshBootstrapJobs(store.GetSnapshot(version), 10, 1)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(11), records[0].JobID)
}
