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

func TestSPFreshBulkCleanupCommitsWithJobAndSurvivesOwnerChange(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	record := meta.SPFreshBulkCleanup{JobID: 17, RunID: "run-j17", StorageURI: "s3://spfresh-mvp/run-j17", WorkerEndpoints: []string{"http://worker:19000"}}
	read := func() (bool, []*meta.SPFreshBulkCleanup) {
		version, err := store.CurrentVersion(kv.GlobalTxnScope)
		require.NoError(t, err)
		active, pending, err := meta.ReadSPFreshBulkCleanup(store.GetSnapshot(version), "", 1)
		require.NoError(t, err)
		return active, pending
	}
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).RegisterSPFreshBulkCleanup(record))
	active, pending := read()
	require.False(t, active)
	require.Empty(t, pending)
	require.NoError(t, txn.Commit(ctx))
	active, pending = read()
	require.True(t, active)
	require.Empty(t, pending)

	// A failed terminal transaction must leave the live-build fence intact.
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishSPFreshBulkCleanup(17, record.RunID))
	require.NoError(t, txn.Rollback())
	active, pending = read()
	require.True(t, active)
	require.Empty(t, pending)

	// A fresh mutator models the new owner, without any old ReorgContext.
	txn, err = store.Begin()
	require.NoError(t, err)
	require.Error(t, meta.NewMutator(txn).FinishSPFreshBulkCleanup(18, record.RunID))
	require.NoError(t, txn.Rollback())
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishSPFreshBulkCleanup(17, record.RunID))
	require.NoError(t, txn.Commit(ctx))
	active, pending = read()
	require.False(t, active)
	require.Len(t, pending, 1)
	require.Equal(t, record.RunID, pending[0].RunID)
	require.Positive(t, pending[0].FinishedAtUnixMilli)
	finishedAt := pending[0].FinishedAtUnixMilli
	txn, err = store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishSPFreshBulkCleanup(17, record.RunID))
	require.Error(t, meta.NewMutator(txn).FinishSPFreshBulkCleanup(18, record.RunID))
	require.NoError(t, txn.Commit(ctx))
	_, pending = read()
	require.Equal(t, finishedAt, pending[0].FinishedAtUnixMilli)
	txn, err = store.Begin()
	require.NoError(t, err)
	require.Error(t, meta.NewMutator(txn).RegisterSPFreshBulkCleanup(record))
	require.NoError(t, txn.Rollback())
	for range 2 {
		txn, err = store.Begin()
		require.NoError(t, err)
		require.NoError(t, meta.NewMutator(txn).ScheduleSPFreshBulkCleanup(record.RunID, finishedAt+3600000))
		require.NoError(t, txn.Commit(ctx))
	}
	active, pending = read()
	require.False(t, active)
	require.Len(t, pending, 1)
	require.Equal(t, finishedAt+3600000, pending[0].NextSweepAtUnixMilli)
	require.Equal(t, finishedAt, pending[0].FinishedAtUnixMilli)
}

func TestSPFreshBulkCleanupPagesPastFailedRuns(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, id := range []string{"a-j17", "b-j17", "c-j17"} {
		txn, err := store.Begin()
		require.NoError(t, err)
		m := meta.NewMutator(txn)
		require.NoError(t, m.RegisterSPFreshBulkCleanup(meta.SPFreshBulkCleanup{JobID: 17, RunID: id, StorageURI: "s3://bucket/" + id, WorkerEndpoints: []string{"http://worker:19000"}}))
		require.NoError(t, m.FinishSPFreshBulkCleanup(17, id))
		require.NoError(t, txn.Commit(context.Background()))
	}
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	var after string
	for _, expected := range []string{"a-j17", "b-j17", "c-j17"} {
		active, pending, err := meta.ReadSPFreshBulkCleanup(store.GetSnapshot(version), after, 1)
		require.NoError(t, err)
		require.False(t, active)
		require.Len(t, pending, 1)
		require.Equal(t, expected, pending[0].RunID)
		after = expected
	}
	_, pending, err := meta.ReadSPFreshBulkCleanup(store.GetSnapshot(version), after, 1)
	require.NoError(t, err)
	require.Empty(t, pending)
}
