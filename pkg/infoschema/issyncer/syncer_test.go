// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issyncer

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestSyncerSkipMDLCheck(t *testing.T) {
	syncer := New(nil, nil, 0, nil, nil, false)
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}))

	syncer = NewCrossKSSyncer(testStoreWithKS{}, nil, 0, nil, nil, "ks1")
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{}))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}))
	require.True(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}))
}

func TestSyncLoopForBR(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	// Test with loadForBR = true
	syncerForBR := New(store, infoschema.NewCache(nil, 1), 100*time.Millisecond, nil, nil, true)
	require.True(t, syncerForBR.loader.loadForBR)

	// Create a context with timeout to ensure SyncLoop exits quickly
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start SyncLoop in a goroutine
	done := make(chan struct{})
	go func() {
		syncerForBR.SyncLoop(ctx)
		close(done)
	}()

	// SyncLoop should exit immediately when loadForBR is true
	// Wait a bit to ensure it has time to check the flag
	select {
	case <-done:
		// Expected: SyncLoop should return immediately
	case <-time.After(200 * time.Millisecond):
		t.Fatal("SyncLoop should have exited immediately when loadForBR is true")
	}
}
