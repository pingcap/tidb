// Copyright 2026 PingCAP, Inc.
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

package crossks

import (
	"fmt"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

type runtimeHandleTestStore struct {
	kv.Storage
	ks string
}

func (s *runtimeHandleTestStore) GetKeyspace() string {
	return s.ks
}

type runtimeHandleTestSessPool struct {
	util.DestroyableSessionPool
	closeCount int
}

func (p *runtimeHandleTestSessPool) Close() {
	p.closeCount++
}

func newRuntimeHandleTestManager(targetKS string) (*Manager, *runtimeEntry, *runtimeHandleTestStore, *runtimeHandleTestSessPool) {
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	targetStore := &runtimeHandleTestStore{ks: targetKS}
	sessPool := &runtimeHandleTestSessPool{}
	entry := &runtimeEntry{
		sessMgr: &SessionManager{
			store:    targetStore,
			sessPool: sessPool,
		},
		activeHolders: make(map[string]struct{}),
	}
	mgr.runtimes[targetKS] = entry
	return mgr, entry, targetStore, sessPool
}

func unusedRuntimeHandleFactoryGetter(t *testing.T) func(string, validatorapi.Validator) pools.Factory {
	return func(string, validatorapi.Validator) pools.Factory {
		t.Fatal("test should use the pre-seeded runtime entry")
		return nil
	}
}

func TestAcquireRuntimeHandle(t *testing.T) {
	t.Run("rejects empty holderID", func(t *testing.T) {
		mgr, _, _, _ := newRuntimeHandleTestManager("ks-runtime-empty-holderID")

		handle, err := mgr.Acquire("ks-runtime-empty-holderID", "", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "holderID")
	})

	t.Run("rejects classic kernel", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("classic-kernel rejection is covered only in classic kernel")
		}
		mgr, _, _, _ := newRuntimeHandleTestManager("ks-runtime-classic")

		handle, err := mgr.Acquire("ks-runtime-classic", "test/holderID", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "cross keyspace is not available in classic kernel or current keyspace")
	})

	t.Run("tracks holder IDs", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
		}
		targetKS := "ks-runtime-holderID"
		mgr, entry, targetStore, sessPool := newRuntimeHandleTestManager(targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)

		first, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Same(t, targetStore, first.Store())
		require.Same(t, sessPool, first.SessPool())
		require.Contains(t, entry.activeHolders, "holder-1")

		duplicate, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.Nil(t, duplicate)
		require.ErrorContains(t, err, "already acquired")

		second, err := mgr.Acquire(targetKS, "holder-2", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")

		first.Release()
		first.Release()
		require.NotContains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")
		require.True(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		second.Release()
		second.Release()
		require.Empty(t, entry.activeHolders)
		require.False(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		reacquired, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Zero(t, sessPool.closeCount)
		reacquired.Release()
	})

	t.Run("concurrently tracks holder IDs for a new runtime", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
		}
		targetKS := "ks-runtime-concurrent-holderID"
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		targetStore := &runtimeHandleTestStore{ks: targetKS}
		sessPool := &runtimeHandleTestSessPool{}
		createCount := 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(createSessionManager *func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*createSessionManager = func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					createCount++
					return &SessionManager{
						store:    targetStore,
						sessPool: sessPool,
					}, nil
				}
			},
		)
		factoryGetter := func(string, validatorapi.Validator) pools.Factory {
			return nil
		}

		uniqueHolderIDs := make([]string, 0, 15)
		for i := range 15 {
			uniqueHolderIDs = append(uniqueHolderIDs, fmt.Sprintf("holderID-%d", i))
		}
		const duplicateAttempts = 8
		const duplicateHolder = "holder-duplicate"
		type acquireResult struct {
			holderID string
			handle   interface{ Release() }
			err      error
		}
		startCh := make(chan struct{})
		resultCh := make(chan acquireResult, len(uniqueHolderIDs)+duplicateAttempts)
		for _, holderID := range uniqueHolderIDs {
			id := holderID
			go func() {
				<-startCh
				handle, err := mgr.Acquire(targetKS, id, factoryGetter)
				resultCh <- acquireResult{holderID: id, handle: handle, err: err}
			}()
		}
		for range duplicateAttempts {
			go func() {
				<-startCh
				handle, err := mgr.Acquire(targetKS, duplicateHolder, factoryGetter)
				resultCh <- acquireResult{holderID: duplicateHolder, handle: handle, err: err}
			}()
		}
		close(startCh)

		successByHolder := make(map[string]int, len(uniqueHolderIDs)+1)
		successfulHandles := make([]interface{ Release() }, 0, len(uniqueHolderIDs)+1)
		duplicateErrorCount := 0
		for range len(uniqueHolderIDs) + duplicateAttempts {
			result := <-resultCh
			if result.err != nil {
				require.Equal(t, duplicateHolder, result.holderID)
				require.ErrorContains(t, result.err, "already acquired")
				duplicateErrorCount++
				continue
			}
			require.NotNil(t, result.handle)
			successByHolder[result.holderID]++
			successfulHandles = append(successfulHandles, result.handle)
		}
		require.Equal(t, duplicateAttempts-1, duplicateErrorCount)
		require.Len(t, successfulHandles, len(uniqueHolderIDs)+1)
		require.Equal(t, 1, createCount)
		require.Len(t, mgr.runtimes, 1)
		entry := mgr.runtimes[targetKS]
		require.NotNil(t, entry)
		require.Same(t, targetStore, entry.sessMgr.store)
		require.Same(t, sessPool, entry.sessMgr.sessPool)
		require.Len(t, entry.activeHolders, len(uniqueHolderIDs)+1)
		require.Equal(t, 1, successByHolder[duplicateHolder])
		require.Contains(t, entry.activeHolders, duplicateHolder)
		for _, holderID := range uniqueHolderIDs {
			require.Equal(t, 1, successByHolder[holderID])
			require.Contains(t, entry.activeHolders, holderID)
		}

		releaseStartCh := make(chan struct{})
		releaseDoneCh := make(chan struct{}, len(successfulHandles))
		for _, handle := range successfulHandles {
			h := handle
			go func() {
				<-releaseStartCh
				h.Release()
				h.Release()
				releaseDoneCh <- struct{}{}
			}()
		}
		close(releaseStartCh)
		for range successfulHandles {
			<-releaseDoneCh
		}
		require.Empty(t, entry.activeHolders)
		require.False(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)
	})
}
