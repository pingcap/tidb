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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type runtimeHandleTestStore struct {
	kv.Storage
	ks         string
	closeCount int
	onClose    func()
	closed     chan struct{}
}

func (s *runtimeHandleTestStore) GetKeyspace() string {
	return s.ks
}

func (s *runtimeHandleTestStore) Close() error {
	s.closeCount++
	if s.onClose != nil {
		s.onClose()
	}
	if s.closed != nil {
		close(s.closed)
	}
	return nil
}

type runtimeHandleTestSessPool struct {
	util.DestroyableSessionPool
	closeCount int
	onClose    func()
}

func (p *runtimeHandleTestSessPool) Close() {
	p.closeCount++
	if p.onClose != nil {
		p.onClose()
	}
}

func newRuntimeHandleTestManager(t *testing.T, targetKS string) (*Manager, *runtimeEntry, *runtimeHandleTestStore, *runtimeHandleTestSessPool) {
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	targetStore := &runtimeHandleTestStore{ks: targetKS}
	sessPool := &runtimeHandleTestSessPool{}
	entry := &runtimeEntry{
		sessMgr:       newRuntimeHandleTestSessionManager(targetStore, sessPool),
		activeHolders: make(map[string]struct{}),
	}
	mgr.runtimes[targetKS] = entry
	t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
	return mgr, entry, targetStore, sessPool
}

func newRuntimeHandleTestSessionManager(targetStore *runtimeHandleTestStore, sessPool *runtimeHandleTestSessPool) *SessionManager {
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli := clientv3.NewCtxClient(context.Background())
	return &SessionManager{
		ctx:             ctx,
		cancel:          cancel,
		exitCh:          make(chan struct{}),
		store:           targetStore,
		etcdCli:         etcdCli,
		autoidClient:    autoid.NewClientDiscover(etcdCli),
		schemaVerSyncer: schemaver.NewMemSyncer(),
		sessPool:        sessPool,
	}
}

func unusedRuntimeHandleFactoryGetter(t *testing.T) func(string, validatorapi.Validator) pools.Factory {
	return func(string, validatorapi.Validator) pools.Factory {
		t.Fatal("test should use the pre-seeded runtime entry")
		return nil
	}
}

func TestAcquireRuntimeHandle(t *testing.T) {
	t.Run("rejects empty holderID", func(t *testing.T) {
		mgr, _, _, _ := newRuntimeHandleTestManager(t, "ks-runtime-empty-holderID")

		handle, err := mgr.Acquire("ks-runtime-empty-holderID", "", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "holderID")
	})

	t.Run("rejects classic kernel", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("classic-kernel rejection is covered only in classic kernel")
		}
		mgr, _, _, _ := newRuntimeHandleTestManager(t, "ks-runtime-classic")

		handle, err := mgr.Acquire("ks-runtime-classic", "test/holderID", unusedRuntimeHandleFactoryGetter(t))

		require.Nil(t, handle)
		require.ErrorContains(t, err, "cross keyspace is not available in classic kernel or current keyspace")
	})

	t.Run("tracks holder IDs", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
		}
		targetKS := "ks-runtime-holderID"
		mgr, entry, targetStore, sessPool := newRuntimeHandleTestManager(t, targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)

		first, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Same(t, targetStore, first.Store())
		require.Same(t, sessPool, first.SysSessionPool())
		require.Contains(t, entry.activeHolders, "holder-1")

		duplicate, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.Nil(t, duplicate)
		require.ErrorContains(t, err, "already acquired")

		second, err := mgr.Acquire(targetKS, "holder-2", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")

		first.Release(false)
		first.Release(false)
		require.NotContains(t, entry.activeHolders, "holder-1")
		require.Contains(t, entry.activeHolders, "holder-2")
		require.True(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		second.Release(false)
		second.Release(false)
		require.Empty(t, entry.activeHolders)
		require.False(t, entry.lastReleaseAt.IsZero())
		require.Zero(t, sessPool.closeCount)

		reacquired, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		require.Contains(t, entry.activeHolders, "holder-1")
		require.Zero(t, sessPool.closeCount)
		reacquired.Release(false)
	})

	t.Run("concurrently tracks holder IDs for a new runtime", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
		}
		targetKS := "ks-runtime-concurrent-holderID"
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		t.Cleanup(mgr.Close)
		targetStore := &runtimeHandleTestStore{ks: targetKS}
		sessPool := &runtimeHandleTestSessPool{}
		createCount := 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(createSessionManager *sessionManagerFactory) {
				*createSessionManager = func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
					createCount++
					return newRuntimeHandleTestSessionManager(targetStore, sessPool), nil, nil
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
			handle   sqlsvrapi.KSRuntimeHandle
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
		successfulHandles := make([]sqlsvrapi.KSRuntimeHandle, 0, len(uniqueHolderIDs)+1)
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
				h.Release(false)
				h.Release(false)
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

func TestEvictRuntime(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}

	t.Run("skips active holders", func(t *testing.T) {
		targetKS := "ks-evict-active"
		mgr, entry, _, sessPool := newRuntimeHandleTestManager(t, targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)

		first, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		second, err := mgr.Acquire(targetKS, "holder-2", factoryGetter)
		require.NoError(t, err)

		first.Release(false)
		entry.lastReleaseAt = time.Now().Add(-crossKSRuntimeIdleTimeout - time.Second)
		mgr.sweepIdleRuntimes(crossKSRuntimeIdleTimeout)

		_, ok := mgr.Get(targetKS)
		require.True(t, ok)
		require.Contains(t, entry.activeHolders, "holder-2")
		require.Zero(t, sessPool.closeCount)
		second.Release(false)
	})

	t.Run("closes idle entry outside manager lock", func(t *testing.T) {
		targetKS := "ks-evict-idle"
		mgr, entry, targetStore, sessPool := newRuntimeHandleTestManager(t, targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)
		sessPool.onClose = func() {
			if !mgr.mu.TryLock() {
				t.Error("idle eviction must close runtimes outside the manager mutex")
				return
			}
			mgr.mu.Unlock()
		}

		handle, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		handle.Release(false)
		entry.lastReleaseAt = time.Now().Add(-crossKSRuntimeIdleTimeout - time.Second)

		mgr.sweepIdleRuntimes(crossKSRuntimeIdleTimeout)

		_, ok := mgr.Get(targetKS)
		require.False(t, ok)
		require.Equal(t, 1, sessPool.closeCount)
		require.Equal(t, 1, targetStore.closeCount)
	})

	t.Run("reacquire creates new runtime", func(t *testing.T) {
		targetKS := "ks-evict-reacquire"
		mgr, entry, oldStore, oldSessPool := newRuntimeHandleTestManager(t, targetKS)
		factoryGetter := unusedRuntimeHandleFactoryGetter(t)
		createCount := 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(createSessionManager *sessionManagerFactory) {
				*createSessionManager = func(ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
					createCount++
					newStore := &runtimeHandleTestStore{ks: ks}
					newSessPool := &runtimeHandleTestSessPool{}
					return newRuntimeHandleTestSessionManager(newStore, newSessPool), nil, nil
				}
			},
		)

		handle, err := mgr.Acquire(targetKS, "holder-1", factoryGetter)
		require.NoError(t, err)
		handle.Release(false)
		entry.lastReleaseAt = time.Now().Add(-crossKSRuntimeIdleTimeout - time.Second)

		mgr.sweepIdleRuntimes(crossKSRuntimeIdleTimeout)
		require.Equal(t, 1, oldSessPool.closeCount)
		require.Equal(t, 1, oldStore.closeCount)

		reacquired, err := mgr.Acquire(targetKS, "holder-2", factoryGetter)
		require.NoError(t, err)
		require.NotSame(t, oldStore, reacquired.Store())
		require.Equal(t, 1, createCount)
		reacquired.Release(false)
	})
}

func TestRuntimeHandleManagerCloseClosesAllEntriesRegardlessOfIdleTimeout(t *testing.T) {
	firstKS := "ks-close-runtime-1"
	secondKS := "ks-close-runtime-2"
	mgr, firstEntry, firstStore, firstSessPool := newRuntimeHandleTestManager(t, firstKS)
	secondStore := &runtimeHandleTestStore{ks: secondKS}
	secondSessPool := &runtimeHandleTestSessPool{}
	mgr.runtimes[secondKS] = &runtimeEntry{
		sessMgr:       newRuntimeHandleTestSessionManager(secondStore, secondSessPool),
		activeHolders: make(map[string]struct{}),
	}
	firstEntry.lastReleaseAt = time.Now()
	firstSessPool.onClose = func() {
		locked := mgr.mu.TryLock()
		if !locked {
			t.Error("shutdown must close runtimes outside the manager mutex")
			return
		}
		mgr.mu.Unlock()
	}

	mgr.Close()

	require.Empty(t, mgr.GetAllKeyspace())
	require.Equal(t, 1, firstSessPool.closeCount)
	require.Equal(t, 1, firstStore.closeCount)
	require.Equal(t, 1, secondSessPool.closeCount)
	require.Equal(t, 1, secondStore.closeCount)
}

func TestGCLoopExitsWhenContextCancelled(t *testing.T) {
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.RunSystemKSGCLoop(ctx)
		close(done)
	}()

	cancel()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
}

func awaitRuntimeTestResult[T any](t *testing.T, ch <-chan T, waitingFor string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", waitingFor)
		var zero T
		return zero
	}
}

func closeRuntimeTestManager(t *testing.T, mgr *Manager) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.Close()
	}()
	awaitRuntimeTestResult(t, done, "manager cleanup")
}

func TestEagerRuntimeRelease(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	type lookupResult struct {
		runtime *SessionManager
		err     error
	}

	t.Run("startup session factory can reenter during eager release", func(t *testing.T) {
		const ks = "ks-release-startup"
		integration.BeforeTestExternal(t)
		cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
		t.Cleanup(func() { cluster.Terminate(t) })
		store, err := mockstore.NewMockStore()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			_, err := meta.NewMutator(txn).CreateMySQLDatabaseIfNotExists()
			return err
		}))
		targetStore := &runtimeHandleTestStore{Storage: store, ks: ks}
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
			func(open *func(string) (kv.Storage, error)) {
				*open = func(string) (kv.Storage, error) { return targetStore, nil }
			})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
			func(cli **clientv3.Client, _ string) {
				*cli = cluster.Client(0)
				cluster.TakeClient(0)
			})

		lookup := make(chan *SessionManager, 1)
		resume := make(chan struct{})
		resumeFactory := sync.OnceFunc(func() { close(resume) })
		defer resumeFactory()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/afterGetOrCreateLookup",
			func(_ string, found **SessionManager) {
				lookup <- *found
				<-resume
				if *found == nil {
					// On regression, let the worker finish even though the initial
					// lookup missed and eager release now owns the keyspace lock.
					*found, _ = mgr.get(ks)
				}
			})
		var initial *SessionManager
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/afterStartRuntimeWorkers",
			func(string) { initial = awaitRuntimeTestResult(t, lookup, "the startup lookup") })

		factoryDone := make(chan lookupResult, 1)
		var firstFactory sync.Once
		factory := func(string, validatorapi.Validator) pools.Factory {
			return func() (pools.Resource, error) {
				firstFactory.Do(func() {
					runtime, err := mgr.GetOrCreate(ks, nil)
					factoryDone <- lookupResult{runtime, err}
				})
				return nil, kv.ErrNotExist // No SQL session is needed for this test.
			}
		}
		handle, err := mgr.Acquire(ks, "job-check", factory)
		require.NoError(t, err)
		entry := handle.(*runtimeHandle).entry
		released := make(chan struct{})
		go func() {
			defer close(released)
			handle.Release(true)
		}()
		awaitRuntimeTestResult(t, entry.sessMgr.ctx.Done(), "eager teardown to cancel the runtime")
		resumeFactory()
		result := awaitRuntimeTestResult(t, factoryDone, "the factory to return from GetOrCreate")
		awaitRuntimeTestResult(t, released, "eager release")
		require.NoError(t, result.err)
		require.Same(t, entry.sessMgr, result.runtime)
		require.Same(t, entry.sessMgr, initial, "the startup lookup must find the published runtime")
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, targetStore.closeCount)
	})

	t.Run("final release closes immediately and once", func(t *testing.T) {
		ks := "ks-release-release"
		mgr, _, store, pool := newRuntimeHandleTestManager(t, ks)
		handle, err := mgr.Acquire(ks, "job-check", unusedRuntimeHandleFactoryGetter(t))
		require.NoError(t, err)
		handle.Release(true)
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, store.closeCount)
		handle.Release(false)
		mgr.sweepIdleRuntimes(0)
		mgr.Close()
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
	})

	t.Run("eager release leaves a shared runtime available for reuse", func(t *testing.T) {
		ks := "ks-release-mixed"
		mgr, _, store, pool := newRuntimeHandleTestManager(t, ks)
		factory := unusedRuntimeHandleFactoryGetter(t)
		ordinary, err := mgr.Acquire(ks, "ordinary", factory)
		require.NoError(t, err)
		check, err := mgr.Acquire(ks, "job-check", factory)
		require.NoError(t, err)
		check.Release(true)
		require.Zero(t, pool.closeCount)
		ordinary.Release(false)
		require.Contains(t, mgr.GetAllKeyspace(), ks)
		require.Zero(t, pool.closeCount)
		require.Zero(t, store.closeCount)
	})

	t.Run("stale release cannot remove a replacement holder", func(t *testing.T) {
		ks := "ks-release-stale-release"
		mgr, _, _, _ := newRuntimeHandleTestManager(t, ks)
		factory := unusedRuntimeHandleFactoryGetter(t)
		oldHandle, err := mgr.Acquire(ks, "same-holder", factory)
		require.NoError(t, err)
		mgr.CloseKS(ks)
		newPool := &runtimeHandleTestSessPool{}
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *sessionManagerFactory) {
				*create = func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
					return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, newPool), nil, nil
				}
			})
		newHandle, err := mgr.Acquire(ks, "same-holder", factory)
		require.NoError(t, err)
		oldHandle.Release(true)
		require.Contains(t, mgr.runtimes[ks].activeHolders, "same-holder")
		require.Zero(t, newPool.closeCount)
		newHandle.Release(false)
	})

	t.Run("release GC and repeated shutdown close once without holding mutex", func(t *testing.T) {
		ks := "ks-release-shutdown"
		mgr, _, store, pool := newRuntimeHandleTestManager(t, ks)
		handle, err := mgr.Acquire(ks, "job-check", unusedRuntimeHandleFactoryGetter(t))
		require.NoError(t, err)
		closing := make(chan struct{})
		finishClose := make(chan struct{})
		var unblock sync.Once
		t.Cleanup(func() { unblock.Do(func() { close(finishClose) }) })
		pool.onClose = func() {
			if mgr.mu.TryLock() {
				mgr.mu.Unlock()
			} else {
				t.Error("eager release must close the runtime outside the manager mutex")
			}
			close(closing)
			<-finishClose
		}
		done := make(chan struct{}, 4)
		run := func(fn func()) {
			go func() {
				defer func() { done <- struct{}{} }()
				fn()
			}()
		}
		run(func() { handle.Release(true) })
		awaitRuntimeTestResult(t, closing, "session pool teardown")
		run(func() { mgr.sweepIdleRuntimes(0) })
		run(mgr.Close)
		run(mgr.Close)
		unblock.Do(func() { close(finishClose) })
		for range 4 {
			awaitRuntimeTestResult(t, done, "release, GC, or shutdown")
		}
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
		require.Empty(t, mgr.GetAllKeyspace())
		ordinary, err := mgr.Acquire(ks, "after-close", unusedRuntimeHandleFactoryGetter(t))
		require.Nil(t, ordinary)
		require.ErrorContains(t, err, "closed")
	})
}

func TestRuntimeShutdownDuringInitialization(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	t.Run("runtime published between shutdown snapshots", func(t *testing.T) {
		const ks = "cold"
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
		store := &runtimeHandleTestStore{ks: ks}
		initializing, finishInit := make(chan struct{}), make(chan struct{})
		snapshotTaken, finishClose := make(chan struct{}), make(chan struct{})
		unblockInit := sync.OnceFunc(func() { close(finishInit) })
		unblockClose := sync.OnceFunc(func() { close(finishClose) })
		defer unblockInit()
		defer unblockClose()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *sessionManagerFactory) {
				*create = func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
					close(initializing)
					<-finishInit
					return newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}), nil, nil
				}
			})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/afterCloseKeyspaceSnapshot", func() {
			close(snapshotTaken)
			<-finishClose
		})
		acquired := make(chan error, 1)
		go func() {
			_, err := mgr.Acquire(ks, "holder", unusedRuntimeHandleFactoryGetter(t))
			acquired <- err
		}()
		awaitRuntimeTestResult(t, initializing, "runtime initialization")
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			mgr.Close()
		}()
		awaitRuntimeTestResult(t, snapshotTaken, "the first shutdown snapshot")
		unblockInit()
		acquireErr := awaitRuntimeTestResult(t, acquired, "the acquire to publish and unlock")
		unblockClose()
		awaitRuntimeTestResult(t, closed, "manager shutdown")
		require.ErrorIs(t, acquireErr, errManagerClosed)
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, store.closeCount)
	})

	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(mgr.Close)
	store := &runtimeHandleTestStore{ks: "cold"}
	started := make(chan struct{})
	unblock := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
		func(create *sessionManagerFactory) {
			*create = func(string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
				close(started)
				<-unblock
				return newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}), nil, nil
			}
		})
	acquired := make(chan error, 1)
	go func() {
		handle, err := mgr.Acquire("cold", "holder", unusedRuntimeHandleFactoryGetter(t))
		if handle != nil {
			handle.Release(false)
		}
		acquired <- err
	}()
	<-started
	closed := make(chan struct{})
	go func() { mgr.Close(); close(closed) }()
	var once sync.Once
	finish := func() {
		once.Do(func() { close(unblock) })
		<-closed
	}
	defer finish()
	require.Eventually(t, func() bool {
		mgr.mu.RLock()
		defer mgr.mu.RUnlock()
		return mgr.closed
	}, 5*time.Second, time.Millisecond)
	select {
	case <-closed:
		t.Fatal("shutdown returned while a runtime was initializing")
	default:
	}
	finish()
	require.ErrorContains(t, <-acquired, "closed")
	require.Empty(t, mgr.GetAllKeyspace())
	require.Equal(t, 1, store.closeCount)
}
