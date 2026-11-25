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
		state:         runtimeReady,
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
				*createSessionManager = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
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
				*createSessionManager = func(_ context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
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
		state:         runtimeReady,
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

// observedRuntimeContext makes entering a cancelable close wait observable.
type observedRuntimeContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *observedRuntimeContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
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

func runRuntimeTestTask(t *testing.T, waitingFor string, fn func()) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { awaitRuntimeTestResult(t, done, waitingFor) })
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

func TestEagerRuntimeRelease(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	type lookupResult struct {
		runtime *SessionManager
		err     error
	}

	t.Run("canceled store open skips runtime initialization", func(t *testing.T) {
		for _, ks := range []string{"ks-cancel-open", keyspace.System} {
			t.Run(ks, func(t *testing.T) {
				mgr := NewManager(&runtimeHandleTestStore{ks: "current"})
				t.Cleanup(mgr.Close)
				store := &runtimeHandleTestStore{ks: ks}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
					func(open *func(string) (kv.Storage, error)) {
						*open = func(string) (kv.Storage, error) {
							cancel()
							return store, nil
						}
					})
				handle, err := mgr.AcquireWithContext(ctx, ks, "holder", func(string, validatorapi.Validator) pools.Factory {
					t.Fatal("canceled store open continued into runtime initialization")
					return nil
				})
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, handle)
				require.Empty(t, mgr.GetAllKeyspace())
				require.Empty(t, mgr.runtimes)
				if ks == keyspace.System {
					require.Zero(t, store.closeCount)
				} else {
					require.Equal(t, 1, store.closeCount)
				}
			})
		}
	})

	t.Run("cancellation during startup leaves an evictable runtime", func(t *testing.T) {
		const ks = "ks-cancel-startup"
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		t.Cleanup(mgr.Close)
		store := &runtimeHandleTestStore{ks: ks}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *sessionManagerFactory) {
				*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
					return newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}), cancel, nil
				}
			})
		handle, err := mgr.AcquireWithContext(ctx, ks, "holder", unusedRuntimeHandleFactoryGetter(t))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, handle)
		entry := mgr.runtimes[ks]
		require.Equal(t, runtimeReady, entry.state)
		require.Empty(t, entry.activeHolders)
		require.False(t, entry.lastReleaseAt.IsZero())
		require.Nil(t, entry.done)
		mgr.sweepIdleRuntimes(0)
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, store.closeCount)
	})

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

	t.Run("worker can begin lookup after eager teardown cancellation", func(t *testing.T) {
		const ks = "ks-release-draining-worker"
		mgr, entry, store, pool := newRuntimeHandleTestManager(t, ks)
		handle, err := mgr.Acquire(ks, "holder", nil)
		require.NoError(t, err)
		runtime := entry.sessMgr
		defer runtime.cancel()
		lookup := make(chan *SessionManager, 1)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/afterGetOrCreateLookup",
			func(_ string, found **SessionManager) {
				lookup <- *found
				if *found == nil {
					// Preserve the observed miss, but let a regressed worker exit
					// instead of waiting on the teardown that is draining it.
					*found = runtime
				}
			})
		workerResult := make(chan lookupResult, 1)
		workerDone := make(chan struct{})
		runtime.wg.RunWithLog(func() {
			defer close(workerDone)
			<-runtime.ctx.Done()
			found, err := mgr.GetOrCreate(ks, nil)
			workerResult <- lookupResult{found, err}
		})
		t.Cleanup(func() { awaitRuntimeTestResult(t, workerDone, "the draining worker") })
		released := runRuntimeTestTask(t, "eager release after worker cancellation", func() { handle.Release(true) })
		observed := awaitRuntimeTestResult(t, lookup, "a worker lookup after cancellation")
		result := awaitRuntimeTestResult(t, workerResult, "the draining worker lookup")
		awaitRuntimeTestResult(t, released, "eager release to drain the worker")
		require.Same(t, runtime, observed, "workers must still find their runtime after cancellation")
		require.NoError(t, result.err)
		require.Same(t, runtime, result.runtime)
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
	})

	t.Run("canceled acquisition leaves existing and missing runtimes unchanged", func(t *testing.T) {
		const readyKS = "ks-cancel-ready"
		mgr, entry, _, _ := newRuntimeHandleTestManager(t, readyKS)
		lastReleaseAt := time.Now().Add(-time.Minute)
		entry.lastReleaseAt = lastReleaseAt
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(*sessionManagerFactory) {
				t.Fatal("canceled acquisition must not initialize runtime resources")
			})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		for _, ks := range []string{readyKS, "ks-cancel-missing"} {
			handle, err := mgr.AcquireWithContext(ctx, ks, "canceled", unusedRuntimeHandleFactoryGetter(t))
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, handle)
		}
		require.Equal(t, []string{readyKS}, mgr.GetAllKeyspace())
		require.Empty(t, entry.activeHolders)
		require.Equal(t, lastReleaseAt, entry.lastReleaseAt)
	})

	t.Run("final release closes immediately and once", func(t *testing.T) {
		ks := "ks-release-release"
		mgr, _, store, pool := newRuntimeHandleTestManager(t, ks)
		handle, err := mgr.Acquire(ks, "job-check", unusedRuntimeHandleFactoryGetter(t))
		require.NoError(t, err)
		handle.Release(true)
		require.Empty(t, mgr.GetAllKeyspace())
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
		handle.Release(false)
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
				*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
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

	t.Run("eager release and repeated shutdown close once without holding mutex", func(t *testing.T) {
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
		closeWaits := make(chan struct{}, 2)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeCloseWait",
			func(string) { closeWaits <- struct{}{} })
		done := make(chan struct{}, 3)
		run := func(fn func()) {
			go func() {
				defer func() { done <- struct{}{} }()
				fn()
			}()
		}
		run(func() { handle.Release(true) })
		awaitRuntimeTestResult(t, closing, "session pool teardown")
		run(mgr.Close)
		run(mgr.Close)
		for range 2 {
			awaitRuntimeTestResult(t, closeWaits, "shutdown to wait for eager teardown")
		}
		unblock.Do(func() { close(finishClose) })
		for range 3 {
			awaitRuntimeTestResult(t, done, "release or shutdown")
		}
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
		require.Empty(t, mgr.GetAllKeyspace())
		ordinary, err := mgr.Acquire(ks, "after-close", unusedRuntimeHandleFactoryGetter(t))
		require.Nil(t, ordinary)
		require.ErrorContains(t, err, "closed")
	})
}

func TestRuntimeInitializationIsolation(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	for _, tc := range []struct {
		name       string
		creatorErr error
	}{
		{"creator cancellation", context.Canceled},
		{"ordinary creation failure", kv.ErrNotExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, _, _, _ := newRuntimeHandleTestManager(t, "ready")
			factory := unusedRuntimeHandleFactoryGetter(t)
			creatorCtx, cancelCreator := context.WithCancel(context.Background())
			defer cancelCreator()
			started := make(chan struct{})
			unblock := make(chan struct{})
			finishCreate := sync.OnceFunc(func() { close(unblock) })
			createCount := 0
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
				func(create *sessionManagerFactory) {
					*create = func(ctx context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
						createCount++
						if ctx == creatorCtx {
							close(started)
							<-unblock
							if err := ctx.Err(); err != nil {
								return nil, nil, err
							}
							return nil, nil, kv.ErrNotExist
						}
						return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}), nil, nil
					}
				})
			var workers sync.WaitGroup
			defer func() {
				finishCreate()
				workers.Wait()
			}()
			acquire := func(ctx context.Context, ks, holder string) <-chan error {
				done := make(chan error, 1)
				workers.Go(func() {
					handle, err := mgr.AcquireWithContext(ctx, ks, holder, factory)
					if err == nil {
						handle.Release(false)
					}
					done <- err
				})
				return done
			}
			creator := acquire(creatorCtx, "cold", "creator")
			<-started
			select {
			case err := <-acquire(context.Background(), "ready", "other"):
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("unrelated keyspace acquire and release waited for cold initialization")
			}

			waitForCreator := func(ctx context.Context, holder string) <-chan error {
				observed := &observedRuntimeContext{Context: ctx, waiting: make(chan struct{})}
				done := acquire(observed, "cold", holder)
				select {
				case <-observed.waiting:
				case <-time.After(5 * time.Second):
					t.Fatal("same-keyspace waiter did not enter a cancelable wait")
				}
				return done
			}
			ctx, cancelWaiter := context.WithCancel(context.Background())
			defer cancelWaiter()
			canceledWaiter := waitForCreator(ctx, "canceled")
			cancelWaiter()
			require.ErrorIs(t, awaitRuntimeTestResult(t, canceledWaiter, "the canceled acquire"), context.Canceled)

			waiter := waitForCreator(context.Background(), "waiting")
			if tc.creatorErr == context.Canceled {
				cancelCreator()
			}
			finishCreate()
			require.ErrorIs(t, <-creator, tc.creatorErr)
			// A failed initialization is not shared, the next waiter retries it.
			require.NoError(t, <-waiter)
			require.Equal(t, 2, createCount)
		})
	}
}

func TestRuntimeShutdownDuringInitialization(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	for _, operation := range []string{"Acquire", "GetOrCreate"} {
		t.Run(operation, func(t *testing.T) {
			const ks = "ks-shutdown-initializing"
			mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
			t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
			store, pool := &runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}
			runtime := newRuntimeHandleTestSessionManager(store, pool)
			initializing, finishInit := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(finishInit) })
			defer unblock()
			workersStarted, publishedDuringCleanup := false, false
			pool.onClose = func() {
				_, publishedDuringCleanup = mgr.get(ks)
			}
			waitingForInit := make(chan struct{})
			signalCloseWait := sync.OnceFunc(func() { close(waitingForInit) })
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeCloseWait",
				func(string) { signalCloseWait() })
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
				func(create *sessionManagerFactory) {
					*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
						close(initializing)
						<-finishInit
						return runtime, func() { workersStarted = true }, nil
					}
				})
			var handle sqlsvrapi.KSRuntimeHandle
			var found *SessionManager
			var operationErr error
			initialized := runRuntimeTestTask(t, "initialization during shutdown", func() {
				if operation == "Acquire" {
					handle, operationErr = mgr.Acquire(ks, "holder", nil)
				} else {
					found, operationErr = mgr.GetOrCreate(ks, nil)
				}
			})
			awaitRuntimeTestResult(t, initializing, "the blocked initializer")
			closed := runRuntimeTestTask(t, "shutdown during initialization", mgr.Close)
			awaitRuntimeTestResult(t, waitingForInit, "shutdown to wait for the initializer")
			select {
			case <-closed:
				t.Fatal("shutdown returned while a runtime was initializing")
			default:
			}
			unblock()
			awaitRuntimeTestResult(t, initialized, "the initializer to reject shutdown")
			awaitRuntimeTestResult(t, closed, "manager shutdown")
			require.ErrorIs(t, operationErr, errManagerClosed)
			require.Nil(t, handle)
			require.Nil(t, found)
			require.False(t, workersStarted, "shutdown won before the runtime could start workers")
			require.False(t, publishedDuringCleanup, "an abandoned initializer must never publish its runtime")
			require.Equal(t, 1, pool.closeCount)
			require.Equal(t, 1, store.closeCount)
			require.Empty(t, mgr.GetAllKeyspace())
		})
	}
}

func TestRuntimeRetiresBeforeStoreClose(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	for _, operation := range []string{"eager release", "idle sweep", "keyspace close"} {
		t.Run(operation, func(t *testing.T) {
			const ks = "ks-retiring"
			mgr, entry, store, pool := newRuntimeHandleTestManager(t, ks)
			handle, err := mgr.Acquire(ks, "holder", nil)
			require.NoError(t, err)
			closing, finishClose := make(chan struct{}), make(chan struct{})
			store.closed = make(chan struct{})
			store.onClose = func() {
				close(closing)
				<-finishClose
			}
			unblock := sync.OnceFunc(func() { close(finishClose) })
			defer unblock()
			var newRuntime *SessionManager
			createdBeforeClose := false
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
				func(create *sessionManagerFactory) {
					*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
						select {
						case <-store.closed:
						default:
							createdBeforeClose = true
						}
						newRuntime = newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{})
						return newRuntime, nil, nil
					}
				})
			lookup := make(chan struct{})
			lookupStarted := sync.OnceFunc(func() { close(lookup) })
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeEntryWait",
				func(string) { lookupStarted() })
			closeRuntime := func() { handle.Release(true) }
			switch operation {
			case "idle sweep":
				handle.Release(false)
				closeRuntime = func() { mgr.sweepIdleRuntimes(0) }
			case "keyspace close":
				closeRuntime = func() { mgr.CloseKS(ks) }
			}
			closed := runRuntimeTestTask(t, "runtime teardown", closeRuntime)
			awaitRuntimeTestResult(t, closing, "store close after workers have drained")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observed := &observedRuntimeContext{Context: ctx, waiting: make(chan struct{})}
			canceledResult := make(chan error, 1)
			runRuntimeTestTask(t, "canceled acquire during teardown", func() {
				handle, err := mgr.AcquireWithContext(observed, ks, "canceled", nil)
				if handle != nil {
					t.Error("canceled acquire returned a handle")
					handle.Release(false)
				}
				canceledResult <- err
			})
			awaitRuntimeTestResult(t, observed.waiting, "the cancelable acquire to wait for cleanup")
			cancel()
			require.ErrorIs(t, awaitRuntimeTestResult(t, canceledResult, "the canceled acquire"), context.Canceled)
			var visible *SessionManager
			var ok bool
			checkedVisibility := runRuntimeTestTask(t, "lookup during store cleanup", func() { visible, ok = mgr.get(ks) })
			awaitRuntimeTestResult(t, checkedVisibility, "runtime visibility during store cleanup")
			var found *SessionManager
			var lookupErr error
			returnedBeforeClose := false
			lookedUp := runRuntimeTestTask(t, "replacement lookup", func() {
				found, lookupErr = mgr.GetOrCreate(ks, nil)
				select {
				case <-store.closed:
				default:
					returnedBeforeClose = true
				}
			})
			awaitRuntimeTestResult(t, lookup, "lookup to wait for blocked store cleanup")
			unblock()
			awaitRuntimeTestResult(t, closed, "the old runtime to close")
			awaitRuntimeTestResult(t, lookedUp, "the replacement runtime")
			require.False(t, ok, "a drained runtime must be hidden while its store closes")
			require.Nil(t, visible)
			require.NoError(t, lookupErr)
			require.Same(t, newRuntime, found)
			require.NotSame(t, entry.sessMgr, found)
			require.False(t, createdBeforeClose, "replacement initialization must wait for store cleanup")
			require.False(t, returnedBeforeClose, "GetOrCreate must wait for store cleanup")
			require.Equal(t, 1, pool.closeCount)
			require.Equal(t, 1, store.closeCount)
		})
	}
}

func TestRuntimeKeyspacesProgressIndependently(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	for _, blockedOperation := range []string{"initialization", "teardown"} {
		for _, otherRuntime := range []string{"warm", "cold"} {
			t.Run(blockedOperation+"/"+otherRuntime, func(t *testing.T) {
				const blockedKS, otherKS = "ks-blocked", "ks-other"
				mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
				t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
				blocked, resume := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(resume) })
				defer unblock()
				blockedStore := &runtimeHandleTestStore{ks: blockedKS}
				otherStore := &runtimeHandleTestStore{ks: otherKS}
				if blockedOperation == "teardown" {
					blockedStore.onClose = func() {
						close(blocked)
						<-resume
					}
				}
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
					func(create *sessionManagerFactory) {
						*create = func(_ context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
							store := otherStore
							if ks == blockedKS {
								store = blockedStore
								if blockedOperation == "initialization" {
									close(blocked)
									<-resume
								}
							}
							return newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}), nil, nil
						}
					})
				if otherRuntime == "warm" {
					handle, err := mgr.Acquire(otherKS, "warmup", nil)
					require.NoError(t, err)
					handle.Release(false)
				}
				var blockedErr error
				var blockingWork func()
				if blockedOperation == "initialization" {
					blockingWork = func() {
						var handle sqlsvrapi.KSRuntimeHandle
						handle, blockedErr = mgr.Acquire(blockedKS, "blocked", nil)
						if handle != nil {
							handle.Release(false)
						}
					}
				} else {
					handle, err := mgr.Acquire(blockedKS, "blocked", nil)
					require.NoError(t, err)
					blockingWork = func() { handle.Release(true) }
				}
				blockedDone := runRuntimeTestTask(t, "the blocked keyspace operation", blockingWork)
				awaitRuntimeTestResult(t, blocked, "the blocked keyspace")
				var otherHandle sqlsvrapi.KSRuntimeHandle
				var otherErr error
				otherDone := runRuntimeTestTask(t, "the independent keyspace operation", func() {
					otherHandle, otherErr = mgr.Acquire(otherKS, "other", nil)
					if otherHandle != nil {
						otherHandle.Release(false)
					}
				})
				awaitRuntimeTestResult(t, otherDone, "another keyspace to acquire and release")
				require.NoError(t, otherErr)
				require.NotNil(t, otherHandle)
				require.Same(t, otherStore, otherHandle.Store())
				unblock()
				awaitRuntimeTestResult(t, blockedDone, "the blocked keyspace to finish")
				require.NoError(t, blockedErr)
			})
		}
	}
}

func TestRuntimeSweepLeavesUnvisitedEntriesAvailable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
	closing, finishClose := make(chan string, 3), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finishClose) })
	defer unblock()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
		func(create *sessionManagerFactory) {
			*create = func(_ context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
				pool := &runtimeHandleTestSessPool{onClose: func() {
					closing <- ks
					<-finishClose
				}}
				return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, pool), nil, nil
			}
		})
	keyspaces := []string{"ks-sweep-a", "ks-sweep-b", "ks-sweep-c"}
	for _, ks := range keyspaces {
		handle, err := mgr.Acquire(ks, "idle", nil)
		require.NoError(t, err)
		handle.Release(false)
	}
	swept := runRuntimeTestTask(t, "idle sweep", func() { mgr.sweepIdleRuntimes(0) })
	closingKS := awaitRuntimeTestResult(t, closing, "the first idle runtime to start teardown")
	otherKS := keyspaces[0]
	if otherKS == closingKS {
		otherKS = keyspaces[1]
	}
	var handle sqlsvrapi.KSRuntimeHandle
	var acquireErr error
	acquired := runRuntimeTestTask(t, "acquire an unvisited idle runtime", func() {
		handle, acquireErr = mgr.Acquire(otherKS, "active", nil)
	})
	awaitRuntimeTestResult(t, acquired, "an unvisited runtime to remain acquirable")
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)
	unblock()
	awaitRuntimeTestResult(t, swept, "idle sweep after cleanup resumes")
	pool := handle.SysSessionPool().(*runtimeHandleTestSessPool)
	require.Zero(t, pool.closeCount, "the sweep must recheck holders before closing another entry")
	visible, ok := mgr.get(otherKS)
	require.True(t, ok)
	require.Same(t, handle.Store(), visible.store)
	handle.Release(false)
}

func TestRuntimeStartupBarrier(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	const ks = "ks-starting"
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
	starting, finishStartup, startupFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finishStartup) })
	defer unblock()
	acquireWaiting, closeWaiting := make(chan struct{}), make(chan struct{})
	signalAcquireWait := sync.OnceFunc(func() { close(acquireWaiting) })
	signalCloseWait := sync.OnceFunc(func() { close(closeWaiting) })
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeEntryWait",
		func(string) { signalAcquireWait() })
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeCloseWait",
		func(string) { signalCloseWait() })
	closedDuringStartup := false
	pool := &runtimeHandleTestSessPool{onClose: func() {
		select {
		case <-startupFinished:
		default:
			closedDuringStartup = true
		}
	}}
	store := &runtimeHandleTestStore{ks: ks}
	runtime := newRuntimeHandleTestSessionManager(store, pool)
	var reentered *SessionManager
	var reenterErr error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
		func(create *sessionManagerFactory) {
			*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
				return runtime, func() {
					reentered, reenterErr = mgr.GetOrCreate(ks, nil)
					close(starting)
					<-finishStartup
					close(startupFinished)
				}, nil
			}
		})
	var initialHandle, waitingHandle sqlsvrapi.KSRuntimeHandle
	var initialErr, waitingErr error
	initialized := runRuntimeTestTask(t, "startup initialization", func() {
		initialHandle, initialErr = mgr.Acquire(ks, "initial", nil)
	})
	awaitRuntimeTestResult(t, starting, "workers to start after runtime publication")
	require.NoError(t, reenterErr)
	require.Same(t, runtime, reentered, "startup session factories must find their published runtime")
	acquiredDuringStartup := false
	waiting := runRuntimeTestTask(t, "acquire during startup", func() {
		waitingHandle, waitingErr = mgr.Acquire(ks, "waiting", nil)
		if waitingHandle != nil {
			select {
			case <-startupFinished:
			default:
				acquiredDuringStartup = true
			}
		}
	})
	awaitRuntimeTestResult(t, acquireWaiting, "the same-keyspace acquisition to wait for startup")
	closed := runRuntimeTestTask(t, "shutdown during startup", mgr.Close)
	awaitRuntimeTestResult(t, closeWaiting, "shutdown to wait for worker registration")
	unblock()
	awaitRuntimeTestResult(t, initialized, "initial acquisition after startup")
	awaitRuntimeTestResult(t, waiting, "the same-keyspace acquisition")
	awaitRuntimeTestResult(t, closed, "shutdown after startup finishes")
	require.False(t, acquiredDuringStartup, "Acquire must wait for worker startup to finish")
	require.False(t, closedDuringStartup, "teardown must wait for worker startup to finish")
	require.Nil(t, initialHandle)
	require.ErrorIs(t, initialErr, errManagerClosed)
	require.Nil(t, waitingHandle)
	require.ErrorIs(t, waitingErr, errManagerClosed)
	require.Equal(t, 1, pool.closeCount)
	require.Equal(t, 1, store.closeCount)
}

func TestRuntimeInitializationFailureWakesWaiters(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	const ks, waiterCount = "ks-retry-initialization", 8
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(func() { closeRuntimeTestManager(t, mgr) })
	initializing, finishInit := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finishInit) })
	defer unblock()
	var createMu sync.Mutex
	createCount := 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
		func(create *sessionManagerFactory) {
			*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error) {
				createMu.Lock()
				createCount++
				attempt := createCount
				createMu.Unlock()
				if attempt == 1 {
					close(initializing)
					<-finishInit
					return nil, nil, kv.ErrNotExist
				}
				return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}), nil, nil
			}
		})
	var initialErr error
	initial := runRuntimeTestTask(t, "the failing initializer", func() {
		handle, err := mgr.Acquire(ks, "initial", nil)
		initialErr = err
		if handle != nil {
			handle.Release(false)
		}
	})
	awaitRuntimeTestResult(t, initializing, "the first initializer")
	type acquireResult struct {
		handle sqlsvrapi.KSRuntimeHandle
		err    error
	}
	started := make(chan struct{}, waiterCount)
	results := make(chan acquireResult, waiterCount)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeRuntimeEntryWait",
		func(string) { started <- struct{}{} })
	for i := range waiterCount {
		runRuntimeTestTask(t, "a retrying initializer", func() {
			handle, err := mgr.Acquire(ks, fmt.Sprintf("waiter-%d", i), nil)
			if handle != nil {
				handle.Release(false)
			}
			results <- acquireResult{handle, err}
		})
	}
	for range waiterCount {
		awaitRuntimeTestResult(t, started, "same-keyspace waiters to wait for the failed initializer")
	}
	unblock()
	awaitRuntimeTestResult(t, initial, "the initializer to fail")
	require.ErrorIs(t, initialErr, kv.ErrNotExist)
	var store kv.Storage
	for range waiterCount {
		result := awaitRuntimeTestResult(t, results, "a waiter to retry initialization")
		require.NoError(t, result.err)
		require.NotNil(t, result.handle)
		if store == nil {
			store = result.handle.Store()
		}
		require.Same(t, store, result.handle.Store())
	}
	createMu.Lock()
	attempts := createCount
	createMu.Unlock()
	require.Equal(t, 2, attempts, "the failed initializer must permit exactly one replacement")
}
