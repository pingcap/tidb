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
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	t.Cleanup(mgr.Close)
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
			func(createSessionManager *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*createSessionManager = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					createCount++
					return newRuntimeHandleTestSessionManager(targetStore, sessPool), nil
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
			require.True(t, mgr.mu.TryLock())
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
			func(createSessionManager *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*createSessionManager = func(_ context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					createCount++
					newStore := &runtimeHandleTestStore{ks: ks}
					newSessPool := &runtimeHandleTestSessPool{}
					return newRuntimeHandleTestSessionManager(newStore, newSessPool), nil
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

// observedRuntimeLockContext signals the first cancellation check after it has
// read the current error, allowing cancellation while acquisition waits on mu.
type observedRuntimeLockContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *observedRuntimeLockContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}

func TestEagerRuntimeRelease(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}

	t.Run("cancellation while waiting for mutex does not create runtime", func(t *testing.T) {
		ks := "ks-release-cancel-mutex"
		mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
		t.Cleanup(mgr.Close)
		created := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					created = true
					return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}), nil
				}
			})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		observed := &observedRuntimeLockContext{Context: ctx, checked: make(chan struct{})}
		type result struct {
			handle sqlsvrapi.KSRuntimeHandle
			err    error
		}
		done := make(chan result, 1)
		mgr.mu.Lock()
		go func() {
			handle, err := mgr.AcquireWithContext(observed, ks, "canceled", unusedRuntimeHandleFactoryGetter(t))
			done <- result{handle: handle, err: err}
		}()
		<-observed.checked
		cancel()
		mgr.mu.Unlock()
		acquired := <-done
		if acquired.handle != nil {
			acquired.handle.Release(true)
		}
		require.ErrorIs(t, acquired.err, context.Canceled)
		require.Nil(t, acquired.handle)
		require.False(t, created, "canceled acquisition must not initialize runtime resources")
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
		check, err := mgr.AcquireWithContext(context.Background(), ks, "job-check", factory)
		require.NoError(t, err)
		check.Release(true)
		require.Zero(t, pool.closeCount)
		ordinary.Release(false)
		require.Contains(t, mgr.GetAllKeyspace(), ks)
		require.Zero(t, pool.closeCount)
		require.Zero(t, store.closeCount)
	})

	t.Run("reacquire waits until store closed and raw access remains available", func(t *testing.T) {
		ks := "ks-release-reacquire"
		mgr, entry, store, pool := newRuntimeHandleTestManager(t, ks)
		factory := unusedRuntimeHandleFactoryGetter(t)
		handle, err := mgr.AcquireWithContext(context.Background(), ks, "job-check", factory)
		require.NoError(t, err)
		closing := make(chan struct{})
		finishClose := make(chan struct{})
		var unblock sync.Once
		t.Cleanup(func() { unblock.Do(func() { close(finishClose) }) })
		store.closed = make(chan struct{})
		store.onClose = func() {
			close(closing)
			<-finishClose
		}
		pool.onClose = func() {
			// An in-flight session factory may re-enter Domain's raw getters
			// while SessionManager.close waits for its background goroutines.
			require.True(t, mgr.mu.TryLock())
			mgr.mu.Unlock()
			raw, getErr := mgr.GetOrCreate(ks, factory)
			require.NoError(t, getErr)
			require.Same(t, entry.sessMgr, raw)
		}
		released := make(chan struct{})
		go func() { handle.Release(true); close(released) }()
		<-closing
		raw, err := mgr.GetOrCreate(ks, factory)
		require.NoError(t, err)
		require.Same(t, entry.sessMgr, raw)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		observed := &observedRuntimeContext{Context: ctx, waiting: make(chan struct{})}
		cancelledResult := make(chan error, 1)
		go func() {
			cancelled, acquireErr := mgr.AcquireWithContext(observed, ks, "cancelled", factory)
			if cancelled != nil {
				t.Error("canceled acquire returned a handle")
			}
			cancelledResult <- acquireErr
		}()
		<-observed.waiting
		cancel()
		require.ErrorIs(t, <-cancelledResult, context.Canceled)

		created := make(chan struct{}, 1)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					select {
					case <-store.closed:
					default:
						t.Error("created a replacement before the previous store closed")
					}
					created <- struct{}{}
					return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}), nil
				}
			})
		acquired := make(chan sqlsvrapi.KSRuntimeHandle, 1)
		observedAcquire := &observedRuntimeContext{Context: context.Background(), waiting: make(chan struct{})}
		go func() {
			next, acquireErr := mgr.AcquireWithContext(observedAcquire, ks, "next", factory)
			if acquireErr != nil {
				t.Error(acquireErr)
			}
			acquired <- next
		}()
		<-observedAcquire.waiting
		select {
		case <-created:
			t.Fatal("reacquire created a runtime during store teardown")
		default:
		}
		unblock.Do(func() { close(finishClose) })
		<-released
		next := <-acquired
		require.NotNil(t, next)
		next.Release(false)
		require.Equal(t, 1, store.closeCount)
	})

	t.Run("stale release cannot remove a replacement holder", func(t *testing.T) {
		ks := "ks-release-stale-release"
		mgr, _, _, _ := newRuntimeHandleTestManager(t, ks)
		factory := unusedRuntimeHandleFactoryGetter(t)
		oldHandle, err := mgr.AcquireWithContext(context.Background(), ks, "same-holder", factory)
		require.NoError(t, err)
		mgr.CloseKS(ks)
		newPool := &runtimeHandleTestSessPool{}
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
			func(create *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
				*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
					return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, newPool), nil
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
			require.True(t, mgr.mu.TryLock())
			mgr.mu.Unlock()
			close(closing)
			<-finishClose
		}
		done := make(chan struct{}, 4)
		go func() { handle.Release(true); done <- struct{}{} }()
		<-closing
		go func() { mgr.sweepIdleRuntimes(0); done <- struct{}{} }()
		go func() { mgr.Close(); done <- struct{}{} }()
		go func() { mgr.Close(); done <- struct{}{} }()
		unblock.Do(func() { close(finishClose) })
		for range 4 {
			<-done
		}
		require.Equal(t, 1, pool.closeCount)
		require.Equal(t, 1, store.closeCount)
		require.Empty(t, mgr.GetAllKeyspace())
		handle, err = mgr.AcquireWithContext(context.Background(), ks, "after-close", unusedRuntimeHandleFactoryGetter(t))
		require.Nil(t, handle)
		require.ErrorContains(t, err, "closed")
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
				func(create *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
					*create = func(ctx context.Context, ks string, _ func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
						createCount++
						if ctx == creatorCtx {
							close(started)
							<-unblock
							if err := ctx.Err(); err != nil {
								return nil, err
							}
							return nil, kv.ErrNotExist
						}
						return newRuntimeHandleTestSessionManager(&runtimeHandleTestStore{ks: ks}, &runtimeHandleTestSessPool{}), nil
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
			require.ErrorIs(t, <-canceledWaiter, context.Canceled)

			waiter := waitForCreator(context.Background(), "waiting")
			if tc.creatorErr == context.Canceled {
				cancelCreator()
			}
			finishCreate()
			require.ErrorIs(t, <-creator, tc.creatorErr)
			if tc.creatorErr == context.Canceled {
				require.NoError(t, <-waiter)
				require.Equal(t, 2, createCount)
			} else {
				require.ErrorIs(t, <-waiter, tc.creatorErr)
				require.Equal(t, 1, createCount)
			}
		})
	}
}

func TestGCSweepKeepsRemainingRuntimesAvailable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(mgr.Close)
	closing := make(chan string, 1)
	unblock := make(chan struct{})
	var firstClose sync.Once
	for _, ks := range []string{"a", "b"} {
		store := &runtimeHandleTestStore{ks: ks, onClose: func() {
			firstClose.Do(func() {
				closing <- ks
				<-unblock
			})
		}}
		mgr.runtimes[ks] = &runtimeEntry{
			sessMgr:       newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}),
			activeHolders: make(map[string]struct{}),
			lastReleaseAt: time.Now().Add(-time.Hour),
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.sweepIdleRuntimes(time.Minute)
	}()
	var finish sync.Once
	finishSweep := func() {
		finish.Do(func() { close(unblock) })
		<-done
	}
	defer finishSweep()
	ks := "a"
	if <-closing == ks {
		ks = "b"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handle, err := mgr.AcquireWithContext(ctx, ks, "holder", unusedRuntimeHandleFactoryGetter(t))
	require.NoError(t, err, "a slow close must leave unrelated idle candidates available")
	handle.Release(false)
	finishSweep()
	require.Contains(t, mgr.GetAllKeyspace(), ks, "the newly released runtime is no longer idle long enough")
}

func TestRuntimeShutdownDuringInitialization(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtime acquire is supported only in nextgen kernel")
	}
	mgr := NewManager(&runtimeHandleTestStore{ks: keyspace.System})
	t.Cleanup(mgr.Close)
	store := &runtimeHandleTestStore{ks: "cold"}
	started := make(chan struct{})
	unblock := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockCreateSessionManager",
		func(create *func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error)) {
			*create = func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, error) {
				close(started)
				<-unblock
				return newRuntimeHandleTestSessionManager(store, &runtimeHandleTestSessPool{}), nil
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
