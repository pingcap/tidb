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

package crossks

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/jobsubmit"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	crossKSSessPoolSize         = 5
	crossKSRuntimeIdleTimeout   = 30 * time.Minute
	crossKSRuntimeSweepInterval = time.Minute
)

var errManagerClosed = errors.New("cross keyspace runtime manager is closed")

type runtimeState uint8

const (
	runtimeInitializing runtimeState = iota
	runtimeReady
	// Workers may still reenter GetOrCreate while they drain.
	runtimeDraining
	// Workers have stopped; the runtime is hidden until resource cleanup ends.
	runtimeClosing
)

// runtimeEntry is the runtime of one keyspace. Its fields are guarded by
// Manager.mu. sessMgr is immutable after publication because handles retain it.
type runtimeEntry struct {
	sessMgr *SessionManager
	state   runtimeState
	// done is the completion signal for the phase tracked by state. It is
	// closed when initialization or teardown finishes. It is nil while ready
	// and reassigned when teardown starts, so waiters must recheck state.
	done          chan struct{}
	activeHolders map[string]struct{}
	lastReleaseAt time.Time
}

// visible must be called with Manager.mu held. Session factories can use a
// published runtime until its workers have stopped, including during teardown.
func (e *runtimeEntry) visible() bool {
	return e.sessMgr != nil && e.state != runtimeClosing
}

// startClosing must be called with Manager.mu held and the entry ready.
func (e *runtimeEntry) startClosing() {
	e.state = runtimeDraining
	// Draining workers and closing resources share one completion signal:
	// a replacement runtime must wait for both phases to finish.
	e.done = make(chan struct{})
}

// Manager manages all cross keyspace sessions.
//
// Runtime state transitions are serialized by mu. Initialization, worker
// startup, teardown and waits happen outside mu. TiKV store opens and closes
// share a driver mutex, so cold acquisitions can still wait on work for another
// keyspace.
type Manager struct {
	// the store of current instance
	store kv.Storage

	// mu guards runtimes, their entries and closed.
	mu sync.RWMutex
	// keyspace name -> runtime entry. Placeholders serialize initialization and
	// teardown for each keyspace and remain until all resource cleanup finishes.
	runtimes map[string]*runtimeEntry
	closed   bool
}

// NewManager creates a new cross keyspace session manager.
func NewManager(store kv.Storage) *Manager {
	return &Manager{
		store:    store,
		runtimes: make(map[string]*runtimeEntry),
	}
}

// GetAllKeyspace returns all keyspace names that have session managers.
func (m *Manager) GetAllKeyspace() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keyspaces := make([]string, 0, len(m.runtimes))
	for ks, entry := range m.runtimes {
		if entry.visible() {
			keyspaces = append(keyspaces, ks)
		}
	}
	return keyspaces
}

func (m *Manager) get(ks string) (*SessionManager, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if entry := m.runtimes[ks]; entry != nil && entry.visible() {
		return entry.sessMgr, true
	}
	return nil, false
}

// GetOrCreate gets or creates a session manager for the specified keyspace.
func (m *Manager) GetOrCreate(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (*SessionManager, error) {
	if err := m.validateTargetKS(ks); err != nil {
		return nil, err
	}
	// Session factories must find their published runtime during startup and
	// while teardown is waiting for the workers that use those factories.
	mgr, _ := m.get(ks)
	failpoint.InjectCall("afterGetOrCreateLookup", ks, &mgr)
	if mgr != nil {
		return mgr, nil
	}
	entry, err := m.getOrCreateRawEntry(ks, ksSessFactoryGetter)
	if err != nil {
		return nil, err
	}
	return entry.sessMgr, nil
}

// Acquire acquires a runtime handle for the specified keyspace and holderID.
// one holderID is not allowed to acquire the same keyspace multiple times.
// Acquired handle must be released after use, otherwise the resources might
// not be cleaned up in time.
func (m *Manager) Acquire(
	ks string,
	holderID string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (sqlsvrapi.KSRuntimeHandle, error) {
	return m.AcquireWithContext(context.Background(), ks, holderID, ksSessFactoryGetter)
}

// AcquireWithContext honors cancellation while waiting for runtime initialization
// or teardown, before each store-open attempt, and during retry backoff. A driver
// open and runtime initialization each run to completion once started. Acquisition
// checks for cancellation after a successful open and before registering a holder.
func (m *Manager) AcquireWithContext(
	ctx context.Context,
	ks string,
	holderID string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (sqlsvrapi.KSRuntimeHandle, error) {
	if holderID == "" {
		return nil, errors.New("cross keyspace runtime holderID must not be empty")
	}
	if err := m.validateTargetKS(ks); err != nil {
		return nil, err
	}

	entry, holderCount, err := m.acquireEntry(ctx, ks, holderID, ksSessFactoryGetter)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("acquire cross keyspace runtime",
		zap.String("targetKS", ks),
		zap.String("holderID", holderID),
		zap.Int("activeHolderCount", holderCount))
	return &runtimeHandle{
		manager:  m,
		targetKS: ks,
		holderID: holderID,
		entry:    entry,
	}, nil
}

func (m *Manager) validateTargetKS(ks string) error {
	// misusing cross keyspace sessions might cause data written to the wrong
	// keyspace, or corrupt user data, and it's harder to diagnose those issues.
	// so we use runtime check instead of intest.Assert here, in case some code
	// paths are not covered by tests.
	if kerneltype.IsClassic() || m.store.GetKeyspace() == ks {
		return errors.New("cross keyspace is not available in classic kernel or current keyspace")
	}
	return nil
}

type sessionManagerFactory func(context.Context, string, func(string, validatorapi.Validator) pools.Factory) (*SessionManager, func(), error)

// newInitializingEntry must be called with Manager.mu held and no existing
// entry for ks.
func (m *Manager) newInitializingEntry(ks string) *runtimeEntry {
	entry := &runtimeEntry{
		state:         runtimeInitializing,
		done:          make(chan struct{}),
		activeHolders: make(map[string]struct{}),
	}
	m.runtimes[ks] = entry
	return entry
}

type entryLookup struct {
	entry       *runtimeEntry
	wait        <-chan struct{}
	initialize  bool
	holderCount int
	duplicate   bool
}

// lookupOrReserveEntry handles the locked part of runtime lookup. A raw lookup
// (empty holderID) may use a published runtime while its workers start or drain;
// a holder can only enter a ready runtime.
func (m *Manager) lookupOrReserveEntry(ctx context.Context, ks, holderID string) (entryLookup, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return entryLookup{}, err
	}
	if holderID != "" && m.closed {
		return entryLookup{}, errManagerClosed
	}
	entry := m.runtimes[ks]
	if entry == nil {
		if m.closed {
			return entryLookup{}, errManagerClosed
		}
		return entryLookup{entry: m.newInitializingEntry(ks), initialize: true}, nil
	}
	if holderID == "" && entry.visible() {
		return entryLookup{entry: entry}, nil
	}
	if holderID != "" && entry.state == runtimeReady {
		holderCount := len(entry.activeHolders)
		if _, ok := entry.activeHolders[holderID]; ok {
			return entryLookup{holderCount: holderCount, duplicate: true}, nil
		}
		entry.activeHolders[holderID] = struct{}{}
		return entryLookup{entry: entry, holderCount: holderCount + 1}, nil
	}
	return entryLookup{wait: entry.done}, nil
}

// getOrCreateRawEntry can return a published runtime while its workers start
// or drain. It waits for unpublished initialization or teardown to finish.
func (m *Manager) getOrCreateRawEntry(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (*runtimeEntry, error) {
	for {
		lookup, err := m.lookupOrReserveEntry(context.Background(), ks, "")
		if err != nil {
			return nil, err
		}
		if lookup.initialize {
			initialized, _, err := m.initializeEntry(context.Background(), ks, lookup.entry, "", ksSessFactoryGetter)
			return initialized, err
		}
		if lookup.entry != nil {
			return lookup.entry, nil
		}
		// An existing entry is invisible only before publication or after its
		// workers have stopped. Wait for that transition and retry the lookup.
		failpoint.InjectCall("beforeRuntimeEntryWait", ks)
		<-lookup.wait
	}
}

// acquireEntry admits holderID only after initialization finishes.
func (m *Manager) acquireEntry(
	ctx context.Context,
	ks string,
	holderID string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (*runtimeEntry, int, error) {
	for {
		lookup, err := m.lookupOrReserveEntry(ctx, ks, holderID)
		if err != nil {
			return nil, 0, err
		}
		if lookup.initialize {
			return m.initializeEntry(ctx, ks, lookup.entry, holderID, ksSessFactoryGetter)
		}
		if lookup.wait != nil {
			failpoint.InjectCall("beforeRuntimeEntryWait", ks)
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-lookup.wait:
			}
			continue
		}
		if lookup.duplicate {
			logutil.BgLogger().Warn("cross keyspace runtime already acquired",
				zap.String("targetKS", ks),
				zap.String("holderID", holderID),
				zap.Int("activeHolderCount", lookup.holderCount))
			return nil, 0, errors.Errorf("cross keyspace runtime for keyspace %s is already acquired by holderID %s", ks, holderID)
		}
		return lookup.entry, lookup.holderCount, nil
	}
}

// removeEntry finishes an initialization failure or a completed teardown.
// The caller owns the entry's placeholder until this wakes its waiters.
func (m *Manager) removeEntry(ks string, entry *runtimeEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runtimes, ks)
	close(entry.done)
}

func (m *Manager) publishEntry(entry *runtimeEntry, mgr *SessionManager) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	// Publish before startup so session factories can reenter GetOrCreate. The
	// entry stays initializing until all workers have been registered.
	entry.sessMgr = mgr
	return true
}

func (m *Manager) finishInitializingEntry(ctx context.Context, entry *runtimeEntry, holderID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := ctx.Err()
	if m.closed {
		err = errManagerClosed
	}
	if err == nil && holderID != "" {
		// Admit the creator before waking waiters, which could otherwise acquire
		// and eagerly release the runtime before its first holder is registered.
		entry.activeHolders[holderID] = struct{}{}
	} else if err != nil {
		// Let the idle sweep reclaim a runtime published without a holder.
		entry.lastReleaseAt = time.Now()
	}
	holderCount := len(entry.activeHolders)
	entry.state = runtimeReady
	close(entry.done)
	entry.done = nil
	return holderCount, err
}

// initializeEntry owns the initializing placeholder until worker registration
// finishes. Shutdown waits for that same placeholder, including private cleanup
// when shutdown wins before publication.
func (m *Manager) initializeEntry(
	ctx context.Context,
	ks string,
	entry *runtimeEntry,
	holderID string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (*runtimeEntry, int, error) {
	createSessionManager := sessionManagerFactory(m.createSessionManager)
	failpoint.InjectCall("mockCreateSessionManager", &createSessionManager)
	mgr, startWorkers, err := createSessionManager(ctx, ks, ksSessFactoryGetter)
	if err != nil {
		m.removeEntry(ks, entry)
		return nil, 0, err
	}
	if !m.publishEntry(entry, mgr) {
		mgr.close(nil)
		m.removeEntry(ks, entry)
		return nil, 0, errManagerClosed
	}
	if startWorkers != nil {
		startWorkers()
	}
	holderCount, err := m.finishInitializingEntry(ctx, entry, holderID)
	if err != nil {
		return nil, 0, err
	}
	return entry, holderCount, nil
}

// createSessionManager initializes the runtime and returns a function that
// starts its background workers after publication.
func (*Manager) createSessionManager(
	ctx context.Context,
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (_ *SessionManager, _ func(), err error) {
	startTime := time.Now()
	getStoreFn := func(ks string) (kv.Storage, error) { return getOrCreateStore(ctx, ks) }
	failpoint.InjectCall("beforeGetStore", &getStoreFn)
	var store kv.Storage
	store, err = getStoreFn(ks)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		// The SYSTEM store's lifecycle is managed outside the runtime.
		if err != nil && ks != keyspace.System {
			err2 := store.Close()
			if err2 != nil {
				logutil.BgLogger().Warn("failed to close store", zap.Error(err2))
			}
		}
	}()

	// A driver open runs to completion even if the acquisition is canceled.
	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}

	coordinator := newSchemaCoordinator()
	isValidator := isvalidator.New(vardef.GetSchemaLease())
	sessPool := util.NewSessionPool(
		crossKSSessPoolSize, ksSessFactoryGetter(ks, isValidator),
		func(r pools.Resource) {
			_, ok := r.(sessionctx.Context)
			intest.Assert(ok)
			coordinator.StoreInternalSession(r)
		},
		func(r pools.Resource) {
			sctx, ok := r.(sessionctx.Context)
			intest.Assert(ok)
			intest.AssertFunc(func() bool {
				txn, _ := sctx.Txn(false)
				return txn == nil || !txn.Valid()
			})
			coordinator.DeleteInternalSession(r)
		},
		func(r pools.Resource) {
			intest.Assert(r != nil)
			coordinator.DeleteInternalSession(r)
		},
	)

	var etcdCli *clientv3.Client
	etcdCli, err = kvstore.NewEtcdCli(store)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	failpoint.InjectCall("injectETCDCli", &etcdCli, ks)
	// Service discovery and connection creation are deferred until ID allocation.
	autoidClient := autoid.NewClientDiscover(etcdCli)
	ctx, cancel := context.WithCancel(context.Background())
	var svrInfoSyncer *serverinfo.Syncer
	serverInfoRegistered := false
	defer func() {
		if err != nil {
			if serverInfoRegistered {
				svrInfoSyncer.RemoveServerInfo()
				svrInfoSyncer.RevokeSession()
			}
			cancel()
			err2 := etcdCli.Close()
			if err2 != nil {
				logutil.BgLogger().Warn("failed to close etcd client", zap.Error(err2))
			}
			autoidClient.ResetConn(nil)
		}
	}()

	virtualSvrID := uuid.New().String()
	svrInfoSyncer = serverinfo.NewCrossKSSyncer(
		virtualSvrID,
		func() uint64 {
			// this ID is used to allocate connection ID, since we don't accept
			// in cross keyspace, we fix it to 0.
			return 0
		},
		etcdCli,
		&minStartTSReporter{},
		ks,
	)
	if err = svrInfoSyncer.NewSessionAndStoreServerInfo(ctx); err != nil {
		return nil, nil, errors.Trace(err)
	}
	serverInfoRegistered = true

	schemaVerSyncer := schemaver.NewEtcdSyncer(etcdCli, virtualSvrID)
	if err = schemaVerSyncer.Init(ctx); err != nil {
		return nil, nil, errors.Trace(err)
	}
	serverStateSyncer := serverstate.NewEtcdSyncer(etcdCli, ddlutil.ServerGlobalState)
	// The submit-only DDL path refreshes server state synchronously before
	// enqueue. Seed the cache here without Init, because Init starts an etcd
	// watch/session that this runtime does not need or drain.
	if _, err = serverStateSyncer.GetGlobalState(ctx); err != nil {
		return nil, nil, errors.Trace(err)
	}
	infoCache := infoschema.NewCache(store, int(vardef.SchemaVersionCacheLimit.Load()))
	isSyncer := issyncer.NewCrossKSSyncer(store, infoCache, vardef.GetSchemaLease(), sessPool, isValidator, ks)
	isSyncer.InitRequiredFields(
		func() sessmgr.InfoSchemaCoordinator {
			return coordinator
		},
		schemaVerSyncer,
		autoidClient, nil,
	)
	if err = isSyncer.Reload(); err != nil {
		return nil, nil, errors.Trace(err)
	}

	ddlSessPool := sess.NewSessionPool(sessPool)
	sysTblMgr := systable.NewManager(ddlSessPool)
	minJobIDRefresher := systable.NewMinJobIDRefresher(sysTblMgr)
	isSyncer.SetMinJobIDRefresher(minJobIDRefresher)
	ddlClient := newDDLClient(etcdCli, jobsubmit.SubmitOptions{
		Store:             store,
		SessPool:          ddlSessPool,
		SysTblMgr:         sysTblMgr,
		MinJobIDRefresher: minJobIDRefresher,
		ServerStateSyncer: serverStateSyncer,
	})

	mgr := &SessionManager{
		ctx:               ctx,
		cancel:            cancel,
		exitCh:            make(chan struct{}),
		store:             store,
		etcdCli:           etcdCli,
		autoidClient:      autoidClient,
		schemaVerSyncer:   schemaVerSyncer,
		serverStateSyncer: serverStateSyncer,
		infoCache:         infoCache,
		isSyncer:          isSyncer,
		sessPool:          sessPool,
		coordinator:       coordinator,
		isValidator:       isValidator,
		svrInfoSyncer:     svrInfoSyncer,
		ddlClient:         ddlClient,
	}

	mgr.globalVarCache = newGlobalVarCache(ctx, ks, sessPool, etcdCli)
	return mgr, func() {
		mgr.startWorkers(minJobIDRefresher)
		failpoint.InjectCall("afterStartRuntimeWorkers", ks)

		logutil.BgLogger().Info("create cross keyspace session manager",
			zap.String("targetKS", ks), zap.Duration("cost", time.Since(startTime)))
	}, nil
}

func (m *Manager) releaseEntry(h *runtimeHandle, closeIfIdle bool) (*runtimeEntry, int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := h.entry
	if m.runtimes[h.targetKS] != entry || entry.state != runtimeReady {
		// The runtime is already closed, and a replacement may have reused the
		// same holder ID.
		return nil, 0, false
	}
	delete(entry.activeHolders, h.holderID)
	holderCount := len(entry.activeHolders)
	shouldClose := holderCount == 0 && closeIfIdle
	if holderCount == 0 {
		entry.lastReleaseAt = time.Now()
		if shouldClose {
			entry.startClosing()
		}
	}
	return entry, holderCount, shouldClose
}

func (m *Manager) release(h *runtimeHandle, closeIfIdle bool) {
	entry, holderCount, shouldClose := m.releaseEntry(h, closeIfIdle)
	if entry == nil {
		return
	}
	logutil.BgLogger().Info("release cross keyspace runtime",
		zap.String("targetKS", h.targetKS),
		zap.String("holderID", h.holderID),
		zap.Int("activeHolderCount", holderCount))
	if shouldClose {
		m.closeEntry(h.targetKS, entry)
	}
}

func (m *Manager) startClosingRuntime(ks string) (*runtimeEntry, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.runtimes[ks]
	if entry == nil {
		return nil, nil
	}
	if entry.state != runtimeReady {
		return nil, entry.done
	}
	entry.startClosing()
	return entry, nil
}

// closeRuntime waits for in-flight work on ks and then closes its runtime, if
// any.
func (m *Manager) closeRuntime(ks string) {
	for {
		entry, done := m.startClosingRuntime(ks)
		if done != nil {
			failpoint.InjectCall("beforeRuntimeCloseWait", ks)
			<-done
			continue
		}
		if entry == nil {
			return
		}
		m.closeEntry(ks, entry)
		return
	}
}

func (m *Manager) markEntryClosing(entry *runtimeEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry.state = runtimeClosing
}

// closeEntry must be called outside mu by the owner of the teardown transition.
func (m *Manager) closeEntry(ks string, entry *runtimeEntry) {
	entry.sessMgr.close(func() {
		m.markEntryClosing(entry)
	})
	m.removeEntry(ks, entry)
}

// RunSystemKSGCLoop periodically evicts idle cross keyspace runtimes.
// as the name noted, this loop only runs in SYSTEM keyspace, user keyspace
// only access the SYSTEM ks, and will be kept alive.
// Note: there are raw caller to GetOrCreate which conflicts with the GC loop.
// we will make the API more clear in the future after we can refactor those
// callers to use Acquire instead of GetOrCreate directly.
func (m *Manager) RunSystemKSGCLoop(ctx context.Context) {
	interval := crossKSRuntimeSweepInterval
	idleTimeout := crossKSRuntimeIdleTimeout
	failpoint.InjectCall("mockRuntimeGCLoopConfig", &interval, &idleTimeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweepIdleRuntimes(idleTimeout)
		}
	}
}

func (m *Manager) claimIdleRuntime(ks string, cutoff time.Time) (*runtimeEntry, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.runtimes[ks]
	if entry == nil || entry.state != runtimeReady || len(entry.activeHolders) > 0 ||
		entry.lastReleaseAt.IsZero() || entry.lastReleaseAt.After(cutoff) {
		return nil, time.Time{}
	}
	// Claim only the runtime about to be closed. Other idle runtimes remain
	// available for acquisition while its cleanup is in progress.
	entry.startClosing()
	return entry, entry.lastReleaseAt
}

func (m *Manager) sweepIdleRuntimes(idleTimeout time.Duration) {
	cutoff := time.Now().Add(-idleTimeout)
	for _, ks := range m.GetAllKeyspace() {
		entry, lastReleaseAt := m.claimIdleRuntime(ks, cutoff)
		if entry == nil {
			continue
		}
		logutil.BgLogger().Info("evict idle cross keyspace runtime",
			zap.String("targetKS", ks),
			zap.Duration("idleTimeout", idleTimeout),
			zap.Time("lastReleaseAt", lastReleaseAt))
		m.closeEntry(ks, entry)
	}
}

func (m *Manager) closeKeyspaces() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	// The map includes every in-flight initialization and teardown. Repeated
	// Close calls also wait for those transitions instead of returning early.
	return slices.Collect(maps.Keys(m.runtimes))
}

// Close closes all session managers and their associated resources.
func (m *Manager) Close() {
	keyspaces := m.closeKeyspaces()
	slices.Sort(keyspaces)
	for _, ks := range keyspaces {
		m.closeRuntime(ks)
	}
}

func getOrCreateStore(ctx context.Context, targetKS string) (kv.Storage, error) {
	if targetKS == keyspace.System {
		return kvstore.GetSystemStorage(), nil
	}
	return kvstore.InitStorageWithContext(ctx, targetKS)
}

type runtimeHandle struct {
	manager     *Manager
	targetKS    string
	holderID    string
	entry       *runtimeEntry
	releaseOnce sync.Once
}

func (h *runtimeHandle) Store() kv.Storage {
	return h.entry.sessMgr.Store()
}

func (h *runtimeHandle) SysSessionPool() util.DestroyableSessionPool {
	return h.entry.sessMgr.SysSessionPool()
}

func (h *runtimeHandle) AlterTableMode(ctx context.Context, target model.AlterTableModeTarget) error {
	return h.entry.sessMgr.alterTableMode(ctx, target)
}

func (h *runtimeHandle) Release(closeIfIdle bool) {
	h.releaseOnce.Do(func() {
		h.manager.release(h, closeIfIdle)
	})
}

// SessionManager manages sessions for a specific keyspace.
type SessionManager struct {
	ctx               context.Context
	cancel            context.CancelFunc
	wg                util.WaitGroupWrapper
	exitCh            chan struct{}
	store             kv.Storage
	etcdCli           *clientv3.Client
	autoidClient      *autoid.ClientDiscover
	schemaVerSyncer   schemaver.Syncer
	serverStateSyncer serverstate.Syncer
	infoCache         *infoschema.InfoCache
	isSyncer          *issyncer.Syncer
	sessPool          util.DestroyableSessionPool
	globalVarCache    *globalVarCache
	coordinator       *schemaCoordinator
	isValidator       validatorapi.Validator
	svrInfoSyncer     *serverinfo.Syncer
	ddlClient         *ddlClient
}

// Store returns the kv.Storage instance used by the session manager.
func (m *SessionManager) Store() kv.Storage {
	return m.store
}

// InfoCache returns the InfoCache instance used by the session manager.
func (m *SessionManager) InfoCache() *infoschema.InfoCache {
	return m.infoCache
}

// GlobalVarAccessor returns the global variable accessor of the sessions of
// the runtime.
func (m *SessionManager) GlobalVarAccessor() variable.GlobalVarAccessor {
	return m.globalVarCache
}

// SysSessionPool returns the session pool used by the session manager.
func (m *SessionManager) SysSessionPool() util.DestroyableSessionPool {
	return m.sessPool
}

func (m *SessionManager) alterTableMode(ctx context.Context, target model.AlterTableModeTarget) error {
	return m.ddlClient.alterTableMode(ctx, target)
}

// Coordinator returns the InfoSchemaCoordinator used by the session manager.
func (m *SessionManager) Coordinator() sessmgr.InfoSchemaCoordinator {
	return m.coordinator
}

// startWorkers must run after the runtime is published in Manager.runtimes.
func (m *SessionManager) startWorkers(minJobIDRefresher *systable.MinJobIDRefresher) {
	m.wg.RunWithLog(m.globalVarCache.refreshLoop)
	m.wg.RunWithLog(func() {
		m.svrInfoSyncer.ServerInfoSyncLoop(m.ctx, m.store, m.exitCh)
	})
	m.wg.RunWithLog(func() {
		m.isSyncer.SyncLoop(m.ctx)
	})
	m.wg.RunWithLog(func() {
		m.isSyncer.MDLCheckLoop(m.ctx)
	})
	shouldRunMinJobIDRefresher := true
	failpoint.InjectCall("skipMinJobIDRefresher", &shouldRunMinJobIDRefresher)
	if shouldRunMinJobIDRefresher {
		m.wg.RunWithLog(func() {
			minJobIDRefresher.Start(m.ctx)
		})
	}
}

func (m *SessionManager) close(afterWorkersStopped func()) {
	ks := m.store.GetKeyspace()
	logger := logutil.BgLogger().With(zap.String("targetKS", ks))
	logger.Info("close cross keyspace session manager")
	m.sessPool.Close()
	close(m.exitCh)
	m.cancel()
	m.wg.Wait()
	if afterWorkersStopped != nil {
		// Hide the drained runtime before removing metadata or closing clients
		// and storage, but keep its placeholder until that cleanup is complete.
		afterWorkersStopped()
	}
	if m.svrInfoSyncer != nil {
		m.svrInfoSyncer.RemoveServerInfo()
		m.svrInfoSyncer.RevokeSession()
	}
	m.schemaVerSyncer.Close()
	// Close etcd first, so an auto-ID allocation that looks up the leader fails
	// instead of holding the client while it is reset, or connecting again.
	if err := m.etcdCli.Close(); err != nil {
		logger.Warn("failed to close etcd client", zap.Error(err))
	}
	m.autoidClient.ResetConn(nil)
	// lifecycle of SYSTEM store is managed outside, skip close.
	needCloseStore := ks != keyspace.System
	failpoint.InjectCall("skipCloseStore", &needCloseStore)
	if needCloseStore {
		if err := m.store.Close(); err != nil {
			logger.Warn("failed to close store", zap.Error(err))
		}
	}
}
