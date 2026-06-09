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
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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

type runtimeEntry struct {
	sessMgr       *SessionManager
	activeHolders map[string]struct{}
	lastReleaseAt time.Time
}

// Manager manages all cross keyspace sessions.
type Manager struct {
	mu sync.RWMutex
	// the store of current instance
	store kv.Storage
	// keyspace name -> runtime entry
	runtimes map[string]*runtimeEntry
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
	return slices.Collect(maps.Keys(m.runtimes))
}

func (m *Manager) get(ks string) (*SessionManager, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getWithoutLock(ks)
}

func (m *Manager) getWithoutLock(ks string) (*SessionManager, bool) {
	entry, ok := m.runtimes[ks]
	if !ok {
		return nil, false
	}
	return entry.sessMgr, true
}

// GetOrCreate gets or creates a session manager for the specified keyspace.
func (m *Manager) GetOrCreate(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (_ *SessionManager, err error) {
	if err := m.validateTargetKS(ks); err != nil {
		return nil, err
	}
	if mgr, ok := m.get(ks); ok {
		return mgr, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, err := m.getOrCreateEntryWithoutLock(ks, ksSessFactoryGetter)
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
	if holderID == "" {
		return nil, errors.New("cross keyspace runtime holderID must not be empty")
	}
	if err := m.validateTargetKS(ks); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, err := m.getOrCreateEntryWithoutLock(ks, ksSessFactoryGetter)
	if err != nil {
		return nil, err
	}
	if _, ok := entry.activeHolders[holderID]; ok {
		logutil.BgLogger().Warn("cross keyspace runtime already acquired",
			zap.String("targetKS", ks),
			zap.String("holderID", holderID),
			zap.Int("activeHolderCount", len(entry.activeHolders)))
		return nil, errors.Errorf("cross keyspace runtime for keyspace %s is already acquired by holderID %s", ks, holderID)
	}
	entry.activeHolders[holderID] = struct{}{}
	logutil.BgLogger().Info("acquire cross keyspace runtime",
		zap.String("targetKS", ks),
		zap.String("holderID", holderID),
		zap.Int("activeHolderCount", len(entry.activeHolders)))
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

func (m *Manager) getOrCreateEntryWithoutLock(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (*runtimeEntry, error) {
	if entry, ok := m.runtimes[ks]; ok {
		return entry, nil
	}

	createSessionManager := m.createSessionManager
	failpoint.InjectCall("mockCreateSessionManager", &createSessionManager)
	mgr, err := createSessionManager(ks, ksSessFactoryGetter)
	if err != nil {
		return nil, err
	}
	entry := &runtimeEntry{
		sessMgr:       mgr,
		activeHolders: make(map[string]struct{}),
	}
	m.runtimes[ks] = entry
	return entry, nil
}

func (*Manager) createSessionManager(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (_ *SessionManager, err error) {
	startTime := time.Now()
	getStoreFn := getOrCreateStore
	failpoint.InjectCall("beforeGetStore", &getStoreFn)
	var store kv.Storage
	store, err = getStoreFn(ks)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err2 := store.Close()
			if err2 != nil {
				logutil.BgLogger().Warn("failed to close store", zap.Error(err2))
			}
		}
	}()

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
		return nil, errors.Trace(err)
	}
	failpoint.InjectCall("injectETCDCli", &etcdCli, ks)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cancel()
			err2 := etcdCli.Close()
			if err2 != nil {
				logutil.BgLogger().Warn("failed to close etcd client", zap.Error(err2))
			}
		}
	}()

	virtualSvrID := uuid.New().String()
	svrInfoSyncer := serverinfo.NewCrossKSSyncer(
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
		return nil, errors.Trace(err)
	}

	schemaVerSyncer := schemaver.NewEtcdSyncer(etcdCli, virtualSvrID)
	if err = schemaVerSyncer.Init(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	infoCache := infoschema.NewCache(store, int(vardef.SchemaVersionCacheLimit.Load()))
	isSyncer := issyncer.NewCrossKSSyncer(store, infoCache, vardef.GetSchemaLease(), sessPool, isValidator, ks)
	isSyncer.InitRequiredFields(
		func() sessmgr.InfoSchemaCoordinator {
			return coordinator
		},
		schemaVerSyncer,
		nil, nil,
	)
	if err = isSyncer.Reload(); err != nil {
		return nil, errors.Trace(err)
	}

	sysTblMgr := systable.NewManager(sess.NewSessionPool(sessPool))
	minJobIDRefresher := systable.NewMinJobIDRefresher(sysTblMgr)
	isSyncer.SetMinJobIDRefresher(minJobIDRefresher)

	mgr := &SessionManager{
		ctx:             ctx,
		cancel:          cancel,
		exitCh:          make(chan struct{}),
		store:           store,
		etcdCli:         etcdCli,
		schemaVerSyncer: schemaVerSyncer,
		infoCache:       infoCache,
		isSyncer:        isSyncer,
		sessPool:        sessPool,
		coordinator:     coordinator,
		isValidator:     isValidator,
		svrInfoSyncer:   svrInfoSyncer,
	}

	mgr.wg.RunWithLog(func() {
		svrInfoSyncer.ServerInfoSyncLoop(store, mgr.exitCh)
	})
	mgr.wg.RunWithLog(func() {
		isSyncer.SyncLoop(ctx)
	})
	mgr.wg.RunWithLog(func() {
		isSyncer.MDLCheckLoop(ctx)
	})
	mgr.wg.RunWithLog(func() {
		minJobIDRefresher.Start(ctx)
	})

	logutil.BgLogger().Info("create cross keyspace session manager",
		zap.String("targetKS", ks), zap.Duration("cost", time.Since(startTime)))

	return mgr, nil
}

// release releases the runtime handle for the specified keyspace and holderID.
// the resources will be cleaned up if there is no active holder after enough
// time.
func (m *Manager) release(targetKS string, holderID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.runtimes[targetKS]
	if !ok {
		return
	}
	delete(entry.activeHolders, holderID)
	if len(entry.activeHolders) == 0 {
		entry.lastReleaseAt = time.Now()
	}
	logutil.BgLogger().Info("release cross keyspace runtime",
		zap.String("targetKS", targetKS),
		zap.String("holderID", holderID),
		zap.Int("activeHolderCount", len(entry.activeHolders)))
}

// RunSystemKSGCLoop periodically evicts idle cross keyspace runtimes.
// as the name noted, this loop only runs in SYSTEM keyspace, user keyspace
// only access the SYSTEM ks, and will be kept alive.
// Note: there are raw caller to GetOrCreate which conflicts with the GC loop.
// we will make the API more clear in the future after we can refactor those
// callers to use Acquire instead of GetOrCreate directly.
func (m *Manager) RunSystemKSGCLoop(ctx context.Context) {
	interval := crossKSRuntimeSweepInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweepIdleRuntimes(crossKSRuntimeIdleTimeout)
		}
	}
}

func (m *Manager) sweepIdleRuntimes(idleTimeout time.Duration) {
	type evictedRuntime struct {
		targetKS string
		entry    *runtimeEntry
	}

	evicted := make([]evictedRuntime, 0, 1)
	now := time.Now()
	m.mu.Lock()
	for targetKS, entry := range m.runtimes {
		if len(entry.activeHolders) > 0 || entry.lastReleaseAt.IsZero() {
			continue
		}
		if now.Sub(entry.lastReleaseAt) < idleTimeout {
			continue
		}
		delete(m.runtimes, targetKS)
		evicted = append(evicted, evictedRuntime{targetKS: targetKS, entry: entry})
	}
	m.mu.Unlock()

	for _, item := range evicted {
		logutil.BgLogger().Info("evict idle cross keyspace runtime",
			zap.String("targetKS", item.targetKS),
			zap.Duration("idleTimeout", idleTimeout),
			zap.Time("lastReleaseAt", item.entry.lastReleaseAt))
		item.entry.sessMgr.close()
	}
}

// Close closes all session managers and their associated resources.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range m.runtimes {
		entry.sessMgr.close()
	}
	m.runtimes = make(map[string]*runtimeEntry)
}

func getOrCreateStore(targetKS string) (kv.Storage, error) {
	if targetKS == keyspace.System {
		return kvstore.GetSystemStorage(), nil
	}
	return kvstore.InitStorage(targetKS)
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

func (h *runtimeHandle) Release() {
	h.releaseOnce.Do(func() {
		h.manager.release(h.targetKS, h.holderID)
	})
}

// SessionManager manages sessions for a specific keyspace.
type SessionManager struct {
	ctx             context.Context
	cancel          context.CancelFunc
	wg              util.WaitGroupWrapper
	exitCh          chan struct{}
	store           kv.Storage
	etcdCli         *clientv3.Client
	schemaVerSyncer schemaver.Syncer
	infoCache       *infoschema.InfoCache
	isSyncer        *issyncer.Syncer
	sessPool        util.DestroyableSessionPool
	coordinator     *schemaCoordinator
	isValidator     validatorapi.Validator
	svrInfoSyncer   *serverinfo.Syncer
}

// Store returns the kv.Storage instance used by the session manager.
func (m *SessionManager) Store() kv.Storage {
	return m.store
}

// InfoCache returns the InfoCache instance used by the session manager.
func (m *SessionManager) InfoCache() *infoschema.InfoCache {
	return m.infoCache
}

// SysSessionPool returns the session pool used by the session manager.
func (m *SessionManager) SysSessionPool() util.DestroyableSessionPool {
	return m.sessPool
}

// Coordinator returns the InfoSchemaCoordinator used by the session manager.
func (m *SessionManager) Coordinator() sessmgr.InfoSchemaCoordinator {
	return m.coordinator
}

func (m *SessionManager) close() {
	ks := m.store.GetKeyspace()
	logger := logutil.BgLogger().With(zap.String("targetKS", ks))
	logger.Info("close cross keyspace session manager")
	m.sessPool.Close()
	close(m.exitCh)
	m.cancel()
	m.wg.Wait()
	m.schemaVerSyncer.Close()
	if err := m.etcdCli.Close(); err != nil {
		logger.Warn("failed to close etcd client", zap.Error(err))
	}
	// lifecycle of SYSTEM store is managed outside, skip close.
	needCloseStore := ks != keyspace.System
	failpoint.InjectCall("skipCloseStore", &needCloseStore)
	if needCloseStore {
		if err := m.store.Close(); err != nil {
			logger.Warn("failed to close store", zap.Error(err))
		}
	}
}
