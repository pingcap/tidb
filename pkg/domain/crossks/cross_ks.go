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

const crossKSSessPoolSize = 5

// Manager manages all cross keyspace sessions.
type Manager struct {
	mu sync.RWMutex
	// the store of current instance
	store kv.Storage
	// keyspace name -> session manager
	sessMgrs map[string]*SessionManager
}

// NewManager creates a new cross keyspace session manager.
func NewManager(store kv.Storage) *Manager {
	return &Manager{
		store:    store,
		sessMgrs: make(map[string]*SessionManager),
	}
}

// GetAllKeyspace returns all keyspace names that have session managers.
// used in tests.
func (m *Manager) GetAllKeyspace() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Collect(maps.Keys(m.sessMgrs))
}

// Get gets a session manager for the specified keyspace.
// exported for test only.
func (m *Manager) Get(ks string) (*SessionManager, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getWithoutLock(ks)
}

func (m *Manager) getWithoutLock(ks string) (*SessionManager, bool) {
	sessMgr, ok := m.sessMgrs[ks]
	return sessMgr, ok
}

// GetOrCreate gets or creates a session manager for the specified keyspace.
func (m *Manager) GetOrCreate(
	ks string,
	ksSessFactoryGetter func(string, validatorapi.Validator) pools.Factory,
) (_ *SessionManager, err error) {
	// misusing this function might cause data written to the wrong keyspace, or
	// corrupt user data, and it's harder to diagnose those issues, so we use
	// runtime check instead of intest.Assert here, in case some code path are not
	// covered by tests.
	if kerneltype.IsClassic() || m.store.GetKeyspace() == ks {
		return nil, errors.New("cross keyspace session manager is not available in classic kernel or current keyspace")
	}
	if mgr, ok := m.Get(ks); ok {
		return mgr, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if mgr, ok := m.getWithoutLock(ks); ok {
		return mgr, nil
	}

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
	m.sessMgrs[ks] = mgr

	logutil.BgLogger().Info("create cross keyspace session manager",
		zap.String("targetKS", ks), zap.Duration("cost", time.Since(startTime)))

	return mgr, nil
}

// CloseKS closes the session manager for the specified keyspace.
func (m *Manager) CloseKS(targetKS string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mgr, ok := m.sessMgrs[targetKS]; ok {
		mgr.close()
		delete(m.sessMgrs, targetKS)
	}
}

// Close closes all session managers and their associated resources.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, mgr := range m.sessMgrs {
		mgr.close()
	}
	m.sessMgrs = make(map[string]*SessionManager)
}

func getOrCreateStore(targetKS string) (kv.Storage, error) {
	if targetKS == keyspace.System {
		return kvstore.GetSystemStorage(), nil
	}
	return kvstore.InitStorage(targetKS)
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

// SessPool returns the session pool used by the session manager.
func (m *SessionManager) SessPool() util.DestroyableSessionPool {
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
