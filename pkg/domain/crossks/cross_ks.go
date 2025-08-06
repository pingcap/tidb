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
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
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
	"go.uber.org/zap"
)

const crossKSSessPoolSize = 5

// Manager manages all cross keyspace sessions.
type Manager struct {
	mu sync.RWMutex
	// keyspace name -> session manager
	sessMgrs map[string]*SessionManager
}

// NewManager creates a new cross keyspace session manager.
func NewManager() *Manager {
	return &Manager{
		sessMgrs: make(map[string]*SessionManager),
	}
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
	if kerneltype.IsClassic() || config.GetGlobalKeyspaceName() == ks {
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

	getStoreFn := getOrCreateStore
	failpoint.InjectCall("beforeGetStore", &getStoreFn)
	store, err := getStoreFn(ks)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err2 := store.Close()
			if err2 != nil {
				logutil.BgLogger().Error("failed to close store", zap.Error(err2))
			}
		}
	}()

	coordinator := newSchemaCoordinator()
	isValidator := isvalidator.NewNoop()
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

	infoCache := infoschema.NewCache(store, int(vardef.SchemaVersionCacheLimit.Load()))
	isLoader := issyncer.NewLoaderForCrossKS(store, infoCache)
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, err
	}

	_, _, _, _, err = isLoader.LoadWithTS(ver.Ver, false)
	if err != nil {
		return nil, err
	}

	mgr := &SessionManager{
		store:       store,
		infoCache:   infoCache,
		isLoader:    isLoader,
		sessPool:    sessPool,
		coordinator: coordinator,
	}
	m.sessMgrs[ks] = mgr
	return mgr, nil
}

// Close closes all session managers and their associated resources.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ks, mgr := range m.sessMgrs {
		mgr.sessPool.Close()
		if ks == keyspace.System {
			// lifecycle of SYSTEM store is managed outside, skip close.
			continue
		}
		err := mgr.store.Close()
		if err != nil {
			logutil.BgLogger().Error("failed to close store", zap.String("keyspace", ks), zap.Error(err))
		}
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
	store       kv.Storage
	infoCache   *infoschema.InfoCache
	isLoader    *issyncer.Loader
	sessPool    util.DestroyableSessionPool
	coordinator *schemaCoordinator
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
