// Copyright 2021 PingCAP, Inc.
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

//go:build !codes

package testkit

import (
	"flag"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcemanager"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
)

// WithTiKV flag is only used for debugging locally with real tikv cluster.
var WithTiKV = flag.String("with-tikv", "", "address of tikv cluster, if set, running test with real tikv cluster")

// CreateMockStore return a new mock kv.Storage.
func CreateMockStore(t testing.TB, opts ...mockstore.MockTiKVStoreOption) kv.Storage {
	if *WithTiKV != "" {
		var d driver.TiKVDriver
		var err error
		store, err := d.Open("tikv://" + *WithTiKV)
		require.NoError(t, err)

		var dom *domain.Domain
		dom, err = session.BootstrapSession(store)
		t.Cleanup(func() {
			dom.Close()
			err := store.Close()
			require.NoError(t, err)
			view.Stop()
		})
		require.NoError(t, err)
		return store
	}
	t.Cleanup(func() {
		view.Stop()
	})
	gctuner.GlobalMemoryLimitTuner.Stop()
	tryMakeImage()
	store, _ := CreateMockStoreAndDomain(t, opts...)
	_ = store.(helper.Storage)
	return store
}

// tryMakeImage tries to create a bootstraped storage, the store is used as image for testing later.
func tryMakeImage(opts ...mockstore.MockTiKVStoreOption) {
	if mockstore.ImageAvailable() {
		return
	}
	retry, err := false, error(nil)
	for err == nil {
		retry, err = tryMakeImageOnce()
		if !retry {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func tryMakeImageOnce() (retry bool, err error) {
	const lockFile = "/tmp/tidb-unistore-bootstraped-image-lock-file"
	lock, err := os.Create(lockFile)
	if err != nil {
		return true, nil
	}
	defer func() { err = os.Remove(lockFile) }()
	defer lock.Close()

	// Prevent other process from creating the image concurrently
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return true, nil
	}
	defer func() { err = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	// Now this is the only instance to do the operation.
	store, err := mockstore.NewMockStore(
		mockstore.WithStoreType(mockstore.EmbedUnistore),
		mockstore.WithPath(mockstore.ImageFilePath))
	if err != nil {
		return false, err
	}

	session.SetSchemaLease(500 * time.Millisecond)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()
	dom, err := session.BootstrapSession(store)
	if err != nil {
		return false, err
	}
	dom.SetStatsUpdating(true)

	dom.Close()
	err = store.Close()

	return false, err
}

// DistExecutionContext is the context
// that used in Distributed execution test for Dist task framework and DDL.
// TODO remove it after we can start multiple DDL job scheduler separately.
type DistExecutionContext struct {
	Store          kv.Storage
	domains        []*domain.Domain
	deletedDomains []*domain.Domain
	t              testing.TB
	mu             sync.Mutex
}

// TriggerOwnerChange set one mock domain to DDL Owner by idx.
func (d *DistExecutionContext) TriggerOwnerChange() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, dom := range d.domains {
		om := dom.DDL().OwnerManager()
		if om.IsOwner() {
			_ = om.ResignOwner(nil)
			break
		}
	}
}

// AddDomain add 1 domain which is not ddl owner.
func (d *DistExecutionContext) AddDomain() {
	d.mu.Lock()
	defer d.mu.Unlock()
	dom := bootstrap4DistExecution(d.t, d.Store, 500*time.Millisecond)
	dom.InfoSyncer().SetSessionManager(d.domains[0].InfoSyncer().GetSessionManager())
	dom.DDL().OwnerManager().RetireOwner()
	d.domains = append(d.domains, dom)
}

// Close cleanup running goroutines, release resources used.
func (d *DistExecutionContext) Close() {
	d.t.Cleanup(func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		gctuner.GlobalMemoryLimitTuner.Stop()

		var wg tidbutil.WaitGroupWrapper
		for _, dom := range d.deletedDomains {
			wg.Run(dom.Close)
		}

		for _, dom := range d.domains {
			wg.Run(dom.Close)
		}

		wg.Wait()
		err := d.Store.Close()
		require.NoError(d.t, err)
	})
}

// GetDomain get domain by index.
func (d *DistExecutionContext) GetDomain(idx int) *domain.Domain {
	return d.domains[idx]
}

// GetDomainCnt get domain count.
func (d *DistExecutionContext) GetDomainCnt() int {
	return len(d.domains)
}

// NewDistExecutionContext create DistExecutionContext for testing.
func NewDistExecutionContext(t testing.TB, serverNum int) *DistExecutionContext {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	gctuner.GlobalMemoryLimitTuner.Stop()
	domains := make([]*domain.Domain, 0, serverNum)
	sm := MockSessionManager{}

	var domInfo []string
	for i := 0; i < serverNum; i++ {
		dom := bootstrap4DistExecution(t, store, 500*time.Millisecond)
		if i != serverNum-1 {
			dom.SetOnClose(func() { /* don't delete the store in domain map */ })
		}
		domains = append(domains, dom)
		domains[i].InfoSyncer().SetSessionManager(&sm)
		domInfo = append(domInfo, dom.DDL().GetID())
	}
	logutil.BgLogger().Info("domain DDL IDs", zap.Strings("IDs", domInfo))

	res := DistExecutionContext{
		schematracker.UnwrapStorage(store), domains, []*domain.Domain{}, t, sync.Mutex{}}
	return &res
}

// CreateMockStoreAndDomain return a new mock kv.Storage and *domain.Domain.
func CreateMockStoreAndDomain(t testing.TB, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain) {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	dom := bootstrap(t, store, 500*time.Millisecond)
	sm := MockSessionManager{}
	dom.InfoSyncer().SetSessionManager(&sm)
	t.Cleanup(func() {
		view.Stop()
		gctuner.GlobalMemoryLimitTuner.Stop()
	})
	store = schematracker.UnwrapStorage(store)
	_ = store.(helper.Storage)
	return store, dom
}

func bootstrap4DistExecution(t testing.TB, store kv.Storage, lease time.Duration) *domain.Domain {
	session.SetSchemaLease(lease)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()
	dom, err := session.BootstrapSession4DistExecution(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)
	return dom
}

func bootstrap(t testing.TB, store kv.Storage, lease time.Duration) *domain.Domain {
	session.SetSchemaLease(lease)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	t.Cleanup(func() {
		dom.Close()
		view.Stop()
		err := store.Close()
		require.NoError(t, err)
		resourcemanager.InstanceResourceManager.Reset()
	})
	return dom
}

// CreateMockStoreWithSchemaLease return a new mock kv.Storage.
func CreateMockStoreWithSchemaLease(t testing.TB, lease time.Duration, opts ...mockstore.MockTiKVStoreOption) kv.Storage {
	store, _ := CreateMockStoreAndDomainWithSchemaLease(t, lease, opts...)
	return schematracker.UnwrapStorage(store)
}

// CreateMockStoreAndDomainWithSchemaLease return a new mock kv.Storage and *domain.Domain.
func CreateMockStoreAndDomainWithSchemaLease(t testing.TB, lease time.Duration, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain) {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	dom := bootstrap(t, store, lease)
	sm := MockSessionManager{}
	dom.InfoSyncer().SetSessionManager(&sm)
	return schematracker.UnwrapStorage(store), dom
}
