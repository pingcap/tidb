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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/schematracker"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/gctuner"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
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
	store, _ := CreateMockStoreAndDomain(t, opts...)
	return store
}

type DistExecutionTestContext struct {
	Store   kv.Storage
	domains []*domain.Domain
	t       testing.TB
}

func (d *DistExecutionTestContext) InitOwner() {
	for _, dom := range d.domains {
		dom.DDL().OwnerManager().RetireOwner()
	}
	d.domains[len(d.domains)-1].DDL().OwnerManager().CampaignOwner()
}

func (d *DistExecutionTestContext) SetOwner(idx int) error {
	if idx >= len(d.domains) || idx < 0 {
		return errors.New("server idx out of bound")
	}
	for _, dom := range d.domains {
		dom.DDL().OwnerManager().RetireOwner()
	}
	d.domains[idx].DDL().OwnerManager().CampaignOwner()
	return nil
}

func (d *DistExecutionTestContext) AddServer() {
	dom := bootstrap4DistExecution(d.t, d.Store, 500*time.Microsecond)
	dom.InfoSyncer().SetSessionManager(d.domains[0].InfoSyncer().GetSessionManager())
	dom.DDL().OwnerManager().RetireOwner()
	d.domains = append(d.domains, dom)
}

func (d *DistExecutionTestContext) DeleteServer(idx int) error {
	if idx >= len(d.domains) || idx < 0 {
		return errors.New("server idx out of bound")
	}
	if len(d.domains) == 1 {
		return errors.New("can't delete server, since server num = 1")
	}
	var err error
	if d.domains[idx].DDL().OwnerManager().IsOwner() {
		err = d.SetOwner(0)
	}
	d.domains = append(d.domains[:idx], d.domains[idx+1:]...)
	infosync.DeleteServerInfo(idx)
	return err
}

func NewDistExecutionTestContext(t testing.TB, serverNum int) *DistExecutionTestContext {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	gctuner.GlobalMemoryLimitTuner.Stop()
	domains := make([]*domain.Domain, 0, serverNum)
	sm := MockSessionManager{}

	for i := 0; i < serverNum; i++ {
		domains = append(domains, bootstrap4DistExecution(t, store, 500*time.Millisecond))
		domains[i].InfoSyncer().SetSessionManager(&sm)
	}
	t.Cleanup(func() {
		err := store.Close()
		require.NoError(t, err)
		gctuner.GlobalMemoryLimitTuner.Stop()
	})
	res := DistExecutionTestContext{schematracker.UnwrapStorage(store), domains, t}
	res.InitOwner()
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
		err := store.Close()
		require.NoError(t, err)
		gctuner.GlobalMemoryLimitTuner.Stop()
	})
	return schematracker.UnwrapStorage(store), dom
}

func bootstrap4DistExecution(t testing.TB, store kv.Storage, lease time.Duration) *domain.Domain {
	return bootstrapImpl(t, store, lease, session.BootstrapSession4DistExecution)
}

func bootstrap(t testing.TB, store kv.Storage, lease time.Duration) *domain.Domain {
	return bootstrapImpl(t, store, lease, session.BootstrapSession)
}

func bootstrapImpl(t testing.TB, store kv.Storage, lease time.Duration, bootstrapSessionImpl func(store kv.Storage) (*domain.Domain, error)) *domain.Domain {
	session.SetSchemaLease(lease)
	session.DisableStats4Test()
	domain.DisablePlanReplayerBackgroundJob4Test()
	domain.DisableDumpHistoricalStats4Test()
	dom, err := bootstrapSessionImpl(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)
	t.Cleanup(func() {
		dom.Close()
		view.Stop()
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
