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
	"testing"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

// CreateMockStore return a new mock kv.Storage.
func CreateMockStore(t testing.TB, opts ...mockstore.MockTiKVStoreOption) (store kv.Storage, clean func()) {
	store, _, clean = CreateMockStoreAndDomain(t, opts...)
	return
}

// CreateMockStoreAndDomain return a new mock kv.Storage and *domain.Domain.
func CreateMockStoreAndDomain(t testing.TB, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain, func()) {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	dom, clean := bootstrap(t, store, 0)
	return store, dom, clean
}

func bootstrap(t testing.TB, store kv.Storage, lease time.Duration) (*domain.Domain, func()) {
	session.SetSchemaLease(lease)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	dom.SetStatsUpdating(true)

	clean := func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}
	return dom, clean
}

// CreateMockStoreWithSchemaLease return a new mock kv.Storage.
func CreateMockStoreWithSchemaLease(t testing.TB, lease time.Duration, opts ...mockstore.MockTiKVStoreOption) (store kv.Storage, clean func()) {
	store, _, clean = CreateMockStoreAndDomainWithSchemaLease(t, lease, opts...)
	return
}

// CreateMockStoreAndDomainWithSchemaLease return a new mock kv.Storage and *domain.Domain.
func CreateMockStoreAndDomainWithSchemaLease(t testing.TB, lease time.Duration, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain, func()) {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	dom, clean := bootstrap(t, store, lease)
	return store, dom, clean
}

// CreateMockStoreWithOracle returns a new mock kv.Storage and *domain.Domain, providing the oracle for the store.
func CreateMockStoreWithOracle(t testing.TB, oracle oracle.Oracle, opts ...mockstore.MockTiKVStoreOption) (kv.Storage, *domain.Domain, func()) {
	store, err := mockstore.NewMockStore(opts...)
	require.NoError(t, err)
	store.GetOracle().Close()
	store.(tikv.Storage).SetOracle(oracle)
	dom, clean := bootstrap(t, store, 0)
	return store, dom, clean
}
