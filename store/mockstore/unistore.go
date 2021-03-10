// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mockstore

import (
	"crypto/tls"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	driver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/util/execdetails"
)

func newUnistore(opts *mockOptions) (kv.Storage, error) {
	client, pdClient, cluster, err := unistore.New(opts.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts.clusterInspector(cluster)
	pdClient = execdetails.InterceptedPDClient{
		Client: pdClient,
	}

	kvstore, err := tikv.NewTestTiKVStore(client, pdClient, opts.clientHijacker, opts.pdClientHijacker, opts.txnLocalLatches)
	if err != nil {
		return nil, err
	}
	return NewMockStorage(kvstore), nil
}

// Wraps tikv.KVStore and make it compatible with kv.Storage.
type mockStorage struct {
	*tikv.KVStore
	*copr.Store
	memCache kv.MemManager
}

// NewMockStorage wraps tikv.KVStore as kv.Storage.
func NewMockStorage(tikvStore *tikv.KVStore) kv.Storage {
	coprConfig := config.DefaultConfig().TiKVClient.CoprCache
	coprStore, err := copr.NewStore(tikvStore, &coprConfig)
	if err != nil {
		panic(err)
	}
	return &mockStorage{
		KVStore:  tikvStore,
		Store:    coprStore,
		memCache: kv.NewCacheDB(),
	}
}

func (s *mockStorage) EtcdAddrs() ([]string, error) {
	return nil, nil
}

func (s *mockStorage) TLSConfig() *tls.Config {
	return nil
}

// GetMemCache return memory mamager of the storage
func (s *mockStorage) GetMemCache() kv.MemManager {
	return s.memCache
}

func (s *mockStorage) StartGCWorker() error {
	return nil
}

func (s *mockStorage) Name() string {
	return "mock-storage"
}

func (s *mockStorage) Describe() string {
	return ""
}

// Begin a global transaction.
func (s *mockStorage) Begin() (kv.Transaction, error) {
	txn, err := s.KVStore.Begin()
	return newTiKVTxn(txn, err)
}

// BeginWithOption begins a transaction with given option
func (s *mockStorage) BeginWithOption(option kv.TransactionOption) (kv.Transaction, error) {
	txn, err := s.KVStore.BeginWithOption(option)
	return newTiKVTxn(txn, err)
}

func newTiKVTxn(txn *tikv.KVTxn, err error) (kv.Transaction, error) {
	if err != nil {
		return nil, err
	}
	return driver.NewTiKVTxn(txn), nil
}

func (s *mockStorage) Close() error {
	s.Store.Close()
	return s.KVStore.Close()
}
