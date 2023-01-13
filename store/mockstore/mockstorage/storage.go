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

package mockstorage

import (
	"context"
	"crypto/tls"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/copr"
	driver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
)

// Wraps tikv.KVStore and make it compatible with kv.Storage.
type mockStorage struct {
	*tikv.KVStore
	*copr.Store
	memCache  kv.MemManager
	LockWaits []*deadlockpb.WaitForEntry
}

// NewMockStorage wraps tikv.KVStore as kv.Storage.
func NewMockStorage(tikvStore *tikv.KVStore) (kv.Storage, error) {
	coprConfig := config.DefaultConfig().TiKVClient.CoprCache
	coprStore, err := copr.NewStore(tikvStore, &coprConfig)
	if err != nil {
		return nil, err
	}
	return &mockStorage{
		KVStore:  tikvStore,
		Store:    coprStore,
		memCache: kv.NewCacheDB(),
	}, nil
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
func (s *mockStorage) Begin(opts ...tikv.TxnOption) (kv.Transaction, error) {
	txn, err := s.KVStore.Begin(opts...)
	return newTiKVTxn(txn, err)
}

// ShowStatus returns the specified status of the storage
func (s *mockStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, kv.ErrNotImplemented
}

// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
func (s *mockStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	return driver.NewSnapshot(s.KVStore.GetSnapshot(ver.Ver))
}

// CurrentVersion returns current max committed version with the given txnScope (local or global).
func (s *mockStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	ver, err := s.KVStore.CurrentTimestamp(txnScope)
	return kv.NewVersion(ver), err
}

// GetMinSafeTS return the minimal SafeTS of the storage with given txnScope.
func (s *mockStorage) GetMinSafeTS(txnScope string) uint64 {
	return 0
}

func newTiKVTxn(txn *tikv.KVTxn, err error) (kv.Transaction, error) {
	if err != nil {
		return nil, err
	}
	return driver.NewTiKVTxn(txn), nil
}

func (s *mockStorage) GetLockWaits() ([]*deadlockpb.WaitForEntry, error) {
	return s.LockWaits, nil
}

func (s *mockStorage) Close() error {
	s.Store.Close()
	return s.KVStore.Close()
}

func (s *mockStorage) GetCodec() tikv.Codec {
	pdClient := s.KVStore.GetPDClient()
	pdCodecCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	return pdCodecCli.GetCodec()
}

// MockLockWaitSetter is used to set the mocked lock wait information, which helps implementing tests that uses the
// GetLockWaits function.
type MockLockWaitSetter interface {
	SetMockLockWaits(lockWaits []*deadlockpb.WaitForEntry)
}

func (s *mockStorage) SetMockLockWaits(lockWaits []*deadlockpb.WaitForEntry) {
	s.LockWaits = lockWaits
}
