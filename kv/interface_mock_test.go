// Copyright 2016 PingCAP, Inc.
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

package kv

import (
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// mockTxn is a txn that returns a retryAble error when called Commit.
type mockTxn struct {
	opts  map[int]interface{}
	valid bool
}

// Commit always returns a retryable error.
func (t *mockTxn) Commit(ctx context.Context) error {
	return ErrTxnRetryable
}

func (t *mockTxn) Rollback() error {
	t.valid = false
	return nil
}

func (t *mockTxn) String() string {
	return ""
}

func (t *mockTxn) LockKeys(_ context.Context, _ *LockCtx, _ ...Key) error {
	return nil
}

func (t *mockTxn) SetOption(opt int, val interface{}) {
	t.opts[opt] = val
}

func (t *mockTxn) DelOption(opt int) {
	delete(t.opts, opt)
}

func (t *mockTxn) GetOption(opt int) interface{} {
	return t.opts[opt]
}

func (t *mockTxn) IsReadOnly() bool {
	return true
}

func (t *mockTxn) StartTS() uint64 {
	return uint64(0)
}
func (t *mockTxn) Get(ctx context.Context, k Key) ([]byte, error) {
	return nil, nil
}

func (t *mockTxn) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	return nil, nil
}

func (t *mockTxn) Iter(k Key, upperBound Key) (Iterator, error) {
	return nil, nil
}

func (t *mockTxn) IterReverse(k Key) (Iterator, error) {
	return nil, nil
}

func (t *mockTxn) Set(k Key, v []byte) error {
	return nil
}
func (t *mockTxn) Delete(k Key) error {
	return nil
}

func (t *mockTxn) Valid() bool {
	return t.valid
}

func (t *mockTxn) Len() int {
	return 0
}

func (t *mockTxn) Size() int {
	return 0
}

func (t *mockTxn) GetMemBuffer() MemBuffer {
	return nil
}

func (t *mockTxn) GetSnapshot() Snapshot {
	return nil
}

func (t *mockTxn) NewStagingBuffer() MemBuffer {
	return nil
}

func (t *mockTxn) Flush() (int, error) {
	return 0, nil
}

func (t *mockTxn) Discard() {

}

func (t *mockTxn) Reset() {
	t.valid = false
}

func (t *mockTxn) SetVars(vars interface{}) {

}

func (t *mockTxn) GetVars() interface{} {
	return nil
}

func (t *mockTxn) CacheTableInfo(id int64, info *model.TableInfo) {

}

func (t *mockTxn) GetTableInfo(id int64) *model.TableInfo {
	return nil
}

// newMockTxn new a mockTxn.
func newMockTxn() Transaction {
	return &mockTxn{
		opts:  make(map[int]interface{}),
		valid: true,
	}
}

// mockStorage is used to start a must commit-failed txn.
type mockStorage struct {
}

func (s *mockStorage) Begin() (Transaction, error) {
	return newMockTxn(), nil
}

func (s *mockStorage) BeginWithOption(option tikv.StartTSOption) (Transaction, error) {
	return newMockTxn(), nil
}

func (*mockTxn) IsPessimistic() bool {
	return false
}

func (s *mockStorage) GetSnapshot(ver Version) Snapshot {
	return &mockSnapshot{
		store: newMockMap(),
	}
}

func (s *mockStorage) Close() error {
	return nil
}

func (s *mockStorage) UUID() string {
	return ""
}

// CurrentVersion returns current max committed version.
func (s *mockStorage) CurrentVersion(txnScope string) (Version, error) {
	return NewVersion(1), nil
}

func (s *mockStorage) GetClient() Client {
	return nil
}

func (s *mockStorage) GetMPPClient() MPPClient {
	return nil
}

func (s *mockStorage) GetOracle() oracle.Oracle {
	return nil
}

func (s *mockStorage) SupportDeleteRange() (supported bool) {
	return false
}

func (s *mockStorage) Name() string {
	return "KVMockStorage"
}

func (s *mockStorage) Describe() string {
	return "KVMockStorage is a mock Store implementation, only for unittests in KV package"
}

func (s *mockStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, nil
}

func (s *mockStorage) GetMemCache() MemManager {
	return nil
}

func (s *mockStorage) GetMinSafeTS(txnScope string) uint64 {
	return 0
}

// newMockStorage creates a new mockStorage.
func newMockStorage() Storage {
	return &mockStorage{}
}

type mockSnapshot struct {
	store Retriever
}

func (s *mockSnapshot) Get(ctx context.Context, k Key) ([]byte, error) {
	return s.store.Get(ctx, k)
}

func (s *mockSnapshot) SetPriority(priority int) {

}

func (s *mockSnapshot) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	m := make(map[string][]byte, len(keys))
	for _, k := range keys {
		v, err := s.store.Get(ctx, k)
		if IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		m[string(k)] = v
	}
	return m, nil
}

func (s *mockSnapshot) Iter(k Key, upperBound Key) (Iterator, error) {
	return s.store.Iter(k, upperBound)
}

func (s *mockSnapshot) IterReverse(k Key) (Iterator, error) {
	return s.store.IterReverse(k)
}

func (s *mockSnapshot) SetOption(opt int, val interface{}) {}
func (s *mockSnapshot) DelOption(opt int)                  {}
