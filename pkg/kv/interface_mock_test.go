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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

// mockTxn is a txn that returns a retryAble error when called Commit.
type mockTxn struct {
	opts  map[int]any
	valid bool
}

func (t *mockTxn) SetAssertion(_ []byte, _ ...FlagsOp) error {
	return nil
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

func (t *mockTxn) LockKeysFunc(_ context.Context, _ *LockCtx, fn func(), _ ...Key) error {
	if fn != nil {
		fn()
	}
	return nil
}

func (t *mockTxn) SetOption(opt int, val any) {
	t.opts[opt] = val
}

func (t *mockTxn) GetOption(opt int) any {
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

func (t *mockTxn) IterReverse(k Key, lowerBound Key) (Iterator, error) {
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

func (t *mockTxn) SetVars(vars any) {
}

func (t *mockTxn) GetVars() any {
	return nil
}

func (t *mockTxn) CacheTableInfo(id int64, info *model.TableInfo) {
}

func (t *mockTxn) GetTableInfo(id int64) *model.TableInfo {
	return nil
}

func (t *mockTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	// TODO nothing
}

func (t *mockTxn) GetMemDBCheckpoint() *tikv.MemDBCheckpoint {
	return nil
}

func (t *mockTxn) RollbackMemDBToCheckpoint(_ *tikv.MemDBCheckpoint) {
	// TODO nothing
}

func (t *mockTxn) ClearDiskFullOpt() {
	// TODO nothing
}

func (t *mockTxn) UpdateMemBufferFlags(_ []byte, _ ...FlagsOp) {

}

func (t *mockTxn) SetMemoryFootprintChangeHook(func(uint64)) {

}

func (t *mockTxn) Mem() uint64 {
	return 0
}

func (t *mockTxn) StartFairLocking() error                   { return nil }
func (t *mockTxn) RetryFairLocking(_ context.Context) error  { return nil }
func (t *mockTxn) CancelFairLocking(_ context.Context) error { return nil }
func (t *mockTxn) DoneFairLocking(_ context.Context) error   { return nil }
func (t *mockTxn) IsInFairLockingMode() bool                 { return false }
func (t *mockTxn) IsPipelined() bool                         { return false }
func (t *mockTxn) MayFlush() error                           { return nil }

// newMockTxn new a mockTxn.
func newMockTxn() Transaction {
	return &mockTxn{
		opts:  make(map[int]any),
		valid: true,
	}
}

// mockStorage is used to start a must commit-failed txn.
type mockStorage struct{}

func (s *mockStorage) GetCodec() tikv.Codec {
	return nil
}

func (s *mockStorage) Begin(opts ...tikv.TxnOption) (Transaction, error) {
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

func (s *mockStorage) ShowStatus(ctx context.Context, key string) (any, error) {
	return nil, nil
}

func (s *mockStorage) GetMemCache() MemManager {
	return nil
}

func (s *mockStorage) GetLockWaits() ([]*deadlockpb.WaitForEntry, error) {
	return nil, nil
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

func (s *mockSnapshot) IterReverse(k Key, lowerBound Key) (Iterator, error) {
	return s.store.IterReverse(k, lowerBound)
}

func (s *mockSnapshot) SetOption(opt int, val any) {}

func (s *mockSnapshot) GetLockWaits() []deadlockpb.WaitForEntry {
	return nil
}
