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

// mockTxn is a txn that returns a retryAble error when called Commit.
type mockTxn struct {
	opts map[Option]interface{}
}

// Always returns a retryable error.
func (t *mockTxn) Commit() error {
	return ErrRetryable
}

func (t *mockTxn) Rollback() error {
	return nil
}

func (t *mockTxn) String() string {
	return ""
}

func (t *mockTxn) LockKeys(keys ...Key) error {
	return nil
}

func (t *mockTxn) SetOption(opt Option, val interface{}) {
	t.opts[opt] = val
	return
}

func (t *mockTxn) DelOption(opt Option) {
	delete(t.opts, opt)
	return
}

func (t *mockTxn) GetOption(opt Option) interface{} {
	return t.opts[opt]
}

func (t *mockTxn) IsReadOnly() bool {
	return true
}

func (t *mockTxn) GetClient() Client {
	return nil
}

func (t *mockTxn) StartTS() uint64 {
	return uint64(0)
}
func (t *mockTxn) Get(k Key) ([]byte, error) {
	return nil, nil
}

func (t *mockTxn) Seek(k Key) (Iterator, error) {
	return nil, nil
}

func (t *mockTxn) SeekReverse(k Key) (Iterator, error) {
	return nil, nil
}

func (t *mockTxn) Set(k Key, v []byte) error {
	return nil
}
func (t *mockTxn) Delete(k Key) error {
	return nil
}

// mockStorage is used to start a must commit-failed txn.
type mockStorage struct {
}

func (s *mockStorage) Begin() (Transaction, error) {
	tx := &mockTxn{
		opts: make(map[Option]interface{}),
	}
	return tx, nil

}
func (s *mockStorage) GetSnapshot(ver Version) (Snapshot, error) {
	return nil, nil
}
func (s *mockStorage) Close() error {
	return nil
}

func (s *mockStorage) UUID() string {
	return ""
}

// CurrentVersion returns current max committed version.
func (s *mockStorage) CurrentVersion() (Version, error) {
	return Version{uint64(1)}, nil
}

// MockTxn is used for test cases that need more interfaces than Transaction.
type MockTxn interface {
	Transaction
	GetOption(opt Option) interface{}
}

// NewMockStorage creates a new mockStorage.
func NewMockStorage() Storage {
	return &mockStorage{}
}
