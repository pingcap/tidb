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
	"sync"

	"github.com/tikv/client-go/v2/tikv"
)

// InjectionConfig is used for fault injections for KV components.
type InjectionConfig struct {
	sync.RWMutex
	getError    error // kv.Get() always return this error.
	commitError error // Transaction.Commit() always return this error.
}

// SetGetError injects an error for all kv.Get() methods.
func (c *InjectionConfig) SetGetError(err error) {
	c.Lock()
	defer c.Unlock()

	c.getError = err
}

// SetCommitError injects an error for all Transaction.Commit() methods.
func (c *InjectionConfig) SetCommitError(err error) {
	c.Lock()
	defer c.Unlock()
	c.commitError = err
}

// InjectedStore wraps a Storage with injections.
type InjectedStore struct {
	Storage
	cfg *InjectionConfig
}

// NewInjectedStore creates a InjectedStore with config.
func NewInjectedStore(store Storage, cfg *InjectionConfig) Storage {
	return &InjectedStore{
		Storage: store,
		cfg:     cfg,
	}
}

// Begin creates an injected Transaction.
func (s *InjectedStore) Begin(opts ...tikv.TxnOption) (Transaction, error) {
	txn, err := s.Storage.Begin(opts...)
	return &InjectedTransaction{
		Transaction: txn,
		cfg:         s.cfg,
	}, err
}

// GetSnapshot creates an injected Snapshot.
func (s *InjectedStore) GetSnapshot(ver Version) Snapshot {
	snapshot := s.Storage.GetSnapshot(ver)
	return &InjectedSnapshot{
		Snapshot: snapshot,
		cfg:      s.cfg,
	}
}

// InjectedTransaction wraps a Transaction with injections.
type InjectedTransaction struct {
	Transaction
	cfg *InjectionConfig
}

// Get returns an error if cfg.getError is set.
func (t *InjectedTransaction) Get(ctx context.Context, k Key) ([]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Transaction.Get(ctx, k)
}

// BatchGet returns an error if cfg.getError is set.
func (t *InjectedTransaction) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Transaction.BatchGet(ctx, keys)
}

// Commit returns an error if cfg.commitError is set.
func (t *InjectedTransaction) Commit(ctx context.Context) error {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.commitError != nil {
		return t.cfg.commitError
	}
	return t.Transaction.Commit(ctx)
}

// InjectedSnapshot wraps a Snapshot with injections.
type InjectedSnapshot struct {
	Snapshot
	cfg *InjectionConfig
}

// Get returns an error if cfg.getError is set.
func (t *InjectedSnapshot) Get(ctx context.Context, k Key) ([]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Snapshot.Get(ctx, k)
}

// BatchGet returns an error if cfg.getError is set.
func (t *InjectedSnapshot) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Snapshot.BatchGet(ctx, keys)
}
