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

import "sync"

type InjectionConfig struct {
	sync.RWMutex
	getError error // kv.Get() always return this error.
}

func (c *InjectionConfig) SetGetError(err error) {
	c.Lock()
	defer c.Unlock()

	c.getError = err
}

type InjectedStore struct {
	Storage
	cfg *InjectionConfig
}

func NewInjectedStore(store Storage, cfg *InjectionConfig) Storage {
	return &InjectedStore{
		Storage: store,
		cfg:     cfg,
	}
}

func (s *InjectedStore) Begin() (Transaction, error) {
	txn, err := s.Storage.Begin()
	return &InjectedTransaction{
		Transaction: txn,
		cfg:         s.cfg,
	}, err
}

type InjectedTransaction struct {
	Transaction
	cfg *InjectionConfig
}

func (t *InjectedTransaction) Get(k Key) ([]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Transaction.Get(k)
}

type InjectedSnapshot struct {
	Snapshot
	cfg *InjectionConfig
}

func (t *InjectedSnapshot) Get(k Key) ([]byte, error) {
	t.cfg.RLock()
	defer t.cfg.RUnlock()
	if t.cfg.getError != nil {
		return nil, t.cfg.getError
	}
	return t.Snapshot.Get(k)
}
