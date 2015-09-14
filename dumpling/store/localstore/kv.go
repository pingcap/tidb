// Copyright 2015 PingCAP, Inc.
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

package localstore

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/twinj/uuid"
)

var (
	_ kv.Storage = (*dbStore)(nil)
)

type dbStore struct {
	mu sync.Mutex
	db engine.DB

	txns       map[int64]*dbTxn
	keysLocked map[string]int64
	uuid       string
	path       string
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*dbStore
}

var (
	globalID int64
	mc       storeCache
)

func init() {
	mc.cache = make(map[string]*dbStore)
}

// Driver implements kv.Driver interface.
type Driver struct {
	// engine.Driver is the engine driver for different local db engine.
	engine.Driver
}

// Open opens or creates a storage with specific format for a local engine Driver.
func (d Driver) Open(schema string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if store, ok := mc.cache[schema]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		log.Info("cache store", schema)
		return store, nil
	}

	db, err := d.Driver.Open(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("New store", schema)
	s := &dbStore{
		txns:       make(map[int64]*dbTxn),
		keysLocked: make(map[string]int64),
		uuid:       uuid.NewV4().String(),
		path:       schema,
		db:         db,
	}

	mc.cache[schema] = s

	return s, nil
}

func (s *dbStore) UUID() string {
	return s.uuid
}

// Begin transaction
func (s *dbStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	txn := &dbTxn{
		startTs:      time.Now(),
		tID:          atomic.AddInt64(&globalID, 1),
		valid:        true,
		store:        s,
		snapshotVals: make(map[string][]byte),
	}
	log.Debugf("Begin txn:%d", txn.tID)
	txn.UnionStore, err = kv.NewUnionStore(&dbSnapshot{snapshot})
	if err != nil {
		return nil, err
	}
	return txn, nil
}

func (s *dbStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.cache, s.path)
	return s.db.Close()
}

func (s *dbStore) writeBatch(b engine.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Commit(b)
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (s *dbStore) newBatch() engine.Batch {
	return s.db.NewBatch()
}

// Both lock and unlock are used for simulating scenario of percolator papers

func (s *dbStore) tryConditionLockKey(tID int64, key string, snapshotVal []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.keysLocked[key]; ok {
		return errors.Trace(kv.ErrLockConflict)
	}

	currValue, err := s.db.Get([]byte(key))
	if err != nil {
		return errors.Trace(err)
	}

	if !bytes.Equal(currValue, snapshotVal) {
		log.Warnf("txn:%d, tryLockKey condition not match for key %s, currValue:%q, snapshotVal:%q", tID, key, currValue, snapshotVal)
		return errors.Trace(kv.ErrConditionNotMatch)
	}

	s.keysLocked[key] = tID

	return nil
}

func (s *dbStore) unLockKeys(keys ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		if _, ok := s.keysLocked[key]; !ok {
			return kv.ErrNotExist
		}

		delete(s.keysLocked, key)
	}

	return nil
}
