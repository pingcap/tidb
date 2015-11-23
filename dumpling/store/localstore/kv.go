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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/twinj/uuid"
)

var (
	_ kv.Storage = (*dbStore)(nil)
)

type dbStore struct {
	mu       sync.Mutex
	snapLock sync.RWMutex

	db engine.DB

	txns       map[uint64]*dbTxn
	keysLocked map[string]uint64
	uuid       string
	path       string
	compactor  *localstoreCompactor

	closed bool
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*dbStore
}

var (
	globalID int64

	providerMu            sync.Mutex
	globalVersionProvider kv.VersionProvider
	mc                    storeCache

	// ErrDBClosed is the error meaning db is closed and we can use it anymore.
	ErrDBClosed = errors.New("db is closed")
)

func init() {
	mc.cache = make(map[string]*dbStore)
	globalVersionProvider = &LocalVersionProvider{}
}

// Driver implements kv.Driver interface.
type Driver struct {
	// engine.Driver is the engine driver for different local db engine.
	engine.Driver
}

// IsLocalStore checks whether a storage is local or not.
func IsLocalStore(s kv.Storage) bool {
	_, ok := s.(*dbStore)
	return ok
}

func lockVersionProvider() {
	providerMu.Lock()
}

func unlockVersionProvider() {
	providerMu.Unlock()
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
		txns:       make(map[uint64]*dbTxn),
		keysLocked: make(map[string]uint64),
		uuid:       uuid.NewV4().String(),
		path:       schema,
		db:         db,
		compactor:  newLocalCompactor(localCompactDefaultPolicy, db),
		closed:     false,
	}
	mc.cache[schema] = s
	s.compactor.Start()
	return s, nil
}

func (s *dbStore) UUID() string {
	return s.uuid
}

func (s *dbStore) GetSnapshot(ver kv.Version) (kv.MvccSnapshot, error) {
	s.snapLock.RLock()
	defer s.snapLock.RUnlock()

	if s.closed {
		return nil, errors.Trace(ErrDBClosed)
	}

	currentVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if ver.Cmp(currentVer) > 0 {
		ver = currentVer
	}

	return &dbSnapshot{
		store:   s,
		db:      s.db,
		version: ver,
	}, nil
}

func (s *dbStore) CurrentVersion() (kv.Version, error) {
	return globalVersionProvider.CurrentVersion()
}

// Begin transaction
func (s *dbStore) Begin() (kv.Transaction, error) {
	lockVersionProvider()
	beginVer, err := globalVersionProvider.CurrentVersion()
	unlockVersionProvider()
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.Trace(ErrDBClosed)
	}
	txn := &dbTxn{
		tid:          beginVer.Ver,
		valid:        true,
		store:        s,
		version:      kv.MinVersion,
		snapshotVals: make(map[string]struct{}),
		opts:         make(map[kv.Option]interface{}),
	}
	log.Debugf("Begin txn:%d", txn.tid)
	txn.UnionStore = kv.NewUnionStore(&dbSnapshot{
		store:   s,
		db:      s.db,
		version: beginVer,
	}, options(txn.opts))
	return txn, nil
}

func (s *dbStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapLock.Lock()
	defer s.snapLock.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	mc.mu.Lock()
	defer mc.mu.Unlock()
	s.compactor.Stop()
	delete(mc.cache, s.path)
	return s.db.Close()
}

func (s *dbStore) writeBatch(b engine.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.Trace(ErrDBClosed)
	}

	err := s.db.Commit(b)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	return nil
}

func (s *dbStore) newBatch() engine.Batch {
	return s.db.NewBatch()
}

// Both lock and unlock are used for simulating scenario of percolator papers.
func (s *dbStore) tryConditionLockKey(tid uint64, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.Trace(ErrDBClosed)
	}

	if _, ok := s.keysLocked[key]; ok {
		return errors.Trace(kv.ErrLockConflict)
	}

	metaKey := codec.EncodeBytes(nil, []byte(key))
	currValue, err := s.db.Get(metaKey)
	if terror.ErrorEqual(err, kv.ErrNotExist) {
		s.keysLocked[key] = tid
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	// key not exist.
	if currValue == nil {
		s.keysLocked[key] = tid
		return nil
	}
	_, ver, err := codec.DecodeUint(currValue)
	if err != nil {
		return errors.Trace(err)
	}

	// If there's newer version of this key, returns error.
	if ver > tid {
		log.Warnf("txn:%d, tryLockKey condition not match for key %s, currValue:%q", tid, key, currValue)
		return errors.Trace(kv.ErrConditionNotMatch)
	}

	s.keysLocked[key] = tid
	return nil
}

func (s *dbStore) unLockKeys(keys ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.Trace(ErrDBClosed)
	}

	for _, key := range keys {
		if _, ok := s.keysLocked[key]; !ok {
			return errors.Trace(kv.ErrNotExist)
		}

		delete(s.keysLocked, key)
	}

	return nil
}
