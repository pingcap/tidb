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
	"net/url"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/segmentmap"
	"github.com/twinj/uuid"
)

var (
	_ kv.Storage = (*dbStore)(nil)
)

const (
	lowerWaterMark = 10 // second
)

func (s *dbStore) prepareSeek(startTS uint64) error {
	for {
		var conflict bool
		s.mu.RLock()
		if s.closed {
			s.mu.RUnlock()
			return ErrDBClosed
		}
		if s.committingTS != 0 && s.committingTS < startTS {
			// We not sure if we can read the committing value,
			conflict = true
		} else {
			s.wg.Add(1)
		}
		s.mu.RUnlock()
		if conflict {
			// Wait for committing to be finished and try again.
			time.Sleep(time.Microsecond)
			continue
		}
		return nil
	}
}

// Seek searches for the first key in the engine which is >= key in byte order, returns (nil, nil, ErrNotFound)
// if such key is not found.
func (s *dbStore) Seek(key []byte, startTS uint64) ([]byte, []byte, error) {
	err := s.prepareSeek(startTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	key, val, err := s.db.Seek(key)
	s.wg.Done()
	return key, val, err
}

// SeekReverse searches for the first key in the engine which is less than key in byte order.
// Returns (nil, nil, ErrNotFound) if such key is not found.
func (s *dbStore) SeekReverse(key []byte, startTS uint64) ([]byte, []byte, error) {
	err := s.prepareSeek(startTS)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	key, val, err := s.db.SeekReverse(key)
	s.wg.Done()
	return key, val, err
}

// Commit writes the changed data in Batch.
func (s *dbStore) CommitTxn(txn *dbTxn) error {
	if len(txn.lockedKeys) == 0 {
		return nil
	}
	return s.doCommit(txn)
}

func (s *dbStore) cleanRecentUpdates(segmentIndex int64) {
	m, err := s.recentUpdates.GetSegment(segmentIndex)
	if err != nil {
		log.Error(err)
		return
	}

	now := time.Now().Unix()
	for k, v := range m {
		dis := now - version2Second(v.(kv.Version))
		if dis > lowerWaterMark {
			delete(m, k)
		}
	}
}

func (s *dbStore) tryLock(txn *dbTxn) (err error) {
	// check conflict
	for k := range txn.lockedKeys {
		if _, ok := s.keysLocked[k]; ok {
			return errors.Trace(kv.ErrLockConflict)
		}

		lastVer, ok := s.recentUpdates.Get([]byte(k))
		if !ok {
			continue
		}
		// If there's newer version of this key, returns error.
		if lastVer.(kv.Version).Cmp(kv.Version{Ver: txn.tid}) > 0 {
			return errors.Trace(kv.ErrConditionNotMatch)
		}
	}

	// record
	for k := range txn.lockedKeys {
		s.keysLocked[k] = txn.tid
	}

	return nil
}

func (s *dbStore) doCommit(txn *dbTxn) error {
	var commitVer kv.Version
	var err error
	for {
		// Atomically get commit version
		s.mu.Lock()
		closed := s.closed
		committing := s.committingTS != 0
		if !closed && !committing {
			commitVer, err = globalVersionProvider.CurrentVersion()
			if err != nil {
				s.mu.Unlock()
				return errors.Trace(err)
			}
			s.committingTS = commitVer.Ver
			s.wg.Add(1)
		}
		s.mu.Unlock()

		if closed {
			return ErrDBClosed
		}
		if committing {
			time.Sleep(time.Microsecond)
			continue
		}
		break
	}
	defer func() {
		s.mu.Lock()
		s.committingTS = 0
		s.wg.Done()
		s.mu.Unlock()
	}()
	// Here we are sure no concurrent committing happens.
	err = s.tryLock(txn)
	if err != nil {
		return errors.Trace(err)
	}
	b := s.db.NewBatch()
	txn.us.WalkBuffer(func(k kv.Key, value []byte) error {
		mvccKey := MvccEncodeVersionKey(kv.Key(k), commitVer)
		if len(value) == 0 { // Deleted marker
			b.Put(mvccKey, nil)
			s.compactor.OnDelete(k)
		} else {
			b.Put(mvccKey, value)
			s.compactor.OnSet(k)
		}
		return nil
	})
	err = s.writeBatch(b)
	if err != nil {
		return errors.Trace(err)
	}
	// Update commit version.
	txn.version = commitVer
	err = s.unLockKeys(txn)
	if err != nil {
		return errors.Trace(err)
	}

	// Clean recent updates.
	now := time.Now()
	if now.Sub(s.lastCleanTime) > time.Second {
		s.cleanRecentUpdates(s.cleanIdx)
		s.cleanIdx++
		if s.cleanIdx == s.recentUpdates.SegmentCount() {
			s.cleanIdx = 0
		}
		s.lastCleanTime = now
	}
	return nil
}

func (s *dbStore) NewBatch() engine.Batch {
	return s.db.NewBatch()
}

type dbStore struct {
	db engine.DB

	txns       map[uint64]*dbTxn
	keysLocked map[string]uint64

	recentUpdates *segmentmap.SegmentMap
	cleanIdx      int64
	lastCleanTime time.Time

	uuid      string
	path      string
	compactor *localstoreCompactor
	wg        sync.WaitGroup

	mu           sync.RWMutex
	closed       bool
	committingTS uint64

	pd localPD
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*dbStore
}

var (
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

// MockRemoteStore mocks remote store. It makes IsLocalStore return false.
var MockRemoteStore bool

// IsLocalStore checks whether a storage is local or not.
func IsLocalStore(s kv.Storage) bool {
	if MockRemoteStore {
		return false
	}
	_, ok := s.(*dbStore)
	return ok
}

// Open opens or creates a storage with specific format for a local engine Driver.
// The path should be a URL format which is described in tidb package.
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	engineSchema := filepath.Join(u.Host, u.Path)
	if store, ok := mc.cache[engineSchema]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		log.Info("[kv] cache store", engineSchema)
		return store, nil
	}

	db, err := d.Driver.Open(engineSchema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("[kv] New store", engineSchema)
	s := &dbStore{
		txns:       make(map[uint64]*dbTxn),
		keysLocked: make(map[string]uint64),
		uuid:       uuid.NewV4().String(),
		path:       engineSchema,
		db:         db,
		compactor:  newLocalCompactor(localCompactDefaultPolicy, db),
		closed:     false,
	}
	s.recentUpdates, err = segmentmap.NewSegmentMap(100)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionServers := buildLocalRegionServers(s)
	var infos []*regionInfo
	for _, rs := range regionServers {
		ri := &regionInfo{startKey: rs.startKey, endKey: rs.endKey, rs: rs}
		infos = append(infos, ri)
	}
	s.pd.SetRegionInfo(infos)
	mc.cache[engineSchema] = s
	s.compactor.Start()
	return s, nil
}

func (s *dbStore) UUID() string {
	return s.uuid
}

func (s *dbStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrDBClosed
	}
	s.mu.RUnlock()

	currentVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if ver.Cmp(currentVer) > 0 {
		ver = currentVer
	}

	return &dbSnapshot{
		store:   s,
		version: ver,
	}, nil
}

func (s *dbStore) GetClient() kv.Client {
	return &dbClient{store: s, regionInfo: s.pd.GetRegionInfo()}
}

func (s *dbStore) CurrentVersion() (kv.Version, error) {
	return globalVersionProvider.CurrentVersion()
}

// Begin transaction
func (s *dbStore) Begin() (kv.Transaction, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrDBClosed
	}
	s.mu.RUnlock()

	beginVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return newTxn(s, beginVer), nil
}

// BeginWithStartTS begins transaction with startTS.
func (s *dbStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	return s.Begin()
}

func (s *dbStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrDBClosed
	}
	s.closed = true
	s.mu.Unlock()
	s.compactor.Stop()
	s.wg.Wait()
	delete(mc.cache, s.path)
	return s.db.Close()
}

func (s *dbStore) writeBatch(b engine.Batch) error {
	if b.Len() == 0 {
		return nil
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

func (s *dbStore) unLockKeys(txn *dbTxn) error {
	for k := range txn.lockedKeys {
		if tid, ok := s.keysLocked[k]; !ok || tid != txn.tid {
			debug.PrintStack()
			return errors.Errorf("should never happened:%v, %v", tid, txn.tid)
		}

		delete(s.keysLocked, k)
		s.recentUpdates.Set([]byte(k), txn.version, true)
	}
	return nil
}
