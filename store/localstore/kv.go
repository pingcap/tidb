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
	"runtime/debug"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/codec"
	"github.com/twinj/uuid"
)

var (
	_ kv.Storage = (*dbStore)(nil)
)

type op int

const (
	opGet = iota + 1
	opSeek
	opCommit
)

type command struct {
	op    op
	txn   *dbTxn
	ver   kv.Version
	args  interface{}
	reply interface{}
	done  chan error
}

type getArgs struct {
	key []byte
}

type getReply struct {
	value []byte
	err   error
}

type seekReply struct {
	key   []byte
	value []byte
	err   error
}

type commitReply struct {
	err error
}

type seekArgs struct {
	key []byte
}

type commitArgs struct {
}

//scheduler

// Get gets the associated value with key, returns (nil, ErrNotFound) if no value found.
func (s *dbStore) Get(key []byte) ([]byte, error) {
	c := &command{
		op:   opGet,
		args: &getArgs{key: key},
		done: make(chan error, 1),
	}

	s.commandCh <- c
	err := <-c.done
	if err != nil {
		return nil, err
	}

	reply := c.reply.(*getReply)
	return reply.value, nil
}

// Seek searches for the first key in the engine which is >= key in byte order, returns (nil, nil, ErrNotFound)
// if such key is not found.
func (s *dbStore) Seek(key []byte) ([]byte, []byte, error) {
	c := &command{
		op:   opSeek,
		args: &seekArgs{key: key},
		done: make(chan error, 1),
	}

	s.commandCh <- c
	err := <-c.done
	if err != nil {
		return nil, nil, err
	}

	reply := c.reply.(*seekReply)
	return reply.key, reply.value, nil
}

// Commit writes the changed data in Batch.
func (s *dbStore) CommitTxn(txn *dbTxn) error {
	c := &command{
		op:   opCommit,
		txn:  txn,
		args: &commitArgs{},
		done: make(chan error, 1),
	}

	s.commandCh <- c
	err := <-c.done
	return err
}

func (s *dbStore) scheduler() {
	closed := false
	for {
		var pending []*command
		select {
		case cmd := <-s.commandCh:
			if closed {
				cmd.done <- ErrDBClosed
				continue
			}
			pending = append(pending, cmd)
			cnt := len(s.commandCh)
			for i := 0; i < cnt; i++ {
				pending = append(pending, <-s.commandCh)
			}
		case <-s.closeCh:
			closed = true
			s.wg.Done()
		}
		s.handleCommand(pending)
	}
}

func (s *dbStore) tryLock(txn *dbTxn) (err error) {
	// check conflict
	for k := range txn.snapshotVals {
		if _, ok := s.keysLocked[k]; ok {
			return errors.Trace(kv.ErrLockConflict)
		}

		lastVer, ok := s.recentUpdates[k]
		if !ok {
			continue
		}
		// If there's newer version of this key, returns error.
		if lastVer.Cmp(kv.Version{txn.tid}) > 0 {
			return errors.Trace(kv.ErrConditionNotMatch)
		}
	}

	// record
	for k := range txn.snapshotVals {
		s.keysLocked[k] = txn.tid
	}

	return nil
}

func (s *dbStore) handleCommand(commands []*command) {
	wg := &sync.WaitGroup{}
	wg.Add(len(commands))
	var getCmds []*command
	var seekCmds []*command
	for _, cmd := range commands {
		txn := cmd.txn
		switch cmd.op {
		case opGet:
			getCmds = append(getCmds, cmd)
		case opSeek:
			seekCmds = append(seekCmds, cmd)
		case opCommit:
			wg.Done()
			// TODO: lock keys
			curVer, err := globalVersionProvider.CurrentVersion()
			if err != nil {
				log.Fatal(err)
			}
			err = s.tryLock(txn)
			if err != nil {
				cmd.done <- err
				continue
			}
			// Update commit version.
			txn.version = curVer
			b := s.db.NewBatch()
			txn.WalkBuffer(func(k kv.Key, value []byte) error {
				metaKey := codec.EncodeBytes(nil, k)
				// put dummy meta key, write current version
				b.Put(metaKey, codec.EncodeUint(nil, curVer.Ver))
				mvccKey := MvccEncodeVersionKey(kv.Key(k), curVer)
				if len(value) == 0 { // Deleted marker
					b.Put(mvccKey, nil)
				} else {
					b.Put(mvccKey, value)
				}
				return nil
			})
			err = s.writeBatch(b)
			s.unLockKeys(txn)
			cmd.done <- err
		default:
			panic("should never happend")
		}
	}

	// batch get command
	go func() {
		for _, cmd := range getCmds {
			reply := &getReply{}
			var err error
			reply.value, err = s.db.Get(cmd.args.(*getArgs).key)
			cmd.reply = reply
			cmd.done <- err
			wg.Done()
		}
	}()

	// batch seek command
	go s.doSeek(wg, seekCmds)
	wg.Wait()
}

func (s *dbStore) doSeek(wg *sync.WaitGroup, seekCmds []*command) {
	keys := make([][]byte, 0, len(seekCmds))

	for _, cmd := range seekCmds {
		keys = append(keys, cmd.args.(*seekArgs).key)
	}

	cnt := len(keys)
	step := 5
	jobs := cnt / step
	if cnt%step != 0 {
		jobs++
	}
	resCh := make([][]*engine.MSeekResult, jobs)
	var wgTemp sync.WaitGroup
	wgTemp.Add(jobs)
	for i := 0; i < jobs; i++ {
		go func(i int) {
			defer wgTemp.Done()
			if cnt >= i*step+step {
				resCh[i] = s.db.MultiSeek(keys[i*step : i*step+step])
			} else {
				resCh[i] = s.db.MultiSeek(keys[i*step:])
			}
		}(i)
	}
	wgTemp.Wait()

	results := make([]*engine.MSeekResult, 0, len(keys))
	for _, res := range resCh {
		results = append(results, res...)
	}

	for i, cmd := range seekCmds {
		reply := &seekReply{}
		var err error
		reply.key, reply.value, err = results[i].Key, results[i].Value, results[i].Err
		cmd.reply = reply
		cmd.done <- err
		wg.Done()
	}
}

func (s *dbStore) NewBatch() engine.Batch {
	return s.db.NewBatch()
}

//end of scheduler

type dbStore struct {
	db engine.DB

	txns          map[uint64]*dbTxn
	keysLocked    map[string]uint64
	recentUpdates map[string]kv.Version
	uuid          string
	path          string
	compactor     *localstoreCompactor
	wg            *sync.WaitGroup

	commandCh chan *command
	closeCh   chan struct{}

	mu     sync.Mutex
	closed bool
}

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*dbStore
}

var (
	globalID int64

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

// Open opens or creates a storage with specific format for a local engine Driver.
func (d Driver) Open(schema string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if store, ok := mc.cache[schema]; ok {
		// TODO: check the cache store has the same engine with this Driver.
		log.Info("[kv] cache store", schema)
		return store, nil
	}

	db, err := d.Driver.Open(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("[kv] New store", schema)
	s := &dbStore{
		txns:          make(map[uint64]*dbTxn),
		keysLocked:    make(map[string]uint64),
		uuid:          uuid.NewV4().String(),
		path:          schema,
		db:            db,
		compactor:     newLocalCompactor(localCompactDefaultPolicy, db),
		commandCh:     make(chan *command, 1000),
		closed:        false,
		closeCh:       make(chan struct{}),
		recentUpdates: make(map[string]kv.Version),
		wg:            &sync.WaitGroup{},
	}
	mc.cache[schema] = s
	s.compactor.Start()
	s.wg.Add(1)
	go s.scheduler()
	return s, nil
}

func (s *dbStore) UUID() string {
	return s.uuid
}

func (s *dbStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrDBClosed
	}
	s.mu.Unlock()

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

func (s *dbStore) CurrentVersion() (kv.Version, error) {
	return globalVersionProvider.CurrentVersion()
}

// Begin transaction
func (s *dbStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrDBClosed
	}
	s.mu.Unlock()

	beginVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// todo: send to chan
	txn := &dbTxn{
		tid:          beginVer.Ver,
		valid:        true,
		store:        s,
		version:      kv.MinVersion,
		snapshotVals: make(map[string]struct{}),
	}
	log.Debugf("[kv] Begin txn:%d", txn.tid)
	txn.UnionStore = kv.NewUnionStore(newSnapshot(s, beginVer))
	return txn, nil
}

func (s *dbStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrDBClosed
	}

	s.closed = true
	s.mu.Unlock()

	mc.mu.Lock()
	defer mc.mu.Unlock()
	s.compactor.Stop()
	s.closeCh <- struct{}{}
	s.wg.Wait()
	delete(mc.cache, s.path)
	return s.db.Close()
}

func (s *dbStore) writeBatch(b engine.Batch) error {
	if b.Len() == 0 {
		return nil
	}

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
func (s *dbStore) unLockKeys(txn *dbTxn) error {
	for k := range txn.snapshotVals {
		if tid, ok := s.keysLocked[k]; !ok || tid != txn.tid {
			debug.PrintStack()
			log.Fatalf("should never happend:%v, %v", tid, txn.tid)
		}

		delete(s.keysLocked, k)
		s.recentUpdates[k] = txn.version
	}

	return nil
}
