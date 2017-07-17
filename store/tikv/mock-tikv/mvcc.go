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

package mocktikv

import (
	"bytes"
	"sort"
	"sync"

	"github.com/juju/errors"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

type mvccValueType int

const (
	typePut mvccValueType = iota
	typeDelete
	typeRollback
)

type mvccValue struct {
	valueType mvccValueType
	startTS   uint64
	commitTS  uint64
	value     []byte
}

type mvccLock struct {
	startTS uint64
	primary []byte
	value   []byte
	op      kvrpcpb.Op
	ttl     uint64
}

type mvccEntry struct {
	key    MvccKey
	values []mvccValue
	lock   *mvccLock
}

func newEntry(key MvccKey) *mvccEntry {
	return &mvccEntry{
		key: key,
	}
}

func (e *mvccEntry) Clone() *mvccEntry {
	var entry mvccEntry
	entry.key = append([]byte(nil), e.key...)
	for _, v := range e.values {
		entry.values = append(entry.values, mvccValue{
			valueType: v.valueType,
			startTS:   v.startTS,
			commitTS:  v.commitTS,
			value:     append([]byte(nil), v.value...),
		})
	}
	if e.lock != nil {
		entry.lock = &mvccLock{
			startTS: e.lock.startTS,
			primary: append([]byte(nil), e.lock.primary...),
			value:   append([]byte(nil), e.lock.value...),
			op:      e.lock.op,
			ttl:     e.lock.ttl,
		}
	}
	return &entry
}

func (e *mvccEntry) Less(than llrb.Item) bool {
	return bytes.Compare(e.key, than.(*mvccEntry).key) < 0
}

func (e *mvccEntry) lockErr() error {
	return &ErrLocked{
		Key:     e.key,
		Primary: e.lock.primary,
		StartTS: e.lock.startTS,
		TTL:     e.lock.ttl,
	}
}

func (e *mvccEntry) Get(ts uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	if isoLevel == kvrpcpb.IsolationLevel_SI {
		if e.lock != nil && e.lock.startTS <= ts {
			return nil, e.lockErr()
		}
	}
	for _, v := range e.values {
		if v.commitTS <= ts && v.valueType != typeRollback {
			return v.value, nil
		}
	}
	return nil, nil
}

func (e *mvccEntry) Prewrite(mutation *kvrpcpb.Mutation, startTS uint64, primary []byte, ttl uint64) error {
	if len(e.values) > 0 {
		if e.values[0].commitTS >= startTS {
			return ErrRetryable("write conflict")
		}
	}
	if e.lock != nil {
		if e.lock.startTS != startTS {
			return e.lockErr()
		}
		return nil
	}
	e.lock = &mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.GetOp(),
		ttl:     ttl,
	}
	return nil
}

func (e *mvccEntry) getTxnCommitInfo(startTS uint64) *mvccValue {
	for _, v := range e.values {
		if v.startTS == startTS {
			return &v
		}
	}
	return nil
}

func (e *mvccEntry) Commit(startTS, commitTS uint64) error {
	if e.lock == nil || e.lock.startTS != startTS {
		if c := e.getTxnCommitInfo(startTS); c != nil && c.valueType != typeRollback {
			return nil
		}
		return ErrRetryable("txn not found")
	}
	if e.lock.op != kvrpcpb.Op_Lock {
		var valueType mvccValueType
		if e.lock.op == kvrpcpb.Op_Put {
			valueType = typePut
		} else {
			valueType = typeDelete
		}
		e.addValue(mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     e.lock.value,
		})
	}
	e.lock = nil
	return nil
}

func (e *mvccEntry) Rollback(startTS uint64) error {
	// If current transaction's lock exist.
	if e.lock != nil && e.lock.startTS == startTS {
		e.lock = nil
		e.addValue(mvccValue{
			valueType: typeRollback,
			startTS:   startTS,
			commitTS:  startTS,
		})
		return nil
	}

	// If current transaction's lock not exist.
	// If commit info of current transaction exist.
	if c := e.getTxnCommitInfo(startTS); c != nil {
		// If current transaction is already committed.
		if c.valueType != typeRollback {
			return ErrAlreadyCommitted(c.commitTS)
		}
		// If current transaction is already rollback.
		return nil
	}
	// If current transaction is not prewritted before.
	e.addValue(mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	})
	return nil
}

func (e *mvccEntry) addValue(v mvccValue) {
	i := sort.Search(len(e.values), func(i int) bool { return e.values[i].commitTS <= v.commitTS })
	if i >= len(e.values) {
		e.values = append(e.values, v)
	} else {
		e.values = append(e.values[:i+1], e.values[i:]...)
		e.values[i] = v
	}
}

type rawEntry struct {
	key   []byte
	value []byte
}

func newRawEntry(key []byte) *rawEntry {
	return &rawEntry{
		key: key,
	}
}

func (e *rawEntry) Less(than llrb.Item) bool {
	return bytes.Compare(e.key, than.(*rawEntry).key) < 0
}

// MvccStore is an in-memory, multi-versioned, transaction-supported kv storage.
type MvccStore struct {
	sync.RWMutex
	tree  *llrb.LLRB
	rawkv *llrb.LLRB
}

// NewMvccStore creates a MvccStore.
func NewMvccStore() *MvccStore {
	return &MvccStore{
		tree:  llrb.New(),
		rawkv: llrb.New(),
	}
}

// Get reads a key by ts.
func (s *MvccStore) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()

	return s.get(NewMvccKey(key), startTS, isoLevel)
}

func (s *MvccStore) get(key MvccKey, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	entry := s.tree.Get(newEntry(key))
	if entry == nil {
		return nil, nil
	}
	return entry.(*mvccEntry).Get(startTS, isoLevel)
}

// A Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair struct {
	Key   []byte
	Value []byte
	Err   error
}

// BatchGet gets values with keys and ts.
func (s *MvccStore) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	s.RLock()
	defer s.RUnlock()

	var pairs []Pair
	for _, k := range ks {
		val, err := s.get(NewMvccKey(k), startTS, isoLevel)
		if val == nil && err == nil {
			continue
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: val,
			Err:   err,
		})
	}
	return pairs
}

func regionContains(startKey []byte, endKey []byte, key []byte) bool {
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}

// Scan reads up to a limited number of Pairs that greater than or equal to startKey and less than endKey.
func (s *MvccStore) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	s.RLock()
	defer s.RUnlock()

	startKey = NewMvccKey(startKey)
	endKey = NewMvccKey(endKey)

	var pairs []Pair
	iterator := func(item llrb.Item) bool {
		if len(pairs) >= limit {
			return false
		}
		k := item.(*mvccEntry).key
		if !regionContains(startKey, endKey, k) {
			return false
		}
		val, err := s.get(k, startTS, isoLevel)
		if val != nil || err != nil {
			pairs = append(pairs, Pair{
				Key:   k.Raw(),
				Value: val,
				Err:   err,
			})
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startKey), iterator)
	return pairs
}

// ReverseScan reads up to a limited number of Pairs that greater than or equal to startKey and less than endKey
// in descending order.
func (s *MvccStore) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	s.RLock()
	defer s.RUnlock()

	startKey = NewMvccKey(startKey)
	endKey = NewMvccKey(endKey)

	var pairs []Pair
	iterator := func(item llrb.Item) bool {
		if len(pairs) >= limit {
			return false
		}
		k := item.(*mvccEntry).key
		if bytes.Equal(k, endKey) {
			return true
		}
		if bytes.Compare(k, startKey) < 0 {
			return false
		}
		val, err := s.get(k, startTS, isoLevel)
		if val != nil || err != nil {
			pairs = append(pairs, Pair{
				Key:   k.Raw(),
				Value: val,
				Err:   err,
			})
		}
		return true
	}
	s.tree.DescendLessOrEqual(newEntry(endKey), iterator)
	return pairs
}

func (s *MvccStore) getOrNewEntry(key []byte) *mvccEntry {
	if item := s.tree.Get(newEntry(key)); item != nil {
		return item.(*mvccEntry).Clone()
	}
	return newEntry(key)
}

// submit writes entries into the rbtree.
func (s *MvccStore) submit(ents ...*mvccEntry) {
	for _, ent := range ents {
		s.tree.ReplaceOrInsert(ent)
	}
}

// Prewrite acquires a lock on a key. (1st phase of 2PC).
func (s *MvccStore) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	s.Lock()
	defer s.Unlock()

	var errs []error
	for _, m := range mutations {
		entry := s.getOrNewEntry(NewMvccKey(m.Key))
		err := entry.Prewrite(m, startTS, primary, ttl)
		s.submit(entry)
		errs = append(errs, err)
	}
	return errs
}

// Commit commits the lock on a key. (2nd phase of 2PC).
func (s *MvccStore) Commit(keys [][]byte, startTS, commitTS uint64) error {
	s.Lock()
	defer s.Unlock()

	var ents []*mvccEntry
	for _, k := range keys {
		entry := s.getOrNewEntry(NewMvccKey(k))
		err := entry.Commit(startTS, commitTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// Cleanup cleanups a lock, often used when resolving a expired lock.
func (s *MvccStore) Cleanup(key []byte, startTS uint64) error {
	s.Lock()
	defer s.Unlock()

	entry := s.getOrNewEntry(NewMvccKey(key))
	err := entry.Rollback(startTS)
	if err != nil {
		return err
	}
	s.submit(entry)
	return nil
}

// Rollback cleanups multiple locks, often used when rolling back a conflict txn.
func (s *MvccStore) Rollback(keys [][]byte, startTS uint64) error {
	s.Lock()
	defer s.Unlock()

	var ents []*mvccEntry
	for _, k := range keys {
		entry := s.getOrNewEntry(NewMvccKey(k))
		err := entry.Rollback(startTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// ScanLock scans all orphan locks in a Region.
func (s *MvccStore) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	s.RLock()
	defer s.RUnlock()

	var locks []*kvrpcpb.LockInfo
	iterator := func(item llrb.Item) bool {
		ent := item.(*mvccEntry)
		if !regionContains(startKey, endKey, ent.key) {
			return false
		}
		if ent.lock != nil && ent.lock.startTS <= maxTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: ent.lock.primary,
				LockVersion: ent.lock.startTS,
				Key:         ent.key.Raw(),
			})
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startKey), iterator)
	return locks, nil
}

// ResolveLock resolves all orphan locks belong to a transaction.
func (s *MvccStore) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	s.Lock()
	defer s.Unlock()

	var ents []*mvccEntry
	var err error
	iterator := func(item llrb.Item) bool {
		ent := item.(*mvccEntry)
		if !regionContains(startKey, endKey, ent.key) {
			return false
		}
		if ent.lock != nil && ent.lock.startTS == startTS {
			if commitTS > 0 {
				err = ent.Commit(startTS, commitTS)
			} else {
				err = ent.Rollback(startTS)
			}
			if err != nil {
				return false
			}
			ents = append(ents, ent)
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startKey), iterator)
	if err != nil {
		return errors.Trace(err)
	}
	s.submit(ents...)
	return nil
}

// RawGet queries value with the key.
func (s *MvccStore) RawGet(key []byte) []byte {
	s.RLock()
	defer s.RUnlock()

	entry := s.rawkv.Get(newRawEntry(key))
	if entry == nil {
		return nil
	}
	return entry.(*rawEntry).value
}

// RawPut stores a key-value pair.
func (s *MvccStore) RawPut(key, value []byte) {
	s.Lock()
	defer s.Unlock()
	if value == nil {
		value = []byte{}
	}
	entry := s.rawkv.Get(newRawEntry(key))
	if entry != nil {
		entry.(*rawEntry).value = value
	} else {
		s.rawkv.ReplaceOrInsert(&rawEntry{
			key:   key,
			value: value,
		})
	}
}

// RawDelete deletes a key-value pair.
func (s *MvccStore) RawDelete(key []byte) {
	s.Lock()
	defer s.Unlock()
	s.rawkv.Delete(newRawEntry(key))
}

// RawScan reads up to a limited number of rawkv Pairs.
func (s *MvccStore) RawScan(startKey, endKey []byte, limit int) []Pair {
	s.RLock()
	defer s.RUnlock()

	var pairs []Pair
	iterator := func(item llrb.Item) bool {
		if len(pairs) >= limit {
			return false
		}
		k := item.(*rawEntry).key
		if !regionContains(startKey, endKey, k) {
			return false
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: item.(*rawEntry).value,
		})
		return true
	}
	s.rawkv.AscendGreaterOrEqual(newRawEntry(startKey), iterator)
	return pairs
}

// MvccKey is the encoded key type.
// On TiKV, keys are encoded before they are saved into storage engine.
type MvccKey []byte

// NewMvccKey encodes a key into MvccKey.
func NewMvccKey(key []byte) MvccKey {
	if len(key) == 0 {
		return nil
	}
	return codec.EncodeBytes(nil, key)
}

// Raw decodes a MvccKey to original key.
func (key MvccKey) Raw() []byte {
	if len(key) == 0 {
		return nil
	}
	_, k, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return k
}
