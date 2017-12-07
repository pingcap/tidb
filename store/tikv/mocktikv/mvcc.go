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
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

type mvccValueType int

const btreeDegree = 32

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

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *mvccLock) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, l.startTS)
	mh.WriteSlice(&buf, l.primary)
	mh.WriteSlice(&buf, l.value)
	mh.WriteNumber(&buf, l.op)
	mh.WriteNumber(&buf, l.ttl)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (l *mvccLock) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	mh.ReadNumber(buf, &l.startTS)
	mh.ReadSlice(buf, &l.primary)
	mh.ReadSlice(buf, &l.value)
	mh.ReadNumber(buf, &l.op)
	mh.ReadNumber(buf, &l.ttl)
	return errors.Trace(mh.err)
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (v mvccValue) MarshalBinary() ([]byte, error) {
	var (
		mh  marshalHelper
		buf bytes.Buffer
	)
	mh.WriteNumber(&buf, int64(v.valueType))
	mh.WriteNumber(&buf, v.startTS)
	mh.WriteNumber(&buf, v.commitTS)
	mh.WriteSlice(&buf, v.value)
	return buf.Bytes(), errors.Trace(mh.err)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (v *mvccValue) UnmarshalBinary(data []byte) error {
	var mh marshalHelper
	buf := bytes.NewBuffer(data)
	var vt int64
	mh.ReadNumber(buf, &vt)
	v.valueType = mvccValueType(vt)
	mh.ReadNumber(buf, &v.startTS)
	mh.ReadNumber(buf, &v.commitTS)
	mh.ReadSlice(buf, &v.value)
	return errors.Trace(mh.err)
}

type marshalHelper struct {
	err error
}

func (mh *marshalHelper) WriteSlice(buf io.Writer, slice []byte) {
	if mh.err != nil {
		return
	}
	var tmp [binary.MaxVarintLen64]byte
	off := binary.PutUvarint(tmp[:], uint64(len(slice)))
	if err := writeFull(buf, tmp[:off]); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	if err := writeFull(buf, slice); err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) WriteNumber(buf io.Writer, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func writeFull(w io.Writer, slice []byte) error {
	written := 0
	for written < len(slice) {
		n, err := w.Write(slice[written:])
		if err != nil {
			return errors.Trace(err)
		}
		written += n
	}
	return nil
}

func (mh *marshalHelper) ReadNumber(r io.Reader, n interface{}) {
	if mh.err != nil {
		return
	}
	err := binary.Read(r, binary.LittleEndian, n)
	if err != nil {
		mh.err = errors.Trace(err)
	}
}

func (mh *marshalHelper) ReadSlice(r *bytes.Buffer, slice *[]byte) {
	if mh.err != nil {
		return
	}
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		mh.err = errors.Trace(err)
		return
	}
	const c10M = 10 * 1024 * 1024
	if sz > c10M {
		mh.err = errors.New("too large slice, maybe something wrong")
		return
	}
	data := make([]byte, sz)
	if _, err := io.ReadFull(r, data); err != nil {
		mh.err = errors.Trace(err)
		return
	}
	*slice = data
}

func newEntry(key MvccKey) *mvccEntry {
	return &mvccEntry{
		key: key,
	}
}

// lockErr returns ErrLocked.
// Note that parameter key is raw key, while key in ErrLocked is mvcc key.
func (l *mvccLock) lockErr(key []byte) error {
	return &ErrLocked{
		Key:     mvccEncode(key, lockVer),
		Primary: l.primary,
		StartTS: l.startTS,
		TTL:     l.ttl,
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

func (e *mvccEntry) Less(than btree.Item) bool {
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

func (e *mvccEntry) containsStartTS(startTS uint64) bool {
	if e.lock != nil && e.lock.startTS == startTS {
		return true
	}
	for _, item := range e.values {
		if item.startTS == startTS {
			return true
		}
		if item.commitTS < startTS {
			return false
		}
	}
	return false
}

func (e *mvccEntry) dumpMvccInfo() *kvrpcpb.MvccInfo {
	info := &kvrpcpb.MvccInfo{}
	if e.lock != nil {
		info.Lock = &kvrpcpb.LockInfo{
			Key:         e.key,
			PrimaryLock: e.lock.primary,
			LockVersion: e.lock.startTS,
			LockTtl:     e.lock.ttl,
		}
	}

	info.Writes = make([]*kvrpcpb.WriteInfo, len(e.values))
	info.Values = make([]*kvrpcpb.ValueInfo, len(e.values))

	for id, item := range e.values {
		var tp kvrpcpb.Op
		switch item.valueType {
		case typePut:
			tp = kvrpcpb.Op_Put
		case typeDelete:
			tp = kvrpcpb.Op_Del
		case typeRollback:
			tp = kvrpcpb.Op_Rollback
		}
		info.Writes[id] = &kvrpcpb.WriteInfo{
			StartTs:  item.startTS,
			Type:     tp,
			CommitTs: item.commitTS,
		}

		info.Values[id] = &kvrpcpb.ValueInfo{
			Value: item.value,
			Ts:    item.startTS,
		}
	}
	return info
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

func (e *rawEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.key, than.(*rawEntry).key) < 0
}

// MVCCStore is a mvcc key-value storage.
type MVCCStore interface {
	Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error)
	Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair
	Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error
	Commit(keys [][]byte, startTS, commitTS uint64) error
	Rollback(keys [][]byte, startTS uint64) error
	Cleanup(key []byte, startTS uint64) error
	ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error)
	ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error
}

// RawKV is a key-value storage. MVCCStore can be implemented upon it with timestamp encoded into key.
type RawKV interface {
	RawGet(key []byte) []byte
	RawScan(startKey, endKey []byte, limit int) []Pair
	RawPut(key, value []byte)
	RawDelete(key []byte)
}

// MVCCDebugger is for debugging.
type MVCCDebugger interface {
	MvccGetByStartTS(startKey, endKey []byte, starTS uint64) (*kvrpcpb.MvccInfo, []byte)
	MvccGetByKey(key []byte) *kvrpcpb.MvccInfo
}

// MvccStore is an in-memory, multi-versioned, transaction-supported kv storage.
type MvccStore struct {
	sync.RWMutex
	tree  *btree.BTree
	rawkv *btree.BTree
}

// NewMvccStore creates a MvccStore.
func NewMvccStore() *MvccStore {
	return &MvccStore{
		tree:  btree.New(btreeDegree),
		rawkv: btree.New(btreeDegree),
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

// Pair is a KV pair read from MvccStore or an error if any occurs.
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
	iterator := func(item btree.Item) bool {
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
	iterator := func(item btree.Item) bool {
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

	errs := make([]error, 0, len(mutations))
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
	iterator := func(item btree.Item) bool {
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
	iterator := func(item btree.Item) bool {
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
	iterator := func(item btree.Item) bool {
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

// MvccGetByStartTS gets mvcc info for the primary key with startTS
func (s *MvccStore) MvccGetByStartTS(startKey, endKey []byte, starTS uint64) (*kvrpcpb.MvccInfo, []byte) {
	s.RLock()
	defer s.RUnlock()

	var info *kvrpcpb.MvccInfo
	var key []byte
	iterator := func(item btree.Item) bool {
		k := item.(*mvccEntry)
		if !regionContains(startKey, endKey, k.key) {
			return false
		}
		if k.containsStartTS(starTS) {
			info = k.dumpMvccInfo()
			key = k.key
			return false
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startKey), iterator)
	return info, key
}

// MvccGetByKey gets mvcc info for the key
func (s *MvccStore) MvccGetByKey(key []byte) *kvrpcpb.MvccInfo {
	s.RLock()
	defer s.RUnlock()

	resp := s.tree.Get(newEntry(NewMvccKey(key)))
	if resp == nil {
		return nil
	}
	entry := resp.(*mvccEntry)
	return entry.dumpMvccInfo()
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
