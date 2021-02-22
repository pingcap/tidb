// Copyright 2017 PingCAP, Inc.
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
	"context"
	"encoding/hex"
	"math"
	"sync"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"github.com/pingcap/goleveldb/leveldb/storage"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/deadlock"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// MVCCLevelDB implements the MVCCStore interface.
type MVCCLevelDB struct {
	// Key layout:
	// ...
	// Key_lock        -- (0)
	// Key_verMax      -- (1)
	// ...
	// Key_ver+1       -- (2)
	// Key_ver         -- (3)
	// Key_ver-1       -- (4)
	// ...
	// Key_0           -- (5)
	// NextKey_lock    -- (6)
	// NextKey_verMax  -- (7)
	// ...
	// NextKey_ver+1   -- (8)
	// NextKey_ver     -- (9)
	// NextKey_ver-1   -- (10)
	// ...
	// NextKey_0       -- (11)
	// ...
	// EOF

	// db represents leveldb
	db *leveldb.DB
	// mu used for lock
	// leveldb can not guarantee multiple operations to be atomic, for example, read
	// then write, another write may happen during it, so this lock is necessory.
	mu               sync.RWMutex
	deadlockDetector *deadlock.Detector
}

const lockVer uint64 = math.MaxUint64

// ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
var ErrInvalidEncodedKey = errors.New("invalid encoded key")

// mvccEncode returns the encoded key.
func mvccEncode(key []byte, ver uint64) []byte {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver)
	return ret
}

// mvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func mvccDecode(encodedKey []byte) ([]byte, uint64, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes(encodedKey, nil)
	if err != nil {
		// should never happen
		return nil, 0, errors.Trace(err)
	}
	// if it's meta key
	if len(remainBytes) == 0 {
		return key, 0, nil
	}
	var ver uint64
	remainBytes, ver, err = codec.DecodeUintDesc(remainBytes)
	if err != nil {
		// should never happen
		return nil, 0, errors.Trace(err)
	}
	if len(remainBytes) != 0 {
		return nil, 0, ErrInvalidEncodedKey
	}
	return key, ver, nil
}

// MustNewMVCCStore is used for testing, use NewMVCCLevelDB instead.
func MustNewMVCCStore() MVCCStore {
	mvccStore, err := NewMVCCLevelDB("")
	if err != nil {
		panic(err)
	}
	return mvccStore
}

// NewMVCCLevelDB returns a new MVCCLevelDB object.
func NewMVCCLevelDB(path string) (*MVCCLevelDB, error) {
	var (
		d   *leveldb.DB
		err error
	)
	if path == "" {
		d, err = leveldb.Open(storage.NewMemStorage(), nil)
	} else {
		d, err = leveldb.OpenFile(path, &opt.Options{BlockCacheCapacity: 600 * 1024 * 1024})
	}

	return &MVCCLevelDB{db: d, deadlockDetector: deadlock.NewDetector()}, errors.Trace(err)
}

// Iterator wraps iterator.Iterator to provide Valid() method.
type Iterator struct {
	iterator.Iterator
	valid bool
}

// Next moves the iterator to the next key/value pair.
func (iter *Iterator) Next() {
	iter.valid = iter.Iterator.Next()
}

// Valid returns whether the iterator is exhausted.
func (iter *Iterator) Valid() bool {
	return iter.valid
}

func newIterator(db *leveldb.DB, slice *util.Range) *Iterator {
	iter := &Iterator{db.NewIterator(slice, nil), true}
	iter.Next()
	return iter
}

func newScanIterator(db *leveldb.DB, startKey, endKey []byte) (*Iterator, []byte, error) {
	var start, end []byte
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		end = mvccEncode(endKey, lockVer)
	}
	iter := newIterator(db, &util.Range{
		Start: start,
		Limit: end,
	})
	// newScanIterator must handle startKey is nil, in this case, the real startKey
	// should be change the frist key of the store.
	if len(startKey) == 0 && iter.Valid() {
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		startKey = key
	}
	return iter, startKey, nil
}

// iterDecoder tries to decode an Iterator value.
// If current iterator value can be decoded by this decoder, store the value and call iter.Next(),
// Otherwise current iterator is not touched and returns false.
type iterDecoder interface {
	Decode(iter *Iterator) (bool, error)
}

type lockDecoder struct {
	lock      mvccLock
	expectKey []byte
}

// Decode decodes the lock value if current iterator is at expectKey::lock.
func (dec *lockDecoder) Decode(iter *Iterator) (bool, error) {
	if iter.Error() != nil || !iter.Valid() {
		return false, iter.Error()
	}

	iterKey := iter.Key()
	key, ver, err := mvccDecode(iterKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !bytes.Equal(key, dec.expectKey) {
		return false, nil
	}
	if ver != lockVer {
		return false, nil
	}

	var lock mvccLock
	err = lock.UnmarshalBinary(iter.Value())
	if err != nil {
		return false, errors.Trace(err)
	}
	dec.lock = lock
	iter.Next()
	return true, nil
}

type valueDecoder struct {
	value     mvccValue
	expectKey []byte
}

// Decode decodes a mvcc value if iter key is expectKey.
func (dec *valueDecoder) Decode(iter *Iterator) (bool, error) {
	if iter.Error() != nil || !iter.Valid() {
		return false, iter.Error()
	}

	key, ver, err := mvccDecode(iter.Key())
	if err != nil {
		return false, errors.Trace(err)
	}
	if !bytes.Equal(key, dec.expectKey) {
		return false, nil
	}
	if ver == lockVer {
		return false, nil
	}

	var value mvccValue
	err = value.UnmarshalBinary(iter.Value())
	if err != nil {
		return false, errors.Trace(err)
	}
	dec.value = value
	iter.Next()
	return true, nil
}

type skipDecoder struct {
	currKey []byte
}

// Decode skips the iterator as long as its key is currKey, the new key would be stored.
func (dec *skipDecoder) Decode(iter *Iterator) (bool, error) {
	if iter.Error() != nil {
		return false, iter.Error()
	}
	for iter.Valid() {
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			return false, errors.Trace(err)
		}
		if !bytes.Equal(key, dec.currKey) {
			dec.currKey = key
			return true, nil
		}
		iter.Next()
	}
	return false, nil
}

type mvccEntryDecoder struct {
	expectKey []byte
	// mvccEntry represents values and lock is valid.
	mvccEntry
}

// Decode decodes a mvcc entry.
func (dec *mvccEntryDecoder) Decode(iter *Iterator) (bool, error) {
	ldec := lockDecoder{expectKey: dec.expectKey}
	ok, err := ldec.Decode(iter)
	if err != nil {
		return ok, errors.Trace(err)
	}
	if ok {
		dec.mvccEntry.lock = &ldec.lock
	}
	for iter.Valid() {
		vdec := valueDecoder{expectKey: dec.expectKey}
		ok, err = vdec.Decode(iter)
		if err != nil {
			return ok, errors.Trace(err)
		}
		if !ok {
			break
		}
		dec.mvccEntry.values = append(dec.mvccEntry.values, vdec.value)
	}
	succ := dec.mvccEntry.lock != nil || len(dec.mvccEntry.values) > 0
	return succ, nil
}

// Get implements the MVCCStore interface.
// key cannot be nil or []byte{}
func (mvcc *MVCCLevelDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	return mvcc.getValue(key, startTS, isoLevel)
}

func (mvcc *MVCCLevelDB) getValue(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	return getValue(iter, key, startTS, isoLevel)
}

func getValue(iter *Iterator, key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	dec1 := lockDecoder{expectKey: key}
	ok, err := dec1.Decode(iter)
	if ok && isoLevel == kvrpcpb.IsolationLevel_SI {
		startTS, err = dec1.lock.check(startTS, key)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	dec2 := valueDecoder{expectKey: key}
	for iter.Valid() {
		ok, err := dec2.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			break
		}

		value := &dec2.value
		if value.valueType == typeRollback {
			continue
		}
		// Read the first committed value that can be seen at startTS.
		if value.commitTS <= startTS {
			if value.valueType == typeDelete {
				return nil, nil
			}
			return value.value, nil
		}
	}
	return nil, nil
}

// BatchGet implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	pairs := make([]Pair, 0, len(ks))
	for _, k := range ks {
		v, err := mvcc.getValue(k, startTS, isoLevel)
		if v == nil && err == nil {
			continue
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: v,
			Err:   errors.Trace(err),
		})
	}
	return pairs
}

// Scan implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		logutil.Logger(context.Background()).Error("scan new iterator fail", zap.Error(err))
		return nil
	}

	ok := true
	var pairs []Pair
	for len(pairs) < limit && ok {
		value, err := getValue(iter, currKey, startTS, isoLevel)
		if err != nil {
			pairs = append(pairs, Pair{
				Key: currKey,
				Err: errors.Trace(err),
			})
		}
		if value != nil {
			pairs = append(pairs, Pair{
				Key:   currKey,
				Value: value,
			})
		}

		skip := skipDecoder{currKey}
		ok, err = skip.Decode(iter)
		if err != nil {
			logutil.Logger(context.Background()).Error("seek to next key error", zap.Error(err))
			break
		}
		currKey = skip.currKey
	}
	return pairs
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (mvcc *MVCCLevelDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	var mvccEnd []byte
	if len(endKey) != 0 {
		mvccEnd = mvccEncode(endKey, lockVer)
	}
	iter := mvcc.db.NewIterator(&util.Range{
		Limit: mvccEnd,
	}, nil)
	defer iter.Release()

	succ := iter.Last()
	currKey, _, err := mvccDecode(iter.Key())
	// TODO: return error.
	terror.Log(errors.Trace(err))
	helper := reverseScanHelper{
		startTS:  startTS,
		isoLevel: isoLevel,
		currKey:  currKey,
	}

	for succ && len(helper.pairs) < limit {
		key, ver, err := mvccDecode(iter.Key())
		if err != nil {
			break
		}
		if bytes.Compare(key, startKey) < 0 {
			break
		}

		if !bytes.Equal(key, helper.currKey) {
			helper.finishEntry()
			helper.currKey = key
		}
		if ver == lockVer {
			var lock mvccLock
			err = lock.UnmarshalBinary(iter.Value())
			helper.entry.lock = &lock
		} else {
			var value mvccValue
			err = value.UnmarshalBinary(iter.Value())
			helper.entry.values = append(helper.entry.values, value)
		}
		if err != nil {
			logutil.Logger(context.Background()).Error("unmarshal fail", zap.Error(err))
			break
		}
		succ = iter.Prev()
	}
	if len(helper.pairs) < limit {
		helper.finishEntry()
	}
	return helper.pairs
}

type reverseScanHelper struct {
	startTS  uint64
	isoLevel kvrpcpb.IsolationLevel
	currKey  []byte
	entry    mvccEntry
	pairs    []Pair
}

func (helper *reverseScanHelper) finishEntry() {
	reverse(helper.entry.values)
	helper.entry.key = NewMvccKey(helper.currKey)
	val, err := helper.entry.Get(helper.startTS, helper.isoLevel)
	if len(val) != 0 || err != nil {
		helper.pairs = append(helper.pairs, Pair{
			Key:   helper.currKey,
			Value: val,
			Err:   err,
		})
	}
	helper.entry = mvccEntry{}
}

func reverse(values []mvccValue) {
	i, j := 0, len(values)-1
	for i < j {
		values[i], values[j] = values[j], values[i]
		i++
		j--
	}
}

// PessimisticLock writes the pessimistic lock.
func (mvcc *MVCCLevelDB) PessimisticLock(mutations []*kvrpcpb.Mutation, primary []byte, startTS, forUpdateTS uint64, ttl uint64) []error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(mutations))
	for _, m := range mutations {
		err := mvcc.pessimisticLockMutation(batch, m, startTS, forUpdateTS, primary, ttl)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		return errs
	}
	if err := mvcc.db.Write(batch, nil); err != nil {
		return []error{err}
	}

	return errs
}

func (mvcc *MVCCLevelDB) pessimisticLockMutation(batch *leveldb.Batch, mutation *kvrpcpb.Mutation, startTS, forUpdateTS uint64, primary []byte, ttl uint64) error {
	startKey := mvccEncode(mutation.Key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: mutation.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		if dec.lock.startTS != startTS {
			errDeadlock := mvcc.deadlockDetector.Detect(startTS, dec.lock.startTS, farm.Fingerprint64(mutation.Key))
			if errDeadlock != nil {
				return &ErrDeadlock{
					LockKey:        mutation.Key,
					LockTS:         dec.lock.startTS,
					DealockKeyHash: errDeadlock.KeyHash,
				}
			}
			return dec.lock.lockErr(mutation.Key)
		}
		return nil
	}
	if err = checkConflictValue(iter, mutation, forUpdateTS); err != nil {
		return err
	}

	lock := mvccLock{
		startTS:     startTS,
		primary:     primary,
		op:          kvrpcpb.Op_PessimisticLock,
		ttl:         ttl,
		forUpdateTS: forUpdateTS,
	}
	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}

	batch.Put(writeKey, writeValue)
	return nil
}

// PessimisticRollback implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) PessimisticRollback(keys [][]byte, startTS, forUpdateTS uint64) []error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(keys))
	for _, key := range keys {
		err := pessimisticRollbackKey(mvcc.db, batch, key, startTS, forUpdateTS)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		return errs
	}
	if err := mvcc.db.Write(batch, nil); err != nil {
		return []error{err}
	}
	return errs
}

func pessimisticRollbackKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS, forUpdateTS uint64) error {
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		lock := dec.lock
		if lock.op == kvrpcpb.Op_PessimisticLock && lock.startTS == startTS && lock.forUpdateTS <= forUpdateTS {
			batch.Delete(startKey)
		}
	}
	return nil
}

// Prewrite implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Prewrite(req *kvrpcpb.PrewriteRequest) []error {
	mutations := req.Mutations
	primary := req.PrimaryLock
	startTS := req.StartVersion
	forUpdateTS := req.GetForUpdateTs()
	ttl := req.LockTtl
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(mutations))
	txnSize := req.TxnSize
	for i, m := range mutations {
		// If the operation is Insert, check if key is exists at first.
		var err error
		// no need to check insert values for pessimistic transaction.
		op := m.GetOp()
		if (op == kvrpcpb.Op_Insert || op == kvrpcpb.Op_CheckNotExists) && forUpdateTS == 0 {
			v, err := mvcc.getValue(m.Key, startTS, kvrpcpb.IsolationLevel_SI)
			if err != nil {
				errs = append(errs, err)
				anyError = true
				continue
			}
			if v != nil {
				err = &ErrKeyAlreadyExist{
					Key: m.Key,
				}
				errs = append(errs, err)
				anyError = true
				continue
			}
		}
		if op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		err = prewriteMutation(mvcc.db, batch, m, startTS, primary, ttl, txnSize, isPessimisticLock)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		return errs
	}
	if err := mvcc.db.Write(batch, nil); err != nil {
		return []error{err}
	}

	return errs
}

func checkConflictValue(iter *Iterator, m *kvrpcpb.Mutation, startTS uint64) error {
	dec := valueDecoder{
		expectKey: m.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		return nil
	}
	// Note that it's a write conflict here, even if the value is a rollback one.
	if dec.value.commitTS >= startTS {
		return &ErrConflict{
			StartTS:          startTS,
			ConflictTS:       dec.value.startTS,
			ConflictCommitTS: dec.value.commitTS,
			Key:              m.Key,
		}
	}
	if m.Op == kvrpcpb.Op_PessimisticLock && m.Assertion == kvrpcpb.Assertion_NotExist {
		// Skip rollback keys.
		for dec.value.valueType == typeRollback {
			ok, err = dec.Decode(iter)
			if err != nil {
				return errors.Trace(err)
			}
			if !ok {
				return nil
			}
		}
		if dec.value.valueType == typeDelete {
			return nil
		}
		return &ErrKeyAlreadyExist{
			Key: m.Key,
		}
	}
	return nil
}

func prewriteMutation(db *leveldb.DB, batch *leveldb.Batch, mutation *kvrpcpb.Mutation, startTS uint64, primary []byte, ttl uint64, txnSize uint64, isPessimisticLock bool) error {
	startKey := mvccEncode(mutation.Key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: mutation.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		if dec.lock.startTS != startTS {
			if isPessimisticLock {
				// NOTE: A special handling.
				// When pessimistic txn prewrite meets lock, set the TTL = 0 means
				// telling TiDB to rollback the transaction **unconditionly**.
				dec.lock.ttl = 0
			}
			return dec.lock.lockErr(mutation.Key)
		}
		if dec.lock.op != kvrpcpb.Op_PessimisticLock {
			return nil
		}
		// Overwrite the pessimistic lock.
		if ttl < dec.lock.ttl {
			// Maybe ttlManager has already set the lock TTL, don't decrease it.
			ttl = dec.lock.ttl
		}
	} else {
		if isPessimisticLock {
			return ErrAbort("pessimistic lock not found")
		}
		err = checkConflictValue(iter, mutation, startTS)
		if err != nil {
			return err
		}
	}

	op := mutation.GetOp()
	if op == kvrpcpb.Op_Insert {
		op = kvrpcpb.Op_Put
	}
	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      op,
		ttl:     ttl,
		txnSize: txnSize,
	}
	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}

	// Check assertions.
	if (ok && mutation.Assertion == kvrpcpb.Assertion_NotExist) ||
		(!ok && mutation.Assertion == kvrpcpb.Assertion_Exist) {
		logutil.Logger(context.Background()).Error("ASSERTION FAIL!!!", zap.Stringer("mutation", mutation))
	}

	batch.Put(writeKey, writeValue)
	return nil
}

// Commit implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Commit(keys [][]byte, startTS, commitTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	batch := &leveldb.Batch{}
	for _, k := range keys {
		err := commitKey(mvcc.db, batch, k, startTS, commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return mvcc.db.Write(batch, nil)
}

func commitKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS, commitTS uint64) error {
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec := lockDecoder{
		expectKey: key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok || dec.lock.startTS != startTS {
		// If the lock of this transaction is not found, or the lock is replaced by
		// another transaction, check commit information of this transaction.
		c, ok, err1 := getTxnCommitInfo(iter, key, startTS)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if ok && c.valueType != typeRollback {
			// c.valueType != typeRollback means the transaction is already committed, do nothing.
			return nil
		}
		return ErrRetryable("txn not found")
	}

	if err = commitLock(batch, dec.lock, key, startTS, commitTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func commitLock(batch *leveldb.Batch, lock mvccLock, key []byte, startTS, commitTS uint64) error {
	if lock.op != kvrpcpb.Op_Lock {
		var valueType mvccValueType
		if lock.op == kvrpcpb.Op_Put {
			valueType = typePut
		} else {
			valueType = typeDelete
		}
		value := mvccValue{
			valueType: valueType,
			startTS:   startTS,
			commitTS:  commitTS,
			value:     lock.value,
		}
		writeKey := mvccEncode(key, commitTS)
		writeValue, err := value.MarshalBinary()
		if err != nil {
			return errors.Trace(err)
		}
		batch.Put(writeKey, writeValue)
	}
	batch.Delete(mvccEncode(key, lockVer))
	return nil
}

// Rollback implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Rollback(keys [][]byte, startTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	batch := &leveldb.Batch{}
	for _, k := range keys {
		err := rollbackKey(mvcc.db, batch, k, startTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return mvcc.db.Write(batch, nil)
}

func rollbackKey(db *leveldb.DB, batch *leveldb.Batch, key []byte, startTS uint64) error {
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	if iter.Valid() {
		dec := lockDecoder{
			expectKey: key,
		}
		ok, err := dec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		// If current transaction's lock exist.
		if ok && dec.lock.startTS == startTS {
			if err = rollbackLock(batch, dec.lock, key, startTS); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		// If current transaction's lock not exist.
		// If commit info of current transaction exist.
		c, ok, err := getTxnCommitInfo(iter, key, startTS)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			// If current transaction is already committed.
			if c.valueType != typeRollback {
				return ErrAlreadyCommitted(c.commitTS)
			}
			// If current transaction is already rollback.
			return nil
		}
	}

	// If current transaction is not prewritted before.
	value := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvccEncode(key, startTS)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(writeKey, writeValue)
	return nil
}

func rollbackLock(batch *leveldb.Batch, lock mvccLock, key []byte, startTS uint64) error {
	tomb := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvccEncode(key, startTS)
	writeValue, err := tomb.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(writeKey, writeValue)
	batch.Delete(mvccEncode(key, lockVer))
	return nil
}

func getTxnCommitInfo(iter *Iterator, expectKey []byte, startTS uint64) (mvccValue, bool, error) {
	for iter.Valid() {
		dec := valueDecoder{
			expectKey: expectKey,
		}
		ok, err := dec.Decode(iter)
		if err != nil || !ok {
			return mvccValue{}, ok, errors.Trace(err)
		}

		if dec.value.startTS == startTS {
			return dec.value, true, nil
		}
	}
	return mvccValue{}, false, nil
}

// Cleanup implements the MVCCStore interface.
// Cleanup API is deprecated, use CheckTxnStatus instead.
func (mvcc *MVCCLevelDB) Cleanup(key []byte, startTS, currentTS uint64) error {
	mvcc.mu.Lock()
	defer func() {
		mvcc.mu.Unlock()
		mvcc.deadlockDetector.CleanUp(startTS)
	}()

	batch := &leveldb.Batch{}
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	if iter.Valid() {
		dec := lockDecoder{
			expectKey: key,
		}
		ok, err := dec.Decode(iter)
		if err != nil {
			return err
		}
		// If current transaction's lock exists.
		if ok && dec.lock.startTS == startTS {
			// If the lock has already outdated, clean up it.
			if currentTS == 0 || uint64(oracle.ExtractPhysical(dec.lock.startTS))+dec.lock.ttl < uint64(oracle.ExtractPhysical(currentTS)) {
				logutil.Logger(context.Background()).Info("rollback expired lock and write rollback record",
					zap.String("key", hex.EncodeToString(key)),
					zap.Uint64("lock startTS", dec.lock.startTS),
					zap.Stringer("lock op", dec.lock.op))
				if err = rollbackLock(batch, dec.lock, key, startTS); err != nil {
					return err
				}
				return mvcc.db.Write(batch, nil)
			}

			// Otherwise, return a locked error with the TTL information.
			return dec.lock.lockErr(key)
		}

		// If current transaction's lock does not exist.
		// If the commit information of the current transaction exist.
		c, ok, err := getTxnCommitInfo(iter, key, startTS)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			// If the current transaction has already committed.
			if c.valueType != typeRollback {
				return ErrAlreadyCommitted(c.commitTS)
			}
			// If the current transaction has already rollbacked.
			return nil
		}
	}

	// If current transaction is not prewritted before.
	value := mvccValue{
		valueType: typeRollback,
		startTS:   startTS,
		commitTS:  startTS,
	}
	writeKey := mvccEncode(key, startTS)
	writeValue, err := value.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	batch.Put(writeKey, writeValue)
	return nil
}

// TxnHeartBeat implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) TxnHeartBeat(key []byte, startTS uint64, adviseTTL uint64) (uint64, error) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	if iter.Valid() {
		dec := lockDecoder{
			expectKey: key,
		}
		ok, err := dec.Decode(iter)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if ok && dec.lock.startTS == startTS {
			if !bytes.Equal(dec.lock.primary, key) {
				return 0, errors.New("txnHeartBeat on non-primary key, the code should not run here")
			}

			lock := dec.lock
			batch := &leveldb.Batch{}
			// Increase the ttl of this transaction.
			if adviseTTL > lock.ttl {
				lock.ttl = adviseTTL
				writeKey := mvccEncode(key, lockVer)
				writeValue, err := lock.MarshalBinary()
				if err != nil {
					return 0, errors.Trace(err)
				}
				batch.Put(writeKey, writeValue)
				if err = mvcc.db.Write(batch, nil); err != nil {
					return 0, errors.Trace(err)
				}
			}
			return lock.ttl, nil
		}
	}
	return 0, errors.New("lock doesn't exist")
}

// ScanLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var locks []*kvrpcpb.LockInfo
	for iter.Valid() {
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ok && dec.lock.startTS <= maxTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: dec.lock.primary,
				LockVersion: dec.lock.startTS,
				Key:         currKey,
			})
		}

		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		currKey = skip.currKey
	}
	return locks, nil
}

// ResolveLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	if len(startKey) > 0 {
		startKey = []byte{}
	}
	if len(endKey) > 0 {
		endKey = []byte{}
	}

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		return errors.Trace(err)
	}

	batch := &leveldb.Batch{}
	for iter.Valid() {
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && dec.lock.startTS == startTS {
			if commitTS > 0 {
				err = commitLock(batch, dec.lock, currKey, startTS, commitTS)
			} else {
				err = rollbackLock(batch, dec.lock, currKey, startTS)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}

		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		currKey = skip.currKey
	}
	return mvcc.db.Write(batch, nil)
}

// BatchResolveLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) BatchResolveLock(startKey, endKey []byte, txnInfos map[uint64]uint64) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		return errors.Trace(err)
	}

	batch := &leveldb.Batch{}
	for iter.Valid() {
		dec := lockDecoder{expectKey: currKey}
		ok, err := dec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			if commitTS, ok := txnInfos[dec.lock.startTS]; ok {
				if commitTS > 0 {
					err = commitLock(batch, dec.lock, currKey, dec.lock.startTS, commitTS)
				} else {
					err = rollbackLock(batch, dec.lock, currKey, dec.lock.startTS)
				}
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		skip := skipDecoder{currKey: currKey}
		_, err = skip.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		currKey = skip.currKey
	}
	return mvcc.db.Write(batch, nil)
}

// DeleteRange implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) DeleteRange(startKey, endKey []byte) error {
	return mvcc.doRawDeleteRange(codec.EncodeBytes(nil, startKey), codec.EncodeBytes(nil, endKey))
}

// Close calls leveldb's Close to free resources.
func (mvcc *MVCCLevelDB) Close() error {
	return mvcc.db.Close()
}

// RawPut implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawPut(key, value []byte) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	if value == nil {
		value = []byte{}
	}
	terror.Log(mvcc.db.Put(key, value, nil))
}

// RawBatchPut implements the RawKV interface
func (mvcc *MVCCLevelDB) RawBatchPut(keys, values [][]byte) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}
	for i, key := range keys {
		value := values[i]
		if value == nil {
			value = []byte{}
		}
		batch.Put(key, value)
	}
	terror.Log(mvcc.db.Write(batch, nil))
}

// RawGet implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawGet(key []byte) []byte {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	ret, err := mvcc.db.Get(key, nil)
	terror.Log(err)
	return ret
}

// RawBatchGet implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawBatchGet(keys [][]byte) [][]byte {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	values := make([][]byte, 0, len(keys))
	for _, key := range keys {
		value, err := mvcc.db.Get(key, nil)
		terror.Log(err)
		values = append(values, value)
	}
	return values
}

// RawDelete implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawDelete(key []byte) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	terror.Log(mvcc.db.Delete(key, nil))
}

// RawBatchDelete implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawBatchDelete(keys [][]byte) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}
	for _, key := range keys {
		batch.Delete(key)
	}
	terror.Log(mvcc.db.Write(batch, nil))
}

// RawScan implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawScan(startKey, endKey []byte, limit int) []Pair {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
	}, nil)

	var pairs []Pair
	for iter.Next() && len(pairs) < limit {
		key := iter.Key()
		value := iter.Value()
		err := iter.Error()
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			break
		}
		pairs = append(pairs, Pair{
			Key:   append([]byte{}, key...),
			Value: append([]byte{}, value...),
			Err:   err,
		})
	}
	return pairs
}

// RawReverseScan implements the RawKV interface.
// Scan the range of [endKey, startKey)
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (mvcc *MVCCLevelDB) RawReverseScan(startKey, endKey []byte, limit int) []Pair {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter := mvcc.db.NewIterator(&util.Range{
		Limit: startKey,
	}, nil)

	success := iter.Last()

	var pairs []Pair
	for success && len(pairs) < limit {
		key := iter.Key()
		value := iter.Value()
		err := iter.Error()
		if bytes.Compare(key, endKey) < 0 {
			break
		}
		pairs = append(pairs, Pair{
			Key:   append([]byte{}, key...),
			Value: append([]byte{}, value...),
			Err:   err,
		})
		success = iter.Prev()
	}
	return pairs
}

// RawDeleteRange implements the RawKV interface.
func (mvcc *MVCCLevelDB) RawDeleteRange(startKey, endKey []byte) {
	terror.Log(mvcc.doRawDeleteRange(startKey, endKey))
}

// doRawDeleteRange deletes all keys in a range and return the error if any.
func (mvcc *MVCCLevelDB) doRawDeleteRange(startKey, endKey []byte) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	batch := &leveldb.Batch{}

	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
		Limit: endKey,
	}, nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	return mvcc.db.Write(batch, nil)
}
