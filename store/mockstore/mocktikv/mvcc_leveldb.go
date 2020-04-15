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
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
	"github.com/pingcap/tidb/v4/util/codec"
	"github.com/pingcap/tidb/v4/util/deadlock"
	"github.com/pingcap/tidb/v4/util/logutil"
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

// Get implements the MVCCStore interface.
// key cannot be nil or []byte{}
func (mvcc *MVCCLevelDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	return mvcc.getValue(key, startTS, isoLevel, resolvedLocks)
}

func (mvcc *MVCCLevelDB) getValue(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	return getValue(iter, key, startTS, isoLevel, resolvedLocks)
}

func getValue(iter *Iterator, key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) ([]byte, error) {
	dec1 := lockDecoder{expectKey: key}
	ok, err := dec1.Decode(iter)
	if ok && isoLevel == kvrpcpb.IsolationLevel_SI {
		startTS, err = dec1.lock.check(startTS, key, resolvedLocks)
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
		if value.valueType == typeRollback || value.valueType == typeLock {
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
func (mvcc *MVCCLevelDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	pairs := make([]Pair, 0, len(ks))
	for _, k := range ks {
		v, err := mvcc.getValue(k, startTS, isoLevel, resolvedLocks)
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
func (mvcc *MVCCLevelDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLock []uint64) []Pair {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		logutil.BgLogger().Error("scan new iterator fail", zap.Error(err))
		return nil
	}

	ok := true
	var pairs []Pair
	for len(pairs) < limit && ok {
		value, err := getValue(iter, currKey, startTS, isoLevel, resolvedLock)
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
			logutil.BgLogger().Error("seek to next key error", zap.Error(err))
			break
		}
		currKey = skip.currKey
	}
	return pairs
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (mvcc *MVCCLevelDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel, resolvedLocks []uint64) []Pair {
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
		startTS:       startTS,
		isoLevel:      isoLevel,
		currKey:       currKey,
		resolvedLocks: resolvedLocks,
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
			logutil.BgLogger().Error("unmarshal fail", zap.Error(err))
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
	startTS       uint64
	isoLevel      kvrpcpb.IsolationLevel
	resolvedLocks []uint64
	currKey       []byte
	entry         mvccEntry
	pairs         []Pair
}

func (helper *reverseScanHelper) finishEntry() {
	reverse(helper.entry.values)
	helper.entry.key = NewMvccKey(helper.currKey)
	val, err := helper.entry.Get(helper.startTS, helper.isoLevel, helper.resolvedLocks)
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

type lockCtx struct {
	startTS     uint64
	forUpdateTS uint64
	primary     []byte
	ttl         uint64
	minCommitTs uint64

	returnValues bool
	values       [][]byte
}

// PessimisticLock writes the pessimistic lock.
func (mvcc *MVCCLevelDB) PessimisticLock(req *kvrpcpb.PessimisticLockRequest) *kvrpcpb.PessimisticLockResponse {
	resp := &kvrpcpb.PessimisticLockResponse{}
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()
	mutations := req.Mutations
	lCtx := &lockCtx{
		startTS:      req.StartVersion,
		forUpdateTS:  req.ForUpdateTs,
		primary:      req.PrimaryLock,
		ttl:          req.LockTtl,
		minCommitTs:  req.MinCommitTs,
		returnValues: req.ReturnValues,
	}
	lockWaitTime := req.WaitTimeout

	anyError := false
	batch := &leveldb.Batch{}
	errs := make([]error, 0, len(mutations))
	for _, m := range mutations {
		err := mvcc.pessimisticLockMutation(batch, m, lCtx)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
		if lockWaitTime == kv.LockNoWait {
			if _, ok := err.(*ErrLocked); ok {
				break
			}
		}
	}
	if anyError {
		if lockWaitTime != kv.LockNoWait {
			// TODO: remove this when implement sever side wait.
			simulateServerSideWaitLock(errs)
		}
		resp.Errors = convertToKeyErrors(errs)
		return resp
	}
	if err := mvcc.db.Write(batch, nil); err != nil {
		resp.Errors = convertToKeyErrors([]error{err})
		return resp
	}
	if req.ReturnValues {
		resp.Values = lCtx.values
	}
	return resp
}

func (mvcc *MVCCLevelDB) pessimisticLockMutation(batch *leveldb.Batch, mutation *kvrpcpb.Mutation, lctx *lockCtx) error {
	startTS := lctx.startTS
	forUpdateTS := lctx.forUpdateTS
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

	// For pessimisticLockMutation, check the correspond rollback record, there may be rollbackLock
	// operation between startTS and forUpdateTS
	val, err := checkConflictValue(iter, mutation, forUpdateTS, startTS, true)
	if err != nil {
		return err
	}
	if lctx.returnValues {
		lctx.values = append(lctx.values, val)
	}

	lock := mvccLock{
		startTS:     startTS,
		primary:     lctx.primary,
		op:          kvrpcpb.Op_PessimisticLock,
		ttl:         lctx.ttl,
		forUpdateTS: forUpdateTS,
		minCommitTS: lctx.minCommitTs,
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
	minCommitTS := req.MinCommitTs
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
			v, err := mvcc.getValue(m.Key, startTS, kvrpcpb.IsolationLevel_SI, req.Context.ResolvedLocks)
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
		err = prewriteMutation(mvcc.db, batch, m, startTS, primary, ttl, txnSize, isPessimisticLock, minCommitTS)
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

func checkConflictValue(iter *Iterator, m *kvrpcpb.Mutation, forUpdateTS uint64, startTS uint64, getVal bool) ([]byte, error) {
	dec := &valueDecoder{
		expectKey: m.Key,
	}
	ok, err := dec.Decode(iter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	// Note that it's a write conflict here, even if the value is a rollback one, or a op_lock record
	if dec.value.commitTS > forUpdateTS {
		return nil, &ErrConflict{
			StartTS:          forUpdateTS,
			ConflictTS:       dec.value.startTS,
			ConflictCommitTS: dec.value.commitTS,
			Key:              m.Key,
		}
	}

	needGetVal := getVal
	needCheckAssertion := m.Assertion == kvrpcpb.Assertion_NotExist
	needCheckRollback := true
	var retVal []byte
	// do the check or get operations within one iteration to make CI faster
	for ok {
		if needCheckRollback {
			if dec.value.valueType == typeRollback {
				if dec.value.commitTS == startTS {
					logutil.BgLogger().Warn("rollback value found",
						zap.Uint64("txnID", startTS),
						zap.Int32("rollbacked.valueType", int32(dec.value.valueType)),
						zap.Uint64("rollbacked.startTS", dec.value.startTS),
						zap.Uint64("rollbacked.commitTS", dec.value.commitTS))
					return nil, &ErrAlreadyRollbacked{
						startTS: startTS,
						key:     m.Key,
					}
				}
			}
			if dec.value.commitTS < startTS {
				needCheckRollback = false
			}
		}
		if needCheckAssertion {
			if dec.value.valueType == typePut || dec.value.valueType == typeLock {
				if m.Op == kvrpcpb.Op_PessimisticLock {
					return nil, &ErrKeyAlreadyExist{
						Key: m.Key,
					}
				}
			} else if dec.value.valueType == typeDelete {
				needCheckAssertion = false
			}
		}
		if needGetVal {
			if dec.value.valueType == typeDelete || dec.value.valueType == typePut {
				retVal = dec.value.value
				needGetVal = false
			}
		}
		if !needCheckAssertion && !needGetVal && !needCheckRollback {
			break
		}
		ok, err = dec.Decode(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if getVal {
		return retVal, nil
	}
	return nil, nil
}

func prewriteMutation(db *leveldb.DB, batch *leveldb.Batch,
	mutation *kvrpcpb.Mutation, startTS uint64,
	primary []byte, ttl uint64, txnSize uint64,
	isPessimisticLock bool, minCommitTS uint64) error {
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
		if minCommitTS < dec.lock.minCommitTS {
			// The minCommitTS has been pushed forward.
			minCommitTS = dec.lock.minCommitTS
		}
	} else {
		if isPessimisticLock {
			return ErrAbort("pessimistic lock not found")
		}
		_, err = checkConflictValue(iter, mutation, startTS, startTS, false)
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
	// Write minCommitTS on the primary lock.
	if bytes.Equal(primary, mutation.GetKey()) {
		lock.minCommitTS = minCommitTS
	}

	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
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
	// Reject the commit request whose commitTS is less than minCommiTS.
	if dec.lock.minCommitTS > commitTS {
		return &ErrCommitTSExpired{
			kvrpcpb.CommitTsExpired{
				StartTs:           startTS,
				AttemptedCommitTs: commitTS,
				Key:               key,
				MinCommitTs:       dec.lock.minCommitTS,
			}}
	}

	if err = commitLock(batch, dec.lock, key, startTS, commitTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func commitLock(batch *leveldb.Batch, lock mvccLock, key []byte, startTS, commitTS uint64) error {
	var valueType mvccValueType
	if lock.op == kvrpcpb.Op_Put {
		valueType = typePut
	} else if lock.op == kvrpcpb.Op_Lock {
		valueType = typeLock
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
			if err = rollbackLock(batch, key, startTS); err != nil {
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

func writeRollback(batch *leveldb.Batch, key []byte, startTS uint64) error {
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
	return nil
}

func rollbackLock(batch *leveldb.Batch, key []byte, startTS uint64) error {
	err := writeRollback(batch, key, startTS)
	if err != nil {
		return err
	}
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
				if err = rollbackLock(batch, key, startTS); err != nil {
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

// CheckTxnStatus checks the primary lock of a transaction to decide its status.
// The return values are (ttl, commitTS, err):
// If the transaction is active, this function returns the ttl of the lock;
// If the transaction is committed, this function returns the commitTS;
// If the transaction is rollbacked, this function returns (0, 0, nil)
// Note that CheckTxnStatus may also push forward the `minCommitTS` of the
// transaction, so it's not simply a read-only operation.
//
// primaryKey + lockTS together could locate the primary lock.
// callerStartTS is the start ts of reader transaction.
// currentTS is the current ts, but it may be inaccurate. Just use it to check TTL.
func (mvcc *MVCCLevelDB) CheckTxnStatus(primaryKey []byte, lockTS, callerStartTS, currentTS uint64,
	rollbackIfNotExist bool) (ttl uint64, commitTS uint64, action kvrpcpb.Action, err error) {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	action = kvrpcpb.Action_NoAction

	startKey := mvccEncode(primaryKey, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	if iter.Valid() {
		dec := lockDecoder{
			expectKey: primaryKey,
		}
		var ok bool
		ok, err = dec.Decode(iter)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		// If current transaction's lock exists.
		if ok && dec.lock.startTS == lockTS {
			lock := dec.lock
			batch := &leveldb.Batch{}

			// If the lock has already outdated, clean up it.
			if uint64(oracle.ExtractPhysical(lock.startTS))+lock.ttl < uint64(oracle.ExtractPhysical(currentTS)) {
				if err = rollbackLock(batch, primaryKey, lockTS); err != nil {
					err = errors.Trace(err)
					return
				}
				if err = mvcc.db.Write(batch, nil); err != nil {
					err = errors.Trace(err)
					return
				}
				return 0, 0, kvrpcpb.Action_TTLExpireRollback, nil
			}

			// If this is a large transaction and the lock is active, push forward the minCommitTS.
			// lock.minCommitTS == 0 may be a secondary lock, or not a large transaction (old version TiDB).
			if lock.minCommitTS > 0 {
				action = kvrpcpb.Action_MinCommitTSPushed
				// We *must* guarantee the invariance lock.minCommitTS >= callerStartTS + 1
				if lock.minCommitTS < callerStartTS+1 {
					lock.minCommitTS = callerStartTS + 1

					// Remove this condition should not affect correctness.
					// We do it because pushing forward minCommitTS as far as possible could avoid
					// the lock been pushed again several times, and thus reduce write operations.
					if lock.minCommitTS < currentTS {
						lock.minCommitTS = currentTS
					}

					writeKey := mvccEncode(primaryKey, lockVer)
					writeValue, err1 := lock.MarshalBinary()
					if err1 != nil {
						err = errors.Trace(err1)
						return
					}
					batch.Put(writeKey, writeValue)
					if err1 = mvcc.db.Write(batch, nil); err1 != nil {
						err = errors.Trace(err1)
						return
					}
				}
			}

			return lock.ttl, 0, action, nil
		}

		// If current transaction's lock does not exist.
		// If the commit info of the current transaction exists.
		c, ok, err1 := getTxnCommitInfo(iter, primaryKey, lockTS)
		if err1 != nil {
			err = errors.Trace(err1)
			return
		}
		if ok {
			// If current transaction is already committed.
			if c.valueType != typeRollback {
				return 0, c.commitTS, action, nil
			}
			// If current transaction is already rollback.
			return 0, 0, kvrpcpb.Action_NoAction, nil
		}
	}

	// If current transaction is not prewritted before, it may be pessimistic lock.
	// When pessimistic txn rollback statement, it may not leave a 'rollbacked' tombstone.

	// Or maybe caused by concurrent prewrite operation.
	// Especially in the non-block reading case, the secondary lock is likely to be
	// written before the primary lock.

	if rollbackIfNotExist {
		// Write rollback record, but not delete the lock on the primary key. There may exist lock which has
		// different lock.startTS with input lockTS, for example the primary key could be already
		// locked by the caller transaction, deleting this key will mistakenly delete the lock on
		// primary key, see case TestSingleStatementRollback in session_test suite for example
		batch := &leveldb.Batch{}
		if err1 := writeRollback(batch, primaryKey, lockTS); err1 != nil {
			err = errors.Trace(err1)
			return
		}
		if err1 := mvcc.db.Write(batch, nil); err1 != nil {
			err = errors.Trace(err1)
			return
		}
		return 0, 0, kvrpcpb.Action_LockNotExistRollback, nil
	}

	return 0, 0, action, &ErrTxnNotFound{kvrpcpb.TxnNotFound{
		StartTs:    lockTS,
		PrimaryKey: primaryKey,
	}}
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
				err = rollbackLock(batch, currKey, startTS)
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
					err = rollbackLock(batch, currKey, dec.lock.startTS)
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

// GC implements the MVCCStore interface
func (mvcc *MVCCLevelDB) GC(startKey, endKey []byte, safePoint uint64) error {
	mvcc.mu.Lock()
	defer mvcc.mu.Unlock()

	iter, currKey, err := newScanIterator(mvcc.db, startKey, endKey)
	defer iter.Release()
	if err != nil {
		return errors.Trace(err)
	}

	// Mock TiKV usually doesn't need to process large amount of data. So write it in a single batch.
	batch := &leveldb.Batch{}

	for iter.Valid() {
		lockDec := lockDecoder{expectKey: currKey}
		ok, err := lockDec.Decode(iter)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && lockDec.lock.startTS <= safePoint {
			return errors.Errorf(
				"key %+q has lock with startTs %v which is under safePoint %v",
				currKey,
				lockDec.lock.startTS,
				safePoint)
		}

		keepNext := true
		dec := valueDecoder{expectKey: currKey}

		for iter.Valid() {
			ok, err := dec.Decode(iter)
			if err != nil {
				return errors.Trace(err)
			}

			if !ok {
				// Go to the next key
				currKey, _, err = mvccDecode(iter.Key())
				if err != nil {
					return errors.Trace(err)
				}
				break
			}

			if dec.value.commitTS > safePoint {
				continue
			}

			if dec.value.valueType == typePut || dec.value.valueType == typeDelete {
				// Keep the latest version if it's `typePut`
				if !keepNext || dec.value.valueType == typeDelete {
					batch.Delete(mvccEncode(currKey, dec.value.commitTS))
				}
				keepNext = false
			} else {
				// Delete all other types
				batch.Delete(mvccEncode(currKey, dec.value.commitTS))
			}
		}
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

// MvccGetByStartTS implements the MVCCDebugger interface.
func (mvcc *MVCCLevelDB) MvccGetByStartTS(starTS uint64) (*kvrpcpb.MvccInfo, []byte) {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	var key []byte
	iter := newIterator(mvcc.db, nil)
	defer iter.Release()

	// find the first committed key for which `start_ts` equals to `ts`
	for iter.Valid() {
		var value mvccValue
		err := value.UnmarshalBinary(iter.Value())
		if err == nil && value.startTS == starTS {
			if _, key, err = codec.DecodeBytes(iter.Key(), nil); err != nil {
				return nil, nil
			}
			break
		}
		iter.Next()
	}

	return mvcc.MvccGetByKey(key), key
}

var valueTypeOpMap = [...]kvrpcpb.Op{
	typePut:      kvrpcpb.Op_Put,
	typeDelete:   kvrpcpb.Op_Del,
	typeRollback: kvrpcpb.Op_Rollback,
	typeLock:     kvrpcpb.Op_Lock,
}

// MvccGetByKey implements the MVCCDebugger interface.
func (mvcc *MVCCLevelDB) MvccGetByKey(key []byte) *kvrpcpb.MvccInfo {
	mvcc.mu.RLock()
	defer mvcc.mu.RUnlock()

	info := &kvrpcpb.MvccInfo{}

	startKey := mvccEncode(key, lockVer)
	iter := newIterator(mvcc.db, &util.Range{
		Start: startKey,
	})
	defer iter.Release()

	dec1 := lockDecoder{expectKey: key}
	ok, err := dec1.Decode(iter)
	if err != nil {
		return nil
	}
	if ok {
		var shortValue []byte
		if isShortValue(dec1.lock.value) {
			shortValue = dec1.lock.value
		}
		info.Lock = &kvrpcpb.MvccLock{
			Type:       dec1.lock.op,
			StartTs:    dec1.lock.startTS,
			Primary:    dec1.lock.primary,
			ShortValue: shortValue,
		}
	}

	dec2 := valueDecoder{expectKey: key}
	var writes []*kvrpcpb.MvccWrite
	var values []*kvrpcpb.MvccValue
	for iter.Valid() {
		ok, err := dec2.Decode(iter)
		if err != nil {
			return nil
		}
		if !ok {
			iter.Next()
			break
		}
		var shortValue []byte
		if isShortValue(dec2.value.value) {
			shortValue = dec2.value.value
		}
		write := &kvrpcpb.MvccWrite{
			Type:       valueTypeOpMap[dec2.value.valueType],
			StartTs:    dec2.value.startTS,
			CommitTs:   dec2.value.commitTS,
			ShortValue: shortValue,
		}
		writes = append(writes, write)
		value := &kvrpcpb.MvccValue{
			StartTs: dec2.value.startTS,
			Value:   dec2.value.value,
		}
		values = append(values, value)
	}
	info.Writes = writes
	info.Values = values

	return info
}

const shortValueMaxLen = 64

func isShortValue(value []byte) bool {
	return len(value) <= shortValueMaxLen
}
