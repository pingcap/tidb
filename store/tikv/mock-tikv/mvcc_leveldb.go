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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"github.com/pingcap/goleveldb/leveldb/storage"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
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
	// NextKey_verMax  -- (6)
	// ...
	// NextKey_ver+1   -- (7)
	// NextKey_ver     -- (8)
	// NextKey_ver-1   -- (9)
	// ...
	// NextKey_0       -- (10)
	// ...
	// EOF
	db *leveldb.DB
	sync.RWMutex
}

var lockVer uint64 = math.MaxUint64

func newMVCCLevelDB(path string) (*MVCCLevelDB, error) {
	var (
		d   *leveldb.DB
		err error
	)
	if path == "" {
		d, err = leveldb.Open(storage.NewMemStorage(), nil)
	} else {
		d, err = leveldb.OpenFile(path, &opt.Options{BlockCacheCapacity: 600 * 1024 * 1024})
	}

	return &MVCCLevelDB{db: d}, err
}

// Get implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Get(key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	startKey := mvccEncode(key, lockVer)
	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
	}, nil)
	defer iter.Release()

	more := iter.Next()
	if !more {
		return nil, nil
	}
	return getValue(iter, key, startTS, isoLevel)
}

func getValue(iter iterator.Iterator, key []byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) ([]byte, error) {
	more := true
	lock, ok, err := meetLock(iter, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ok {
		if isoLevel == kvrpcpb.IsolationLevel_SI {
			if lock.startTS <= startTS {
				return nil, lock.lockErr(key)
			}
		}
		more = iter.Next()
	}

	for more {
		value, ok, err := getMvccValue(iter, key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			return nil, nil
		}
		if value.valueType == typeRollback {
			more = iter.Next()
			continue
		}

		if value.commitTS <= startTS {
			if value.valueType == typeDelete {
				return nil, nil
			}
			return value.value, nil
		}
		more = iter.Next()
	}
	return nil, nil
}

// BatchGet implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) BatchGet(ks [][]byte, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	var pairs []Pair
	for _, k := range ks {
		v, err := mvcc.Get(k, startTS, isoLevel)
		if v == nil && err == nil {
			continue
		}
		pairs = append(pairs, Pair{
			Key:   k,
			Value: v,
			Err:   err,
		})
	}
	return pairs
}

// Scan implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Scan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	var start, end []byte
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		end = mvccEncode(endKey, lockVer)
	}
	iter := mvcc.db.NewIterator(&util.Range{
		Start: start,
		Limit: end,
	}, nil)
	defer iter.Release()

	var pairs []Pair
	more := iter.Next()
	if more && len(startKey) == 0 {
		first, _, err := mvccDecode(iter.Key())
		if err != nil {
			log.Error("data corrupt:", err)
			return nil
		}
		startKey = first
	}
	currKey := startKey
	for more && len(pairs) < limit {
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

		currKey, more, err = seekToNext(iter, currKey)
		if err != nil {
			log.Error("seekToNext error:", errors.ErrorStack(err))
			break
		}
	}
	return pairs
}

func seekToNext(iter iterator.Iterator, curr []byte) ([]byte, bool, error) {
	more := true
	for more {
		key, _, err := mvccDecode(iter.Key())
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if !bytes.Equal(key, curr) {
			return key, true, nil
		}
		more = iter.Next()
	}
	return nil, false, nil
}

// ReverseScan implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, isoLevel kvrpcpb.IsolationLevel) []Pair {
	return nil
}

func meetLock(iter iterator.Iterator, expectKey []byte) (mvccLock, bool, error) {
	var lock mvccLock
	iterKey := iter.Key()
	iterValue := iter.Value()
	key, ver, err := mvccDecode(iterKey)
	if err != nil {
		return lock, false, errors.Trace(err)
	}
	if !bytes.Equal(key, expectKey) {
		return lock, false, nil
	}

	if ver != lockVer {
		return lock, false, nil
	}

	err = lock.UnmarshalBinary(iterValue)
	if err != nil {
		return lock, false, errors.Trace(err)
	}

	return lock, true, nil
}

func getMvccValue(iter iterator.Iterator, expectKey []byte) (mvccValue, bool, error) {
	var value mvccValue
	iterKey := iter.Key()
	iterValue := iter.Value()
	key, ver, err := mvccDecode(iterKey)
	if err != nil {
		return value, false, errors.Trace(err)
	}
	if !bytes.Equal(key, expectKey) && len(expectKey) != 0 {
		return value, false, nil
	}
	if ver == lockVer {
		return value, false, errors.New("getMvccValue meet lock")
	}
	err = value.UnmarshalBinary(iterValue)
	if err != nil {
		return value, false, errors.Trace(err)
	}
	return value, true, nil
}

// Prewrite implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	txn, err := mvcc.db.OpenTransaction()
	if err != nil {
		panic(err)
		// return errors.Trace(err)
	}

	anyError := false
	errs := make([]error, 0, len(mutations))
	for _, m := range mutations {
		err := prewriteMutation(txn, m, startTS, primary, ttl)
		errs = append(errs, err)
		if err != nil {
			anyError = true
		}
	}
	if anyError {
		txn.Discard()
		return errs
	}

	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	return errs
}

func prewriteMutation(txn *leveldb.Transaction, mutation *kvrpcpb.Mutation, startTS uint64, primary []byte, ttl uint64) error {
	startKey := mvccEncode(mutation.Key, lockVer)
	iter := txn.NewIterator(&util.Range{
		Start: startKey,
	}, nil)
	defer iter.Release()

	if iter.Next() {
		lock, ok, err := meetLock(iter, mutation.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			if lock.startTS != startTS {
				return lock.lockErr(mutation.Key)
			}
			return nil
		}

		value, ok, err := getMvccValue(iter, mutation.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && value.valueType != typeRollback && value.commitTS >= startTS {
			return ErrRetryable("write conflict")
		}
	}

	lock := mvccLock{
		startTS: startTS,
		primary: primary,
		value:   mutation.Value,
		op:      mutation.GetOp(),
		ttl:     ttl,
	}
	writeKey := mvccEncode(mutation.Key, lockVer)
	writeValue, err := lock.MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Put(writeKey, writeValue, nil)
	return errors.Trace(err)
}

// Commit implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Commit(keys [][]byte, startTS, commitTS uint64) error {
	txn, err := mvcc.db.OpenTransaction()
	if err != nil {
		return errors.Trace(err)
	}

	for _, k := range keys {
		err := commitKey(txn, k, startTS, commitTS)
		if err != nil {
			txn.Discard()
			return err
		}
	}
	err = txn.Commit()
	return errors.Trace(err)
}

func commitKey(txn *leveldb.Transaction, key []byte, startTS, commitTS uint64) error {
	startKey := mvccEncode(key, lockVer)
	iter := txn.NewIterator(&util.Range{
		Start: startKey,
	}, nil)
	defer iter.Release()

	if !iter.Next() {
		return errors.New("commitKey fail")
	}
	lock, ok, err := meetLock(iter, key)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok || lock.startTS != startTS {
		c, ok, err := getTxnCommitInfo(iter, key, startTS)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && c.valueType != typeRollback {
			return nil
		}
		return ErrRetryable("txn not found")
	}

	if err := commitLock(txn, lock, key, startTS, commitTS); err != nil {
		return errors.Trace(err)
	}
	err = txn.Delete(startKey, nil)
	return errors.Trace(err)
}

func commitLock(txn *leveldb.Transaction, lock mvccLock, key []byte, startTS, commitTS uint64) error {
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
		err = txn.Put(writeKey, writeValue, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Delete(mvccEncode(key, lockVer), nil)
}

// Rollback implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Rollback(keys [][]byte, startTS uint64) error {
	txn, err := mvcc.db.OpenTransaction()
	if err != nil {
		return errors.Trace(err)
	}

	for _, k := range keys {
		err := rollbackKey(txn, k, startTS)
		if err != nil {
			txn.Discard()
			return err
		}
	}
	err = txn.Commit()
	return errors.Trace(err)
}

func rollbackLock(txn *leveldb.Transaction, lock mvccLock, key []byte, startTS uint64) error {
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
	if err := txn.Put(writeKey, writeValue, nil); err != nil {
		return errors.Trace(err)
	}
	return txn.Delete(mvccEncode(key, lockVer), nil)
}

func rollbackKey(txn *leveldb.Transaction, key []byte, startTS uint64) error {
	startKey := mvccEncode(key, lockVer)
	iter := txn.NewIterator(&util.Range{
		Start: startKey,
	}, nil)
	defer iter.Release()

	if iter.Next() {
		lock, ok, err := meetLock(iter, key)
		if err != nil {
			return errors.Trace(err)
		}
		// If current transaction's lock exist.
		if ok && lock.startTS == startTS {
			if err := rollbackLock(txn, lock, key, startTS); err != nil {
				return errors.Trace(err)
			}
			err := txn.Delete(startKey, nil)
			return errors.Trace(err)
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
	err = txn.Put(writeKey, writeValue, nil)
	return errors.Trace(err)
}

// Cleanup implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) Cleanup(key []byte, startTS uint64) error {
	txn, err := mvcc.db.OpenTransaction()
	if err != nil {
		return errors.Trace(err)
	}
	err = rollbackKey(txn, key, startTS)
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.Commit()
	return errors.Trace(err)
}

func getTxnCommitInfo(iter iterator.Iterator, expectKey []byte, startTS uint64) (mvccValue, bool, error) {
	more := true
	for more {
		v, ok, err := getMvccValue(iter, expectKey)
		if err != nil {
			return v, ok, errors.Trace(err)
		}

		if v.startTS == startTS {
			return v, true, nil
		}

		more = iter.Next()
	}
	return mvccValue{}, false, nil
}

// ScanLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ScanLock(startKey, endKey []byte, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var start, end []byte
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		end = mvccEncode(endKey, lockVer)
	}
	iter := mvcc.db.NewIterator(&util.Range{
		Start: start,
		Limit: end,
	}, nil)
	defer iter.Release()

	more := iter.Next()
	if more && len(startKey) == 0 {
		first, _, err := mvccDecode(iter.Key())
		if err != nil {
			return nil, errors.Trace(err)
		}
		startKey = first
	}
	currKey := startKey
	var locks []*kvrpcpb.LockInfo
	for more {
		l, ok, err := meetLock(iter, currKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ok && l.startTS <= maxTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: l.primary,
				LockVersion: l.startTS,
				Key:         currKey,
			})
		}

		currKey, more, err = seekToNext(iter, currKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return locks, nil
}

// ResolveLock implements the MVCCStore interface.
func (mvcc *MVCCLevelDB) ResolveLock(startKey, endKey []byte, startTS, commitTS uint64) error {
	txn, err := mvcc.db.OpenTransaction()
	if err != nil {
		return errors.Trace(err)
	}

	var start, end []byte
	if len(startKey) > 0 {
		start = mvccEncode(startKey, lockVer)
	}
	if len(endKey) > 0 {
		end = mvccEncode(endKey, lockVer)
	}
	iter := txn.NewIterator(&util.Range{
		Start: start,
		Limit: end,
	}, nil)
	defer iter.Release()

	more := iter.Next()
	if more && len(startKey) == 0 {
		first, _, err := mvccDecode(iter.Key())
		if err != nil {
			return errors.Trace(err)
		}
		startKey = first
	}
	currKey := startKey
	for more {
		l, ok, err := meetLock(iter, currKey)
		if err != nil {
			return errors.Trace(err)
		}
		if ok && l.startTS == startTS {
			if commitTS > 0 {
				err = commitLock(txn, l, currKey, startTS, commitTS)
			} else {
				err = rollbackLock(txn, l, currKey, startTS)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}

		currKey, more, err = seekToNext(iter, currKey)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit()
}

// mvccEncode returns the encoded key.
func mvccEncode(key []byte, ver uint64) []byte {
	b := codec.EncodeBytes(nil, key)
	ret := codec.EncodeUintDesc(b, ver)
	return ret
}

// ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
var ErrInvalidEncodedKey = errors.New("invalid encoded key")

// mvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
// just returns the origin key.
func mvccDecode(encodedKey []byte) ([]byte, uint64, error) {
	// Skip DataPrefix
	remainBytes, key, err := codec.DecodeBytes([]byte(encodedKey))
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

// RawGet queries value with the key.
func (mvcc *MVCCLevelDB) RawGet(key []byte) []byte {
	value, err := mvcc.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	return value
}

// RawPut stores a key-value pair.
func (mvcc *MVCCLevelDB) RawPut(key, value []byte) {
	err := mvcc.db.Put(key, value, nil)
	if err != nil {
		panic(err)
	}
}

// RawDelete deletes a key-value pair.
func (mvcc *MVCCLevelDB) RawDelete(key []byte) {
	mvcc.db.Delete(key, nil)
}

// RawScan reads up to a limited number of rawkv Pairs.
func (mvcc *MVCCLevelDB) RawScan(startKey, endKey []byte, limit int) []Pair {
	iter := mvcc.db.NewIterator(&util.Range{
		Start: startKey,
		Limit: endKey,
	}, nil)
	defer iter.Release()

	var pairs []Pair
	for iter.Next() {
		pair := Pair{
			Key:   iter.Key(),
			Value: iter.Value(),
			Err:   iter.Error(),
		}
		pairs = append(pairs, pair)
	}

	return pairs
}
