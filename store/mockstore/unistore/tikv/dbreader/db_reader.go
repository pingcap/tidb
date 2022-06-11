// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbreader

import (
	"bytes"
	"math"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

// NewDBReader returns a new *DBReader.
func NewDBReader(startKey, endKey []byte, txn *badger.Txn) *DBReader {
	return &DBReader{
		StartKey: startKey,
		EndKey:   endKey,
		txn:      txn,
	}
}

// NewIterator returns a new *badger.Iterator.
func NewIterator(txn *badger.Txn, reverse bool, startKey, endKey []byte) *badger.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	if len(startKey) > 0 {
		opts.StartKey = y.KeyWithTs(startKey, math.MaxUint64)
	}
	if len(endKey) > 0 {
		opts.EndKey = y.KeyWithTs(endKey, math.MaxUint64)
	}
	return txn.NewIterator(opts)
}

// DBReader reads data from DB, for read-only requests, the locks must already be checked before DBReader is created.
type DBReader struct {
	StartKey  []byte
	EndKey    []byte
	txn       *badger.Txn
	iter      *badger.Iterator
	extraIter *badger.Iterator
	revIter   *badger.Iterator
	RcCheckTS bool
}

// GetMvccInfoByKey fills MvccInfo reading committed keys from db
func (r *DBReader) GetMvccInfoByKey(key []byte, isRowKey bool, mvccInfo *kvrpcpb.MvccInfo) error {
	it := r.GetIter()
	it.SetAllVersions(true)
	for it.Seek(key); it.Valid(); it.Next() {
		item := it.Item()
		if !bytes.Equal(item.Key(), key) {
			break
		}
		var val []byte
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		userMeta := mvcc.DBUserMeta(item.UserMeta())
		var tp kvrpcpb.Op
		if len(val) == 0 {
			tp = kvrpcpb.Op_Del
		} else {
			tp = kvrpcpb.Op_Put
		}
		mvccInfo.Writes = append(mvccInfo.Writes, &kvrpcpb.MvccWrite{
			Type:       tp,
			StartTs:    userMeta.StartTS(),
			CommitTs:   userMeta.CommitTS(),
			ShortValue: val,
		})
	}
	return nil
}

// Get gets a value with the key and start ts.
func (r *DBReader) Get(key []byte, startTS uint64) ([]byte, error) {
	r.txn.SetReadTS(startTS)
	if r.RcCheckTS {
		r.txn.SetReadTS(math.MaxUint64)
	}
	item, err := r.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, errors.Trace(err)
	}
	if item == nil {
		return nil, nil
	}
	err = r.CheckWriteItemForRcCheckTSRead(startTS, item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return item.Value()
}

// GetIter returns the *badger.Iterator of a *DBReader.
func (r *DBReader) GetIter() *badger.Iterator {
	if r.iter == nil {
		r.iter = NewIterator(r.txn, false, r.StartKey, r.EndKey)
	}
	return r.iter
}

// GetExtraIter returns the extra *badger.Iterator of a *DBReader.
func (r *DBReader) GetExtraIter() *badger.Iterator {
	if r.extraIter == nil {
		rbStartKey := append([]byte{}, r.StartKey...)
		if len(rbStartKey) != 0 {
			rbStartKey[0]++
		}
		rbEndKey := append([]byte{}, r.EndKey...)
		if len(rbEndKey) != 0 {
			rbEndKey[0]++
		}
		r.extraIter = NewIterator(r.txn, false, rbStartKey, rbEndKey)
	}
	return r.extraIter
}

func (r *DBReader) getReverseIter() *badger.Iterator {
	if r.revIter == nil {
		r.revIter = NewIterator(r.txn, true, r.StartKey, r.EndKey)
	}
	return r.revIter
}

// BatchGetFunc defines a batch get function.
type BatchGetFunc = func(key, value []byte, err error)

// BatchGet batch gets keys.
func (r *DBReader) BatchGet(keys [][]byte, startTS uint64, f BatchGetFunc) {
	r.txn.SetReadTS(startTS)
	if r.RcCheckTS {
		r.txn.SetReadTS(math.MaxUint64)
	}
	items, err := r.txn.MultiGet(keys)
	if err != nil {
		for _, key := range keys {
			f(key, nil, err)
		}
		return
	}
	for i, item := range items {
		key := keys[i]
		var val []byte
		if item != nil {
			val, err = item.Value()
			if err == nil {
				err = r.CheckWriteItemForRcCheckTSRead(startTS, item)
			}
		}
		f(key, val, err)
	}
}

// ErrScanBreak is returned by ScanFunc to break the scan loop.
var ErrScanBreak = errors.New("scan break error")

// ScanFunc accepts key and value, should not keep reference to them.
// Returns ErrScanBreak will break the scan loop.
type ScanFunc = func(key, value []byte) error

// ScanProcessor process the key/value pair.
type ScanProcessor interface {
	// Process accepts key and value, should not keep reference to them.
	// Returns ErrScanBreak will break the scan loop.
	Process(key, value []byte) error
	// SkipValue returns if we can skip the value.
	SkipValue() bool
}

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}

// Scan scans the key range with the given ScanProcessor.
func (r *DBReader) Scan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
	r.txn.SetReadTS(startTS)
	if r.RcCheckTS {
		r.txn.SetReadTS(math.MaxUint64)
	}
	skipValue := proc.SkipValue()
	iter := r.GetIter()
	var cnt int
	var err error
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		err = r.CheckWriteItemForRcCheckTSRead(startTS, item)
		if err != nil {
			return errors.Trace(err)
		}
		if item.IsEmpty() {
			continue
		}
		var val []byte
		if !skipValue {
			val, err = item.Value()
			if err != nil {
				return errors.Trace(err)
			}
		}
		err = proc.Process(key, val)
		if err != nil {
			if err == ErrScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

// GetKeyByStartTs gets a key with the start ts.
func (r *DBReader) GetKeyByStartTs(startKey, endKey []byte, startTs uint64) ([]byte, error) {
	iter := r.GetIter()
	iter.SetAllVersions(true)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		curItem := iter.Item()
		curKey := curItem.Key()
		if len(endKey) != 0 && bytes.Compare(curKey, endKey) >= 0 {
			break
		}
		meta := mvcc.DBUserMeta(curItem.UserMeta())
		if meta.StartTS() == startTs {
			return curItem.KeyCopy(nil), nil
		}
	}
	return nil, nil
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (r *DBReader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
	skipValue := proc.SkipValue()
	iter := r.getReverseIter()
	r.txn.SetReadTS(startTS)
	if r.RcCheckTS {
		r.txn.SetReadTS(math.MaxUint64)
	}
	var cnt int
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if bytes.Compare(key, startKey) < 0 {
			break
		}
		if cnt == 0 && bytes.Equal(key, endKey) {
			continue
		}
		var err error
		err = r.CheckWriteItemForRcCheckTSRead(startTS, item)
		if err != nil {
			return errors.Trace(err)
		}
		if item.IsEmpty() {
			continue
		}
		var val []byte
		if !skipValue {
			val, err = item.Value()
			if err != nil {
				return errors.Trace(err)
			}
		}
		err = proc.Process(key, val)
		if err != nil {
			if err == ErrScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

// CheckWriteItemForRcCheckTSRead checks the data version if `RcCheckTS` isolation level is used.
func (r *DBReader) CheckWriteItemForRcCheckTSRead(readTS uint64, item *badger.Item) error {
	if item == nil {
		return nil
	}
	if !r.RcCheckTS {
		return nil
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.CommitTS() > readTS {
		return &kverrors.ErrConflict{
			StartTS:          readTS,
			ConflictTS:       userMeta.StartTS(),
			ConflictCommitTS: userMeta.CommitTS(),
		}
	}
	return nil
}

// GetTxn gets the *badger.Txn of the *DBReader.
func (r *DBReader) GetTxn() *badger.Txn {
	return r.txn
}

// Close closes the *DBReader.
func (r *DBReader) Close() {
	if r.iter != nil {
		r.iter.Close()
	}
	if r.revIter != nil {
		r.revIter.Close()
	}
	if r.extraIter != nil {
		r.extraIter.Close()
	}
	r.txn.Discard()
}
