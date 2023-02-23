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

package tikv

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/util/mathutil"
)

const (
	batchChanSize = 1024
)

type writeDBBatch struct {
	entries []*badger.Entry
	err     error
	wg      sync.WaitGroup
}

func newWriteDBBatch() *writeDBBatch {
	return &writeDBBatch{}
}

func (batch *writeDBBatch) set(key y.Key, val []byte, userMeta []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	})
}

// delete is a badger level operation, only used in DeleteRange, so we don't need to set UserMeta.
// Then we can tell the entry is delete if UserMeta is nil.
func (batch *writeDBBatch) delete(key y.Key) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key: key,
	})
}

type writeLockBatch struct {
	entries []*badger.Entry
	err     error
	wg      sync.WaitGroup
}

func (batch *writeLockBatch) set(key, val []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      y.KeyWithTs(key, 0),
		Value:    val,
		UserMeta: mvcc.LockUserMetaNone,
	})
}

func (batch *writeLockBatch) delete(key []byte) {
	batch.entries = append(batch.entries, &badger.Entry{
		Key:      y.KeyWithTs(key, 0),
		UserMeta: mvcc.LockUserMetaDelete,
	})
}

type writeDBWorker struct {
	batchCh chan *writeDBBatch
	writer  *dbWriter
}

func (w writeDBWorker) run() {
	defer w.writer.wg.Done()
	var batches []*writeDBBatch
	for {
		for i := range batches {
			batches[i] = nil
		}
		batches = batches[:0]
		select {
		case <-w.writer.closeCh:
			return
		case batch := <-w.batchCh:
			batches = append(batches, batch)
		}
		chLen := len(w.batchCh)
		for i := 0; i < chLen; i++ {
			batches = append(batches, <-w.batchCh)
		}
		if len(batches) > 0 {
			w.updateBatchGroup(batches)
		}
	}
}

func (w writeDBWorker) updateBatchGroup(batchGroup []*writeDBBatch) {
	e := w.writer.bundle.DB.Update(func(txn *badger.Txn) error {
		for _, batch := range batchGroup {
			for _, entry := range batch.entries {
				var err error
				if len(entry.UserMeta) == 0 {
					entry.SetDelete()
				}
				err = txn.SetEntry(entry)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	for _, batch := range batchGroup {
		batch.err = e
		batch.wg.Done()
	}
}

type writeLockWorker struct {
	batchCh chan *writeLockBatch
	writer  *dbWriter
}

func (w writeLockWorker) run() {
	defer w.writer.wg.Done()
	ls := w.writer.bundle.LockStore
	var batches []*writeLockBatch
	for {
		for i := range batches {
			batches[i] = nil
		}
		batches = batches[:0]
		select {
		case <-w.writer.closeCh:
			return
		case batch := <-w.batchCh:
			batches = append(batches, batch)
		}
		chLen := len(w.batchCh)
		for i := 0; i < chLen; i++ {
			batches = append(batches, <-w.batchCh)
		}
		hint := new(lockstore.Hint)
		var delCnt, insertCnt int
		for _, batch := range batches {
			for _, entry := range batch.entries {
				switch entry.UserMeta[0] {
				case mvcc.LockUserMetaDeleteByte:
					delCnt++
					// Ignore if the key doesn't exist
					ls.DeleteWithHint(entry.Key.UserKey, hint)
				default:
					insertCnt++
					ls.PutWithHint(entry.Key.UserKey, entry.Value, hint)
				}
			}
			batch.wg.Done()
		}
	}
}

type dbWriter struct {
	bundle   *mvcc.DBBundle
	dbCh     chan<- *writeDBBatch
	lockCh   chan<- *writeLockBatch
	wg       sync.WaitGroup
	closeCh  chan struct{}
	latestTS uint64
}

// NewDBWriter returns a new DBWriter.
func NewDBWriter(bundle *mvcc.DBBundle) mvcc.DBWriter {
	return &dbWriter{
		bundle:  bundle,
		closeCh: make(chan struct{}),
	}
}

func (writer *dbWriter) Open() {
	writer.wg.Add(2)

	dbCh := make(chan *writeDBBatch, batchChanSize)
	writer.dbCh = dbCh
	go writeDBWorker{
		batchCh: dbCh,
		writer:  writer,
	}.run()

	lockCh := make(chan *writeLockBatch, batchChanSize)
	writer.lockCh = lockCh
	go writeLockWorker{
		batchCh: lockCh,
		writer:  writer,
	}.run()
}

func (writer *dbWriter) Close() {
	close(writer.closeCh)
	writer.wg.Wait()
}

func (writer *dbWriter) Write(batch mvcc.WriteBatch) error {
	wb := batch.(*writeBatch)
	if len(wb.dbBatch.entries) > 0 {
		wb.dbBatch.wg.Add(1)
		writer.dbCh <- &wb.dbBatch
		wb.dbBatch.wg.Wait()
		err := wb.dbBatch.err
		if err != nil {
			return err
		}
	}
	if len(wb.lockBatch.entries) > 0 {
		// We must delete lock after commit succeed, or there will be inconsistency.
		wb.lockBatch.wg.Add(1)
		writer.lockCh <- &wb.lockBatch
		wb.lockBatch.wg.Wait()
		return wb.lockBatch.err
	}
	return nil
}

type writeBatch struct {
	startTS   uint64
	commitTS  uint64
	dbBatch   writeDBBatch
	lockBatch writeLockBatch
}

func (wb *writeBatch) Prewrite(key []byte, lock *mvcc.Lock) {
	wb.lockBatch.set(key, lock.MarshalBinary())
}

func (wb *writeBatch) Commit(key []byte, lock *mvcc.Lock) {
	userMeta := mvcc.NewDBUserMeta(wb.startTS, wb.commitTS)
	k := y.KeyWithTs(key, wb.commitTS)
	if lock.Op != uint8(kvrpcpb.Op_Lock) {
		wb.dbBatch.set(k, lock.Value, userMeta)
	} else if bytes.Equal(key, lock.Primary) {
		opLockKey := y.KeyWithTs(mvcc.EncodeExtraTxnStatusKey(key, wb.startTS), wb.startTS)
		wb.dbBatch.set(opLockKey, nil, userMeta)
	}
	wb.lockBatch.delete(key)
}

func (wb *writeBatch) Rollback(key []byte, deleteLock bool) {
	rollbackKey := y.KeyWithTs(mvcc.EncodeExtraTxnStatusKey(key, wb.startTS), wb.startTS)
	userMeta := mvcc.NewDBUserMeta(wb.startTS, 0)
	wb.dbBatch.set(rollbackKey, nil, userMeta)
	if deleteLock {
		wb.lockBatch.delete(key)
	}
}

func (wb *writeBatch) PessimisticLock(key []byte, lock *mvcc.Lock) {
	wb.lockBatch.set(key, lock.MarshalBinary())
}

func (wb *writeBatch) PessimisticRollback(key []byte) {
	wb.lockBatch.delete(key)
}

func (writer *dbWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	if commitTS > 0 {
		writer.updateLatestTS(commitTS)
	} else {
		writer.updateLatestTS(startTS)
	}
	return &writeBatch{
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *dbWriter) getLatestTS() uint64 {
	return atomic.LoadUint64(&writer.latestTS)
}

func (writer *dbWriter) updateLatestTS(ts uint64) {
	latestTS := writer.getLatestTS()
	if ts != math.MaxUint64 && ts > latestTS {
		atomic.CompareAndSwapUint64(&writer.latestTS, latestTS, ts)
	}
}

const delRangeBatchSize = 4096

func (writer *dbWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	keys := make([]y.Key, 0, delRangeBatchSize)
	txn := writer.bundle.DB.NewTransaction(false)
	defer txn.Discard()
	reader := dbreader.NewDBReader(startKey, endKey, txn)
	keys = writer.collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
	reader.Close()
	return writer.deleteKeysInBatch(latchHandle, keys, delRangeBatchSize)
}

func (writer *dbWriter) collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys []y.Key) []y.Key {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, y.KeyWithTs(key, item.Version()))
	}
	return keys
}

func (writer *dbWriter) deleteKeysInBatch(latchHandle mvcc.LatchHandle, keys []y.Key, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		hashVals := userKeysToHashVals(batchKeys...)
		dbBatch := newWriteDBBatch()
		for _, key := range batchKeys {
			key.Version++
			dbBatch.delete(key)
		}
		latchHandle.AcquireLatches(hashVals)
		dbBatch.wg.Add(1)
		writer.dbCh <- dbBatch
		dbBatch.wg.Wait()
		latchHandle.ReleaseLatches(hashVals)
		if dbBatch.err != nil {
			return dbBatch.err
		}
	}
	return nil
}
