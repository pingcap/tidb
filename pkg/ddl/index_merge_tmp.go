// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	driver "github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (w *mergeIndexWorker) batchCheckTemporaryUniqueKey(
	txn kv.Transaction,
	idxRecords []*temporaryIndexRecord,
) error {
	if !w.currentIndex.Unique {
		// non-unique key need no check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	batchVals, err := txn.BatchGet(context.Background(), w.originIdxKeys)
	if err != nil {
		return errors.Trace(err)
	}

	for i, key := range w.originIdxKeys {
		if val, found := batchVals[string(key)]; found {
			// Found a value in the original index key.
			err := checkTempIndexKey(txn, idxRecords[i], val, w.table)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) {
					return driver.ExtractKeyExistsErrFromIndex(key, val, w.table.Meta(), w.currentIndex.ID)
				}
				return errors.Trace(err)
			}
		} else if idxRecords[i].distinct {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = idxRecords[i].vals
		}
	}
	return nil
}

func checkTempIndexKey(txn kv.Transaction, tmpRec *temporaryIndexRecord, originIdxVal []byte, tblInfo table.Table) error {
	if !tmpRec.delete {
		if tmpRec.distinct && !bytes.Equal(originIdxVal, tmpRec.vals) {
			return kv.ErrKeyExists
		}
		// The key has been found in the original index, skip merging it.
		tmpRec.skip = true
		return nil
	}
	// Delete operation.
	distinct := tablecodec.IndexKVIsUnique(originIdxVal)
	if !distinct {
		// For non-distinct key, it is consist of a null value and the handle.
		// Same as the non-unique indexes, replay the delete operation on non-distinct keys.
		return nil
	}
	// For distinct index key values, prevent deleting an unexpected index KV in original index.
	hdInVal, err := tablecodec.DecodeHandleInIndexValue(originIdxVal)
	if err != nil {
		return errors.Trace(err)
	}
	if !tmpRec.handle.Equal(hdInVal) {
		// The inequality means multiple modifications happened in the same key.
		// We use the handle in origin index value to check if the row exists.
		rowKey := tablecodec.EncodeRecordKey(tblInfo.RecordPrefix(), hdInVal)
		_, err := txn.Get(context.Background(), rowKey)
		if err != nil {
			if kv.IsErrNotFound(err) {
				// The row is deleted, so we can merge the delete operation to the origin index.
				tmpRec.skip = false
				return nil
			}
			// Unexpected errors.
			return errors.Trace(err)
		}
		// Don't delete the index key if the row exists.
		tmpRec.skip = true
		return nil
	}
	return nil
}

// temporaryIndexRecord is the record information of an index.
type temporaryIndexRecord struct {
	vals     []byte
	skip     bool // skip indicates that the index key is already exists, we should not add it.
	delete   bool
	unique   bool
	distinct bool
	handle   kv.Handle
}

type mergeIndexWorker struct {
	*backfillCtx

	indexes []table.Index

	tmpIdxRecords []*temporaryIndexRecord
	originIdxKeys []kv.Key
	tmpIdxKeys    []kv.Key

	needValidateKey        bool
	currentTempIndexPrefix []byte
	currentIndex           *model.IndexInfo
}

func newMergeTempIndexWorker(bfCtx *backfillCtx, t table.PhysicalTable, elements []*meta.Element) *mergeIndexWorker {
	allIndexes := make([]table.Index, 0, len(elements))
	for _, elem := range elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		allIndexes = append(allIndexes, index)
	}

	return &mergeIndexWorker{
		backfillCtx: bfCtx,
		indexes:     allIndexes,
	}
}

func (w *mergeIndexWorker) validateTaskRange(taskRange *reorgBackfillTask) (skip bool, err error) {
	tmpID, err := tablecodec.DecodeIndexID(taskRange.startKey)
	if err != nil {
		return false, err
	}
	startIndexID := tmpID & tablecodec.IndexIDMask
	tmpID, err = tablecodec.DecodeIndexID(taskRange.endKey)
	if err != nil {
		return false, err
	}
	endIndexID := tmpID & tablecodec.IndexIDMask

	w.needValidateKey = startIndexID != endIndexID
	containsTargetID := false
	for _, idx := range w.indexes {
		idxInfo := idx.Meta()
		if idxInfo.ID == startIndexID {
			containsTargetID = true
			w.currentIndex = idxInfo
			break
		}
		if idxInfo.ID == endIndexID {
			containsTargetID = true
		}
	}
	return !containsTargetID, nil
}

// BackfillData merge temp index data in txn.
func (w *mergeIndexWorker) BackfillData(taskRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	skip, err := w.validateTaskRange(&taskRange)
	if skip || err != nil {
		return taskCtx, err
	}

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)

	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, taskRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(taskRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		tmpIdxRecords, nextKey, taskDone, err := w.fetchTempIndexVals(txn, taskRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckTemporaryUniqueKey(txn, tmpIdxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for i, idxRecord := range tmpIdxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			// If all batch are skipped, update first index key to make txn commit to release lock.
			if idxRecord.skip {
				continue
			}

			// Lock the corresponding row keys so that it doesn't modify the index KVs
			// that are changing by a pessimistic transaction.
			rowKey := tablecodec.EncodeRecordKey(w.table.RecordPrefix(), idxRecord.handle)
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), rowKey)
			if err != nil {
				return errors.Trace(err)
			}

			if idxRecord.delete {
				if idxRecord.unique {
					err = txn.GetMemBuffer().DeleteWithFlags(w.originIdxKeys[i], kv.SetNeedLocked)
				} else {
					err = txn.GetMemBuffer().Delete(w.originIdxKeys[i])
				}
			} else {
				err = txn.GetMemBuffer().Set(w.originIdxKeys[i], idxRecord.vals)
			}
			if err != nil {
				return err
			}
			taskCtx.addedCount++
		}
		return nil
	})

	failpoint.Inject("mockDMLExecutionMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionMerging != nil {
			MockDMLExecutionMerging()
		}
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexMergeDataInTxn", 3000)
	return
}

func (*mergeIndexWorker) AddMetricInfo(float64) {
}

func (*mergeIndexWorker) String() string {
	return typeAddIndexMergeTmpWorker.String()
}

func (w *mergeIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *mergeIndexWorker) prefixIsChanged(newKey kv.Key) bool {
	return len(w.currentTempIndexPrefix) == 0 || !bytes.HasPrefix(newKey, w.currentTempIndexPrefix)
}

func (w *mergeIndexWorker) updateCurrentIndexInfo(newIndexKey kv.Key) (skip bool, err error) {
	tempIdxID, err := tablecodec.DecodeIndexID(newIndexKey)
	if err != nil {
		return false, err
	}
	idxID := tablecodec.IndexIDMask & tempIdxID
	var curIdx *model.IndexInfo
	for _, idx := range w.indexes {
		if idx.Meta().ID == idxID {
			curIdx = idx.Meta()
		}
	}
	if curIdx == nil {
		// Index IDs are always increasing, but not always continuous:
		// if DDL adds another index between these indexes, it is possible that:
		//   multi-schema add index IDs = [1, 2, 4, 5]
		//   another index ID = [3]
		// If the new index get rollback, temp index 0xFFxxx03 may have dirty records.
		// We should skip these dirty records.
		return true, nil
	}
	pfx := tablecodec.CutIndexPrefix(newIndexKey)

	w.currentTempIndexPrefix = kv.Key(pfx).Clone()
	w.currentIndex = curIdx

	return false, nil
}

func (w *mergeIndexWorker) fetchTempIndexVals(
	txn kv.Transaction,
	taskRange reorgBackfillTask,
) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	w.tmpIdxRecords = w.tmpIdxRecords[:0]
	w.tmpIdxKeys = w.tmpIdxKeys[:0]
	w.originIdxKeys = w.originIdxKeys[:0]
	// taskDone means that the merged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	idxPrefix := w.table.IndexPrefix()
	var lastKey kv.Key
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, idxPrefix, txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(_ kv.Handle, indexKey kv.Key, rawValue []byte) (more bool, err error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterate temporary index in merge process", 0)
			oprStartTime = oprEndTime

			taskDone = indexKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.tmpIdxRecords) >= w.batchCnt {
				return false, nil
			}

			if w.needValidateKey && w.prefixIsChanged(indexKey) {
				skip, err := w.updateCurrentIndexInfo(indexKey)
				if err != nil || skip {
					return skip, err
				}
			}

			tempIdxVal, err := tablecodec.DecodeTempIndexValue(rawValue)
			if err != nil {
				return false, err
			}
			tempIdxVal, err = decodeTempIndexHandleFromIndexKV(indexKey, tempIdxVal, len(w.currentIndex.Columns))
			if err != nil {
				return false, err
			}

			tempIdxVal = tempIdxVal.FilterOverwritten()

			// Extract the operations on the original index and replay them later.
			for _, elem := range tempIdxVal {
				if elem.KeyVer == tables.TempIndexKeyTypeMerge || elem.KeyVer == tables.TempIndexKeyTypeDelete {
					// For 'm' version kvs, they are double-written.
					// For 'd' version kvs, they are written in the delete-only state and can be dropped safely.
					continue
				}

				originIdxKey := make([]byte, len(indexKey))
				copy(originIdxKey, indexKey)
				tablecodec.TempIndexKey2IndexKey(originIdxKey)

				idxRecord := &temporaryIndexRecord{
					handle: elem.Handle,
					delete: elem.Delete,
					unique: elem.Distinct,
					skip:   false,
				}
				if !elem.Delete {
					idxRecord.vals = elem.Value
					idxRecord.distinct = tablecodec.IndexKVIsUnique(elem.Value)
				}
				w.tmpIdxRecords = append(w.tmpIdxRecords, idxRecord)
				w.originIdxKeys = append(w.originIdxKeys, originIdxKey)
				w.tmpIdxKeys = append(w.tmpIdxKeys, indexKey)
			}

			lastKey = indexKey
			return true, nil
		})

	if len(w.tmpIdxRecords) == 0 {
		taskDone = true
	}
	var nextKey kv.Key
	if taskDone {
		nextKey = taskRange.endKey
	} else {
		nextKey = lastKey
	}

	logutil.DDLLogger().Debug("merge temp index txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.tmpIdxRecords, nextKey.Next(), taskDone, errors.Trace(err)
}

func decodeTempIndexHandleFromIndexKV(indexKey kv.Key, tmpVal tablecodec.TempIndexValue, idxColLen int) (ret tablecodec.TempIndexValue, err error) {
	for _, elem := range tmpVal {
		if elem.Handle == nil {
			// If the handle is not found in the value of the temp index, it means
			// 1) This is not a deletion marker, the handle is in the key or the origin value.
			// 2) This is a deletion marker, but the handle is in the key of temp index.
			elem.Handle, err = tablecodec.DecodeIndexHandle(indexKey, elem.Value, idxColLen)
			if err != nil {
				return nil, err
			}
		}
	}
	return tmpVal, nil
}
