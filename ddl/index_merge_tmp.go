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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// IsEnableFastReorg check whether Fast Reorg is allowed.
func IsEnableFastReorg() bool {
	return variable.EnableFastReorg.Load()
}

func (w *mergeIndexWorker) batchCheckTemporaryUniqueKey(txn kv.Transaction, idxRecords []*temporaryIndexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need no check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	batchVals, err := txn.BatchGet(context.Background(), w.originIdxKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.originIdxKeys {
		if val, found := batchVals[string(key)]; found {
			if idxRecords[i].distinct && !bytes.Equal(val, idxRecords[i].vals) {
				return kv.ErrKeyExists
			}
			if !idxRecords[i].delete {
				idxRecords[i].skip = true
			}
		} else if idxRecords[i].distinct {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = idxRecords[i].vals
		}
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
}

type mergeIndexWorker struct {
	*backfillWorker

	index table.Index

	tmpIdxRecords []*temporaryIndexRecord
	originIdxKeys []kv.Key
	tmpIdxKeys    []kv.Key
	jobContext    *JobContext
}

func newMergeTempIndexWorker(sessCtx sessionctx.Context, id int, t table.PhysicalTable, reorgInfo *reorgInfo, jc *JobContext) *mergeIndexWorker {
	indexInfo := model.FindIndexInfoByID(t.Meta().Indices, reorgInfo.currElement.ID)

	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)

	return &mergeIndexWorker{
		backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo, typeAddIndexMergeTmpWorker),
		index:          index,
		jobContext:     jc,
	}
}

// BackfillDataInTxn merge temp index data in txn.
func (w *mergeIndexWorker) BackfillDataInTxn(taskRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

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
	logSlowOperations(time.Since(oprStartTime), "AddIndexMergeDataInTxn", 3000)
	return
}

func (w *mergeIndexWorker) AddMetricInfo(cnt float64) {
}

func (w *mergeIndexWorker) fetchTempIndexVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	w.tmpIdxRecords = w.tmpIdxRecords[:0]
	w.tmpIdxKeys = w.tmpIdxKeys[:0]
	w.originIdxKeys = w.originIdxKeys[:0]
	// taskDone means that the merged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	idxPrefix := w.table.IndexPrefix()
	var lastKey kv.Key
	err := iterateSnapshotKeys(w.reorgInfo.d.jobContext(w.reorgInfo.Job), w.sessCtx.GetStore(), w.priority, idxPrefix, txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(_ kv.Handle, indexKey kv.Key, rawValue []byte) (more bool, err error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterate temporary index in merge process", 0)
			oprStartTime = oprEndTime

			if taskRange.endInclude {
				taskDone = indexKey.Cmp(taskRange.endKey) > 0
			} else {
				taskDone = indexKey.Cmp(taskRange.endKey) >= 0
			}

			if taskDone || len(w.tmpIdxRecords) >= w.batchCnt {
				return false, nil
			}

			isDelete := false
			unique := false
			length := len(rawValue)
			keyVer := rawValue[length-1]
			if keyVer == tables.TempIndexKeyTypeMerge {
				// The kv is written in the merging state. It has been written to the origin index, we can skip it.
				return true, nil
			}
			rawValue = rawValue[:length-1]
			if bytes.Equal(rawValue, tables.DeleteMarker) {
				isDelete = true
			} else if bytes.Equal(rawValue, tables.DeleteMarkerUnique) {
				isDelete = true
				unique = true
			}

			originIdxKey := make([]byte, len(indexKey))
			copy(originIdxKey, indexKey)
			tablecodec.TempIndexKey2IndexKey(w.index.Meta().ID, originIdxKey)

			idxRecord := &temporaryIndexRecord{
				delete: isDelete,
				unique: unique,
				skip:   false,
			}
			if !isDelete {
				idxRecord.vals = rawValue
				idxRecord.distinct = tablecodec.IndexKVIsUnique(rawValue)
			}
			w.tmpIdxRecords = append(w.tmpIdxRecords, idxRecord)
			w.originIdxKeys = append(w.originIdxKeys, originIdxKey)
			w.tmpIdxKeys = append(w.tmpIdxKeys, indexKey)
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

	logutil.BgLogger().Debug("[ddl] merge temp index txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.tmpIdxRecords, nextKey.Next(), taskDone, errors.Trace(err)
}
