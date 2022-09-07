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
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	telemetryAddIndexLightningUsage = metrics.TelemetryAddIndexLightningCnt
)

// IsEnableFastReorg check whether Fast Reorg is allowed.
func IsEnableFastReorg() bool {
	// Only when both TiDBDDLEnableFastReorg is set to on and Lightning env has been inited successful,
	// the add index could choose lightning path to do backfill procedure.
	if variable.EnableFastReorg.Load() {
		// Increase telemetryAddIndexLightningUsage
		telemetryAddIndexLightningUsage.Inc()
		return true
	}
	return false
}

// Check if PiTR is enabled in cluster.
func isPiTREnable(w *worker) bool {
	ctx, err := w.sessPool.get()
	if err != nil {
		return true
	}
	defer w.sessPool.put(ctx)
	failpoint.Inject("EnablePiTR", func() {
		logutil.BgLogger().Info("Lightning: Failpoint enable PiTR.")
		failpoint.Return(true)
	})
	return utils.CheckLogBackupEnabled(ctx)
}

// importIndexDataToStore import local index sst file into TiKV.
func importIndexDataToStore(jobID int64, indexID int64, unique bool, tbl table.Table) error {
	if bc, ok := ingest.LitBackCtxMgr.Load(jobID); ok {
		err := bc.FinishImport(indexID, unique, tbl)
		if err != nil {
			err = errors.Trace(err)
			return err
		}
	}
	return nil
}

func (w *mergeIndexWorker) batchCheckTemporaryUniqueKey(txn kv.Transaction, idxRecords []*temporaryIndexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	if len(w.idxKeyBufs) < w.batchCnt {
		w.idxKeyBufs = make([][]byte, w.batchCnt)
	}
	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]

	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		distinct := false
		if !record.delete && tablecodec.IndexKVIsUnique(record.vals) {
			distinct = true
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = record.key

		w.batchCheckKeys = append(w.batchCheckKeys, record.key)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := txn.BatchGet(context.Background(), w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				if !bytes.Equal(val, idxRecords[i].vals) {
					return kv.ErrKeyExists
				}
			}
			if !idxRecords[i].delete {
				idxRecords[i].skip = true
			}
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = idxRecords[i].vals
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// temporaryIndexRecord is the record information of an index.
type temporaryIndexRecord struct {
	key    []byte
	vals   []byte
	skip   bool // skip indicates that the index key is already exists, we should not add it.
	delete bool
	unique bool
	keyVer []byte
}
type mergeIndexWorker struct {
	*backfillWorker

	index table.Index

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
	tmpIdxRecords      []*temporaryIndexRecord
	batchCheckTmpKeys  []kv.Key
	jobContext         *JobContext
	skipAll            bool
	firstVal           []byte
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

		temporaryIndexRecords, nextKey, taskDone, err := w.fetchTempIndexVals(txn, taskRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckTemporaryUniqueKey(txn, temporaryIndexRecords)
		if err != nil {
			return errors.Trace(err)
		}

		endPos := len(temporaryIndexRecords)
		for i, idxRecord := range temporaryIndexRecords {
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			// If all batch are skipped, update first index key to make txn commit to release lock.
			if idxRecord.skip && !w.skipAll {
				continue
			}
			if w.skipAll {
				isDelete := false
				unique := false
				length := len(w.firstVal)
				w.firstVal = w.firstVal[:length-1]
				length--

				if bytes.Equal(w.firstVal, []byte("delete")) {
					isDelete = true
					w.firstVal = w.firstVal[:length-6]
				} else if bytes.Equal(w.firstVal, []byte("deleteu")) {
					isDelete = true
					unique = true
					w.firstVal = w.firstVal[:length-7]
				}
				if isDelete {
					if unique {
						err = txn.GetMemBuffer().DeleteWithFlags(w.batchCheckTmpKeys[0], kv.SetNeedLocked)
					} else {
						err = txn.GetMemBuffer().Delete(w.batchCheckTmpKeys[0])
					}
				} else {
					// Set latest key/val back to temp index.
					err = txn.GetMemBuffer().Set(w.batchCheckTmpKeys[0], w.firstVal)
				}
				if err != nil {
					return err
				}
				break
			}

			if idxRecord.delete {
				if idxRecord.unique {
					err = txn.GetMemBuffer().DeleteWithFlags(idxRecord.key, kv.SetNeedLocked)
				} else {
					err = txn.GetMemBuffer().Delete(idxRecord.key)
				}
			} else {
				err = txn.GetMemBuffer().Set(idxRecord.key, idxRecord.vals)
				// If the merge key is batch end should be deleted in temp index to avoid
				// merge twice when do parallel merge procession.
				if i == endPos-1 {
					if idxRecord.unique {
						err = txn.GetMemBuffer().DeleteWithFlags(w.batchCheckTmpKeys[i], kv.SetNeedLocked)
					} else {
						err = txn.GetMemBuffer().Delete(w.batchCheckTmpKeys[i])
					}
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexMergeDataInTxn", 3000)
	return
}

func (w *mergeIndexWorker) AddMetricInfo(cnt float64) {
}

// mergeTempIndex handles the merge temp index state for a table.
func (w *worker) mergeTempIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.mergePhysicalTempIndex(p, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateMergeInfo(tbl, idx.ID, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.mergePhysicalTempIndex(t.(table.PhysicalTable), reorgInfo)
	}
	return errors.Trace(err)
}

// updateMergeInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateMergeInfo(t table.PartitionedTable, idxID int64, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		logutil.BgLogger().Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	start, end := tablecodec.GetTableIndexKeyRange(pid, tablecodec.TempIndexPrefix|idxID)

	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid
	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.BgLogger().Info("[ddl] job update MergeInfo", zap.Int64("jobID", reorg.Job.ID),
		zap.ByteString("elementType", reorg.currElement.TypeKey), zap.Int64("elementID", reorg.currElement.ID),
		zap.Int64("partitionTableID", pid), zap.String("startHandle", tryDecodeToHandleString(start)),
		zap.String("endHandle", tryDecodeToHandleString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

func (w *worker) mergePhysicalTempIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to merge temp index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
}

func (w *mergeIndexWorker) fetchTempIndexVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	w.tmpIdxRecords = w.tmpIdxRecords[:0]
	w.batchCheckTmpKeys = w.batchCheckTmpKeys[:0]
	// taskDone means that the merged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	idxPrefix := w.table.IndexPrefix()
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
			var keyVer []byte
			length := len(rawValue)
			keyVer = append(keyVer, rawValue[length-1:]...)
			rawValue = rawValue[:length-1]
			length--
			// Just skip it.
			if bytes.Equal(keyVer, []byte("m")) {
				return true, nil
			}
			if bytes.Equal(rawValue, []byte("delete")) {
				isDelete = true
				rawValue = rawValue[:length-6]
			} else if bytes.Equal(rawValue, []byte("deleteu")) {
				isDelete = true
				unique = true
				rawValue = rawValue[:length-7]
			}
			var convertedIndexKey []byte
			convertedIndexKey = append(convertedIndexKey, indexKey...)
			tablecodec.TempIndexKey2IndexKey(w.index.Meta().ID, convertedIndexKey)
			idxRecord := &temporaryIndexRecord{key: convertedIndexKey, delete: isDelete, unique: unique, keyVer: keyVer, skip: false}
			if !isDelete {
				idxRecord.vals = rawValue
			}
			w.tmpIdxRecords = append(w.tmpIdxRecords, idxRecord)
			w.batchCheckTmpKeys = append(w.batchCheckTmpKeys, indexKey)
			return true, nil
		})

	if len(w.tmpIdxRecords) == 0 {
		taskDone = true
	}
	var nextKey kv.Key
	if taskDone {
		nextKey = taskRange.endKey
	} else {
		var convertedNextKey []byte
		lastPos := len(w.tmpIdxRecords)
		convertedNextKey = append(convertedNextKey, w.tmpIdxRecords[lastPos-1].key...)
		tablecodec.IndexKey2TempIndexKey(w.index.Meta().ID, convertedNextKey)
		nextKey = convertedNextKey
	}

	logutil.BgLogger().Debug("[ddl] merge temp index txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.tmpIdxRecords, nextKey.Next(), taskDone, errors.Trace(err)
}
