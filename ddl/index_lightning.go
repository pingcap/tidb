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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	BackfillProgressPercent float64 = 0.6
)

// Whether Fast DDl is allowed.
func IsAllowFastDDL() bool {
	// Only when both TiDBFastDDL is set to on and Lightning env is inited successful,
	// the add index could choose lightning path to do backfill procedure.
	// ToDo: need check PiTR is off currently.
	if variable.FastDDL.Load() && lit.GlobalLightningEnv.IsInited {
		return true
	} else {
		return false
	}
}

// Check if PiTR is enable in cluster.
func isPiTREnable(w *worker) bool {
	var (
		ctx    sessionctx.Context
		valStr string = "show config where name = 'log-backup.enable'"
		err    error
		retVal bool = false
	)
	ctx, err = w.sessPool.get()
	if err != nil {
		return true
	}
	defer w.sessPool.put(ctx)
	rows, fields, errSQL := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(context.Background(), nil, valStr)
	if errSQL != nil {
		return true
	}
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		d := row.GetDatum(3, &fields[3].Column.FieldType)
		value, errField := d.ToString()
		if errField != nil {
			return true
		}
		if value == "true" {
			retVal = true
			break
		}
	}
	return retVal
}

func isLightningEnabled(id int64) bool {
	return lit.IsEngineLightningBackfill(id)
}

func setLightningEnabled(id int64, value bool) {
	lit.SetEnable(id, value)
}

func needRestoreJob(id int64) bool {
	return lit.NeedRestore(id)
}

func setNeedRestoreJob(id int64, value bool) {
	lit.SetNeedRestore(id, value)
}

func prepareBackend(ctx context.Context, unique bool, job *model.Job, sqlMode mysql.SQLMode) (err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	// Create and regist backend of lightning
	err = lit.GlobalLightningEnv.LitMemRoot.RegistBackendContext(ctx, unique, bcKey, sqlMode)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
		return err
	}

	return err
}

func prepareLightningEngine(job *model.Job, indexId int64, workerCnt int) (wCnt int, err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	enginKey := lit.GenEngineInfoKey(job.ID, indexId)
	wCnt, err = lit.GlobalLightningEnv.LitMemRoot.RegistEngineInfo(job, bcKey, enginKey, int32(indexId), workerCnt)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
	return wCnt, err
}

// Import local index sst file into TiKV.
func importIndexDataToStore(ctx context.Context, reorg *reorgInfo, indexId int64, unique bool, tbl table.Table) error {
	if isLightningEnabled(reorg.ID) && needRestoreJob(reorg.ID) {
		engineInfoKey := lit.GenEngineInfoKey(reorg.ID, indexId)
		// just log info.
		err := lit.FinishIndexOp(ctx, engineInfoKey, tbl, unique)
		if err != nil {
			err = errors.Trace(err)
			return err
		}
		// After import local data into TiKV, then the progress set to 85.
		metrics.GetBackfillProgressByLabel(metrics.LblAddIndex).Set(85)
	}
	return nil
}

// Used to clean backend,
func cleanUpLightningEnv(reorg *reorgInfo, isCanceled bool, indexIds ...int64) {
	if isLightningEnabled(reorg.ID) {
		bcKey := lit.GenBackendContextKey(reorg.ID)
		// If reorg is cancled, need do clean up engine.
		if isCanceled {
			lit.GlobalLightningEnv.LitMemRoot.ClearEngines(reorg.ID, indexIds...)
		}
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
}

// Disk quota checking and ingest data.
func importPartialDataToTiKV(jobId int64, indexIds int64) error {
	return lit.UnsafeImportEngineData(jobId, indexIds)
}

// Check if this reorg is a restore reorg task
// Check if current lightning reorg task can be executed continuely.
// Otherwise, restart the reorg task.
func canRestoreReorgTask(job *model.Job, indexId int64) bool {
	// The reorg just start, do nothing
	if job.SnapshotVer == 0 {
		return false
	}

	// Check if backend and engine are cached.
	if !lit.CanRestoreReorgTask(job.ID, indexId) {
		job.SnapshotVer = 0
		return false
	}
	return true
}

// Below is lightning worker implement
type addIndexWorkerLit struct {
	addIndexWorker

	// Lightning related variable.
	writerContex *lit.WorkerContext
}

func newAddIndexWorkerLit(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jobId int64) (*addIndexWorkerLit, error) {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap, sessCtx)
	// ToDo: Bear Currently, all the lightning worker share one openengine.
	engineInfoKey := lit.GenEngineInfoKey(jobId, indexInfo.ID)

	lwCtx, err := lit.GlobalLightningEnv.LitMemRoot.RegistWorkerContext(engineInfoKey, id)
	if err != nil {
		return nil, err
	}
	// Add build openengine process.
	return &addIndexWorkerLit{
		addIndexWorker: addIndexWorker{
			baseIndexWorker: baseIndexWorker{
				backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo),
				indexes:        []table.Index{index},
				rowDecoder:     rowDecoder,
				defaultVals:    make([]types.Datum, len(t.WritableCols())),
				rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
				metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_rate"),
				sqlMode:        reorgInfo.ReorgMeta.SQLMode,
			},
			index: index,
		},
		writerContex: lwCtx,
	}, err
}

// BackfillDataInTxn will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexWorkerLit) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	fetchTag := "AddIndexLightningFetchdata" + strconv.Itoa(w.id)
	writeTag := "AddIndexLightningWritedata" + strconv.Itoa(w.id)
	txnTag := "AddIndexLightningBackfillDataInTxn" + strconv.Itoa(w.id)

	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		logSlowOperations(time.Since(oprStartTime), fetchTag, 1000)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// Create the index.
			key, idxVal, _, err := w.index.Create4SST(w.sessCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion)
			if err != nil {
				return errors.Trace(err)
			}
			// Currently, only use one kVCache, later may use multi kvCache to parallel compute/io performance.
			w.writerContex.WriteRow(key, idxVal)

			taskCtx.addedCount++
		}
		logSlowOperations(time.Since(oprStartTime), writeTag, 1000)
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), txnTag, 3000)
	return
}

func (w *backFillIndexWorker) batchCheckTemporaryUniqueKey(txn kv.Transaction, idxRecords []*temporaryIndexRecord) error {
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
			idxRecords[i].skip = true
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

type backFillIndexWorker struct {
	*backfillWorker

	index table.Index

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

func newTempIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) *backFillIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)

	// Add build openengine process.
	return &backFillIndexWorker{
		backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo),
		index:          index,
	}
}

func (w *backFillIndexWorker) BackfillDataInTxn(taskRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	logutil.BgLogger().Info("Merge temp index", zap.ByteString("startKey", taskRange.startKey), zap.ByteString("endKey", taskRange.endKey))
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		temporaryIndexRecords, nextKey, taskDone, err := w.fetchTempIndexVals(txn, taskRange)

		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckTemporaryUniqueKey(txn, temporaryIndexRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range temporaryIndexRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// We need to add this lock to make sure pessimistic transaction can realize this operation.
			// For the normal pessimistic transaction, it's ok. But if async commmit is used, it may lead to inconsistent data and index.
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			if idxRecord.delete {
				if idxRecord.unique {
					err = txn.GetMemBuffer().DeleteWithFlags(idxRecord.key, kv.SetNeedLocked)
				} else {
					err = txn.GetMemBuffer().Delete(idxRecord.key)
				}
				logutil.BgLogger().Info("delete", zap.ByteString("key", idxRecord.key))
			} else {
				err = txn.GetMemBuffer().Set(idxRecord.key, idxRecord.vals)
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

func (w *backFillIndexWorker) AddMetricInfo(cnt float64) {
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) mergeTempIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTempIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateReorgInfo(tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalTempIndex(t.(table.PhysicalTable), idx, reorgInfo)
	}
	return errors.Trace(err)
}

func (w *worker) addPhysicalTempIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to merge temp index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writeTempIndexRecord(t, typeAddIndexWorker, indexInfo, nil, nil, reorgInfo)
}

func (w *backFillIndexWorker) fetchTempIndexVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	temporaryIndexRecords := make([]*temporaryIndexRecord, 0, w.batchCnt)
	// taskDone means that the reorged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotIndexes(w.reorgInfo.d.jobContext(w.reorgInfo.Job), w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startKey, taskRange.endKey, func(indexKey kv.Key, rawValue []byte) (more bool, err error) {
		oprEndTime := time.Now()
		logSlowOperations(oprEndTime.Sub(oprStartTime), "iterate temporary index in merge process", 0)
		oprStartTime = oprEndTime

		taskDone := indexKey.Cmp(taskRange.endKey) > 0

		if taskDone || len(temporaryIndexRecords) >= w.batchCnt {
			logutil.BgLogger().Info("return false")
			return false, nil
		}

		isDelete := false
		unique := false
		if bytes.Equal(rawValue, []byte("delete")) {
			isDelete = true
		} else if bytes.Equal(rawValue, []byte("deleteu")) {
			isDelete = true
			unique = true
		}
		var convertedIndexKey []byte
		convertedIndexKey = append(convertedIndexKey, indexKey...)
		tablecodec.TempIndexKey2IndexKey(w.index.Meta().ID, convertedIndexKey)
		idxRecord := &temporaryIndexRecord{key: convertedIndexKey, delete: isDelete, unique: unique}
		if !isDelete {
			idxRecord.vals = rawValue
		}
		temporaryIndexRecords = append(temporaryIndexRecords, idxRecord)

		return true, nil
	})

	if len(temporaryIndexRecords) == 0 {
		taskDone = true
	}
    var nextKey kv.Key
	if taskDone {
		nextKey = taskRange.endKey
	} else {
		nextKey = temporaryIndexRecords[w.batchCnt-1].key
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return temporaryIndexRecords, nextKey.Next(), taskDone, errors.Trace(err)
}