// Copyright 2015 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	litconfig "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/size"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// indexRecord is the record information of an index.
type indexRecord struct {
	handle kv.Handle
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	rsData []types.Datum // It's the restored data for handle.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type baseIndexWorker struct {
	*backfillCtx
	indexes []table.Index

	tp backfillerType
	// The following attributes are used to reduce memory allocation.
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	rowMap      map[int64]types.Datum
	rowDecoder  *decoder.RowDecoder
}

type addIndexTxnWorker struct {
	baseIndexWorker

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	batchCheckValues   [][]byte
	distinctCheckFlags []bool
	recordIdx          []int
}

func newAddIndexTxnWorker(
	decodeColMap map[int64]decoder.Column,
	t table.PhysicalTable,
	bfCtx *backfillCtx,
	job *model.Job,
	elements []*meta.Element,
	currElement *meta.Element,
) (*addIndexTxnWorker, error) {
	if !bytes.Equal(currElement.TypeKey, meta.IndexElementKey) {
		logutil.DDLLogger().Error("Element type for addIndexTxnWorker incorrect",
			zap.Int64("job ID", job.ID), zap.ByteString("element type", currElement.TypeKey), zap.Int64("element ID", elements[0].ID))
		return nil, errors.Errorf("element type is not index, typeKey: %v", currElement.TypeKey)
	}

	allIndexes := make([]table.Index, 0, len(elements))
	for _, elem := range elements {
		if !bytes.Equal(elem.TypeKey, meta.IndexElementKey) {
			continue
		}
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		index, err := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		if err != nil {
			return nil, err
		}
		allIndexes = append(allIndexes, index)
	}
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)

	return &addIndexTxnWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bfCtx,
			indexes:     allIndexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *baseIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (w *baseIndexWorker) String() string {
	return w.tp.String()
}

func (w *baseIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

// mockNotOwnerErrOnce uses to make sure `notOwnerErr` only mock error once.
var mockNotOwnerErrOnce uint32

// getIndexRecord gets index columns values use w.rowDecoder, and generate indexRecord.
func (w *baseIndexWorker) getIndexRecord(idxInfo *model.IndexInfo, handle kv.Handle, recordKey []byte) (*indexRecord, error) {
	cols := w.table.WritableCols()
	failpoint.Inject("MockGetIndexRecordErr", func(val failpoint.Value) {
		if valStr, ok := val.(string); ok {
			switch valStr {
			case "cantDecodeRecordErr":
				failpoint.Return(nil, errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("index",
					errors.New("mock can't decode record error"))))
			case "modifyColumnNotOwnerErr":
				if idxInfo.Name.O == "_Idx$_idx_0" && handle.IntValue() == 7168 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 0, 1) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			case "addIdxNotOwnerErr":
				// For the case of the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
				// First step, we need to exit "addPhysicalTableIndex".
				if idxInfo.Name.O == "idx2" && handle.IntValue() == 6144 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 1, 2) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			}
		}
	})
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	var err error
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.exprCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		idxVal[j] = idxColumnVal
	}

	rsData := tables.TryGetHandleRestoredDataWrapper(w.table.Meta(), nil, w.rowMap, idxInfo)
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal, rsData: rsData}
	return idxRecord, nil
}

func (w *baseIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// getNextKey gets next key of entry that we are going to process.
func (w *baseIndexWorker) getNextKey(taskRange reorgBackfillTask, taskDone bool) (nextKey kv.Key) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		lastHandle := w.idxRecords[len(w.idxRecords)-1].handle
		recordKey := tablecodec.EncodeRecordKey(taskRange.physicalTable.RecordPrefix(), lastHandle)
		return recordKey.Next()
	}
	return taskRange.endKey
}

func (w *baseIndexWorker) updateRowDecoder(handle kv.Handle, rawRecord []byte) error {
	sysZone := w.loc
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.exprCtx, handle, rawRecord, sysZone, w.rowMap)
	return errors.Trace(err)
}

// fetchRowColVals fetch w.batchCnt count records that need to reorganize indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *baseIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the reorged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(), txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in baseIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			// Decode one row, generate records of this row.
			err := w.updateRowDecoder(handle, rawRow)
			if err != nil {
				return false, err
			}

			for _, index := range w.indexes {
				if index.Meta().HasCondition() {
					return false, dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg")
				}
				actualHandle := handle
				// For global indexes V1+ on partitioned tables, we need to wrap the handle
				// with the partition ID to create a PartitionHandle.
				// This is critical for non-clustered tables after EXCHANGE PARTITION,
				// where duplicate _tidb_rowid values exist across partitions.
				// Legacy indexes (version 0) don't use PartitionHandle in the key.
				if index.Meta().Global && index.Meta().GlobalIndexVersion >= model.GlobalIndexVersionV1 {
					actualHandle = kv.NewPartitionHandle(taskRange.physicalTable.GetPhysicalID(), handle)
				}
				idxRecord, err1 := w.getIndexRecord(index.Meta(), actualHandle, recordKey)
				if err1 != nil {
					return false, errors.Trace(err1)
				}
				w.idxRecords = append(w.idxRecords, idxRecord)
			}
			// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
			// the generated value, so we need to clear up the reusing map.
			w.cleanRowMap()

			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.DDLLogger().Debug("txn fetches handle info", zap.Stringer("worker", w), zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextKey(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexTxnWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.batchCheckValues = w.batchCheckValues[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
	w.recordIdx = w.recordIdx[:0]
}

func (w *addIndexTxnWorker) checkHandleExists(idxInfo *model.IndexInfo, key kv.Key, value []byte, handle kv.Handle) error {
	tblInfo := w.table.Meta()
	idxColLen := len(idxInfo.Columns)
	h, err := tablecodec.DecodeIndexHandle(key, value, idxColLen)
	if err != nil {
		return errors.Trace(err)
	}
	hasBeenBackFilled := h.Equal(handle)
	if hasBeenBackFilled {
		return nil
	}
	return ddlutil.GenKeyExistsErr(key, value, idxInfo, tblInfo)
}

// batchCheckUniqueKey checks the unique keys in the batch.
// Note that `idxRecords` may belong to multiple indexes.
func (w *addIndexTxnWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	w.initBatchCheckBufs(len(idxRecords))
	evalCtx := w.exprCtx.GetEvalCtx()
	ec := evalCtx.ErrCtx()
	uniqueBatchKeys := make([]kv.Key, 0, len(idxRecords))
	cnt := 0
	for i, record := range idxRecords {
		idx := w.indexes[i%len(w.indexes)]
		if !idx.Meta().Unique {
			// non-unique key need not to check, use `nil` as a placeholder to keep
			// `idxRecords[i]` belonging to `indexes[i%len(indexes)]`.
			w.batchCheckKeys = append(w.batchCheckKeys, nil)
			w.batchCheckValues = append(w.batchCheckValues, nil)
			w.distinctCheckFlags = append(w.distinctCheckFlags, false)
			w.recordIdx = append(w.recordIdx, 0)
			continue
		}
		// skip by default.
		idxRecords[i].skip = true
		iter := idx.GenIndexKVIter(ec, w.loc, record.vals, record.handle, idxRecords[i].rsData)
		for iter.Valid() {
			var buf []byte
			if cnt < len(w.idxKeyBufs) {
				buf = w.idxKeyBufs[cnt]
			}
			key, val, distinct, err := iter.Next(buf, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if cnt < len(w.idxKeyBufs) {
				w.idxKeyBufs[cnt] = key
			} else {
				w.idxKeyBufs = append(w.idxKeyBufs, key)
			}
			cnt++
			w.batchCheckKeys = append(w.batchCheckKeys, key)
			w.batchCheckValues = append(w.batchCheckValues, val)
			w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
			w.recordIdx = append(w.recordIdx, i)
			uniqueBatchKeys = append(uniqueBatchKeys, key)
		}
	}

	if len(uniqueBatchKeys) == 0 {
		return nil
	}

	batchVals, err := kv.BatchGetValue(context.Background(), txn, uniqueBatchKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if len(key) == 0 {
			continue
		}
		idx := w.indexes[i%len(w.indexes)]
		val, found := batchVals[string(key)]
		if found {
			if w.distinctCheckFlags[i] {
				if err := w.checkHandleExists(idx.Meta(), key, val, idxRecords[w.recordIdx[i]].handle); err != nil {
					return errors.Trace(err)
				}
			}
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = w.batchCheckValues[i]
		}
		idxRecords[w.recordIdx[i]].skip = found && idxRecords[w.recordIdx[i]].skip
	}
	return nil
}

func getLocalWriterConfig(indexCnt, writerCnt int) *backend.LocalWriterConfig {
	writerCfg := &backend.LocalWriterConfig{}
	// avoid unit test panic
	memRoot := ingest.LitMemRoot
	if memRoot == nil {
		return writerCfg
	}

	// leave some room for objects overhead
	availMem := memRoot.MaxMemoryQuota() - memRoot.CurrentUsage() - int64(10*size.MB)
	memLimitPerWriter := availMem / int64(indexCnt) / int64(writerCnt)
	memLimitPerWriter = min(memLimitPerWriter, litconfig.DefaultLocalWriterMemCacheSize)
	writerCfg.Local.MemCacheSize = memLimitPerWriter
	return writerCfg
}

func writeChunk(
	ctx context.Context,
	writers []ingest.Writer,
	indexes []table.Index,
	indexConditionCheckers []func(row chunk.Row) (bool, error),
	copCtx copr.CopContext,
	loc *time.Location,
	errCtx errctx.Context,
	writeStmtBufs *variable.WriteStmtBufs,
	copChunk *chunk.Chunk,
	tblInfo *model.TableInfo,
) (rowCnt int, bytes int, err error) {
	iter := chunk.NewIterator4Chunk(copChunk)
	c := copCtx.GetBase()
	ectx := c.ExprCtx.GetEvalCtx()

	maxIdxColCnt := maxIndexColumnCount(indexes)
	idxDataBuf := make([]types.Datum, maxIdxColCnt)
	handleDataBuf := make([]types.Datum, len(c.HandleOutputOffsets))
	var restoreDataBuf []types.Datum
	count := 0
	totalBytes := 0

	unlockFns := make([]func(), 0, len(writers))
	for _, w := range writers {
		unlock := w.LockForWrite()
		unlockFns = append(unlockFns, unlock)
	}
	defer func() {
		for _, unlock := range unlockFns {
			unlock()
		}
	}()
	needRestoreForIndexes := make([]bool, len(indexes))
	restore, pkNeedRestore := false, false
	if c.PrimaryKeyInfo != nil && c.TableInfo.IsCommonHandle && c.TableInfo.CommonHandleVersion != 0 {
		pkNeedRestore = tables.NeedRestoredData(c.PrimaryKeyInfo.Columns, c.TableInfo.Columns)
	}
	for i, index := range indexes {
		needRestore := pkNeedRestore || tables.NeedRestoredData(index.Meta().Columns, c.TableInfo.Columns)
		needRestoreForIndexes[i] = needRestore
		restore = restore || needRestore
	}
	if restore {
		restoreDataBuf = make([]types.Datum, len(c.HandleOutputOffsets))
	}

	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		handleDataBuf := ExtractDatumByOffsets(ectx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
		if restore {
			// restoreDataBuf should not truncate index values.
			for i, datum := range handleDataBuf {
				restoreDataBuf[i] = *datum.Clone()
			}
		}
		h, err := BuildHandle(handleDataBuf, c.TableInfo, c.PrimaryKeyInfo, loc, errCtx)
		if err != nil {
			return 0, totalBytes, errors.Trace(err)
		}
		for i, index := range indexes {
			// If the `IndexRecordChunk.conditionPushed` is true and we have only 1 index, the `indexConditionCheckers`
			// will not be initialized.
			if index.Meta().HasCondition() && indexConditionCheckers != nil {
				ok, err := indexConditionCheckers[i](row)
				if err != nil {
					return 0, 0, errors.Trace(err)
				}
				if !ok {
					continue
				}
			}

			idxID := index.Meta().ID
			idxDataBuf = ExtractDatumByOffsets(ectx,
				row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
			idxData := idxDataBuf[:len(index.Meta().Columns)]
			var rsData []types.Datum
			if needRestoreForIndexes[i] {
				rsData = getRestoreData(c.TableInfo, copCtx.IndexInfo(idxID), c.PrimaryKeyInfo, restoreDataBuf)
			}
			kvBytes, err := writeOneKV(ctx, writers[i], index, loc, errCtx, writeStmtBufs, idxData, rsData, h)
			if err != nil {
				err = ingest.TryConvertToKeyExistsErr(err, index.Meta(), tblInfo)
				return 0, totalBytes, errors.Trace(err)
			}
			totalBytes += int(kvBytes)
		}
		count++
	}
	return count, totalBytes, nil
}

func maxIndexColumnCount(indexes []table.Index) int {
	maxCnt := 0
	for _, idx := range indexes {
		colCnt := len(idx.Meta().Columns)
		if colCnt > maxCnt {
			maxCnt = colCnt
		}
	}
	return maxCnt
}

func writeOneKV(
	ctx context.Context,
	writer ingest.Writer,
	index table.Index,
	loc *time.Location,
	errCtx errctx.Context,
	writeBufs *variable.WriteStmtBufs,
	idxDt, rsData []types.Datum,
	handle kv.Handle,
) (int64, error) {
	var kvBytes int64
	iter := index.GenIndexKVIter(errCtx, loc, idxDt, handle, rsData)
	for iter.Valid() {
		key, idxVal, _, err := iter.Next(writeBufs.IndexKeyBuf, writeBufs.RowValBuf)
		if err != nil {
			return kvBytes, errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterPanic", func() {
			panic("mock panic")
		})
		err = writer.WriteRow(ctx, key, idxVal, handle)
		if err != nil {
			return kvBytes, errors.Trace(err)
		}
		// TODO this size doesn't consider keyspace prefix.
		kvBytes += int64(len(key) + len(idxVal))
		failpoint.Inject("mockLocalWriterError", func() {
			failpoint.Return(0, errors.New("mock engine error"))
		})
		writeBufs.IndexKeyBuf = key
		writeBufs.RowValBuf = idxVal
	}
	return kvBytes, nil
}

// BackfillData will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillData will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexTxnWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	jobID := handleRange.getJobID()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) (err error) {
		taskCtx.finishTS = txn.StartTS()
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(jobID); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.InjectCall("addIndexTxnWorkerBackfillData", len(idxRecords))

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// We need to add this lock to make sure pessimistic transaction can realize this operation.
			// For the normal pessimistic transaction, it's ok. But if async commit is used, it may lead to inconsistent data and index.
			// TODO: For global index, lock the correct key?! Currently it locks the partition (phyTblID) and the handle or actual key?
			// but should really lock the table's ID + key col(s)
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			handle, err := w.indexes[i%len(w.indexes)].Create(
				w.tblCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData,
				table.WithIgnoreAssertion,
				table.FromBackfill,
				// Constrains is already checked in batchCheckUniqueKey
				table.DupKeyCheckSkip,
			)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle.Equal(handle) {
					// Index already exists, skip it.
					continue
				}
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillData", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})
	failpoint.InjectCall("mockAddIndexTxnWorkerStuck")
	return
}

// MockDMLExecution is only used for test.
var MockDMLExecution func()

// MockDMLExecutionMerging is only used for test.
var MockDMLExecutionMerging func()
