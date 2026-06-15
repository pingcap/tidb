// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/pkg/kv"
)

// doReorgWorkForAddAutoIncrementColumn backfills values for a newly-added AUTO_INCREMENT column.
func doReorgWorkForAddAutoIncrementColumn(
	jobCtx *jobContext,
	w *worker,
	job *model.Job,
	tbl table.Table,
	col *model.ColumnInfo,
) (done bool, ver int64, err error) {
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return false, ver, errors.Trace(err1)
	}
	defer w.sessPool.Put(sctx)

	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	elements := []*meta.Element{{ID: col.ID, TypeKey: meta.ColumnElementKey}}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, tbl, elements, false)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version and
		// then run the reorg next time.
		return false, ver, errors.Trace(err)
	}

	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (reorgErr error) {
		defer util.Recover(metrics.LabelDDL, "onAddColumnAutoIncrementBackfill",
			func() {
				reorgErr = dbterror.ErrCancelledDDLJob.GenWithStack("backfill table `%v` auto_increment column `%v` panic", tbl.Meta().Name, col.Name)
			}, false)
		return w.backfillAutoIncrementColumn(jobCtx.stepCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) || dbterror.ErrNotOwner.Equal(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("run add auto_increment column backfill failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.String("job", job.String()), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("run add auto_increment column backfill failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		job.State = model.JobStateRollingback
		return false, ver, errors.Trace(err)
	}
	return true, ver, nil
}

func (w *worker) backfillAutoIncrementColumn(ctx context.Context, t table.Table, reorgInfo *reorgInfo) error {
	if tbl, ok := t.(table.PartitionedTable); ok {
		done := false
		for !done {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err := w.writePhysicalTableRecord(ctx, w.sessPool, p, typeAddAutoIncrementColumnWorker, reorgInfo)
			if err != nil {
				return err
			}
			var err1 error
			done, err1 = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err1 != nil {
				return errors.Trace(err1)
			}
		}
		return nil
	}
	if tbl, ok := t.(table.PhysicalTable); ok {
		return w.writePhysicalTableRecord(ctx, w.sessPool, tbl, typeAddAutoIncrementColumnWorker, reorgInfo)
	}
	return dbterror.ErrCancelledDDLJob.GenWithStack("internal error for phys tbl id: %d tbl id: %d", reorgInfo.PhysicalTableID, t.Meta().ID)
}

type addAutoIncrementColumnWorker struct {
	*backfillCtx

	colInfo *model.ColumnInfo
	alloc   autoid.Allocator

	// The following attributes are used to reduce memory allocation.
	rowRecords []*rowRecord
	rowDecoder *decoder.RowDecoder
	rowMap     map[int64]types.Datum
	colTps     map[int64]*types.FieldType

	// rawRowRecords buffers rows that need backfill so we can allocate AUTO_INCREMENT values in batches.
	// This is especially important when `AUTO_ID_CACHE=1`, where the allocator is single-point and each
	// Alloc() call is an RPC.
	rawRowRecords []rawRowRecord

	checksumNeeded bool
}

type rawRowRecord struct {
	handle    kv.Handle
	recordKey kv.Key
	rawRow    []byte
}

func newAddAutoIncrementColumnWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*addAutoIncrementColumnWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblUpdateColRate, false, false)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		return nil, errors.Errorf("element type is not column, typeKey: %v", reorgInfo.currElement.TypeKey)
	}

	colInfo := t.Meta().FindColumnByID(reorgInfo.currElement.ID)
	if colInfo == nil {
		return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %d", reorgInfo.currElement.ID)
	}
	if !mysql.HasAutoIncrementFlag(colInfo.GetFlag()) {
		return nil, errors.Errorf("internal error: reorg column %s is not AUTO_INCREMENT", colInfo.Name.O)
	}

	alloc := t.Allocators(bCtx.tblCtx).Get(autoid.AutoIncrementType)
	if alloc == nil {
		return nil, errors.Errorf("auto_increment allocator not found for table %s", t.Meta().Name.O)
	}

	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	return &addAutoIncrementColumnWorker{
		backfillCtx:    bCtx,
		colInfo:        colInfo,
		alloc:          alloc,
		rowDecoder:     rowDecoder,
		rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
		colTps:         map[int64]*types.FieldType{colInfo.ID: &colInfo.FieldType},
		checksumNeeded: variable.EnableRowLevelChecksum.Load(),
		rowRecords:     make([]*rowRecord, 0, bCtx.batchCnt),
		rawRowRecords:  make([]rawRowRecord, 0, bCtx.batchCnt),
	}, nil
}

func (w *addAutoIncrementColumnWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*addAutoIncrementColumnWorker) String() string {
	return typeAddAutoIncrementColumnWorker.String()
}

func (w *addAutoIncrementColumnWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *addAutoIncrementColumnWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

func (w *addAutoIncrementColumnWorker) fetchRowColVals(ctx context.Context, txn kv.Transaction, taskRange reorgBackfillTask) ([]*rowRecord, kv.Key, bool, int, error) {
	w.rowRecords = w.rowRecords[:0]
	w.rawRowRecords = w.rawRowRecords[:0]
	startTime := time.Now()

	taskDone := false
	var lastAccessedHandle kv.Key
	scannedCnt := 0

	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(),
		txn.StartTS(), taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			taskDone = recordKey.Cmp(taskRange.endKey) >= 0
			if taskDone || scannedCnt >= w.batchCnt {
				return false, nil
			}
			scannedCnt++
			need, err1 := w.shouldBackfillRow(rawRow)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			if need {
				// The iterator may reuse the underlying buffers across Next() calls.
				// Keep our own copies since we build the rewritten row values after the scan finishes.
				keyCopy := make([]byte, len(recordKey))
				copy(keyCopy, recordKey)
				rowCopy := make([]byte, len(rawRow))
				copy(rowCopy, rawRow)
				w.rawRowRecords = append(w.rawRowRecords, rawRowRecord{
					handle:    handle,
					recordKey: kv.Key(keyCopy),
					rawRow:    rowCopy,
				})
			}
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if scannedCnt == 0 {
		// No records in range.
		taskDone = true
	}
	if err == nil {
		// Allocate AUTO_INCREMENT IDs in a single batch and build the row rewrites.
		err = w.buildRowRecords(ctx)
	}

	logutil.DDLLogger().Debug("txn fetches handle info",
		zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()),
		zap.Int("scannedCnt", scannedCnt),
		zap.Duration("takeTime", time.Since(startTime)))
	return w.rowRecords, getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, scannedCnt, errors.Trace(err)
}

func (w *addAutoIncrementColumnWorker) shouldBackfillRow(rawRow []byte) (bool, error) {
	sysTZ := w.loc

	rowMap, err := tablecodec.DecodeRowToDatumMap(rawRow, w.colTps, sysTZ)
	if err != nil {
		return false, errors.Trace(err)
	}

	curVal, ok := rowMap[w.colInfo.ID]
	if !ok || curVal.IsNull() {
		return true, nil
	}
	if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
		return curVal.GetUint64() == 0, nil
	}
	return curVal.GetInt64() == 0, nil
}

// buildRowRecords allocates AUTO_INCREMENT values in a batch and generates encoded row rewrites.
// This avoids per-row Alloc() calls (which are especially slow when AUTO_ID_CACHE=1 uses a single-point allocator).
func (w *addAutoIncrementColumnWorker) buildRowRecords(ctx context.Context) error {
	if len(w.rawRowRecords) == 0 {
		return nil
	}

	minID, maxID, err := w.alloc.Alloc(ctx, uint64(len(w.rawRowRecords)), 1, 1)
	if err != nil {
		return errors.Trace(err)
	}

	nextID := minID + 1
	for _, rr := range w.rawRowRecords {
		if w.autoIncrementRangeExhausted(nextID, maxID) {
			return w.newAutoIncrementRangeExhaustedError(nextID, maxID)
		}
		used, err := w.buildRowRecord(ctx, rr.handle, rr.recordKey, rr.rawRow, nextID)
		if err != nil {
			return errors.Trace(err)
		}
		if used {
			nextID++
		}
	}
	return nil
}

func (w *addAutoIncrementColumnWorker) autoIncrementRangeExhausted(nextID, maxID int64) bool {
	if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
		return uint64(nextID) > uint64(maxID)
	}
	return nextID > maxID
}

func (w *addAutoIncrementColumnWorker) newAutoIncrementRangeExhaustedError(nextID, maxID int64) error {
	if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
		return errors.Errorf("allocated auto_increment range exhausted: next=%d end=%d", uint64(nextID), uint64(maxID))
	}
	return errors.Errorf("allocated auto_increment range exhausted: next=%d end=%d", nextID, maxID)
}

func (w *addAutoIncrementColumnWorker) buildRowRecord(ctx context.Context, handle kv.Handle, recordKey kv.Key, rawRow []byte, newID int64) (bool, error) {
	sysTZ := w.loc

	_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
	if err != nil {
		return false, errors.Trace(err)
	}

	curVal, ok := w.rowMap[w.colInfo.ID]
	if ok && !curVal.IsNull() {
		if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
			if curVal.GetUint64() != 0 {
				w.cleanRowMap()
				return false, nil
			}
		} else {
			if curVal.GetInt64() != 0 {
				w.cleanRowMap()
				return false, nil
			}
		}
	}

	var d types.Datum
	if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
		d.SetUint64(uint64(newID))
	} else {
		d.SetInt64(newID)
	}
	castedVal, err := table.CastColumnValue(w.exprCtx, d, w.colInfo, false, false)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Prevent truncation from silently creating duplicate AUTO_INCREMENT values.
	// This matches the insert/update path behavior that returns ErrAutoincReadFailed on out-of-range.
	if mysql.HasUnsignedFlag(w.colInfo.GetFlag()) {
		// For unsigned columns, newID may carry an unsigned value in int64 form.
		if castedVal.GetUint64() < uint64(newID) {
			return false, errors.Trace(autoid.ErrAutoincReadFailed)
		}
	} else {
		if castedVal.GetInt64() < newID {
			return false, errors.Trace(autoid.ErrAutoincReadFailed)
		}
	}
	w.rowMap[w.colInfo.ID] = castedVal

	_, err = w.rowDecoder.EvalRemainedExprColumnMap(w.exprCtx, w.rowMap)
	if err != nil {
		return false, errors.Trace(err)
	}

	newColumnIDs := make([]int64, 0, len(w.rowMap))
	newRow := make([]types.Datum, 0, len(w.rowMap))
	for colID, val := range w.rowMap {
		newColumnIDs = append(newColumnIDs, colID)
		newRow = append(newRow, val)
	}

	rd := w.tblCtx.GetRowEncodingConfig().RowEncoder
	ec := w.exprCtx.GetEvalCtx().ErrCtx()
	var checksum rowcodec.Checksum
	if w.checksumNeeded {
		checksum = rowcodec.RawChecksum{Handle: handle}
	}
	newRowVal, err := tablecodec.EncodeRow(sysTZ, newRow, newColumnIDs, nil, nil, checksum, rd)
	err = ec.HandleError(err)
	if err != nil {
		return false, errors.Trace(err)
	}

	w.rowRecords = append(w.rowRecords, &rowRecord{key: recordKey, vals: newRowVal})
	w.cleanRowMap()
	return true, nil
}

func (w *addAutoIncrementColumnWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)

	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)

		// Avoid TiCDC replicating these internal row rewrites.
		var txnSource uint64
		if val := txn.GetOption(kv.TxnSource); val != nil {
			txnSource, _ = val.(uint64)
		}
		err := kv.SetLossyDDLReorgSource(&txnSource, kv.LossyDDLColumnReorgSource)
		if err != nil {
			return errors.Trace(err)
		}
		txn.SetOption(kv.TxnSource, txnSource)

		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		rowRecords, nextKey, taskDone, scannedCnt, err := w.fetchRowColVals(ctx, txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone
		taskCtx.scanCount = scannedCnt

		for _, rowRecord := range rowRecords {
			err = txn.Set(rowRecord.key, rowRecord.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		// This backfill does not produce casting warnings.
		taskCtx.warnings = nil
		taskCtx.warningsCount = nil

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)
	return
}
