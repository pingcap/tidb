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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	log "github.com/sirupsen/logrus"
)

const maxPrefixLength = 3072
const maxCommentLength = 1024

func buildIndexColumns(columns []*model.ColumnInfo, idxColNames []*ast.IndexColName) ([]*model.IndexColumn, error) {
	// Build offsets.
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))

	// The sum of length of all index columns.
	sumLength := 0

	for _, ic := range idxColNames {
		col := findCol(columns, ic.Column.Name.O)
		if col == nil {
			return nil, errKeyColumnDoesNotExits.Gen("column does not exist: %s", ic.Column.Name)
		}

		if col.Flen == 0 {
			return nil, errors.Trace(errWrongKeyColumn.GenByArgs(ic.Column.Name))
		}

		// JSON column cannot index.
		if col.FieldType.Tp == mysql.TypeJSON {
			return nil, errors.Trace(errJSONUsedAsKey.GenByArgs(col.Name.O))
		}

		// Length must be specified for BLOB and TEXT column indexes.
		if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
			return nil, errors.Trace(errBlobKeyWithoutLength)
		}

		// Length can only be specified for specifiable types.
		if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Key length must be shorter or equal to the column length.
		if ic.Length != types.UnspecifiedLength &&
			types.IsTypeChar(col.FieldType.Tp) && col.Flen < ic.Length {
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Specified length must be shorter than the max length for prefix.
		if ic.Length > maxPrefixLength {
			return nil, errors.Trace(errTooLongKey)
		}

		// Take care of the sum of length of all index columns.
		if ic.Length != types.UnspecifiedLength {
			sumLength += ic.Length
		} else {
			// Specified data types.
			if col.Flen != types.UnspecifiedLength {
				// Special case for the bit type.
				if col.FieldType.Tp == mysql.TypeBit {
					sumLength += (col.Flen + 7) >> 3
				} else {
					sumLength += col.Flen
				}
			} else {
				if length, ok := mysql.DefaultLengthOfMysqlTypes[col.FieldType.Tp]; ok {
					sumLength += length
				} else {
					return nil, errUnknownTypeLength.GenByArgs(col.FieldType.Tp)
				}

				// Special case for time fraction.
				if types.IsTypeFractionable(col.FieldType.Tp) &&
					col.FieldType.Decimal != types.UnspecifiedLength {
					if length, ok := mysql.DefaultLengthOfTimeFraction[col.FieldType.Decimal]; ok {
						sumLength += length
					} else {
						return nil, errUnknownFractionLength.GenByArgs(col.FieldType.Tp, col.FieldType.Decimal)
					}
				}
			}
		}

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > maxPrefixLength {
			return nil, errors.Trace(errTooLongKey)
		}

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}

	return idxColumns, nil
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, idxColNames []*ast.IndexColName, state model.SchemaState) (*model.IndexInfo, error) {
	idxColumns, err := buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
	}
}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag &= ^mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag &= ^mysql.MultipleKeyFlag
	}

	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(tblInfo, index)
	}
}

func (d *ddl) onCreateIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = d.onDropIndex(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*ast.IndexColName
		indexOption *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames, &indexOption)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := findIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, errDupKeyName.Gen("index already exist %s", indexName)
	}

	if indexInfo == nil {
		indexInfo, err = buildIndexInfo(tblInfo, indexName, idxColNames, model.StateNone)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if indexOption != nil {
			indexInfo.Comment = indexOption.Comment
			if indexOption.Tp == model.IndexTypeInvalid {
				// Use btree as default index type.
				indexInfo.Tp = model.IndexTypeBtree
			} else {
				indexInfo.Tp = indexOption.Tp
			}
		} else {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		}
		indexInfo.Primary = false
		indexInfo.Unique = unique
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
		log.Infof("[ddl] add index, run DDL job %s, index info %#v", job, indexInfo)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteReorganization:
		// reorganization -> public
		var tbl table.Table
		tbl, err = d.getTable(schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var reorgInfo *reorgInfo
		reorgInfo, err = d.getReorgInfo(t, job)
		if err != nil || reorgInfo.first {
			if err == nil {
				// Get the first handle of this table.
				err = iterateSnapshotRows(d.store, tbl, reorgInfo.SnapshotVer, math.MinInt64,
					func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
						reorgInfo.Handle = h
						return false, nil
					})
				return ver, errors.Trace(t.UpdateDDLReorgHandle(reorgInfo.Job, reorgInfo.Handle))
			}
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = d.runReorgJob(t, job, func() error {
			return d.addTableIndexFromSplitRanges(tbl, indexInfo, reorgInfo, job)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) {
				log.Warnf("[ddl] run DDL job %v err %v, convert job to rollback job", job, err)
				ver, err = d.convert2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			cleanNotify(d.reorgCtx.notifyCancelReorgJob)
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		cleanNotify(d.reorgCtx.notifyCancelReorgJob)

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = ErrInvalidIndexState.Gen("invalid index state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) convert2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (int64, error) {
	job.State = model.JobStateRollingback
	job.Args = []interface{}{indexInfo.Name}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	indexInfo.State = model.StateDeleteOnly
	originalState := indexInfo.State
	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}

	if kv.ErrKeyExists.Equal(err) {
		return ver, kv.ErrKeyExists.Gen("Duplicate for key %s", indexInfo.Name.O)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := findIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.Gen("index %s doesn't exist", indexName)
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexName.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		}
		job.Args = append(job.Args, indexInfo.ID)
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func (w *worker) fetchRowColVals(txn kv.Transaction, t table.Table, colMap map[int64]*types.FieldType) (
	[]*indexRecord, *taskResult) {
	startTime := time.Now()
	w.idxRecords = w.idxRecords[:0]
	ret := &taskResult{outOfRangeHandle: w.taskRange.endHandle}
	isEnd := true
	err := iterateSnapshotRows(w.ctx.GetStore(), t, txn.StartTS(), w.taskRange.startHandle,
		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
			if h >= w.taskRange.endHandle {
				ret.outOfRangeHandle = h
				isEnd = false
				return false, nil
			}
			indexRecord := &indexRecord{handle: h, key: rowKey}
			err1 := w.getIndexRecord(t, colMap, rawRecord, indexRecord)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			w.idxRecords = append(w.idxRecords, indexRecord)
			return true, nil
		})
	if err != nil {
		ret.err = errors.Trace(err)
		return nil, ret
	}

	if isEnd {
		ret.isAllDone = true
	}
	ret.count = len(w.idxRecords)
	log.Debugf("[ddl] txn %v fetches handle info %v, ret %v, takes time %v", txn.StartTS(), w.taskRange, ret, time.Since(startTime))

	return w.idxRecords, ret
}

func (w *worker) getIndexRecord(t table.Table, colMap map[int64]*types.FieldType, rawRecord []byte, idxRecord *indexRecord) error {
	cols := t.Cols()
	idxInfo := w.index.Meta()
	_, err := tablecodec.DecodeRowWithMap(rawRecord, colMap, time.UTC, w.rowMap)
	if err != nil {
		return errors.Trace(err)
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		if col.IsPKHandleColumn(t.Meta()) {
			if mysql.HasUnsignedFlag(col.Flag) {
				idxVal[j].SetUint64(uint64(idxRecord.handle))
			} else {
				idxVal[j].SetInt64(idxRecord.handle)
			}
			continue
		}
		idxColumnVal := w.rowMap[col.ID]
		if _, ok := w.rowMap[col.ID]; ok {
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.ctx, col, w.defaultVals)
		if err != nil {
			return errors.Trace(err)
		}
		idxVal[j] = idxColumnVal
	}
	idxRecord.vals = idxVal
	return nil
}

const (
	minTaskHandledCnt    = 32 // minTaskHandledCnt is the minimum number of handles per batch.
	defaultTaskHandleCnt = 128
	maxTaskHandleCnt     = 1 << 20 // maxTaskHandleCnt is the maximum number of handles per batch.
	defaultWorkers       = 16
)

// taskResult is the result of the task.
type taskResult struct {
	count            int   // The number of records that has been processed in the task.
	outOfRangeHandle int64 // This is the handle out of the range.
	isAllDone        bool  // If all rows are all done.
	err              error
}

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
}

type worker struct {
	id          int
	ctx         sessionctx.Context
	index       table.Index
	defaultVals []types.Datum  // It's used to reduce the number of new slice.
	idxRecords  []*indexRecord // It's used to reduce the number of new slice.
	taskRange   handleInfo     // Every task's handle range.
	taskRet     *taskResult
	batchSize   int
	rowMap      map[int64]types.Datum // It's the index column values map. It is used to reduce the number of making map.
}

func newWorker(ctx sessionctx.Context, id, batch, colsLen, indexColsLen int) *worker {
	return &worker{
		id:          id,
		ctx:         ctx,
		batchSize:   batch,
		idxRecords:  make([]*indexRecord, 0, batch),
		defaultVals: make([]types.Datum, colsLen),
		rowMap:      make(map[int64]types.Datum, indexColsLen),
	}
}

func (w *worker) setTaskNewRange(startHandle, endHandle int64) {
	w.taskRange.startHandle = startHandle
	w.taskRange.endHandle = endHandle
}

// handleInfo records the range of [start handle, end handle) that is used in a task.
type handleInfo struct {
	startHandle int64
	endHandle   int64
}

func getEndHandle(baseHandle, batch int64) int64 {
	if baseHandle >= math.MaxInt64-batch {
		return math.MaxInt64
	}
	return baseHandle + batch
}

// addTableIndex adds index into table.
// TODO: Move this to doc or wiki.
// How to add index in reorganization state?
// Concurrently process the defaultTaskHandleCnt tasks. Each task deals with a handle range of the index record.
// The handle range size is defaultTaskHandleCnt.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow of the each task of data is as follows:
//  1. Open a goroutine. Traverse the snapshot to obtain the handle range, while accessing the corresponding row key and
// raw index value. Then notify to start the next task.
//  2. Decode this task of raw index value to get the corresponding index value.
//  3. Deal with these index records one by one. If the index record exists, skip to the next row.
// If the index doesn't exist, create the index and then continue to handle the next row.
//  4. When the handle of a range is completed, return the corresponding task result.
// The above operations are completed in a transaction.
// When concurrent tasks are processed, the task result returned by each task is sorted by the worker number. Then traverse the
// task results, get the total number of rows in the concurrent task and update the processed handle value. If
// an error message is displayed, exit the traversal.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (d *ddl) addTableIndex(t table.Table, indexInfo *model.IndexInfo, reorgInfo *reorgInfo, job *model.Job) error {
	cols := t.Cols()
	colMap := make(map[int64]*types.FieldType)
	for _, v := range indexInfo.Columns {
		col := cols[v.Offset]
		colMap[col.ID] = &col.FieldType
	}
	workerCnt := defaultWorkers
	addedCount := job.GetRowCount()
	baseHandle, logStartHandle := reorgInfo.Handle, reorgInfo.Handle

	workers := make([]*worker, workerCnt)
	for i := 0; i < workerCnt; i++ {
		ctx := d.newContext()
		workers[i] = newWorker(ctx, i, defaultTaskHandleCnt, len(cols), len(colMap))
		// Make sure every worker has its own index buffer.
		workers[i].index = tables.NewIndexWithBuffer(t.Meta(), indexInfo)
	}
	for {
		startTime := time.Now()
		wg := sync.WaitGroup{}
		currentBatchSize := int64(workers[0].batchSize)
		for i := 0; i < workerCnt; i++ {
			wg.Add(1)
			endHandle := getEndHandle(baseHandle, currentBatchSize)
			workers[i].setTaskNewRange(baseHandle, endHandle)
			// TODO: Consider one worker to one goroutine.
			go workers[i].doBackfillIndexTask(t, colMap, &wg)
			baseHandle = endHandle
		}
		wg.Wait()

		taskAddedCount, nextHandle, isEnd, err := getCountAndHandle(workers)
		addedCount += taskAddedCount
		sub := time.Since(startTime).Seconds()
		if err == nil {
			err = d.isReorgRunnable()
		}
		if err != nil {
			// Update the reorg handle that has been processed.
			err1 := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
				return errors.Trace(reorgInfo.UpdateHandle(txn, nextHandle))
			})
			log.Warnf("[ddl] total added index for %d rows, this task [%d,%d) add index for %d failed %v, batch %d, take time %v, update handle err %v",
				addedCount, logStartHandle, nextHandle, taskAddedCount, err, currentBatchSize, sub, err1)
			return errors.Trace(err)
		}
		d.reorgCtx.setRowCountAndHandle(addedCount, nextHandle)
		metrics.BatchAddIdxHistogram.Observe(sub)
		log.Infof("[ddl] total added index for %d rows, this task [%d,%d) added index for %d rows, batch %d, take time %v",
			addedCount, logStartHandle, nextHandle, taskAddedCount, currentBatchSize, sub)

		if isEnd {
			return nil
		}
		baseHandle, logStartHandle = nextHandle, nextHandle
	}
}

func getCountAndHandle(workers []*worker) (int64, int64, bool, error) {
	taskAddedCount, nextHandle := int64(0), workers[0].taskRange.startHandle
	var err error
	var isEnd bool
	starvingWorkers := 0
	largerDefaultWorkers := 0
	for _, worker := range workers {
		ret := worker.taskRet
		if ret.err != nil {
			err = ret.err
			break
		}
		taskAddedCount += int64(ret.count)
		if ret.count < minTaskHandledCnt {
			starvingWorkers++
		} else if ret.count > defaultTaskHandleCnt {
			largerDefaultWorkers++
		}
		nextHandle = ret.outOfRangeHandle
		isEnd = ret.isAllDone
	}

	// Adjust the worker's batch size.
	halfWorkers := len(workers) / 2
	if starvingWorkers >= halfWorkers && workers[0].batchSize < maxTaskHandleCnt {
		// If the index data is discrete, we need to increase the batch size to speed up.
		for _, worker := range workers {
			worker.batchSize *= 2
		}
	} else if largerDefaultWorkers >= halfWorkers && workers[0].batchSize > defaultTaskHandleCnt {
		// If the batch size exceeds the limit after we increase it,
		// we need to decrease the batch size to reduce write conflict.
		for _, worker := range workers {
			worker.batchSize /= 2
		}
	}
	return taskAddedCount, nextHandle, isEnd, errors.Trace(err)
}

func (w *worker) doBackfillIndexTask(t table.Table, colMap map[int64]*types.FieldType, wg *sync.WaitGroup) {
	defer wg.Done()

	startTime := time.Now()
	var ret *taskResult
	err := kv.RunInNewTxn(w.ctx.GetStore(), true, func(txn kv.Transaction) error {
		ret = w.doBackfillIndexTaskInTxn(t, txn, colMap)
		return errors.Trace(ret.err)
	})
	if err != nil {
		ret.err = errors.Trace(err)
	}

	w.taskRet = ret
	log.Debugf("[ddl] add index completes backfill index task %v takes time %v err %v",
		w.taskRange, time.Since(startTime), ret.err)
}

// doBackfillIndexTaskInTxn deals with a part of backfilling index data in a Transaction.
// This part of the index data rows is defaultTaskHandleCnt.
func (w *worker) doBackfillIndexTaskInTxn(t table.Table, txn kv.Transaction, colMap map[int64]*types.FieldType) *taskResult {
	idxRecords, taskRet := w.fetchRowColVals(txn, t, colMap)
	if taskRet.err != nil {
		taskRet.err = errors.Trace(taskRet.err)
		return taskRet
	}

	for _, idxRecord := range idxRecords {
		log.Debugf("[ddl] txn %v backfill index handle...%v", txn.StartTS(), idxRecord.handle)
		err := txn.LockKeys(idxRecord.key)
		if err != nil {
			taskRet.err = errors.Trace(err)
			return taskRet
		}

		// Create the index.
		handle, err := w.index.Create(w.ctx, txn, idxRecord.vals, idxRecord.handle)
		if err != nil {
			if kv.ErrKeyExists.Equal(err) && idxRecord.handle == handle {
				// Index already exists, skip it.
				continue
			}
			taskRet.err = errors.Trace(err)
			return taskRet
		}
	}
	return taskRet
}

type addIndexWorker struct {
	id          int
	d           *ddl
	batchCnt    int
	sessCtx     sessionctx.Context
	taskCh      chan *backfillIndexTask
	resultCh    chan *addIndexResult
	index       table.Index
	table       table.Table
	colFieldMap map[int64]*types.FieldType
	closed      bool

	defaultVals []types.Datum         // It's used to reduce the number of new slice.
	idxRecords  []*indexRecord        // It's used to reduce the number of new slice.
	rowMap      map[int64]types.Datum // It's the index column values map. It is used to reduce the number of making map.
}

type reorgIndexRange struct {
	startHandle int64
	endHandle   int64
}

type backfillIndexTask struct {
	handleRange reorgIndexRange
}

type addIndexResult struct {
	originalTaskRange reorgIndexRange
	addedCount        int
	scanCount         int
	nextHandle        int64
	err               error
}

func newAddIndexWoker(sessCtx sessionctx.Context, d *ddl, id int, index table.Index, t table.Table, colFieldMap map[int64]*types.FieldType) *addIndexWorker {
	return &addIndexWorker{
		id:          id,
		d:           d,
		batchCnt:    defaultTaskHandleCnt,
		sessCtx:     sessCtx,
		taskCh:      make(chan *backfillIndexTask),
		resultCh:    make(chan *addIndexResult),
		index:       index,
		table:       t,
		colFieldMap: colFieldMap,

		defaultVals: make([]types.Datum, len(t.Cols())),
		rowMap:      make(map[int64]types.Datum, len(colFieldMap)),
	}
}

func (w *addIndexWorker) close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

func (w *addIndexWorker) getIndexRecord(t table.Table, colMap map[int64]*types.FieldType, rawRecord []byte, idxRecord *indexRecord) error {
	cols := t.Cols()
	idxInfo := w.index.Meta()
	_, err := tablecodec.DecodeRowWithMap(rawRecord, colMap, time.UTC, w.rowMap)
	if err != nil {
		return errors.Trace(err)
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		if col.IsPKHandleColumn(t.Meta()) {
			if mysql.HasUnsignedFlag(col.Flag) {
				idxVal[j].SetUint64(uint64(idxRecord.handle))
			} else {
				idxVal[j].SetInt64(idxRecord.handle)
			}
			continue
		}
		idxColumnVal := w.rowMap[col.ID]
		if _, ok := w.rowMap[col.ID]; ok {
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			return errors.Trace(err)
		}
		idxVal[j] = idxColumnVal
	}
	idxRecord.vals = idxVal
	return nil
}

func (w *addIndexWorker) fetchRowColVals(txn kv.Transaction, t table.Table, colMap map[int64]*types.FieldType, taskRange reorgIndexRange) ([]*indexRecord, bool, error) {
	// TODO: use TableReader to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()
	handleOutOfRange := false
	err := iterateSnapshotRows(w.sessCtx.GetStore(), t, txn.StartTS(), taskRange.startHandle,
		func(handle int64, recordKey kv.Key, rawRow []byte) (bool, error) {
			// Don't create endHandle index.
			handleOutOfRange = handle >= taskRange.endHandle
			if handleOutOfRange || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			idxRecord := &indexRecord{handle: handle, key: recordKey}
			err1 := w.getIndexRecord(t, colMap, rawRow, idxRecord)
			if err1 != nil {
				return false, errors.Trace(err1)
			}

			w.idxRecords = append(w.idxRecords, idxRecord)
			return true, nil
		})

	log.Debugf("[ddl] txn %v fetches handle info %v, takes time %v", txn.StartTS(), taskRange, time.Since(startTime))
	return w.idxRecords, handleOutOfRange, errors.Trace(err)
}

func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexRange) (nextHandle int64, addedCount, scanCount int, errInTxn error) {
	addedCount = 0
	scanCount = 0
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		idxRecords, handleOutofRange, err := w.fetchRowColVals(txn, w.table, w.colFieldMap, handleRange)

		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			err := txn.LockKeys(idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}
			scanCount++

			// Create the index.
			// TODO: backfill unique-key will check constraint every row, we can speed up this case by using batch check.
			handle, err := w.index.Create(w.sessCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle == handle {
					// Index already exists, skip it.
					continue
				}

				return errors.Trace(err)
			}
			addedCount++
		}

		if handleOutofRange || len(idxRecords) == 0 {
			nextHandle = handleRange.endHandle
		} else {
			nextHandle = idxRecords[len(idxRecords)-1].handle + 1
		}
		return nil
	})

	return
}

func (w *addIndexWorker) handleBackfillTask(task *backfillIndexTask) *addIndexResult {
	handleRange := task.handleRange
	result := &addIndexResult{originalTaskRange: handleRange, addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	for {
		addedCount := 0
		nextHandle, addedCount, scanCount, err := w.backfillIndexInTxn(handleRange)
		if err == nil {
			err = w.d.isReorgRunnable()
			if err != nil {
				// This task is finished, job canceled is not a real error,
				// so we need to update the processed handle.
				result.nextHandle = nextHandle
				result.addedCount += addedCount
			}
		}

		if err != nil {
			result.err = err
			return result
		}

		result.nextHandle = nextHandle
		result.addedCount += addedCount
		result.scanCount += scanCount
		if result.scanCount-lastLogCount >= 30000 {
			lastLogCount = result.scanCount
			log.Infof("[ddl-reorg] worker(%v), finish batch addedCount:%v backfill, task addedCount:%v, task scanCount:%v, nextHandle:%v",
				w.id, addedCount, result.addedCount, result.scanCount, nextHandle)
		}

		// If the worker is the first one, we can update the finished handle to reorgCtx.
		// The doneHandle will be updated to tikv in ddl.runReorgJob.
		if w.id == 0 {
			w.d.reorgCtx.setRowCountAndHandle(int64(result.addedCount), nextHandle)
		}
		handleRange.startHandle = nextHandle
		if handleRange.startHandle >= handleRange.endHandle {
			break
		}
	}

	return result
}

func (w *addIndexWorker) run() {
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		log.Debug("[ddl-reorg] got backfill index task:#v", task)
		result := w.handleBackfillTask(task)
		w.resultCh <- result
	}
}

func makeupIndexColFieldMap(t table.Table, indexInfo *model.IndexInfo) map[int64]*types.FieldType {
	cols := t.Cols()
	colFieldMap := make(map[int64]*types.FieldType, len(indexInfo.Columns))
	for _, v := range indexInfo.Columns {
		col := cols[v.Offset]
		colFieldMap[col.ID] = &col.FieldType
	}
	return colFieldMap
}

func (d *ddl) splitTableRanges(t table.Table, startHandle int64) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(math.MaxInt64)
	kvRange := kv.KeyRange{startRecordKey, endRecordKey}
	store, ok := d.store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackoffer(context.Background(), maxSleep)
	ranges, err := tikv.SplitRegionRanges(bo, store.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func decodeHandleRange(keyRange kv.KeyRange) (int64, int64, error) {
	_, startHandle, err := tablecodec.DecodeRecordKey(keyRange.StartKey)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	_, endHandle, err := tablecodec.DecodeRecordKey(keyRange.EndKey)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return startHandle, endHandle, nil
}

func closeAddIndexWorkers(workers []*addIndexWorker) {
	for _, worker := range workers {
		worker.close()
	}
}

func (d *ddl) handleTaskResults(workers []*addIndexWorker, workingIdx int) (int64, int64, error) {
	results := make([]*addIndexResult, 0, len(workers))
	for i, worker := range workers {
		if i > workingIdx {
			break
		}
		result := <-worker.resultCh
		results = append(results, result)
	}

	var firstErr error
	var addedCount int64
	var nextHandle int64
	for i, result := range results {
		if i == 0 {
			// init nextHandle
			nextHandle = result.originalTaskRange.startHandle
		}
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			if errCancelledDDLJob.Equal(firstErr) {
				nextHandle = result.nextHandle
			}
		}

		// addedCount it any way.
		addedCount += int64(result.addedCount)
		if firstErr == nil {
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, firstErr
}

func (d *ddl) finishBatchTasks(startTime time.Time, startHandle int64, reorgInfo *reorgInfo, job *model.Job, workers []*addIndexWorker, workingIdx int) error {
	addedCount := job.GetRowCount()
	nextHandle, taskAddedCount, err := d.handleTaskResults(workers, workingIdx)
	addedCount += taskAddedCount
	elapsedTime := time.Since(startTime).Seconds()
	if err == nil {
		err = d.isReorgRunnable()
	}

	if err != nil {
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			return errors.Trace(reorgInfo.UpdateHandle(txn, nextHandle))
		})
		log.Warnf("[ddl-reorg] total added index for %d rows, this task [%d,%d) add index for %d failed %v, take time %v, update handle err %v",
			addedCount, startHandle, nextHandle, taskAddedCount, err, elapsedTime, err1)
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	d.reorgCtx.setRowCountAndHandle(addedCount, nextHandle)
	metrics.BatchAddIdxHistogram.Observe(elapsedTime)
	log.Infof("[ddl-reorg] total added index for %d rows, this task [%d,%d) added index for %d rows, take time %v",
		addedCount, startHandle, nextHandle, taskAddedCount, elapsedTime)
	return nil
}

func (d *ddl) addTableIndexFromSplitRanges(t table.Table, indexInfo *model.IndexInfo, reorgInfo *reorgInfo, job *model.Job) error {
	log.Infof("[ddl-reorg] addTableIndexFromSplitRanges, job:%s, reorgInfo:%#v", job, reorgInfo)
	colFieldMap := makeupIndexColFieldMap(t, indexInfo)

	workerCnt := defaultWorkers
	workers := make([]*addIndexWorker, workerCnt)
	for i := 0; i < workerCnt; i++ {
		sessCtx := d.newContext()
		index := tables.NewIndexWithBuffer(t.Meta(), indexInfo)
		workers[i] = newAddIndexWoker(sessCtx, d, i, index, t, colFieldMap)
		go workers[i].run()
	}
	defer closeAddIndexWorkers(workers)

	kvRanges, err := d.splitTableRanges(t, reorgInfo.Handle)
	if err != nil {
		return errors.Trace(err)
	}

	workerIdx := 0
	var (
		startTime   time.Time
		startHandle int64
		endHandle   int64
	)
	// TODO: refactor this loop to function
	for _, keyRange := range kvRanges {
		startTime = time.Now()
		var err1 error
		startHandle, endHandle, err1 = decodeHandleRange(keyRange)
		if err1 != nil {
			return errors.Trace(err1)
		}
		task := &backfillIndexTask{
			handleRange: reorgIndexRange{startHandle, endHandle},
		}
		workers[workerIdx].taskCh <- task
		workerIdx++

		if workerIdx == workerCnt {
			// Wait tasks finish.
			err1 = d.finishBatchTasks(startTime, startHandle, reorgInfo, job, workers, workerIdx-1)
			if err1 != nil {
				return errors.Trace(err1)
			}
			workerIdx = 0
		}
	}

	if workerIdx > 0 {
		err1 := d.finishBatchTasks(startTime, startHandle, reorgInfo, job, workers, workerIdx-1)
		if err1 != nil {
			return errors.Trace(err1)
		}
	}

	return nil
}

func findIndexByName(idxName string, indices []*model.IndexInfo) *model.IndexInfo {
	for _, idx := range indices {
		if idx.Name.L == idxName {
			return idx
		}
	}
	return nil
}

func allocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h int64, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, t table.Table, version uint64, seekHandle int64, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return errors.Trace(err)
	}

	firstKey := t.RecordKey(seekHandle)
	it, err := snap.Seek(firstKey)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle int64
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
