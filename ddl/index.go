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
	"math"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
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
				if len, ok := mysql.DefaultLengthOfMysqlTypes[col.FieldType.Tp]; ok {
					sumLength += len
				} else {
					return nil, errUnknownTypeLength.GenByArgs(col.FieldType.Tp)
				}

				// Special case for time fraction.
				if types.IsTypeFractionable(col.FieldType.Tp) &&
					col.FieldType.Decimal != types.UnspecifiedLength {
					if len, ok := mysql.DefaultLengthOfTimeFraction[col.FieldType.Decimal]; ok {
						sumLength += len
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
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
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
			return d.addTableIndex(tbl, indexInfo, reorgInfo, job)
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

		job.SchemaState = model.StatePublic
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.State = model.JobStateDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
	default:
		err = ErrInvalidIndexState.Gen("invalid index state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) convert2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (ver int64, _ error) {
	job.State = model.JobStateRollingback
	job.Args = []interface{}{indexInfo.Name}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	indexInfo.State = model.StateDeleteOnly
	originalState := indexInfo.State
	job.SchemaState = model.StateDeleteOnly
	_, err1 := updateTableInfo(t, job, tblInfo, originalState)
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
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
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

		job.SchemaState = model.StateNone
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.State = model.JobStateRollbackDone
		} else {
			job.State = model.JobStateDone
			d.asyncNotifyEvent(&ddlutil.Event{Tp: model.ActionDropIndex, TableInfo: tblInfo, IndexInfo: indexInfo})
		}
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
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
	ctx         context.Context
	index       table.Index
	defaultVals []types.Datum  // It's used to reduce the number of new slice.
	idxRecords  []*indexRecord // It's used to reduce the number of new slice.
	taskRange   handleInfo     // Every task's handle range.
	taskRet     *taskResult
	batchSize   int
	rowMap      map[int64]types.Datum // It's the index column values map. It is used to reduce the number of making map.
}

func newWorker(ctx context.Context, id, batch, colsLen, indexColsLen int) *worker {
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
		batchHandleDataHistogram.WithLabelValues(batchAddIdx).Observe(sub)
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
		handle, err := w.index.Create(txn, idxRecord.vals, idxRecord.handle)
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
