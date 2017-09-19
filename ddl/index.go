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
	//	"os"
	//	"runtime/pprof"
	"math"
	"sync"
	"time"

	//	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

const maxPrefixLength = 3072

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
		tblInfo.Columns[col.Offset].Flag &= ^uint(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[col.Offset].Flag &= ^uint(mysql.MultipleKeyFlag)
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
	// Handle rollback job.
	if job.State == model.JobRollback {
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
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := findIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		job.State = model.JobCancelled
		return ver, errDupKeyName.Gen("index already exist %s", indexName)
	}

	if indexInfo == nil {
		indexInfo, err = buildIndexInfo(tblInfo, indexName, idxColNames, model.StateNone)
		if err != nil {
			job.State = model.JobCancelled
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

		err = d.runReorgJob(job, func() error {
			return d.addTableIndex(tbl, indexInfo, reorgInfo, job)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) {
				log.Warnf("[ddl] run DDL job %v err %v, convert job to rollback job", job, err)
				ver, err = d.convert2RollbackJob(t, job, tblInfo, indexInfo)
			}
			return ver, errors.Trace(err)
		}

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)

		job.SchemaState = model.StatePublic
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
	default:
		err = ErrInvalidIndexState.Gen("invalid index state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) convert2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (ver int64, _ error) {
	job.State = model.JobRollback
	job.Args = []interface{}{indexInfo.Name}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	indexInfo.State = model.StateDeleteOnly
	originalState := indexInfo.State
	job.SchemaState = model.StateDeleteOnly
	_, err := updateTableInfo(t, job, tblInfo, originalState)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, kv.ErrKeyExists.Gen("Duplicate for key %s", indexInfo.Name.O)
}

func (d *ddl) onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := findIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobCancelled
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
		if job.State == model.JobRollback {
			job.State = model.JobRollbackDone
		} else {
			job.State = model.JobDone
		}
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
		job.Args = append(job.Args, indexInfo.ID)
		d.asyncNotifyEvent(&Event{Tp: model.ActionDropIndex, TableInfo: tblInfo, IndexInfo: indexInfo})
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func (w *worker) fetchRowColVals(txn kv.Transaction, t table.Table, taskOpInfo *indexTaskOpInfo) (
	[]*indexRecord, *taskResult) {
	startTime := time.Now()
	w.idxRecords = w.idxRecords[:0]
	startHandle := w.taskRange.startHandle
	ret := &taskResult{doneHandle: startHandle}
	isEnd := true
	err := iterateSnapshotRows(w.ctx.GetStore(), t, txn.StartTS(), startHandle,
		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
			if h >= startHandle+taskOpInfo.taskBatch {
				ret.doneHandle = h - 1
				isEnd = false
				return false, nil
			}
			indexRecord := &indexRecord{handle: h, key: rowKey}
			err1 := w.getIndexRecord(t, taskOpInfo, rawRecord, indexRecord)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			w.idxRecords = append(w.idxRecords, indexRecord)
			ret.doneHandle = h
			if w.taskRange.isFinished(h) {
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		ret.err = errors.Trace(err)
		return nil, ret
	}

	if !w.taskRange.isSent {
		// Record the last handle.
		// Ensure that the handle scope of the task doesn't change,
		// even if the transaction retries it can't effect the other tasks.
		w.taskRange.endHandle = ret.doneHandle
		if isEnd {
			ret.isAllDone = true
		}
	}
	ret.count = len(w.idxRecords)
	log.Debugf("[ddl] txn %v fetches handle info %v, ret %v, takes time %v", txn.StartTS(), w.taskRange, ret, time.Since(startTime))
	w.taskRange.isSent = true
	if ret.count == 0 {
		return nil, ret
	}

	return w.idxRecords, ret
}

func (w *worker) getIndexRecord(t table.Table, taskOpInfo *indexTaskOpInfo, rawRecord []byte, idxRecord *indexRecord) error {
	cols := t.Cols()
	idxInfo := taskOpInfo.tblIndex.Meta()
	err := tablecodec.DecodeRowWithMap(rawRecord, taskOpInfo.colMap, time.UTC, w.rowMap)
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
	defaultBatchCnt      = 1024
	defaultSmallBatchCnt = 128
	defaultTaskHandleCnt = 128
	defaultWorkers       = 16
)

// taskResult is the result of the task.
type taskResult struct {
	count      int   // The number of records that has been processed in the task.
	doneHandle int64 // This is the last reorg handle that has been processed.
	isAllDone  bool
	err        error
}

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
}

// indexTaskOpInfo records the information that is needed in the task.
type indexTaskOpInfo struct {
	tblIndex  table.Index
	colMap    map[int64]*types.FieldType // It's the index columns map.
	taskBatch int64
	reorgInfo *reorgInfo
}

type worker struct {
	id          int
	ctx         context.Context
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	taskRange   handleInfo
	taskRet     *taskResult
	rowMap      map[int64]types.Datum
}

func newWorker(ctx context.Context, id, batch, cols, indexCols int) *worker {
	return &worker{
		id:          id,
		ctx:         ctx,
		idxRecords:  make([]*indexRecord, 0, batch),
		defaultVals: make([]types.Datum, cols),
		rowMap:      make(map[int64]types.Datum, indexCols),
	}
}

func (w *worker) setTaskNewRange(startHandle int64) {
	w.taskRange.startHandle = startHandle
	w.taskRange.endHandle = 0
	w.taskRange.isSent = false
}

// handleInfo records start and end handle that is used in a task.
type handleInfo struct {
	startHandle int64
	endHandle   int64
	isSent      bool // It ensures that the endHandle is assigned only once and is sent once.
}

func (h *handleInfo) isFinished(input int64) bool {
	if !h.isSent || input < h.endHandle-1 {
		return false
	}
	return true
}

// addTableIndex adds index into table.
// TODO: Move this to doc or wiki.
// How to add index in reorganization state?
// Concurrently process the defaultTaskHandleCnt tasks. Each task deals with a handle range of the index record.
// The handle range size is defaultTaskHandleCnt.
// Because each handle range depends on the previous one, it's necessary to obtain the handle range serially.
// Real concurrent processing needs to perform after the handle range has been acquired.
// The operation flow of the each task of data is as follows:
//  1. Open a goroutine. Traverse the snapshot to obtain the handle range, while accessing the corresponding row key and
// raw index value. Then notify to start the next task.
//  2. Decode this task of raw index value to get the corresponding index value.
//  3. Deal with these index records one by one. If the index record exists, skip to the next row.
// If the index doesn't exist, create the index and then continue to handle the next row.
//  4. When the handle of a range is completed, return the corresponding task result.
// The above operations are completed in a transaction.
// When concurrent tasks are processed, the task result returned by each task is sorted by the handle. Then traverse the
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
	taskOpInfo := &indexTaskOpInfo{
		tblIndex:  tables.NewIndex(t.Meta(), indexInfo),
		colMap:    colMap,
		taskBatch: defaultTaskHandleCnt,
		reorgInfo: reorgInfo,
	}

	// f, err := os.Create("cpuprofile")
	// if err != nil {
	// 	log.Error("err:", err)
	// } else {
	// 	pprof.StartCPUProfile(f)
	// 	defer pprof.StopCPUProfile()
	// }

	addedCount := job.GetRowCount()
	baseHandle := reorgInfo.Handle

	workers := make([]*worker, workerCnt)
	for i := 0; i < workerCnt; i++ {
		ctx := d.newContext()
		workers[i] = newWorker(ctx, i, int(taskOpInfo.taskBatch), len(cols), len(taskOpInfo.colMap))
	}
	for {
		startTime := time.Now()
		err := d.isReorgRunnable()
		if err != nil {
			return errors.Trace(err)
		}

		wg := sync.WaitGroup{}
		for i := 0; i < workerCnt; i++ {
			wg.Add(1)
			workers[i].setTaskNewRange(baseHandle)
			// TODO: Consider one worker to one goroutine.
			go workers[i].doBackfillIndexTask(t, taskOpInfo, &wg)
			baseHandle += taskOpInfo.taskBatch
		}
		wg.Wait()
		taskOpInfo.reorgInfo.Handle = baseHandle

		taskAddedCount, doneHandle, isEnd, err := getCountAndHandle(workers)
		addedCount += int64(taskAddedCount)
		sub := time.Since(startTime).Seconds()
		if err != nil {
			log.Warnf("[ddl] total added index for %d rows, this task add index for %d failed, take time %v",
				addedCount, taskAddedCount, sub)
			return errors.Trace(err)
		}
		d.setReorgRowCount(addedCount)
		batchHandleDataHistogram.WithLabelValues(batchAddIdx).Observe(sub)
		log.Infof("[ddl] total added index for %d rows, this task added index for %d rows, take time %v",
			addedCount, taskAddedCount, sub)

		if baseHandle < doneHandle+1 {
			baseHandle = doneHandle + 1
		}
		if isEnd {
			return nil
		}
	}
}

func getCountAndHandle(workers []*worker) (int64, int64, bool, error) {
	taskAddedCount, currHandle := int64(0), int64(0)
	var err error
	var isEnd bool
	for _, worker := range workers {
		ret := worker.taskRet
		if ret.err != nil {
			err = ret.err
			break
		}
		taskAddedCount += int64(ret.count)
		currHandle = ret.doneHandle
		isEnd = ret.isAllDone
	}
	return taskAddedCount, currHandle, isEnd, errors.Trace(err)
}

func (w *worker) doBackfillIndexTask(t table.Table, taskOpInfo *indexTaskOpInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	startTime := time.Now()
	var ret *taskResult
	err := kv.RunInNewTxn(w.ctx.GetStore(), true, func(txn kv.Transaction) error {
		// Update the reorg handle that has been processed.
		if w.id == 0 {
			err1 := taskOpInfo.reorgInfo.UpdateHandle(txn, taskOpInfo.reorgInfo.Handle)
			if err1 != nil {
				log.Warnf("[ddl] add index failed when update handle %d, err %v", taskOpInfo.reorgInfo.Handle, err1)
				ret = &taskResult{err: err1}
				return errors.Trace(ret.err)
			}
		}

		ret = w.doBackfillIndexTaskInTxn(t, txn, taskOpInfo)
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
func (w *worker) doBackfillIndexTaskInTxn(t table.Table, txn kv.Transaction, taskOpInfo *indexTaskOpInfo) *taskResult {
	idxRecords, taskRet := w.fetchRowColVals(txn, t, taskOpInfo)
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
		handle, err := taskOpInfo.tblIndex.Create(txn, idxRecord.vals, idxRecord.handle)
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
