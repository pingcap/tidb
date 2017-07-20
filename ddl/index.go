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
	"sort"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
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
					sumLength += int(math.Ceil(float64(col.Flen+7) / float64(8)))
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
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames)
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
		reorgInfo, err := d.getReorgInfo(t, job)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		var tbl table.Table
		tbl, err = d.getTable(schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		err = d.runReorgJob(job, func() error {
			return d.addTableIndex(tbl, indexInfo, reorgInfo, job)
		})
		if err != nil {
			if terror.ErrorEqual(err, errWaitReorgTimeout) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if terror.ErrorEqual(err, kv.ErrKeyExists) {
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
		err = d.runReorgJob(job, func() error {
			return d.dropTableIndex(indexInfo, job)
		})
		if err != nil {
			// If the timeout happens, we should return.
			// Then check for the owner and re-wait job to finish.
			return ver, errors.Trace(filterError(err, errWaitReorgTimeout))
		}

		// All reorganization jobs are done, drop this index.
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
		d.asyncNotifyEvent(&Event{Tp: model.ActionDropIndex, TableInfo: tblInfo, IndexInfo: indexInfo})
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func (d *ddl) fetchRowColVals(txn kv.Transaction, t table.Table, taskOpInfo *indexTaskOpInfo, handleInfo *handleInfo) (
	[]*indexRecord, *taskResult) {
	startTime := time.Now()
	handleCnt := defaultTaskHandleCnt
	rawRecords := make([][]byte, 0, handleCnt)
	idxRecords := make([]*indexRecord, 0, handleCnt)
	ret := &taskResult{doneHandle: handleInfo.startHandle}
	err := d.iterateSnapshotRows(t, txn.StartTS(), handleInfo.startHandle,
		func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
			rawRecords = append(rawRecords, rawRecord)
			indexRecord := &indexRecord{handle: h, key: rowKey}
			idxRecords = append(idxRecords, indexRecord)
			if len(idxRecords) == handleCnt || handleInfo.isFinished(h) {
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		ret.err = errors.Trace(err)
		return nil, ret
	}

	ret.count = len(idxRecords)
	if ret.count > 0 {
		ret.doneHandle = idxRecords[ret.count-1].handle
	}
	// Be sure to do this operation only once.
	if !handleInfo.isSent {
		// Notice to start the next task operation.
		taskOpInfo.nextCh <- ret.doneHandle
		// Record the last handle.
		// Ensure that the handle scope of the task doesn't change,
		// even if the transaction retries it can't effect the other tasks.
		handleInfo.endHandle = ret.doneHandle
		handleInfo.isSent = true
	}
	log.Debugf("[ddl] txn %v fetches handle info %v takes time %v", txn.StartTS(), handleInfo, time.Since(startTime))
	if ret.count == 0 {
		return nil, ret
	}

	err = d.getIndexRecords(t, taskOpInfo, rawRecords, idxRecords)
	if err != nil {
		ret.err = errors.Trace(err)
	}
	return idxRecords, ret
}

func (d *ddl) getIndexRecords(t table.Table, taskOpInfo *indexTaskOpInfo, rawRecords [][]byte, idxRecords []*indexRecord) error {
	cols := t.Cols()
	ctx := d.newContext()
	idxInfo := taskOpInfo.tblIndex.Meta()
	defaultVals := make([]types.Datum, len(cols))
	for i, idxRecord := range idxRecords {
		rowMap, err := tablecodec.DecodeRow(rawRecords[i], taskOpInfo.colMap, time.UTC)
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
			idxColumnVal := rowMap[col.ID]
			if _, ok := rowMap[col.ID]; ok {
				idxVal[j] = idxColumnVal
				continue
			}
			idxColumnVal, err = tables.GetColDefaultValue(ctx, col, defaultVals)
			if err != nil {
				return errors.Trace(err)
			}
			idxVal[j] = idxColumnVal
		}
		idxRecord.vals = idxVal
	}
	return nil
}

const (
	defaultBatchCnt      = 1024
	defaultSmallBatchCnt = 128
	defaultTaskHandleCnt = 128
	defaultTaskCnt       = 16
)

// taskResult is the result of the task.
type taskResult struct {
	count      int   // The number of records that has been processed in the task.
	doneHandle int64 // This is the last reorg handle that has been processed.
	err        error
}

type taskRetSlice []*taskResult

func (b taskRetSlice) Len() int           { return len(b) }
func (b taskRetSlice) Less(i, j int) bool { return b[i].doneHandle < b[j].doneHandle }
func (b taskRetSlice) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

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
	taskRetCh chan *taskResult           // Get the results of all tasks.
	nextCh    chan int64                 // It notifies to start the next task.
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
	taskCnt := defaultTaskCnt
	taskOpInfo := &indexTaskOpInfo{
		tblIndex:  tables.NewIndex(t.Meta(), indexInfo),
		colMap:    colMap,
		nextCh:    make(chan int64, 1),
		taskRetCh: make(chan *taskResult, taskCnt),
	}

	addedCount := job.GetRowCount()
	taskStartHandle := reorgInfo.Handle

	for {
		startTime := time.Now()
		wg := sync.WaitGroup{}
		for i := 0; i < taskCnt; i++ {
			wg.Add(1)
			go d.doBackfillIndexTask(t, taskOpInfo, taskStartHandle, &wg)
			doneHandle := <-taskOpInfo.nextCh
			// There is no data to seek.
			if doneHandle == taskStartHandle {
				break
			}
			taskStartHandle = doneHandle + 1
		}
		wg.Wait()

		retCnt := len(taskOpInfo.taskRetCh)
		taskAddedCount, doneHandle, err := getCountAndHandle(taskOpInfo)
		// Update the reorg handle that has been processed.
		if taskAddedCount != 0 {
			err1 := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
				return errors.Trace(reorgInfo.UpdateHandle(txn, doneHandle+1))
			})
			if err1 != nil {
				if err == nil {
					err = err1
				} else {
					log.Warnf("[ddl] add index failed when update handle %d, err %v", doneHandle, err)
				}
			}
		}

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

		if retCnt < taskCnt {
			return nil
		}
	}
}

// handleInfo records start and end handle that is used in a task.
type handleInfo struct {
	startHandle int64
	endHandle   int64
	isSent      bool // It ensures that the endHandle is assigned only once and is sent once.
}

func (h *handleInfo) isFinished(input int64) bool {
	if !h.isSent || input < h.endHandle {
		return false
	}
	return true
}

func getCountAndHandle(taskOpInfo *indexTaskOpInfo) (int64, int64, error) {
	l := len(taskOpInfo.taskRetCh)
	taskRets := make([]*taskResult, 0, l)
	for i := 0; i < l; i++ {
		taskRet := <-taskOpInfo.taskRetCh
		taskRets = append(taskRets, taskRet)
	}
	sort.Sort(taskRetSlice(taskRets))

	taskAddedCount, currHandle := int64(0), int64(0)
	var err error
	for _, ret := range taskRets {
		if ret.err != nil {
			err = ret.err
			break
		}
		taskAddedCount += int64(ret.count)
		currHandle = ret.doneHandle
	}
	return taskAddedCount, currHandle, errors.Trace(err)
}

func (d *ddl) doBackfillIndexTask(t table.Table, taskOpInfo *indexTaskOpInfo, startHandle int64, wg *sync.WaitGroup) {
	defer wg.Done()

	startTime := time.Now()
	ret := new(taskResult)
	handleInfo := &handleInfo{startHandle: startHandle}
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		err1 := d.isReorgRunnable(txn, ddlJobFlag)
		if err1 != nil {
			return errors.Trace(err1)
		}
		ret = d.doBackfillIndexTaskInTxn(t, txn, taskOpInfo, handleInfo)
		if ret.err != nil {
			return errors.Trace(ret.err)
		}
		return nil
	})
	if err != nil {
		ret.err = errors.Trace(err)
	}

	// It's failed to fetch row keys.
	if !handleInfo.isSent {
		taskOpInfo.nextCh <- startHandle
	}

	taskOpInfo.taskRetCh <- ret
	log.Debugf("[ddl] add index completes backfill index task %v takes time %v",
		handleInfo, time.Since(startTime))
}

// doBackfillIndexTaskInTxn deals with a part of backfilling index data in a Transaction.
// This part of the index data rows is defaultTaskHandleCnt.
func (d *ddl) doBackfillIndexTaskInTxn(t table.Table, txn kv.Transaction, taskOpInfo *indexTaskOpInfo,
	handleInfo *handleInfo) *taskResult {
	idxRecords, taskRet := d.fetchRowColVals(txn, t, taskOpInfo, handleInfo)
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
			if terror.ErrorEqual(err, kv.ErrKeyExists) && idxRecord.handle == handle {
				// Index already exists, skip it.
				continue
			}
			taskRet.err = errors.Trace(err)
			return taskRet
		}
	}
	return taskRet
}

func (d *ddl) dropTableIndex(indexInfo *model.IndexInfo, job *model.Job) error {
	startKey := tablecodec.EncodeTableIndexPrefix(job.TableID, indexInfo.ID)
	// It's asynchronous so it doesn't need to consider if it completes.
	deleteAll := -1
	_, _, err := d.delKeysWithStartKey(startKey, startKey, ddlJobFlag, job, deleteAll)
	return errors.Trace(err)
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

func (d *ddl) iterateSnapshotRows(t table.Table, version uint64, seekHandle int64, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}
	snap, err := d.store.GetSnapshot(ver)
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
			if terror.ErrorEqual(err, kv.ErrNotExist) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
