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
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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
		col := model.FindColumnInfo(columns, ic.Column.Name.O)
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

func validateRenameIndex(from, to model.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := findIndexByName(from.L, tbl.Indices); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := findIndexByName(to.L, tbl.Indices); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var from, to model.CIStr
	if err := job.DecodeArgs(&from, &to); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in ddl_api.go
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		return ver, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	idx := findIndexByName(from.L, tblInfo.Indices)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onCreateIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(t, job)
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
		return ver, ErrDupKeyName.Gen("index already exist %s", indexName)
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
		tbl, err = getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var reorgInfo *reorgInfo
		reorgInfo, err = getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = w.runReorgJob(t, reorgInfo, d.lease, func() error {
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) {
				log.Warnf("[ddl] run DDL job %v err %v, convert job to rollback job", job, err)
				ver, err = convert2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

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

func convert2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (int64, error) {
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

func onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
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
			job.Args[0] = indexInfo.ID
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

const (
	// DefaultTaskHandleCnt is default batch size of adding indices.
	DefaultTaskHandleCnt = 128
)

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	id          int
	ddlWorker   *worker
	batchCnt    int
	sessCtx     sessionctx.Context
	taskCh      chan *reorgIndexTask
	resultCh    chan *addIndexResult
	index       table.Index
	table       table.Table
	colFieldMap map[int64]*types.FieldType
	closed      bool
	priority    int

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

type reorgIndexTask struct {
	physicalTableID int64
	startHandle     int64
	endHandle       int64
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle int64
	err        error
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, colFieldMap map[int64]*types.FieldType) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	return &addIndexWorker{
		id:          id,
		ddlWorker:   worker,
		batchCnt:    DefaultTaskHandleCnt,
		sessCtx:     sessCtx,
		taskCh:      make(chan *reorgIndexTask, 1),
		resultCh:    make(chan *addIndexResult, 1),
		index:       index,
		table:       t,
		colFieldMap: colFieldMap,
		priority:    kv.PriorityLow,
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

// getIndexRecord gets index columns values from raw binary value row.
func (w *addIndexWorker) getIndexRecord(handle int64, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	t := w.table
	cols := t.Cols()
	idxInfo := w.index.Meta()
	_, err := tablecodec.DecodeRowWithMap(rawRecord, w.colFieldMap, time.UTC, w.rowMap)
	if err != nil {
		return nil, errors.Trace(err)
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		if col.IsPKHandleColumn(t.Meta()) {
			if mysql.HasUnsignedFlag(col.Flag) {
				idxVal[j].SetUint64(uint64(handle))
			} else {
				idxVal[j].SetInt64(handle)
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
			return nil, errors.Trace(err)
		}
		idxVal[j] = idxColumnVal
	}
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal}
	return idxRecord, nil
}

// getNextHandle gets next handle of entry that we are going to process.
func (w *addIndexWorker) getNextHandle(taskRange reorgIndexTask, taskDone bool) (nextHandle int64) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return w.idxRecords[len(w.idxRecords)-1].handle + 1
	}

	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	if taskRange.endHandle == math.MaxInt64 || !taskRange.endIncluded {
		return taskRange.endHandle
	}

	return taskRange.endHandle + 1
}

// fetchRowColVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgIndexTask) ([]*indexRecord, int64, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle,
		func(handle int64, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			w.logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				taskDone = handle >= taskRange.endHandle
			} else {
				taskDone = handle > taskRange.endHandle
			}

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			idxRecord, err1 := w.getIndexRecord(handle, recordKey, rawRow)
			if err1 != nil {
				return false, errors.Trace(err1)
			}

			w.idxRecords = append(w.idxRecords, idxRecord)
			if handle == taskRange.endHandle {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	log.Debugf("[ddl] txn %v fetches handle info %v, takes time %v", txn.StartTS(), taskRange, time.Since(startTime))
	return w.idxRecords, w.getNextHandle(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		log.Infof("[ddl-reorg][SLOW-OPERATIONS] elapsed time: %v, message: %v", elapsed, slowMsg)
	}
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
}

func (w *addIndexWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	w.initBatchCheckBufs(len(idxRecords))
	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, record.vals, record.handle, w.idxKeyBufs[i])
		if err != nil {
			return errors.Trace(err)
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = idxKey

		w.batchCheckKeys = append(w.batchCheckKeys, idxKey)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := kv.BatchGetValues(txn, w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					return errors.Trace(err1)
				}

				if handle != idxRecords[i].handle {
					return errors.Trace(kv.ErrKeyExists)
				}
			}
			idxRecords[i].skip = true
		} else {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			if w.distinctCheckFlags[i] {
				batchVals[string(key)] = tables.EncodeHandle(idxRecords[i].handle)
			}
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// backfillIndexInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// backfillIndexInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (nextHandle int64, taskDone bool, addedCount, scanCount int, errInTxn error) {
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		addedCount = 0
		scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		var (
			idxRecords []*indexRecord
			err        error
		)
		idxRecords, nextHandle, taskDone, err = w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// Lock the row key to notify us that someone delete or update the row,
			// then we should not backfill the index of it, otherwise the adding index is redundant.
			err := txn.LockKeys(idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			// Create the index.
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

		return nil
	})
	w.logSlowOperations(time.Since(oprStartTime), "backfillIndexInTxn", 3000)

	return
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *addIndexWorker) handleBackfillTask(d *ddlCtx, task *reorgIndexTask) *addIndexResult {
	handleRange := *task
	result := &addIndexResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	startTime := time.Now()

	for {
		addedCount := 0
		nextHandle, taskDone, addedCount, scanCount, err := w.backfillIndexInTxn(handleRange)
		if err == nil {
			// Because reorgIndexTask may run a long time,
			// we should check whether this ddl job is still runnable.
			err = w.ddlWorker.isReorgRunnable(d)
		}
		if err != nil {
			result.err = err
			return result
		}

		result.nextHandle = nextHandle
		result.addedCount += addedCount
		result.scanCount += scanCount
		w.ddlWorker.reorgCtx.increaseRowCount(int64(addedCount))

		if result.scanCount-lastLogCount >= 30000 {
			lastLogCount = result.scanCount
			log.Infof("[ddl-reorg] worker(%v), finish batch addedCount:%v backfill, task addedCount:%v, task scanCount:%v, nextHandle:%v",
				w.id, addedCount, result.addedCount, result.scanCount, nextHandle)
		}

		handleRange.startHandle = nextHandle
		if taskDone {
			break
		}
	}
	rightParenthesis := ")"
	if task.endIncluded {
		rightParenthesis = "]"
	}
	log.Infof("[ddl-reorg] worker(%v), finish region %v ranges [%v,%v%s, addedCount:%v, scanCount:%v, nextHandle:%v, elapsed time(s):%v",
		w.id, task.physicalTableID, task.startHandle, task.endHandle, rightParenthesis, result.addedCount, result.scanCount, result.nextHandle, time.Since(startTime).Seconds())

	return result
}

var gofailMockAddindexErrOnceGuard bool

func (w *addIndexWorker) run(d *ddlCtx) {
	log.Infof("[ddl-reorg] worker[%v] start", w.id)
	defer func() {
		r := recover()
		if r != nil {
			buf := util.GetStack()
			log.Errorf("[ddl-reorg] addIndexWorker %v %s", r, buf)
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		}
		w.resultCh <- &addIndexResult{err: errReorgPanic}
	}()
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		log.Debug("[ddl-reorg] got backfill index task:#v", task)
		// gofail: var mockAddIndexErr bool
		//if w.id == 0 && mockAddIndexErr && !gofailMockAddindexErrOnceGuard {
		//	gofailMockAddindexErrOnceGuard = true
		//	result := &addIndexResult{addedCount: 0, nextHandle: 0, err: errors.Errorf("mock add index error")}
		//	w.resultCh <- result
		//	continue
		//}

		result := w.handleBackfillTask(d, task)
		w.resultCh <- result
	}
	log.Infof("[ddl-reorg] worker[%v] exit", w.id)
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

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle int64) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle).Next()

	log.Infof("[ddl-reorg] split partition %v range [%v, %v] from PD", t.GetPhysicalID(), startHandle, endHandle)
	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackoffer(context.Background(), maxSleep)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
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

func (w *worker) waitTaskResults(workers []*addIndexWorker, taskCnt int, totalAddedCount *int64, startHandle int64) (int64, int64, error) {
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		if result.err != nil {
			log.Warnf("[ddl-reorg] worker[%v] return err:%v", i, result.err)
		}

		if firstErr == nil {
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*addIndexWorker, batchTasks []*reorgIndexTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime).Seconds()
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(reorgInfo.d.store, true, func(txn kv.Transaction) error {
			return errors.Trace(reorgInfo.UpdateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime)
		log.Warnf("[ddl-reorg] total added index for %d rows, this task [%d,%d) add index for %d failed %v, take time %v, update handle err %v",
			*totalAddedCount, startHandle, nextHandle, taskAddedCount, err, elapsedTime, err1)
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime)
	log.Infof("[ddl-reorg] total added index for %d rows, this task [%d,%d) added index for %d rows, take time %v",
		*totalAddedCount, startHandle, nextHandle, taskAddedCount, elapsedTime)
	return nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(t table.Table, workers []*addIndexWorker, reorgInfo *reorgInfo, totalAddedCount *int64, kvRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	batchTasks := make([]*reorgIndexTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg indices tasks.
	for _, keyRange := range kvRanges {
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			return nil, errors.Trace(err)
		}

		endKey := t.RecordKey(endHandle)
		endIncluded := false
		if endKey.Cmp(keyRange.EndKey) < 0 {
			endIncluded = true
		}
		task := &reorgIndexTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			break
		}
	}

	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(batchTasks) < len(kvRanges) {
		// there are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	return nil, nil
}

// buildIndexForReorgInfo build backfilling tasks from [reorgInfo.StartHandle, reorgInfo.EndHandle),
// and send these tasks to add index workers, till we finish adding the indices.
func (w *worker) buildIndexForReorgInfo(t table.PhysicalTable, workers []*addIndexWorker, job *model.Job, reorgInfo *reorgInfo) error {
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[ddl-reorg] start to reorg index of %v region ranges, handle range:[%v, %v).", len(kvRanges), startHandle, endHandle)
		remains, err := w.sendRangeTaskToWorkers(t, workers, reorgInfo, &totalAddedCount, kvRanges)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// addPhysicalTableIndex handles the add index reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to add index in reorganization state?
// Concurrently process the defaultTaskHandleCnt tasks. Each task deals with a handle range of the index record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split table key range from PD regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	log.Infof("[ddl-reorg] addTableIndex, job:%s, reorgInfo:%#v", job, reorgInfo)
	colFieldMap := makeupIndexColFieldMap(t, indexInfo)

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	idxWorkers := make([]*addIndexWorker, workerCnt)
	for i := 0; i < int(workerCnt); i++ {
		sessCtx := newContext(reorgInfo.d.store)
		idxWorkers[i] = newAddIndexWorker(sessCtx, w, i, t, indexInfo, colFieldMap)
		idxWorkers[i].priority = job.Priority
		go idxWorkers[i].run(reorgInfo.d)
	}
	defer closeAddIndexWorkers(idxWorkers)
	err := w.buildIndexForReorgInfo(t, idxWorkers, job, reorgInfo)
	return errors.Trace(err)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return errors.Errorf("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTableIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateReorgInfo(tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalTableIndex(t.(table.PhysicalTable), idx, reorgInfo)
	}
	return errors.Trace(err)
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfo(t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		log.Errorf("[ddl-reorg] update reorg fail, %v error stack: %s", t, errors.ErrorStack(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	start, end, err := getTableRange(reorg.d, t.GetPartition(pid), reorg.Job.SnapshotVer, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	log.Infof("[ddl-reorg] job %v update reorgInfo partition %d range [%d %d]", reorg.Job.ID, pid, start, end)
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = kv.RunInNewTxn(reorg.d.store, true, func(txn kv.Transaction) error {
		return errors.Trace(reorg.UpdateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID))
	})
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
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

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, seekHandle int64, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}

	snap, err := store.GetSnapshot(ver)
	snap.SetPriority(priority)
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
