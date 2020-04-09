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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/schemautil"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

const maxPrefixLength = 3072
const maxCommentLength = 1024

func buildIndexColumns(columns []*model.ColumnInfo, idxColNames []*ast.IndexColName) ([]*model.IndexColumn, error) {
	// Build offsets.
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))

	// The sum of length of all index columns.
	sumLength := 0

	for _, ic := range idxColNames {
		col := model.FindColumnInfo(columns, ic.Column.Name.L)
		if col == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Column.Name)
		}

		if err := checkIndexColumn(col, ic); err != nil {
			return nil, err
		}

		indexColumnLength, err := getIndexColumnLength(col, ic.Length)
		if err != nil {
			return nil, err
		}
		sumLength += indexColumnLength

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

func checkIndexPrefixLength(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn) error {
	// The sum of length of all index columns.
	sumLength := 0
	for _, ic := range idxColumns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		indexColumnLength, err := getIndexColumnLength(col, ic.Length)
		if err != nil {
			return err
		}
		sumLength += indexColumnLength
		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > maxPrefixLength {
			return errors.Trace(errTooLongKey)
		}
	}
	return nil
}

func checkIndexColumn(col *model.ColumnInfo, ic *ast.IndexColName) error {
	if col.Flen == 0 && (types.IsTypeChar(col.FieldType.Tp) || types.IsTypeVarchar(col.FieldType.Tp)) {
		return errors.Trace(errWrongKeyColumn.GenWithStackByArgs(ic.Column.Name))
	}

	// JSON column cannot index.
	if col.FieldType.Tp == mysql.TypeJSON {
		return errors.Trace(errJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
	}

	// Length must be specified for BLOB and TEXT column indexes.
	if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
		return errors.Trace(errBlobKeyWithoutLength)
	}

	// Length can only be specified for specifiable types.
	if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
		return errors.Trace(errIncorrectPrefixKey)
	}

	// Key length must be shorter or equal to the column length.
	if ic.Length != types.UnspecifiedLength &&
		types.IsTypeChar(col.FieldType.Tp) && col.Flen < ic.Length {
		return errors.Trace(errIncorrectPrefixKey)
	}

	// Specified length must be shorter than the max length for prefix.
	if ic.Length > maxPrefixLength {
		return errors.Trace(errTooLongKey)
	}
	return nil
}

func getIndexColumnLength(col *model.ColumnInfo, colLen int) (int, error) {
	// Take care of the sum of length of all index columns.
	if colLen != types.UnspecifiedLength {
		return colLen, nil
	}
	// Specified data types.
	if col.Flen != types.UnspecifiedLength {
		// Special case for the bit type.
		if col.FieldType.Tp == mysql.TypeBit {
			return (col.Flen + 7) >> 3, nil
		}
		return col.Flen, nil

	}

	length, ok := mysql.DefaultLengthOfMysqlTypes[col.FieldType.Tp]
	if !ok {
		return length, errUnknownTypeLength.GenWithStackByArgs(col.FieldType.Tp)
	}

	// Special case for time fraction.
	if types.IsTypeFractionable(col.FieldType.Tp) &&
		col.FieldType.Decimal != types.UnspecifiedLength {
		decimalLength, ok := mysql.DefaultLengthOfTimeFraction[col.FieldType.Decimal]
		if !ok {
			return length, errUnknownFractionLength.GenWithStackByArgs(col.FieldType.Tp, col.FieldType.Decimal)
		}
		length += decimalLength
	}
	return length, nil
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, idxColNames []*ast.IndexColName, state model.SchemaState) (*model.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

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
	if fromIdx := schemautil.FindIndexByName(from.L, tbl.Indices); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := schemautil.FindIndexByName(to.L, tbl.Indices); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
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
	idx := schemautil.FindIndexByName(from.L, tblInfo.Indices)
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

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, ErrDupKeyName.GenWithStack("index already exist %s", indexName)
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
		logutil.Logger(ddlLogCtx).Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
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

		err = w.runReorgJob(t, reorgInfo, d.lease, func() (addIndexErr error) {
			defer func() {
				r := recover()
				if r != nil {
					buf := util.GetStack()
					logutil.Logger(ddlLogCtx).Error("[ddl] add table index panic", zap.Any("panic", r), zap.String("stack", string(buf)))
					metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
					addIndexErr = errCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tblInfo.Name, indexInfo.Name)
				}
			}()
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) || errCantDecodeIndex.Equal(err) {
				logutil.Logger(ddlLogCtx).Warn("[ddl] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
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
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", tblInfo.State)
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

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
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
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compability,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", indexInfo.State)
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
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgIndexTask
	resultCh  chan *addIndexResult
	index     table.Index
	table     table.Table
	closed    bool
	priority  int

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	rowDecoder         *decoder.RowDecoder
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

func (r *reorgIndexTask) String() string {
	rightParenthesis := ")"
	if r.endIncluded {
		rightParenthesis = "]"
	}
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + strconv.FormatInt(r.startHandle, 10) + "," + strconv.FormatInt(r.endHandle, 10) + rightParenthesis
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle int64
	err        error
}

// addIndexTaskContext is the context of the batch adding indices.
// After finishing the batch adding indices, result in addIndexTaskContext will be merged into addIndexResult.
type addIndexTaskContext struct {
	nextHandle int64
	done       bool
	addedCount int
	scanCount  int
}

// mergeAddIndexCtxToResult merge partial result in taskCtx into result.
func mergeAddIndexCtxToResult(taskCtx *addIndexTaskContext, result *addIndexResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, decodeColMap)
	return &addIndexWorker{
		id:          id,
		ddlWorker:   worker,
		batchCnt:    int(variable.GetDDLReorgBatchSize()),
		sessCtx:     sessCtx,
		taskCh:      make(chan *reorgIndexTask, 1),
		resultCh:    make(chan *addIndexResult, 1),
		index:       index,
		table:       t,
		rowDecoder:  rowDecoder,
		priority:    kv.PriorityLow,
		defaultVals: make([]types.Datum, len(t.Cols())),
		rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
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
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		return nil, errors.Trace(errCantDecodeIndex.GenWithStackByArgs(err))
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
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if idxColumnVal.Kind() == types.KindMysqlTime {
			t := idxColumnVal.GetMysqlTime()
			if t.Type == mysql.TypeTimestamp && sysZone != time.UTC {
				err := t.ConvertTimeZone(sysZone, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
				}
				idxColumnVal.SetMysqlTime(t)
			}
		}
		idxVal[j] = idxColumnVal
	}
	// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
	// the generated value, so we need to clear up the reusing map.
	w.cleanRowMap()
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal}
	return idxRecord, nil
}

func (w *addIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
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
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
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

	logutil.Logger(ddlLogCtx).Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextHandle(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.Logger(ddlLogCtx).Info("[ddl] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
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

	batchVals, err := txn.BatchGet(w.batchCheckKeys)
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
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (taskCtx addIndexTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		idxRecords, nextHandle, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextHandle = nextHandle
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
			taskCtx.addedCount++
		}

		return nil
	})
	w.logSlowOperations(time.Since(oprStartTime), "backfillIndexInTxn", 3000)

	return
}

var addIndexSpeedCounter = metrics.AddIndexTotalCounter.WithLabelValues("speed")

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *addIndexWorker) handleBackfillTask(d *ddlCtx, task *reorgIndexTask) *addIndexResult {
	handleRange := *task
	result := &addIndexResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		taskCtx, err := w.backfillIndexInTxn(handleRange)
		if err == nil {
			// Because reorgIndexTask may run a long time,
			// we should check whether this ddl job is still runnable.
			err = w.ddlWorker.isReorgRunnable(d)
		}

		if err != nil {
			result.err = err
			return result
		}

		addIndexSpeedCounter.Add(float64(taskCtx.addedCount))
		mergeAddIndexCtxToResult(&taskCtx, result)
		w.ddlWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			logutil.Logger(ddlLogCtx).Info("[ddl] add index worker back fill index", zap.Int("workerID", w.id), zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount), zap.Int64("nextHandle", taskCtx.nextHandle), zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))

			lastLogTime = time.Now()
		}

		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			break
		}
	}
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()), zap.Int("addedCount", result.addedCount), zap.Int("scanCount", result.scanCount), zap.Int64("nextHandle", result.nextHandle), zap.String("takeTime", time.Since(startTime).String()))

	return result
}

func (w *addIndexWorker) run(d *ddlCtx) {
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker start", zap.Int("workerID", w.id))
	defer func() {
		r := recover()
		if r != nil {
			buf := util.GetStack()
			logutil.Logger(ddlLogCtx).Error("[ddl] add index worker panic", zap.Any("panic", r), zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		}
		w.resultCh <- &addIndexResult{err: errReorgPanic}
	}()
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		logutil.Logger(ddlLogCtx).Debug("[ddl] add index worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockAddIndexErr", func() {
			if w.id == 0 {
				result := &addIndexResult{addedCount: 0, nextHandle: 0, err: errors.Errorf("mock add index error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})

		// Dynamic change batch size.
		w.batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task)
		w.resultCh <- result
	}
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker exit", zap.Int("workerID", w.id))

}

func makeupDecodeColMap(sessCtx sessionctx.Context, t table.Table, indexInfo *model.IndexInfo) (map[int64]decoder.Column, error) {
	cols := t.Cols()
	indexedCols := make([]*table.Column, len(indexInfo.Columns))
	for i, v := range indexInfo.Columns {
		indexedCols[i] = cols[v.Offset]
	}

	var containsVirtualCol bool
	decodeColMap, err := decoder.BuildFullDecodeColMap(indexedCols, t, func(genCol *table.Column) (expression.Expression, error) {
		containsVirtualCol = true
		return expression.ParseSimpleExprCastWithTableInfo(sessCtx, genCol.GeneratedExprString, t.Meta(), &genCol.FieldType)
	})
	if err != nil {
		return nil, err
	}

	if containsVirtualCol {
		decoder.SubstituteGenColsInDecodeColMap(decodeColMap)
		decoder.RemoveUnusedVirtualCols(decodeColMap, indexedCols)
	}
	return decodeColMap, nil
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle int64) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle).Next()

	logutil.Logger(ddlLogCtx).Info("[ddl] split table range from PD", zap.Int64("physicalTableID", t.GetPhysicalID()), zap.Int64("startHandle", startHandle), zap.Int64("endHandle", endHandle))
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
			logutil.Logger(ddlLogCtx).Warn("[ddl] add index worker return error", zap.Int("workerID", worker.id), zap.Error(result.err))
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
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(reorgInfo.d.store, true, func(txn kv.Transaction) error {
			return errors.Trace(reorgInfo.UpdateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.Logger(ddlLogCtx).Warn("[ddl] add index worker handle batch tasks failed", zap.Int64("totalAddedCount", *totalAddedCount), zap.Int64("startHandle", startHandle), zap.Int64("nextHandle", nextHandle),
			zap.Int64("batchAddedCount", taskAddedCount), zap.String("taskFailedError", err.Error()), zap.String("takeTime", elapsedTime.String()), zap.NamedError("updateHandleError", err1))

		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.Logger(ddlLogCtx).Info("[ddl] add index worker handle batch tasks successful", zap.Int64("totalAddedCount", *totalAddedCount), zap.Int64("startHandle", startHandle),
		zap.Int64("nextHandle", nextHandle), zap.Int64("batchAddedCount", taskAddedCount), zap.String("takeTime", elapsedTime.String()))
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

var (
	// TestCheckWorkerNumCh use for test adjust add index worker.
	TestCheckWorkerNumCh = make(chan struct{}, 0)
	// TestCheckWorkerNumber use for test adjust add index worker.
	TestCheckWorkerNumber = int32(16)
)

func loadDDLReorgVars(w *worker) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return ddlutil.LoadDDLReorgVars(ctx)
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
	logutil.Logger(ddlLogCtx).Info("[ddl] start to add table index", zap.String("job", job.String()), zap.String("reorgInfo", reorgInfo.String()))
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, t, indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	idxWorkers := make([]*addIndexWorker, 0, workerCnt)
	defer func() {
		closeAddIndexWorkers(idxWorkers)
	}()

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		// For dynamic adjust add index worker number.
		if err := loadDDLReorgVars(w); err != nil {
			logutil.Logger(ddlLogCtx).Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
		}
		workerCnt = variable.GetDDLReorgWorkerCounter()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		for i := len(idxWorkers); i < int(workerCnt); i++ {
			sessCtx := newContext(reorgInfo.d.store)
			idxWorker := newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeColMap)
			idxWorker.priority = job.Priority
			idxWorkers = append(idxWorkers, idxWorker)
			go idxWorkers[i].run(reorgInfo.d)
		}
		// Shrink the worker size.
		if len(idxWorkers) > int(workerCnt) {
			workers := idxWorkers[workerCnt:]
			idxWorkers = idxWorkers[:workerCnt]
			closeAddIndexWorkers(workers)
		}

		failpoint.Inject("checkIndexWorkerNum", func(val failpoint.Value) {
			if val.(bool) {
				num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
				if num != 0 {
					if num > len(kvRanges) {
						if len(idxWorkers) != len(kvRanges) {
							failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
						}
					} else if num != len(idxWorkers) {
						failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
					}
					TestCheckWorkerNumCh <- struct{}{}
				}
			}
		})

		logutil.Logger(ddlLogCtx).Info("[ddl] start add index workers to reorg index", zap.Int("workerCnt", len(idxWorkers)), zap.Int("regionCnt", len(kvRanges)), zap.Int64("startHandle", startHandle), zap.Int64("endHandle", endHandle))
		remains, err := w.sendRangeTaskToWorkers(t, idxWorkers, reorgInfo, &totalAddedCount, kvRanges)
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

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return errCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
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
		logutil.Logger(ddlLogCtx).Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
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
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = kv.RunInNewTxn(reorg.d.store, true, func(txn kv.Transaction) error {
		return errors.Trace(reorg.UpdateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID))
	})
	logutil.Logger(ddlLogCtx).Info("[ddl] job update reorgInfo", zap.Int64("jobID", reorg.Job.ID), zap.Int64("partitionTableID", pid), zap.Int64("startHandle", start), zap.Int64("endHandle", end), zap.Error(err))
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

func allocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h int64, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startHandle int64, endHandle int64, endIncluded bool, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}

	snap, err := store.GetSnapshot(ver)
	snap.SetPriority(priority)
	if err != nil {
		return errors.Trace(err)
	}
	firstKey := t.RecordKey(startHandle)

	// Calculate the exclusive upper bound
	var upperBound kv.Key
	if endIncluded {
		if endHandle == math.MaxInt64 {
			upperBound = t.RecordKey(endHandle).PrefixNext()
		} else {
			// PrefixNext is time costing. Try to avoid it if possible.
			upperBound = t.RecordKey(endHandle + 1)
		}
	} else {
		upperBound = t.RecordKey(endHandle)
	}

	it, err := snap.Iter(firstKey, upperBound)
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
