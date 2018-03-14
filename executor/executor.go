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

package executor

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	_ Executor = &CheckTableExec{}
	_ Executor = &ExistsExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &ShowDDLJobsExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}
	_ Executor = &RecoverIndexExec{}
	_ Executor = &CheckIndexExec{}
	_ Executor = &HashJoinExec{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &MergeJoinExec{}
)

// Error instances.
var (
	ErrUnknownPlan          = terror.ClassExecutor.New(codeUnknownPlan, "Unknown plan")
	ErrPrepareMulti         = terror.ClassExecutor.New(codePrepareMulti, "Can not prepare multiple statements")
	ErrPrepareDDL           = terror.ClassExecutor.New(codePrepareDDL, "Can not prepare DDL statements")
	ErrPasswordNoMatch      = terror.ClassExecutor.New(CodePasswordNoMatch, "Can't find any matching row in the user table")
	ErrResultIsEmpty        = terror.ClassExecutor.New(codeResultIsEmpty, "result is empty")
	ErrBuildExecutor        = terror.ClassExecutor.New(codeErrBuildExec, "Failed to build executor")
	ErrBatchInsertFail      = terror.ClassExecutor.New(codeBatchInsertFail, "Batch insert failed, please clean the table and try again.")
	ErrWrongValueCountOnRow = terror.ClassExecutor.New(codeWrongValueCountOnRow, "Column count doesn't match value count at row %d")
	ErrPasswordFormat       = terror.ClassExecutor.New(codePasswordFormat, "The password hash doesn't have the expected format. Check if the correct password algorithm is being used with the PASSWORD() function.")
)

// Error codes.
const (
	codeUnknownPlan          terror.ErrCode = 1
	codePrepareMulti         terror.ErrCode = 2
	codePrepareDDL           terror.ErrCode = 7
	codeResultIsEmpty        terror.ErrCode = 8
	codeErrBuildExec         terror.ErrCode = 9
	codeBatchInsertFail      terror.ErrCode = 10
	CodePasswordNoMatch      terror.ErrCode = 1133 // MySQL error code
	CodeCannotUser           terror.ErrCode = 1396 // MySQL error code
	codeWrongValueCountOnRow terror.ErrCode = 1136 // MySQL error code
	codePasswordFormat       terror.ErrCode = 1827 // MySQL error code
)

// Row represents a result set row, it may be returned from a table, a join, or a projection.
//
// The following cases will need store the handle information:
//
// If the top plan is update or delete, then every executor will need the handle.
// If there is an union scan, then the below scan plan must store the handle.
// If there is sort need in the double read, then the table scan of the double read must store the handle.
// If there is a select for update. then we need to store the handle until the lock plan. But if there is aggregation, the handle info can be removed.
// Otherwise the executor's returned rows don't need to store the handle information.
type Row = types.DatumRow

type baseExecutor struct {
	ctx             sessionctx.Context
	id              string
	schema          *expression.Schema
	maxChunkSize    int
	children        []Executor
	childrenResults []*chunk.Chunk
	retFieldTypes   []*types.FieldType
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, child.newChunk())
	}
	return nil
}

// Next implements interface Executor.
// To be removed in near future.
func (e *baseExecutor) Next(context.Context) (Row, error) {
	return nil, nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	for _, child := range e.children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.childrenResults = nil
	return nil
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// newChunk creates a new chunk to buffer current executor's result.
func (e *baseExecutor) newChunk() *chunk.Chunk {
	return chunk.NewChunk(e.retTypes())
}

// retTypes returns all output column types.
func (e *baseExecutor) retTypes() []*types.FieldType {
	return e.retFieldTypes
}

// NextChunk fills mutiple rows into a chunk.
func (e *baseExecutor) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id string, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Executor executes a query.
type Executor interface {
	Next(context.Context) (Row, error)
	Close() error
	Open(context.Context) error
	Schema() *expression.Schema
	retTypes() []*types.FieldType
	newChunk() *chunk.Chunk
	// NextChunk fills a chunk with multiple rows
	// NOTE: chunk has to call Reset() method before any use.
	NextChunk(ctx context.Context, chk *chunk.Chunk) error
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context) (Row, error) {
	var row Row
	if e.cursor < len(e.jobIDs) {
		ret := "successful"
		if e.errs[e.cursor] != nil {
			ret = fmt.Sprintf("error: %v", e.errs[e.cursor])
		}
		row = types.MakeDatums(e.jobIDs[e.cursor], ret)
		e.cursor++
	}

	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *CancelDDLJobsExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(e.maxChunkSize, len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		chk.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
		if e.errs[i] != nil {
			chk.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			chk.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	baseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *admin.DDLInfo
	done       bool
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next(ctx context.Context) (Row, error) {
	if e.done {
		return nil, nil
	}

	var ddlJob string
	if e.ddlInfo.Job != nil {
		ddlJob = e.ddlInfo.Job.String()
	}

	row := types.MakeDatums(
		e.ddlInfo.SchemaVer,
		e.ddlOwnerID,
		ddlJob,
		e.selfID,
	)
	e.done = true

	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ShowDDLExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}

	ddlJob := ""
	if e.ddlInfo.Job != nil {
		ddlJob = e.ddlInfo.Job.String()
	}
	chk.AppendInt64(0, e.ddlInfo.SchemaVer)
	chk.AppendString(1, e.ddlOwnerID)
	chk.AppendString(2, ddlJob)
	chk.AppendString(3, e.selfID)
	e.done = true
	return nil
}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobsExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	jobs, err := admin.GetDDLJobs(e.ctx.Txn())
	if err != nil {
		return errors.Trace(err)
	}
	historyJobs, err := admin.GetHistoryDDLJobs(e.ctx.Txn())
	if err != nil {
		return errors.Trace(err)
	}
	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(ctx context.Context) (Row, error) {
	if e.cursor >= len(e.jobs) {
		return nil, nil
	}

	job := e.jobs[e.cursor]
	row := types.MakeDatums(job.String(), job.State.String())
	e.cursor++

	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ShowDDLJobsExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(e.maxChunkSize, len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		chk.AppendString(0, e.jobs[i].String())
		chk.AppendString(1, e.jobs[i].State.String())
	}
	e.cursor += numCurBatch
	return nil
}

// RecoverIndexExec represents a recover index executor.
// It is built from "admin recover index" statement, is used to backfill
// corrupted index.
type RecoverIndexExec struct {
	baseExecutor

	done bool

	index     table.Index
	table     table.Table
	batchSize int

	columns       []*model.ColumnInfo
	colFieldTypes []*types.FieldType
	srcChunk      *chunk.Chunk

	// below buf is used to reduce allocations.
	recoverRows []recoverRows
	idxValsBufs [][]types.Datum
	idxKeyBufs  [][]byte
}

func (e *RecoverIndexExec) columnsTypes() []*types.FieldType {
	if e.colFieldTypes != nil {
		return e.colFieldTypes
	}

	e.colFieldTypes = make([]*types.FieldType, 0, len(e.columns))
	for _, col := range e.columns {
		e.colFieldTypes = append(e.colFieldTypes, &col.FieldType)
	}
	return e.colFieldTypes
}

// Open implements the Executor Open interface.
func (e *RecoverIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.srcChunk = chunk.NewChunk(e.columnsTypes())
	e.batchSize = 2048
	e.recoverRows = make([]recoverRows, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Datum, e.batchSize)
	e.idxKeyBufs = make([][]byte, e.batchSize)
	return nil
}

// Next implements the Executor Next interface.
func (e *RecoverIndexExec) Next(ctx context.Context) (Row, error) {
	return nil, nil
}

func (e *RecoverIndexExec) constructTableScanPB(pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	tblScan := &tipb.TableScan{
		TableId: e.table.Meta().ID,
		Columns: pbColumnInfos,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func (e *RecoverIndexExec) constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func (e *RecoverIndexExec) buildDAGPB(txn kv.Transaction, limitCnt uint64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = txn.StartTS()
	dagReq.TimeZoneOffset = timeZoneOffset(e.ctx)
	sc := e.ctx.GetSessionVars().StmtCtx
	dagReq.Flags = statementContextToFlags(sc)
	for i := range e.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	tblInfo := e.table.Meta()
	pbColumnInfos := plan.ColumnsToProto(e.columns, tblInfo.PKIsHandle)
	err := plan.SetPBColumnsDefaultValue(e.ctx, pbColumnInfos, e.columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tblScanExec := e.constructTableScanPB(pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)

	limitExec := e.constructLimitPB(limitCnt)
	dagReq.Executors = append(dagReq.Executors, limitExec)

	return dagReq, nil
}

func (e *RecoverIndexExec) buildTableScan(ctx context.Context, txn kv.Transaction, t table.Table, startHandle int64, limitCnt uint64) (distsql.SelectResult, error) {
	dagPB, err := e.buildDAGPB(txn, limitCnt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tblInfo := e.table.Meta()
	ranges := []*ranger.NewRange{{LowVal: []types.Datum{types.NewIntDatum(startHandle)}, HighVal: []types.Datum{types.NewIntDatum(math.MaxInt64)}}}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(tblInfo.ID, ranges, nil).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()

	// Actually, with limitCnt, the match datas maybe only in one region, so let the concurrency to be 1,
	// avoid unnecessary region scan.
	kvReq.Concurrency = 1
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.columnsTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

type backfillResult struct {
	nextHandle   int64
	addedCount   int64
	scanRowCount int64
}

func (e *RecoverIndexExec) backfillIndex(ctx context.Context) (int64, int64, error) {
	var (
		nextHandle    = int64(math.MinInt64)
		totalAddedCnt = int64(0)
		totalScanCnt  = int64(0)
		lastLogCnt    = int64(0)
		result        backfillResult
	)
	for {
		errInTxn := kv.RunInNewTxn(e.ctx.GetStore(), true, func(txn kv.Transaction) error {
			var err error
			result, err = e.backfillIndexInTxn(ctx, txn, nextHandle)
			return errors.Trace(err)
		})
		if errInTxn != nil {
			return totalAddedCnt, totalScanCnt, errors.Trace(errInTxn)
		}
		totalAddedCnt += result.addedCount
		totalScanCnt += result.scanRowCount
		if totalScanCnt-lastLogCnt >= 50000 {
			lastLogCnt = totalScanCnt
			log.Infof("[recover-index] recover table:%v, index:%v, totalAddedCnt:%v, totalScanCnt:%v, nextHandle: %v",
				e.table.Meta().Name.O, e.index.Meta().Name.O, totalAddedCnt, totalScanCnt, result.nextHandle)
		}

		// no more rows
		if result.scanRowCount == 0 {
			break
		}
		nextHandle = result.nextHandle
	}
	return totalAddedCnt, totalScanCnt, nil
}

func (e *RecoverIndexExec) fetchIdxVals(row chunk.Row, idxVals []types.Datum) []types.Datum {
	if idxVals == nil {
		idxVals = make([]types.Datum, 0, row.Len()-1)
	} else {
		idxVals = idxVals[:0]
	}

	for i := 0; i < row.Len()-1; i++ {
		colVal := row.GetDatum(i, &e.columns[i].FieldType)
		idxVals = append(idxVals, *colVal.Copy())
	}
	return idxVals
}

type recoverRows struct {
	handle  int64
	idxVals []types.Datum
	skip    bool
}

func (e *RecoverIndexExec) fetchRecoverRows(ctx context.Context, srcResult distsql.SelectResult, result *backfillResult) ([]recoverRows, error) {
	e.recoverRows = e.recoverRows[:0]
	handleIdx := len(e.columns) - 1
	result.scanRowCount = 0
Loop:
	for {
		err := srcResult.NextChunk(ctx, e.srcChunk)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if e.srcChunk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			if result.scanRowCount >= int64(e.batchSize) {
				break Loop
			}
			handle := row.GetInt64(handleIdx)
			idxVals := e.fetchIdxVals(row, e.idxValsBufs[result.scanRowCount])
			e.idxValsBufs[result.scanRowCount] = idxVals
			e.recoverRows = append(e.recoverRows, recoverRows{handle: handle, idxVals: idxVals, skip: false})
			result.scanRowCount++
			result.nextHandle = handle + 1
		}
	}

	return e.recoverRows, nil
}

func (e *RecoverIndexExec) batchMarkDup(txn kv.Transaction, rows []recoverRows) error {
	if len(rows) == 0 {
		return nil
	}

	sc := e.ctx.GetSessionVars().StmtCtx
	batchDistinctKeys := make([]kv.Key, 0, len(rows))
	batchNonDistinctKeys := make([]kv.Key, 0, len(rows))
	distinctOffsets := make([]int, 0, len(rows))
	nonDistinctOffsets := make([]int, 0, len(rows))

	for i, row := range rows {
		idxKey, distinct, err := e.index.GenIndexKey(sc, row.idxVals, row.handle, e.idxKeyBufs[i])
		if err != nil {
			return errors.Trace(err)
		}
		e.idxKeyBufs[i] = idxKey
		if distinct {
			batchDistinctKeys = append(batchDistinctKeys, idxKey)
			distinctOffsets = append(distinctOffsets, i)
		} else {
			batchNonDistinctKeys = append(batchNonDistinctKeys, idxKey)
			nonDistinctOffsets = append(nonDistinctOffsets, i)
		}
	}

	values, err := txn.GetSnapshot().BatchGet(batchDistinctKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range batchDistinctKeys {
		offset := distinctOffsets[i]
		if val, found := values[string(key)]; found {
			handle, err1 := tables.DecodeHandle(val)
			if err1 != nil {
				return errors.Trace(err1)
			}
			if handle == rows[offset].handle {
				rows[offset].skip = true
			} else {
				log.Warnf("[recover-index] The constraint of unique index:%v is broken, handle:%v is not equal handle:%v with idxKey:%v.",
					e.index.Meta().Name.O, handle, rows[offset].handle, key)
				// no need to overwrite the broken index.
				rows[offset].skip = true
			}
		}
	}

	values, err = txn.GetSnapshot().BatchGet(batchNonDistinctKeys)
	if err != nil {
		return errors.Trace(err)
	}

	for i, key := range batchNonDistinctKeys {
		offset := nonDistinctOffsets[i]
		if _, found := values[string(key)]; found {
			rows[offset].skip = true
		}
	}

	return nil
}

func (e *RecoverIndexExec) backfillIndexInTxn(ctx context.Context, txn kv.Transaction, startHandle int64) (result backfillResult, err error) {
	result.nextHandle = startHandle
	srcResult, err := e.buildTableScan(ctx, txn, e.table, startHandle, uint64(e.batchSize))
	if err != nil {
		return result, errors.Trace(err)
	}
	defer terror.Call(srcResult.Close)

	rows, err := e.fetchRecoverRows(ctx, srcResult, &result)
	if err != nil {
		return result, errors.Trace(err)
	}

	err = e.batchMarkDup(txn, rows)
	if err != nil {
		return result, errors.Trace(err)
	}

	oldBatchCheck := e.ctx.GetSessionVars().StmtCtx.BatchCheck
	// Constrains is already checked.
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	for _, row := range rows {
		if row.skip {
			continue
		}

		recordKey := e.table.RecordKey(row.handle)
		err := txn.LockKeys(recordKey)
		if err != nil {
			return result, errors.Trace(err)
		}

		_, err = e.index.Create(e.ctx, txn, row.idxVals, row.handle)
		if err != nil {
			return result, errors.Trace(err)
		}
		result.addedCount++
	}
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = oldBatchCheck
	return result, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *RecoverIndexExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}

	totalAddedCnt, totalScanCnt, err := e.backfillIndex(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	chk.AppendInt64(0, totalAddedCnt)
	chk.AppendInt64(1, totalScanCnt)
	e.done = true
	return nil
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

	tables []*ast.TableName
	done   bool
	is     infoschema.InfoSchema
}

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.done = false
	return nil
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context) (Row, error) {
	if e.done {
		return nil, nil
	}
	err := e.run(ctx)
	e.done = true
	return nil, errors.Trace(err)
}

// NextChunk implements the Executor NextChunk interface.
func (e *CheckTableExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	err := e.run(ctx)
	e.done = true
	return errors.Trace(err)
}

func (e *CheckTableExec) run(ctx context.Context) error {
	dbName := model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)
	for _, t := range e.tables {
		tb, err := e.is.TableByName(dbName, t.Name)
		if err != nil {
			return errors.Trace(err)
		}
		for _, idx := range tb.Indices() {
			txn := e.ctx.Txn()
			err = admin.CompareIndexData(e.ctx.GetSessionVars().StmtCtx, txn, tb, idx)
			if err != nil {
				return errors.Errorf("%v err:%v", t.Name, err)
			}
		}
	}
	return nil
}

// CheckIndexExec represents the executor of checking an index.
// It is built from the "admin check index" statement, and it checks
// the consistency of the index data with the records of the table.
type CheckIndexExec struct {
	baseExecutor

	dbName    string
	tableName string
	idxName   string
	src       *IndexLookUpExecutor
	done      bool
	is        infoschema.InfoSchema
}

// Open implements the Executor Open interface.
func (e *CheckIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := e.src.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.done = false
	return nil
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next(ctx context.Context) (Row, error) {
	if e.done {
		return nil, nil
	}
	defer func() { e.done = true }()

	err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tableName, []string{e.idxName})
	if err != nil {
		return nil, errors.Trace(err)
	}
	for {
		row, err := e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
	}
	return nil, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *CheckIndexExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()

	err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tableName, []string{e.idxName})
	if err != nil {
		return errors.Trace(err)
	}
	chk = e.src.newChunk()
	for {
		err := e.src.NextChunk(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return nil
}

// SelectLockExec represents a select lock executor.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Executor.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	baseExecutor

	Lock ast.SelectLockType
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context) (Row, error) {
	row, err := e.children[0].Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	// If there's no handle or it isn't a `select for update`.
	if len(e.Schema().TblID2Handle) == 0 || e.Lock != ast.SelectLockForUpdate {
		return row, nil
	}
	txn := e.ctx.Txn()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	for id, cols := range e.Schema().TblID2Handle {
		for _, col := range cols {
			handle := row[col.Index].GetInt64()
			lockKey := tablecodec.EncodeRowKeyWithHandle(id, handle)
			err = txn.LockKeys(lockKey)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// This operation is only for schema validator check.
			txnCtx.UpdateDeltaForTable(id, 0, 0)
		}
	}
	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *SelectLockExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.children[0].NextChunk(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.Schema().TblID2Handle) == 0 || e.Lock != ast.SelectLockForUpdate {
		return nil
	}
	txn := e.ctx.Txn()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	keys := make([]kv.Key, 0, chk.NumRows())
	iter := chunk.NewIterator4Chunk(chk)
	for id, cols := range e.Schema().TblID2Handle {
		for _, col := range cols {
			keys = keys[:0]
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				keys = append(keys, tablecodec.EncodeRowKeyWithHandle(id, row.GetInt64(col.Index)))
			}
			err = txn.LockKeys(keys...)
			if err != nil {
				return errors.Trace(err)
			}
			// This operation is only for schema validator check.
			txnCtx.UpdateDeltaForTable(id, 0, 0)
		}
	}
	return nil
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context) (Row, error) {
	for e.cursor < e.begin {
		srcRow, err := e.children[0].Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		e.cursor++
	}
	if e.cursor >= e.end {
		return nil, nil
	}
	srcRow, err := e.children[0].Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	e.cursor++
	return srcRow, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *LimitExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		err := e.children[0].NextChunk(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		batchSize := uint64(e.childrenResults[0].NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			chk.Append(e.childrenResults[0], int(begin), int(end))
			e.cursor += end
			return nil
		}
		e.cursor += batchSize
	}
	err := e.children[0].NextChunk(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}
	batchSize := uint64(chk.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		chk.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) (rows [][]types.Datum, err error) {
		err = sctx.ActivePendingTxn()
		if err != nil {
			return rows, errors.Trace(err)
		}
		e := &executorBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, errors.Trace(err)
		}
		ctx := context.TODO()
		err = exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return rows, errors.Trace(err)
		}
		for {
			chk := exec.newChunk()
			err = exec.NextChunk(ctx, chk)
			if err != nil {
				return rows, errors.Trace(err)
			}
			if chk.NumRows() == 0 {
				return rows, nil
			}
			iter := chunk.NewIterator4Chunk(chk)
			for r := iter.Begin(); r != iter.End(); r = iter.Next() {
				row := r.GetDatumRow(exec.retTypes())
				rows = append(rows, row)
			}
		}
	}
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		CodeCannotUser:           mysql.ErrCannotUser,
		CodePasswordNoMatch:      mysql.ErrPasswordNoMatch,
		codeWrongValueCountOnRow: mysql.ErrWrongValueCountOnRow,
		codePasswordFormat:       mysql.ErrPasswordFormat,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExecutor] = tableMySQLErrCodes
}

// ProjectionExec represents a select fields executor.
type ProjectionExec struct {
	baseExecutor

	exprs            []expression.Expression // Only used in Next().
	evaluatorSuit    *expression.EvaluatorSuit
	calculateNoDelay bool
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Next(ctx context.Context) (retRow Row, err error) {
	srcRow, err := e.children[0].Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	row := make([]types.Datum, 0, len(e.exprs))
	for _, expr := range e.exprs {
		val, err := expr.Eval(srcRow)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row = append(row, val)
	}
	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ProjectionExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if err := e.children[0].NextChunk(ctx, e.childrenResults[0]); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(e.evaluatorSuit.Run(e.ctx, e.childrenResults[0], chk))
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(ctx context.Context) (Row, error) {
	if e.numReturned >= e.numDualRows {
		return nil, nil
	}
	e.numReturned = e.numDualRows
	return Row{}, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *TableDualExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		chk.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			chk.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	baseExecutor

	batched   bool
	filters   []expression.Expression
	selected  []bool
	inputIter *chunk.Iterator4Chunk
	inputRow  chunk.Row
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childrenResults[0])
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plan.Plan Close interface.
func (e *SelectionExec) Close() error {
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}
	e.selected = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context) (Row, error) {
	for {
		srcRow, err := e.children[0].Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		match, err := expression.EvalBool(e.ctx, e.filters, srcRow)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return srcRow, nil
		}
	}
}

// NextChunk implements the Executor NextChunk interface.
func (e *SelectionExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	if !e.batched {
		return errors.Trace(e.unBatchedNextChunk(ctx, chk))
	}

	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if chk.NumRows() == e.maxChunkSize {
				return nil
			}
			chk.AppendRow(e.inputRow)
		}
		err := e.children[0].NextChunk(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if e.childrenResults[0].NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNextChunk filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNextChunk(ctx context.Context, chk *chunk.Chunk) error {
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				return errors.Trace(err)
			}
			if selected {
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		err := e.children[0].NextChunk(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childrenResults[0].NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t              table.Table
	asName         *model.CIStr
	ranges         []ranger.IntColumnRange
	seekHandle     int64
	iter           kv.Iterator
	cursor         int
	columns        []*model.ColumnInfo
	isVirtualTable bool

	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// NextChunk implements the Executor NextChunk interface.
func (e *TableScanExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.isVirtualTable {
		return errors.Trace(e.nextChunk4InfoSchema(ctx, chk))
	}
	handle, found, err := e.nextHandle()
	if err != nil || !found {
		return errors.Trace(err)
	}

	mutableRow := chunk.MutRowFromTypes(e.retTypes())
	for chk.NumRows() < e.maxChunkSize {
		row, err := e.getRow(handle)
		if err != nil {
			return errors.Trace(err)
		}
		e.seekHandle = handle + 1
		mutableRow.SetDatums(row...)
		chk.AppendRow(mutableRow.ToRow())
	}
	return nil
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(e.retTypes(), e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(e.retTypes())
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// nextHandle gets the unique handle for next row.
func (e *TableScanExec) nextHandle() (handle int64, found bool, err error) {
	for {
		if e.cursor >= len(e.ranges) {
			return 0, false, nil
		}
		ran := e.ranges[e.cursor]
		if e.seekHandle < ran.LowVal {
			e.seekHandle = ran.LowVal
		}
		if e.seekHandle > ran.HighVal {
			e.cursor++
			continue
		}
		handle, found, err = e.t.Seek(e.ctx, e.seekHandle)
		if err != nil || !found {
			return 0, false, errors.Trace(err)
		}
		if handle > ran.HighVal {
			// The handle is out of the current range, but may be in following ranges.
			// We seek to the range that may contains the handle, so we
			// don't need to seek key again.
			inRange := e.seekRange(handle)
			if !inRange {
				// The handle may be less than the current range low value, can not
				// return directly.
				continue
			}
		}
		return handle, true, nil
	}
}

// seekRange increments the range cursor to the range
// with high value greater or equal to handle.
func (e *TableScanExec) seekRange(handle int64) (inRange bool) {
	for {
		e.cursor++
		if e.cursor >= len(e.ranges) {
			return false
		}
		ran := e.ranges[e.cursor]
		if handle < ran.LowVal {
			return false
		}
		if handle > ran.HighVal {
			continue
		}
		return true
	}
}

func (e *TableScanExec) getRow(handle int64) (Row, error) {
	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		columns[i] = table.ToColumn(v)
	}
	row, err := e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return row, nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	e.iter = nil
	e.virtualTableChunkList = nil
	e.cursor = 0
	return nil
}

// ExistsExec represents exists executor.
type ExistsExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *ExistsExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.evaluated = false
	return nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ExistsExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.evaluated {
		e.evaluated = true
		err := e.children[0].NextChunk(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		if e.childrenResults[0].NumRows() > 0 {
			chk.AppendInt64(0, 1)
		} else {
			chk.AppendInt64(0, 0)
		}
	}
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.evaluated = false
	return nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *MaxOneRowExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := e.children[0].NextChunk(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}

	if num := chk.NumRows(); num == 0 {
		for i := range e.schema.Columns {
			chk.AppendNull(i)
		}
		return nil
	} else if num == 1 {
		return nil
	}

	return errors.New("subquery returns more than 1 row")
}

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionExec struct {
	baseExecutor

	stopFetchData atomic.Value
	resultCh      chan *execResult
	cursor        int
	wg            sync.WaitGroup

	// finished and all the following variables are used for chunk execution.
	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult
	initialized   bool
}

type execResult struct {
	rows []Row
	err  error
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished(forChunk bool) {
	e.wg.Wait()
	if forChunk {
		close(e.resultPool)
	} else {
		close(e.resultCh)
	}
}

func (e *UnionExec) fetchData(ctx context.Context, idx int) {
	batchSize := 128
	defer e.wg.Done()
	for {
		result := &execResult{
			rows: make([]Row, 0, batchSize),
			err:  nil,
		}
		for i := 0; i < batchSize; i++ {
			if e.stopFetchData.Load().(bool) {
				return
			}
			row, err := e.children[idx].Next(ctx)
			if err != nil {
				result.err = errors.Trace(err)
				break
			}
			if row == nil {
				break
			}
			result.rows = append(result.rows, row)
		}
		if len(result.rows) == 0 && result.err == nil {
			return
		}
		if result.err != nil {
			e.stopFetchData.Store(true)
		}
		select {
		case e.resultCh <- result:
		case <-e.finished:
			return
		}
	}
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context, forChunk bool) {
	if forChunk {
		e.resultPool = make(chan *unionWorkerResult, len(e.children))
		e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
		for i := range e.children {
			e.resourcePools[i] = make(chan *chunk.Chunk, 1)
			e.resourcePools[i] <- e.childrenResults[i]
			e.wg.Add(1)
			go e.resultPuller(ctx, i)
		}
	} else {
		e.resultCh = make(chan *execResult, len(e.children))
		e.cursor = 0
		for i := range e.children {
			e.wg.Add(1)
			go e.fetchData(ctx, i)
		}
	}
	go e.waitAllFinished(forChunk)
}

func (e *UnionExec) resultPuller(ctx context.Context, childID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("resultPuller panic stack is:\n%s", buf)
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		e.wg.Done()
	}()
	for {
		if e.stopFetchData.Load().(bool) {
			return
		}
		select {
		case <-e.finished:
			return
		case result.chk = <-e.resourcePools[childID]:
		}
		result.err = errors.Trace(e.children[childID].NextChunk(ctx, result.chk))
		if result.err == nil && result.chk.NumRows() == 0 {
			return
		}
		e.resultPool <- result
		if result.err != nil {
			e.stopFetchData.Store(true)
			return
		}
	}
}

// NextChunk implements the Executor NextChunk interface.
func (e *UnionExec) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.initialized {
		e.initialize(ctx, true)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	chk.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	close(e.finished)
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	return errors.Trace(e.baseExecutor.Close())
}
