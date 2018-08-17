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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	_ Executor = &CheckTableExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &ShowDDLJobsExec{}
	_ Executor = &ShowDDLJobQueriesExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}
	_ Executor = &CheckIndexExec{}
	_ Executor = &HashJoinExec{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &MergeJoinExec{}
)

type baseExecutor struct {
	ctx           sessionctx.Context
	id            string
	schema        *expression.Schema
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	for _, child := range e.children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
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
	return chunk.NewChunkWithCapacity(e.retTypes(), e.maxChunkSize)
}

// retTypes returns all output column types.
func (e *baseExecutor) retTypes() []*types.FieldType {
	return e.retFieldTypes
}

// Next fills mutiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
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

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	Open(context.Context) error
	Next(ctx context.Context, chk *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema

	retTypes() []*types.FieldType
	newChunk() *chunk.Chunk
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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
func (e *ShowDDLExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}

	ddlJobs := ""
	l := len(e.ddlInfo.Jobs)
	for i, job := range e.ddlInfo.Jobs {
		ddlJobs += job.String()
		if i != l-1 {
			ddlJobs += "\n"
		}
	}
	chk.AppendInt64(0, e.ddlInfo.SchemaVer)
	chk.AppendString(1, e.ddlOwnerID)
	chk.AppendString(2, ddlJobs)
	chk.AppendString(3, e.selfID)
	e.done = true
	return nil
}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor    int
	jobs      []*model.Job
	jobNumber int64
	is        infoschema.InfoSchema
}

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs
type ShowDDLJobQueriesExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	jobs, err := admin.GetDDLJobs(e.ctx.Txn())
	if err != nil {
		return errors.Trace(err)
	}
	historyJobs, err := admin.GetHistoryDDLJobs(e.ctx.Txn(), admin.DefNumHistoryJobs)
	if err != nil {
		return errors.Trace(err)
	}

	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(e.maxChunkSize, len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				chk.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
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
	if e.jobNumber == 0 {
		e.jobNumber = admin.DefNumHistoryJobs
	}
	historyJobs, err := admin.GetHistoryDDLJobs(e.ctx.Txn(), int(e.jobNumber))
	if err != nil {
		return errors.Trace(err)
	}
	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(e.maxChunkSize, len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		chk.AppendInt64(0, e.jobs[i].ID)
		chk.AppendString(1, getSchemaName(e.is, e.jobs[i].SchemaID))
		chk.AppendString(2, getTableName(e.is, e.jobs[i].TableID))
		chk.AppendString(3, e.jobs[i].Type.String())
		chk.AppendString(4, e.jobs[i].SchemaState.String())
		chk.AppendInt64(5, e.jobs[i].SchemaID)
		chk.AppendInt64(6, e.jobs[i].TableID)
		chk.AppendInt64(7, e.jobs[i].RowCount)
		chk.AppendString(8, model.TSConvert2Time(e.jobs[i].StartTS).String())
		chk.AppendString(9, e.jobs[i].State.String())
	}
	e.cursor += numCurBatch
	return nil
}

func getSchemaName(is infoschema.InfoSchema, id int64) string {
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = DBInfo.Name.O
		return schemaName
	}

	return schemaName
}

func getTableName(is infoschema.InfoSchema, id int64) string {
	var tableName string
	table, ok := is.TableByID(id)
	if ok {
		tableName = table.Meta().Name.O
		return tableName
	}

	return tableName
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
func (e *CheckTableExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()
	for _, t := range e.tables {
		dbName := t.DBInfo.Name
		tb, err := e.is.TableByName(dbName, t.Name)
		if err != nil {
			return errors.Trace(err)
		}

		if tb.Meta().GetPartitionInfo() != nil {
			err = e.doCheckPartitionedTable(tb.(table.PartitionedTable))
		} else {
			err = e.doCheckTable(tb)
		}
		if err != nil {
			log.Warnf("%v error:%v", t.Name, errors.ErrorStack(err))
			if admin.ErrDataInConsistent.Equal(err) {
				return ErrAdminCheckTable.Gen("%v err:%v", t.Name, err)
			}

			return errors.Errorf("%v err:%v", t.Name, err)
		}
	}
	return nil
}

func (e *CheckTableExec) doCheckPartitionedTable(tbl table.PartitionedTable) error {
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.GetPartition(pid)
		if err := e.doCheckTable(partition); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *CheckTableExec) doCheckTable(tbl table.Table) error {
	for _, idx := range tbl.Indices() {
		txn := e.ctx.Txn()
		err := admin.CompareIndexData(e.ctx, txn, tbl, idx)
		if err != nil {
			return errors.Trace(err)
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

// Close implements the Executor Close interface.
func (e *CheckIndexExec) Close() error {
	return errors.Trace(e.src.Close())
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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
		err := e.src.Next(ctx, chk)
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

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	for id := range e.Schema().TblID2Handle {
		// This operation is only for schema validator check.
		txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.children[0].Next(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.Schema().TblID2Handle) == 0 || e.Lock != ast.SelectLockForUpdate {
		return nil
	}
	txn := e.ctx.Txn()
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

	childResult *chunk.Chunk
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		err := e.children[0].Next(ctx, e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		batchSize := uint64(e.childResult.NumRows())
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
			e.cursor += end
			if begin == end {
				break
			}
			chk.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	err := e.children[0].Next(ctx, chk)
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
	e.childResult = e.children[0].newChunk()
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	e.childResult = nil
	return errors.Trace(e.baseExecutor.Close())
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
			err = exec.Next(ctx, chk)
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
func (e *TableDualExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.childResult = e.children[0].newChunk()
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plan.Plan Close interface.
func (e *SelectionExec) Close() error {
	e.childResult = nil
	e.selected = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	if !e.batched {
		return errors.Trace(e.unBatchedNext(ctx, chk))
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
		err := e.children[0].Next(ctx, e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
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
		err := e.children[0].Next(ctx, e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t                     table.Table
	seekHandle            int64
	iter                  kv.Iterator
	columns               []*model.ColumnInfo
	isVirtualTable        bool
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, chk *chunk.Chunk) error {
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
		handle, found, err = e.t.Seek(e.ctx, e.seekHandle)
		if err != nil || !found {
			return 0, false, errors.Trace(err)
		}
		return handle, true, nil
	}
}

func (e *TableScanExec) getRow(handle int64) ([]types.Datum, error) {
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

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := e.children[0].Next(ctx, chk)
	if err != nil {
		return errors.Trace(err)
	}

	if num := chk.NumRows(); num == 0 {
		for i := range e.schema.Columns {
			chk.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return errors.New("subquery returns more than 1 row")
	}

	childChunk := e.children[0].newChunk()
	err = e.children[0].Next(ctx, childChunk)
	if childChunk.NumRows() != 0 {
		return errors.New("subquery returns more than 1 row")
	}

	return nil
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
	wg            sync.WaitGroup

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult
	initialized   bool

	childrenResults []*chunk.Chunk
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

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, child.newChunk())
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	e.resultPool = make(chan *unionWorkerResult, len(e.children))
	e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
	for i := range e.children {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.childrenResults[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	go e.waitAllFinished()
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
		result.err = errors.Trace(e.children[childID].Next(ctx, result.chk))
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

// Next implements the Executor Next interface.
func (e *UnionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.initialized {
		e.initialize(ctx)
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
	e.childrenResults = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	return errors.Trace(e.baseExecutor.Close())
}
