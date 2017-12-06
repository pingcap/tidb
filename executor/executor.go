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
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	goctx "golang.org/x/net/context"
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
	ctx             context.Context
	schema          *expression.Schema
	supportChk      bool
	maxChunkSize    int
	children        []Executor
	childrenResults []*chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *baseExecutor) Open(goCtx goctx.Context) error {
	for _, child := range e.children {
		err := child.Open(goCtx)
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

// Close implements the Executor Close interface.
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

// Schema implements the Executor Schema interface.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

func (e *baseExecutor) newChunk() *chunk.Chunk {
	return chunk.NewChunk(e.Schema().GetTypes())
}

func (e *baseExecutor) supportChunk() bool {
	if !e.supportChk {
		return false
	}
	for _, child := range e.children {
		if !child.supportChunk() {
			return false
		}
	}
	return true
}

func (e *baseExecutor) NextChunk(chk *chunk.Chunk) error {
	return nil
}

func newBaseExecutor(schema *expression.Schema, ctx context.Context, children ...Executor) baseExecutor {
	return baseExecutor{
		children:     children,
		ctx:          ctx,
		schema:       schema,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
}

// Executor executes a query.
type Executor interface {
	Next(goctx.Context) (Row, error)
	Close() error
	Open(goctx.Context) error
	Schema() *expression.Schema
	supportChunk() bool
	newChunk() *chunk.Chunk
	NextChunk(chk *chunk.Chunk) error
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	JobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(goCtx goctx.Context) (Row, error) {
	var row Row
	if e.cursor < len(e.JobIDs) {
		ret := "successful"
		if e.errs[e.cursor] != nil {
			ret = fmt.Sprintf("error: %v", e.errs[e.cursor])
		}
		row = types.MakeDatums(e.JobIDs[e.cursor], ret)
		e.cursor++
	}

	return row, nil
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
func (e *ShowDDLExec) Next(goCtx goctx.Context) (Row, error) {
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

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(goCtx goctx.Context) (Row, error) {
	if e.cursor >= len(e.jobs) {
		return nil, nil
	}

	job := e.jobs[e.cursor]
	row := types.MakeDatums(job.String(), job.State.String())
	e.cursor++

	return row, nil
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

	tables []*ast.TableName
	ctx    context.Context
	done   bool
	is     infoschema.InfoSchema
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(goCtx goctx.Context) (Row, error) {
	if e.done {
		return nil, nil
	}

	dbName := model.NewCIStr(e.ctx.GetSessionVars().CurrentDB)

	for _, t := range e.tables {
		tb, err := e.is.TableByName(dbName, t.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, idx := range tb.Indices() {
			txn := e.ctx.Txn()
			err = admin.CompareIndexData(txn, tb, idx)
			if err != nil {
				return nil, errors.Errorf("%v err:%v", t.Name, err)
			}
		}
	}
	e.done = true

	return nil, nil
}

// Close implements plan.Plan Close interface.
func (e *CheckTableExec) Close() error {
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
func (e *SelectLockExec) Next(goCtx goctx.Context) (Row, error) {
	row, err := e.children[0].Next(goCtx)
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
func (e *LimitExec) Next(goCtx goctx.Context) (Row, error) {
	for e.cursor < e.begin {
		srcRow, err := e.children[0].Next(goCtx)
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
	srcRow, err := e.children[0].Next(goCtx)
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
func (e *LimitExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		err := e.children[0].NextChunk(e.childrenResults[0])
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
	err := e.children[0].NextChunk(chk)
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
func (e *LimitExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
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
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) (rows [][]types.Datum, err error) {
		err = ctx.ActivePendingTxn()
		if err != nil {
			return rows, errors.Trace(err)
		}
		e := &executorBuilder{is: is, ctx: ctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, errors.Trace(err)
		}
		goCtx := goctx.TODO()
		err = exec.Open(goCtx)
		if err != nil {
			return rows, errors.Trace(err)
		}
		for {
			row, err := exec.Next(goCtx)
			if err != nil {
				return rows, errors.Trace(err)
			}
			if row == nil {
				return rows, errors.Trace(exec.Close())
			}
			rows = append(rows, row)
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

	exprs        []expression.Expression
	vectorizable bool
}

// Open implements the Executor Open interface.
func (e *ProjectionExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.vectorizable = expression.Vectorizable(e.exprs)
	return nil
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Next(goCtx goctx.Context) (retRow Row, err error) {
	srcRow, err := e.children[0].Next(goCtx)
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
func (e *ProjectionExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if err := e.children[0].NextChunk(e.childrenResults[0]); err != nil {
		return errors.Trace(err)
	}
	if e.vectorizable {
		return errors.Trace(expression.VectorizedExecute(e.ctx, e.exprs, e.childrenResults[0], chk))
	}
	return errors.Trace(expression.UnVectorizedExecute(e.ctx, e.exprs, e.childrenResults[0], chk))
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	rowCount  int
	returnCnt int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(goCtx goctx.Context) error {
	e.returnCnt = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(goCtx goctx.Context) (Row, error) {
	if e.returnCnt >= e.rowCount {
		return nil, nil
	}
	e.returnCnt++
	return Row{}, nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	baseExecutor

	batched  bool
	filters  []expression.Expression
	selected []bool
	inputRow chunk.Row
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputRow = e.childrenResults[0].End()
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
func (e *SelectionExec) Next(goCtx goctx.Context) (Row, error) {
	for {
		srcRow, err := e.children[0].Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		match, err := expression.EvalBool(e.filters, srcRow, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return srcRow, nil
		}
	}
}

// NextChunk implements the Executor NextChunk interface.
func (e *SelectionExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()

	if !e.batched {
		return errors.Trace(e.unBatchedNextChunk(chk))
	}

	for {
		for ; e.inputRow != e.childrenResults[0].End(); e.inputRow = e.inputRow.Next() {
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if chk.NumRows() == e.maxChunkSize {
				return nil
			}
			chk.AppendRow(0, e.inputRow)
		}
		err := e.children[0].NextChunk(e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.childrenResults[0].Begin()
		// no more data.
		if e.childrenResults[0].NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.childrenResults[0], e.selected)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// unBatchedNextChunk filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNextChunk(chk *chunk.Chunk) error {
	for {
		for ; e.inputRow != e.childrenResults[0].End(); e.inputRow = e.inputRow.Next() {
			selected, err := expression.EvalBool(e.filters, e.inputRow, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if selected {
				chk.AppendRow(0, e.inputRow)
				e.inputRow = e.inputRow.Next()
				return nil
			}
		}
		err := e.children[0].NextChunk(e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.childrenResults[0].Begin()
		// no more data.
		if e.childrenResults[0].NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t          table.Table
	asName     *model.CIStr
	ranges     []ranger.IntColumnRange
	seekHandle int64
	iter       kv.Iterator
	cursor     int
	columns    []*model.ColumnInfo

	isVirtualTable     bool
	virtualTableRows   [][]types.Datum
	virtualTableCursor int
}

// Next implements the Executor interface.
func (e *TableScanExec) Next(goCtx goctx.Context) (Row, error) {
	if e.isVirtualTable {
		return e.nextForInfoSchema()
	}
	for {
		if e.cursor >= len(e.ranges) {
			return nil, nil
		}
		ran := e.ranges[e.cursor]
		if e.seekHandle < ran.LowVal {
			e.seekHandle = ran.LowVal
		}
		if e.seekHandle > ran.HighVal {
			e.cursor++
			continue
		}
		handle, found, err := e.t.Seek(e.ctx, e.seekHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !found {
			return nil, nil
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
		row, err := e.getRow(handle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.seekHandle = handle + 1
		return row, nil
	}
}

func (e *TableScanExec) nextForInfoSchema() (Row, error) {
	if e.virtualTableRows == nil {
		columns := make([]*table.Column, e.schema.Len())
		for i, v := range e.columns {
			columns[i] = table.ToColumn(v)
		}
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			e.virtualTableRows = append(e.virtualTableRows, rec)
			return true, nil
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.virtualTableCursor >= len(e.virtualTableRows) {
		return nil, nil
	}
	row := e.virtualTableRows[e.virtualTableCursor]
	e.virtualTableCursor++
	return row, nil
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
func (e *TableScanExec) Open(goCtx goctx.Context) error {
	e.iter = nil
	e.cursor = 0
	return nil
}

func (e *TableScanExec) supportChunk() bool {
	return false
}

// ExistsExec represents exists executor.
type ExistsExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *ExistsExec) Open(goCtx goctx.Context) error {
	e.evaluated = false
	return errors.Trace(e.children[0].Open(goCtx))
}

// Next implements the Executor Next interface.
// We always return one row with one column which has true or false value.
func (e *ExistsExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.children[0].Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return Row{types.NewDatum(srcRow != nil)}, nil
	}
	return nil, nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.evaluated = false
	return nil
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.children[0].Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return make([]types.Datum, e.schema.Len()), nil
		}
		srcRow1, err := e.children[0].Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow1 != nil {
			return nil, errors.New("subquery returns more than 1 row")
		}
		return srcRow, nil
	}
	return nil, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *MaxOneRowExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := e.children[0].NextChunk(e.childrenResults[0])
	if err != nil {
		return errors.Trace(err)
	}
	if chk.NumRows() > 1 {
		return errors.New("subquery returns more than 1 row")
	}
	if chk.NumRows() == 0 {
		for i := range e.schema.Columns {
			chk.AppendNull(i)
		}
	} else {
		chk2 := e.children[0].newChunk()
		err2 := e.children[0].NextChunk(chk2)
		if err2 != nil {
			return errors.Trace(err2)
		}
		if chk2.NumRows() != 0 {
			return errors.New("subquery returns more than 1 row")
		}
	}
	return nil
}

// UnionExec pulls all it's childrens result and returns to its parent directly.
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
	rows          []Row
	cursor        int
	wg            sync.WaitGroup

	// For chunk execution.
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
// "src" is used for Chunk resuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
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

func (e *UnionExec) fetchData(goCtx goctx.Context, idx int) {
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
			row, err := e.children[idx].Next(goCtx)
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
func (e *UnionExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(goCtx goctx.Context, forChunk bool) {
	if forChunk {
		e.resultPool = make(chan *unionWorkerResult, len(e.children))
		e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
		for i := range e.children {
			e.resourcePools[i] = make(chan *chunk.Chunk, 1)
			e.resourcePools[i] <- e.childrenResults[i]
			e.wg.Add(1)
			go e.resultPuller(i)
		}
	} else {
		e.resultCh = make(chan *execResult, len(e.children))
		e.cursor = 0
		for i := range e.children {
			e.wg.Add(1)
			go e.fetchData(goCtx, i)
		}
	}
	go e.waitAllFinished(forChunk)
}

func (e *UnionExec) resultPuller(childID int) {
	defer e.wg.Done()
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
	}
	for {
		if e.stopFetchData.Load().(bool) {
			return
		}
		select {
		case <-e.finished:
			return
		case result.chk = <-e.resourcePools[childID]:
		}
		result.err = errors.Trace(e.children[childID].NextChunk(result.chk))
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
func (e *UnionExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.initialized {
		e.initialize(goCtx, false)
		e.initialized = true
	}
	if e.cursor >= len(e.rows) {
		result, ok := <-e.resultCh
		if !ok {
			return nil, nil
		}
		if result.err != nil {
			return nil, errors.Trace(result.err)
		}
		if len(result.rows) == 0 {
			return nil, nil
		}
		e.rows = result.rows
		e.cursor = 0
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *UnionExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if !e.initialized {
		e.initialize(nil, true)
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
	e.rows = nil
	close(e.finished)
	e.resourcePools = nil
	return errors.Trace(e.baseExecutor.Close())
}
