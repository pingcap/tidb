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
	"container/heap"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &ApplyExec{}
	_ Executor = &CheckTableExec{}
	_ Executor = &DistinctExec{}
	_ Executor = &DummyScanExec{}
	_ Executor = &ExistsExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &ReverseExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopnExec{}
	_ Executor = &TrimExec{}
	_ Executor = &UnionExec{}
)

// Error instances.
var (
	ErrUnknownPlan     = terror.ClassExecutor.New(codeUnknownPlan, "Unknown plan")
	ErrPrepareMulti    = terror.ClassExecutor.New(codePrepareMulti, "Can not prepare multiple statements")
	ErrStmtNotFound    = terror.ClassExecutor.New(codeStmtNotFound, "Prepared statement not found")
	ErrSchemaChanged   = terror.ClassExecutor.New(codeSchemaChanged, "Schema has changed")
	ErrWrongParamCount = terror.ClassExecutor.New(codeWrongParamCount, "Wrong parameter count")
	ErrRowKeyCount     = terror.ClassExecutor.New(codeRowKeyCount, "Wrong row key entry count")
	ErrPrepareDDL      = terror.ClassExecutor.New(codePrepareDDL, "Can not prepare DDL statements")
	ErrPasswordNoMatch = terror.ClassExecutor.New(CodePasswordNoMatch, "Can't find any matching row in the user table")
)

// Error codes.
const (
	codeUnknownPlan     terror.ErrCode = 1
	codePrepareMulti    terror.ErrCode = 2
	codeStmtNotFound    terror.ErrCode = 3
	codeSchemaChanged   terror.ErrCode = 4
	codeWrongParamCount terror.ErrCode = 5
	codeRowKeyCount     terror.ErrCode = 6
	codePrepareDDL      terror.ErrCode = 7
	// MySQL error code
	CodePasswordNoMatch terror.ErrCode = 1133
	CodeCannotUser      terror.ErrCode = 1396
)

// Row represents a result set row, it may be returned from a table, a join, or a projection.
type Row struct {
	// Data is the output record data for current Plan.
	Data []types.Datum
	// RowKeys contains all table row keys in the row.
	RowKeys []*RowKeyEntry
}

// RowKeyEntry represents a row key read from a table.
type RowKeyEntry struct {
	// The table which this row come from.
	Tbl table.Table
	// Row key.
	Handle int64
	// Table alias name.
	TableAsName *model.CIStr
}

// Executor executes a query.
type Executor interface {
	Next() (*Row, error)
	Close() error
	Schema() expression.Schema
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	schema  expression.Schema
	ctx     context.Context
	ddlInfo *inspectkv.DDLInfo
	bgInfo  *inspectkv.DDLInfo
	done    bool
}

// Schema implements the Executor Schema interface.
func (e *ShowDDLExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}
	var ddlOwner, ddlJob string
	if e.ddlInfo.Owner != nil {
		ddlOwner = e.ddlInfo.Owner.String()
	}
	if e.ddlInfo.Job != nil {
		ddlJob = e.ddlInfo.Job.String()
	}

	var bgOwner, bgJob string
	if e.bgInfo.Owner != nil {
		bgOwner = e.bgInfo.Owner.String()
	}
	if e.bgInfo.Job != nil {
		bgJob = e.bgInfo.Job.String()
	}

	row := &Row{}
	row.Data = types.MakeDatums(
		e.ddlInfo.SchemaVer,
		ddlOwner,
		ddlJob,
		e.bgInfo.SchemaVer,
		bgOwner,
		bgJob,
	)
	e.done = true

	return row, nil
}

// Close implements the Executor Close interface.
func (e *ShowDDLExec) Close() error {
	return nil
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	tables []*ast.TableName
	ctx    context.Context
	done   bool
	is     infoschema.InfoSchema
}

// Schema implements the Executor Schema interface.
func (e *CheckTableExec) Schema() expression.Schema {
	return expression.NewSchema(nil)
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next() (*Row, error) {
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
			err = inspectkv.CompareIndexData(txn, tb, idx)
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
	Src    Executor
	Lock   ast.SelectLockType
	ctx    context.Context
	schema expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *SelectLockExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next() (*Row, error) {
	row, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	if len(row.RowKeys) != 0 && e.Lock == ast.SelectLockForUpdate {
		e.ctx.GetSessionVars().TxnCtx.ForUpdate = true
		txn := e.ctx.Txn()
		for _, k := range row.RowKeys {
			lockKey := tablecodec.EncodeRowKeyWithHandle(k.Tbl.Meta().ID, k.Handle)
			err = txn.LockKeys(lockKey)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return row, nil
}

// Close implements the Executor Close interface.
func (e *SelectLockExec) Close() error {
	return e.Src.Close()
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	Src    Executor
	Offset uint64
	Count  uint64
	Idx    uint64
	schema expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *LimitExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next() (*Row, error) {
	for e.Idx < e.Offset {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		e.Idx++
	}
	if e.Idx >= e.Count+e.Offset {
		return nil, nil
	}
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	e.Idx++
	return srcRow, nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	e.Idx = 0
	return e.Src.Close()
}

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key []types.Datum
	row *Row
}

// DistinctExec represents Distinct executor.
// It ignores duplicate rows from source Executor by using a *distinct.Checker which maintains
// a map to check duplication.
// Because every distinct row will be added to the map, the memory usage might be very high.
type DistinctExec struct {
	Src     Executor
	checker *distinct.Checker
	schema  expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *DistinctExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *DistinctExec) Next() (*Row, error) {
	if e.checker == nil {
		e.checker = distinct.CreateDistinctChecker()
	}
	for {
		row, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
		}
		ok, err := e.checker.Check(types.DatumsToInterfaces(row.Data))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ok {
			continue
		}
		return row, nil
	}
}

// Close implements the Executor Close interface.
func (e *DistinctExec) Close() error {
	return e.Src.Close()
}

// ReverseExec produces reverse ordered result, it is used to wrap executors that do not support reverse scan.
type ReverseExec struct {
	Src    Executor
	rows   []*Row
	cursor int
	done   bool
}

// Schema implements the Executor Schema interface.
func (e *ReverseExec) Schema() expression.Schema {
	return e.Src.Schema()
}

// Next implements the Executor Next interface.
func (e *ReverseExec) Next() (*Row, error) {
	if !e.done {
		for {
			row, err := e.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if row == nil {
				break
			}
			e.rows = append(e.rows, row)
		}
		e.cursor = len(e.rows) - 1
		e.done = true
	}
	if e.cursor < 0 {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor--
	return row, nil
}

// Close implements the Executor Close interface.
func (e *ReverseExec) Close() error {
	return e.Src.Close()
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plan.EvalSubquery = func(p plan.PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) (d []types.Datum, err error) {
		err = ctx.ActivePendingTxn()
		if err != nil {
			return d, errors.Trace(err)
		}
		e := &executorBuilder{is: is, ctx: ctx}
		exec := e.build(p)
		row, err := exec.Next()
		if err != nil {
			return d, errors.Trace(err)
		}
		if row == nil {
			return
		}
		return row.Data, nil
	}
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		CodeCannotUser:      mysql.ErrCannotUser,
		CodePasswordNoMatch: mysql.ErrPasswordNoMatch,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExecutor] = tableMySQLErrCodes
}

// ProjectionExec represents a select fields executor.
type ProjectionExec struct {
	Src      Executor
	schema   expression.Schema
	executed bool
	ctx      context.Context
	exprs    []expression.Expression
}

// Schema implements the Executor Schema interface.
func (e *ProjectionExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *ProjectionExec) Next() (retRow *Row, err error) {
	var rowKeys []*RowKeyEntry
	var srcRow *Row
	if e.Src != nil {
		srcRow, err = e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		rowKeys = srcRow.RowKeys
	} else {
		// If Src is nil, only one row should be returned.
		if e.executed {
			return nil, nil
		}
	}
	e.executed = true
	row := &Row{
		RowKeys: rowKeys,
		Data:    make([]types.Datum, 0, len(e.exprs)),
	}
	for _, expr := range e.exprs {
		val, err := expr.Eval(srcRow.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row.Data = append(row.Data, val)
	}
	return row, nil
}

// Close implements the Executor Close interface.
func (e *ProjectionExec) Close() error {
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	schema   expression.Schema
	executed bool
}

// Init implements the Executor Init interface.
func (e *TableDualExec) Init() {
	e.executed = false
}

// Schema implements the Executor Schema interface.
func (e *TableDualExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next() (*Row, error) {
	if e.executed {
		return nil, nil
	}
	e.executed = true
	return &Row{}, nil
}

// Close implements the Executor interface.
func (e *TableDualExec) Close() error {
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	Src       Executor
	Condition expression.Expression
	ctx       context.Context
	schema    expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *SelectionExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next() (*Row, error) {
	for {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		match, err := expression.EvalBool(e.Condition, srcRow.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return srcRow, nil
		}
	}
}

// Close implements the Executor Close interface.
func (e *SelectionExec) Close() error {
	return e.Src.Close()
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	t          table.Table
	asName     *model.CIStr
	ctx        context.Context
	ranges     []plan.TableRange
	seekHandle int64
	iter       kv.Iterator
	cursor     int
	schema     expression.Schema
	columns    []*model.ColumnInfo

	isInfoSchema     bool
	infoSchemaRows   [][]types.Datum
	infoSchemaCursor int
}

// Schema implements the Executor Schema interface.
func (e *TableScanExec) Schema() expression.Schema {
	return e.schema
}

// Next implements the Executor interface.
func (e *TableScanExec) Next() (*Row, error) {
	if e.isInfoSchema {
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

func (e *TableScanExec) nextForInfoSchema() (*Row, error) {
	if e.infoSchemaRows == nil {
		columns := make([]*table.Column, e.schema.Len())
		for i, v := range e.columns {
			columns[i] = table.ToColumn(v)
		}
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			e.infoSchemaRows = append(e.infoSchemaRows, rec)
			return true, nil
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.infoSchemaCursor >= len(e.infoSchemaRows) {
		return nil, nil
	}
	row := &Row{Data: e.infoSchemaRows[e.infoSchemaCursor]}
	e.infoSchemaCursor++
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

func (e *TableScanExec) getRow(handle int64) (*Row, error) {
	row := &Row{}
	var err error

	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		columns[i] = table.ToColumn(v)
	}
	row.Data, err = e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Put rowKey to the tail of record row.
	rke := &RowKeyEntry{
		Tbl:         e.t,
		Handle:      handle,
		TableAsName: e.asName,
	}
	row.RowKeys = append(row.RowKeys, rke)
	return row, nil
}

// Close implements the Executor Close interface.
func (e *TableScanExec) Close() error {
	e.iter = nil
	e.cursor = 0
	return nil
}

// SortExec represents sorting executor.
type SortExec struct {
	Src     Executor
	ByItems []*plan.ByItems
	Rows    []*orderByRow
	ctx     context.Context
	Idx     int
	fetched bool
	err     error
	schema  expression.Schema
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	e.fetched = false
	e.Rows = nil
	return e.Src.Close()
}

// Schema implements the Executor Schema interface.
func (e *SortExec) Schema() expression.Schema {
	return e.schema
}

// Len returns the number of rows.
func (e *SortExec) Len() int {
	return len(e.Rows)
}

// Swap implements sort.Interface Swap interface.
func (e *SortExec) Swap(i, j int) {
	e.Rows[i], e.Rows[j] = e.Rows[j], e.Rows[i]
}

// Less implements sort.Interface Less interface.
func (e *SortExec) Less(i, j int) bool {
	sc := e.ctx.GetSessionVars().StmtCtx
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(sc, v2)
		if err != nil {
			e.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// Next implements the Executor Next interface.
func (e *SortExec) Next() (*Row, error) {
	if !e.fetched {
		for {
			srcRow, err := e.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]types.Datum, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				orderRow.key[i], err = byItem.Expr.Eval(srcRow.Data, e.ctx)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			e.Rows = append(e.Rows, orderRow)
		}
		sort.Sort(e)
		e.fetched = true
	}
	if e.err != nil {
		return nil, errors.Trace(e.err)
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}

// TopnExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopnExec struct {
	SortExec
	limit      *plan.Limit
	totalCount int
	heapSize   int
}

// Less implements heap.Interface Less interface.
func (e *TopnExec) Less(i, j int) bool {
	sc := e.ctx.GetSessionVars().StmtCtx
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(sc, v2)
		if err != nil {
			e.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret > 0 {
			return true
		} else if ret < 0 {
			return false
		}
	}

	return false
}

// Len implements heap.Interface Len interface.
func (e *TopnExec) Len() int {
	return e.heapSize
}

// Push implements heap.Interface Push interface.
func (e *TopnExec) Push(x interface{}) {
	e.Rows = append(e.Rows, x.(*orderByRow))
	e.heapSize++
}

// Pop implements heap.Interface Pop interface.
func (e *TopnExec) Pop() interface{} {
	e.heapSize--
	return nil
}

// Next implements the Executor Next interface.
func (e *TopnExec) Next() (*Row, error) {
	if !e.fetched {
		e.Idx = int(e.limit.Offset)
		e.totalCount = int(e.limit.Offset + e.limit.Count)
		e.Rows = make([]*orderByRow, 0, e.totalCount+1)
		e.heapSize = 0
		for {
			srcRow, err := e.Src.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			// build orderRow from srcRow.
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]types.Datum, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				orderRow.key[i], err = byItem.Expr.Eval(srcRow.Data, e.ctx)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			if e.totalCount == e.heapSize {
				// An equivalent of Push and Pop. We don't use the standard Push and Pop
				// to reduce the number of comparisons.
				e.Rows = append(e.Rows, orderRow)
				if e.Less(0, e.heapSize) {
					e.Swap(0, e.heapSize)
					heap.Fix(e, 0)
				}
				e.Rows = e.Rows[:e.heapSize]
			} else {
				heap.Push(e, orderRow)
			}
		}
		if e.limit.Offset == 0 {
			sort.Sort(&e.SortExec)
		} else {
			for i := 0; i < int(e.limit.Count) && e.Len() > 0; i++ {
				heap.Pop(e)
			}
		}
		e.fetched = true
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}

// ExistsExec represents exists executor.
type ExistsExec struct {
	schema    expression.Schema
	Src       Executor
	evaluated bool
}

// Schema implements the Executor Schema interface.
func (e *ExistsExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *ExistsExec) Close() error {
	e.evaluated = false
	return e.Src.Close()
}

// Next implements the Executor Next interface.
// We always return one row with one column which has true or false value.
func (e *ExistsExec) Next() (*Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &Row{Data: []types.Datum{types.NewDatum(srcRow != nil)}}, nil
	}
	return nil, nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	schema    expression.Schema
	Src       Executor
	evaluated bool
}

// Schema implements the Executor Schema interface.
func (e *MaxOneRowExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *MaxOneRowExec) Close() error {
	e.evaluated = false
	return e.Src.Close()
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next() (*Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return &Row{Data: make([]types.Datum, e.schema.Len())}, nil
		}
		srcRow1, err := e.Src.Next()
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

// TrimExec truncates extra columns in the Src rows.
// Some columns in src rows are not needed in the result.
// For example, in the 'SELECT a from t order by b' statement,
// 'b' is needed for ordering, but not needed in the result.
type TrimExec struct {
	schema expression.Schema
	Src    Executor
	len    int
}

// Schema implements the Executor Schema interface.
func (e *TrimExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *TrimExec) Close() error {
	return e.Src.Close()
}

// Next implements the Executor Next interface.
func (e *TrimExec) Next() (*Row, error) {
	row, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	row.Data = row.Data[:e.len]
	return row, nil
}

// UnionExec represents union executor.
// UnionExec has multiple source Executors, it executes them sequentially, and do conversion to the same type
// as source Executors may has different field type, we need to do conversion.
type UnionExec struct {
	schema   expression.Schema
	Srcs     []Executor
	ctx      context.Context
	inited   bool
	finished atomic.Value
	rowsCh   chan []*Row
	rows     []*Row
	cursor   int
	wg       sync.WaitGroup
	closedCh chan struct{}
	errCh    chan error
}

// Schema implements the Executor Schema interface.
func (e *UnionExec) Schema() expression.Schema {
	return e.schema
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.rowsCh)
	close(e.closedCh)
}

func (e *UnionExec) fetchData(idx int) {
	defer e.wg.Done()
	for {
		rows := make([]*Row, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			if e.finished.Load().(bool) {
				return
			}
			row, err := e.Srcs[idx].Next()
			if err != nil {
				e.finished.Store(true)
				e.errCh <- err
				return
			}
			if row == nil {
				if len(rows) > 0 {
					e.rowsCh <- rows
				}
				return
			}
			// TODO: Add cast function in plan building phase.
			for j := range row.Data {
				col := e.schema.Columns[j]
				val, err := row.Data[j].ConvertTo(e.ctx.GetSessionVars().StmtCtx, col.RetType)
				if err != nil {
					e.finished.Store(true)
					e.errCh <- err
					return
				}
				row.Data[j] = val
			}
			rows = append(rows, row)
		}
		e.rowsCh <- rows
	}
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next() (*Row, error) {
	if !e.inited {
		e.finished.Store(false)
		e.rowsCh = make(chan []*Row, batchSize*len(e.Srcs))
		e.errCh = make(chan error, len(e.Srcs))
		e.closedCh = make(chan struct{})
		for i := range e.Srcs {
			e.wg.Add(1)
			go e.fetchData(i)
		}
		go e.waitAllFinished()
		e.inited = true
	}
	if e.cursor >= len(e.rows) {
		var rows []*Row
		var err error
		select {
		case rows, _ = <-e.rowsCh:
		case err, _ = <-e.errCh:
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rows == nil {
			return nil, nil
		}
		e.rows = rows
		e.cursor = 0
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	e.finished.Store(true)
	<-e.closedCh
	e.cursor = 0
	e.inited = false
	e.rows = nil
	for _, sel := range e.Srcs {
		er := sel.Close()
		if er != nil {
			return errors.Trace(er)
		}
	}
	return nil
}

// DummyScanExec returns zero results, when some where condition never match, there won't be any
// rows to return, so DummyScan is used to avoid real scan on KV.
type DummyScanExec struct {
	schema expression.Schema
}

// Schema implements the Executor Schema interface.
func (e *DummyScanExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *DummyScanExec) Close() error {
	return nil
}

// Next implements the Executor Next interface.
func (e *DummyScanExec) Next() (*Row, error) {
	return nil, nil
}

// CacheExec represents Cache executor.
// it stores the return values of the executor of its child node.
type CacheExec struct {
	schema      expression.Schema
	Src         Executor
	storedRows  []*Row
	cursor      int
	srcFinished bool
}

// Schema implements the Executor Schema interface.
func (e *CacheExec) Schema() expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *CacheExec) Close() error {
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *CacheExec) Next() (*Row, error) {
	if e.srcFinished && e.cursor >= len(e.storedRows) {
		return nil, nil
	}
	if !e.srcFinished {
		row, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			e.srcFinished = true
			err := e.Src.Close()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		e.storedRows = append(e.storedRows, row)
	}
	row := e.storedRows[e.cursor]
	e.cursor++
	return row, nil
}
