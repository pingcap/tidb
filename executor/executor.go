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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/forupdate"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &AggregateExec{}
	_ Executor = &CheckTableExec{}
	_ Executor = &FilterExec{}
	_ Executor = &IndexRangeExec{}
	_ Executor = &IndexScanExec{}
	_ Executor = &LimitExec{}
	_ Executor = &SelectFieldsExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &SortExec{}
	_ Executor = &TableScanExec{}
)

// Error instances.
var (
	ErrUnknownPlan     = terror.ClassExecutor.New(CodeUnknownPlan, "Unknown plan")
	ErrPrepareMulti    = terror.ClassExecutor.New(CodePrepareMulti, "Can not prepare multiple statements")
	ErrStmtNotFound    = terror.ClassExecutor.New(CodeStmtNotFound, "Prepared statement not found")
	ErrSchemaChanged   = terror.ClassExecutor.New(CodeSchemaChanged, "Schema has changed")
	ErrWrongParamCount = terror.ClassExecutor.New(CodeWrongParamCount, "Wrong parameter count")
)

// Error codes.
const (
	CodeUnknownPlan     terror.ErrCode = 1
	CodePrepareMulti    terror.ErrCode = 2
	CodeStmtNotFound    terror.ErrCode = 3
	CodeSchemaChanged   terror.ErrCode = 4
	CodeWrongParamCount terror.ErrCode = 5
)

// Row represents a record row.
type Row struct {
	// Data is the output record data for current Plan.
	Data []interface{}

	RowKeys []*RowKeyEntry
}

// RowKeyEntry is designed for Delete statement in multi-table mode,
// we should know which table this row comes from.
type RowKeyEntry struct {
	// The table which this row come from.
	Tbl table.Table
	// Row key.
	Key string
}

// Executor executes a query.
type Executor interface {
	Fields() []*ast.ResultField
	Next() (*Row, error)
	Close() error
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	fields []*ast.ResultField
	ctx    context.Context
	done   bool
}

// Fields implements Executor Fields interface.
func (e *ShowDDLExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Execution Next interface.
func (e *ShowDDLExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}

	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	info, err := inspectkv.GetDDLInfo(txn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rowData := []interface{}{
		info.SchemaVer,
		"",
		"",
	}
	if info.Owner != nil {
		rowData[1] = info.Owner.String()
	}
	if info.Job != nil {
		rowData[2] = info.Job.String()
	}
	row := &Row{}
	row.Data = rowData
	for i, f := range e.fields {
		f.Expr.SetValue(rowData[i])
	}
	e.done = true

	return row, nil
}

// Close implements Executor Close interface.
func (e *ShowDDLExec) Close() error {
	return nil
}

// CheckTableExec represents a check table executor.
type CheckTableExec struct {
	tables []*ast.TableName
	ctx    context.Context
	done   bool
}

// Fields implements Executor Fields interface.
func (e *CheckTableExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Execution Next interface.
func (e *CheckTableExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}

	dbName := model.NewCIStr(db.GetCurrentSchema(e.ctx))
	is := sessionctx.GetDomain(e.ctx).InfoSchema()
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, t := range e.tables {
		tb, err := is.TableByName(dbName, t.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, idx := range tb.Indices() {
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

// TableScanExec represents a table scan executor.
type TableScanExec struct {
	t          table.Table
	fields     []*ast.ResultField
	iter       kv.Iterator
	ctx        context.Context
	ranges     []plan.TableRange // Disjoint close handle ranges.
	seekHandle int64             // The handle to seek, should be initialized to math.MinInt64.
	cursor     int               // The range cursor, used to locate to current range.
}

// Fields implements Executor Fields interface.
func (e *TableScanExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Execution Next interface.
func (e *TableScanExec) Next() (*Row, error) {
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
		rowKey, err := e.seek()
		if err != nil || rowKey == nil {
			return nil, errors.Trace(err)
		}
		handle, err := tables.DecodeRecordKeyHandle(rowKey)
		if err != nil {
			return nil, errors.Trace(err)
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
		row, err := e.getRow(handle, rowKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.seekHandle = handle + 1
		return row, nil
	}
}

func (e *TableScanExec) seek() (kv.Key, error) {
	seekKey := tables.EncodeRecordKey(e.t.TableID(), e.seekHandle, 0)
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if e.iter != nil {
		e.iter.Close()
	}
	e.iter, err = txn.Seek(seekKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !e.iter.Valid() || !e.iter.Key().HasPrefix(e.t.RecordPrefix()) {
		// No more records in the table, skip to the end.
		e.cursor = len(e.ranges)
		return nil, nil
	}
	return e.iter.Key(), nil
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

func (e *TableScanExec) getRow(handle int64, rowKey kv.Key) (*Row, error) {
	row := &Row{}
	var err error
	row.Data, err = e.t.Row(e.ctx, handle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Set result fields value.
	for i, v := range e.fields {
		v.Expr.SetValue(row.Data[i])
	}

	// Put rowKey to the tail of record row
	rke := &RowKeyEntry{
		Tbl: e.t,
		Key: string(rowKey),
	}
	row.RowKeys = append(row.RowKeys, rke)
	return row, nil
}

// Close implements Executor Close interface.
func (e *TableScanExec) Close() error {
	if e.iter != nil {
		e.iter.Close()
		e.iter = nil
	}
	return nil
}

// IndexRangeExec represents an index range scan executor.
type IndexRangeExec struct {
	scan *IndexScanExec

	// seekVal is different from lowVal, it is casted from lowVal and
	// must be less than or equal to lowVal, used to seek the index.
	lowVals     []interface{}
	lowExclude  bool
	highVals    []interface{}
	highExclude bool

	iter       kv.IndexIterator
	skipLowCmp bool
	finished   bool
}

// Fields implements Executor Fields interface.
func (e *IndexRangeExec) Fields() []*ast.ResultField {
	return e.scan.fields
}

// Next implements Executor Next interface.
func (e *IndexRangeExec) Next() (*Row, error) {
	if e.iter == nil {
		seekVals := make([]interface{}, len(e.scan.idx.Columns))
		for i := 0; i < len(e.lowVals); i++ {
			var err error
			if e.lowVals[i] == plan.MinNotNullVal {
				seekVals[i] = []byte{}
			} else {
				seekVals[i], err = types.Convert(e.lowVals[i], e.scan.valueTypes[i])
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		txn, err := e.scan.ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.iter, _, err = e.scan.idx.X.Seek(txn, seekVals)
		if err != nil {
			return nil, types.EOFAsNil(err)
		}
	}

	for {
		if e.finished {
			return nil, nil
		}
		idxKey, h, err := e.iter.Next()
		if err != nil {
			return nil, types.EOFAsNil(err)
		}
		if !e.skipLowCmp {
			var cmp int
			cmp, err = indexCompare(idxKey, e.lowVals)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if cmp < 0 || (cmp == 0 && e.lowExclude) {
				continue
			}
			e.skipLowCmp = true
		}
		cmp, err := indexCompare(idxKey, e.highVals)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp > 0 || (cmp == 0 && e.highExclude) {
			// This span has finished iteration.
			e.finished = true
			continue
		}
		var row *Row
		row, err = e.lookupRow(h)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return row, nil
	}
}

// indexCompare compares multi column index.
// The length of boundVals may be less than idxKey.
func indexCompare(idxKey []interface{}, boundVals []interface{}) (int, error) {
	for i := 0; i < len(boundVals); i++ {
		cmp, err := indexColumnCompare(idxKey[i], boundVals[i])
		if err != nil {
			return -1, errors.Trace(err)
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

// comparison function that takes minNotNullVal and maxVal into account.
func indexColumnCompare(a interface{}, b interface{}) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	} else if b == nil {
		return 1, nil
	} else if a == nil {
		return -1, nil
	}

	// a and b both not nil
	if a == plan.MinNotNullVal && b == plan.MinNotNullVal {
		return 0, nil
	} else if b == plan.MinNotNullVal {
		return 1, nil
	} else if a == plan.MinNotNullVal {
		return -1, nil
	}

	// a and b both not min value
	if a == plan.MaxVal && b == plan.MaxVal {
		return 0, nil
	} else if a == plan.MaxVal {
		return 1, nil
	} else if b == plan.MaxVal {
		return -1, nil
	}

	n, err := types.Compare(a, b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return n, nil
}

func (e *IndexRangeExec) lookupRow(h int64) (*Row, error) {
	row := &Row{}
	var err error
	row.Data, err = e.scan.tbl.Row(e.scan.ctx, h)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rowKey := &RowKeyEntry{
		Tbl: e.scan.tbl,
		Key: string(e.scan.tbl.RecordKey(h, nil)),
	}
	row.RowKeys = append(row.RowKeys, rowKey)
	return row, nil
}

// Close implements Executor Close interface.
func (e *IndexRangeExec) Close() error {
	if e.iter != nil {
		e.iter.Close()
		e.iter = nil
	}
	e.finished = false
	e.skipLowCmp = false
	return nil
}

// IndexScanExec represents an index scan executor.
type IndexScanExec struct {
	tbl        table.Table
	idx        *column.IndexedCol
	fields     []*ast.ResultField
	Ranges     []*IndexRangeExec
	Desc       bool
	rangeIdx   int
	ctx        context.Context
	valueTypes []*types.FieldType
}

// Fields implements Executor Fields interface.
func (e *IndexScanExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *IndexScanExec) Next() (*Row, error) {
	for e.rangeIdx < len(e.Ranges) {
		ran := e.Ranges[e.rangeIdx]
		row, err := ran.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			for i, val := range row.Data {
				e.fields[i].Expr.SetValue(val)
			}
			return row, nil
		}
		ran.Close()
		e.rangeIdx++
	}
	return nil, nil
}

// Close implements Executor Close interface.
func (e *IndexScanExec) Close() error {
	for e.rangeIdx < len(e.Ranges) {
		e.Ranges[e.rangeIdx].Close()
		e.rangeIdx++
	}
	return nil
}

// SelectFieldsExec represents a select fields executor.
type SelectFieldsExec struct {
	Src          Executor
	ResultFields []*ast.ResultField
	executed     bool
	ctx          context.Context
}

// Fields implements Executor Fields interface.
func (e *SelectFieldsExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
func (e *SelectFieldsExec) Next() (*Row, error) {
	var rowKeys []*RowKeyEntry
	if e.Src != nil {
		srcRow, err := e.Src.Next()
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
		Data:    make([]interface{}, len(e.ResultFields)),
	}
	for i, field := range e.ResultFields {
		val, err := evaluator.Eval(e.ctx, field.Expr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row.Data[i] = val
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *SelectFieldsExec) Close() error {
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
}

// FilterExec represents a filter executor.
type FilterExec struct {
	Src       Executor
	Condition ast.ExprNode
	ctx       context.Context
}

// Fields implements Executor Fields interface.
func (e *FilterExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *FilterExec) Next() (*Row, error) {
	for {
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return nil, nil
		}
		match, err := evaluator.EvalBool(e.ctx, e.Condition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return srcRow, nil
		}
	}
}

// Close implements Executor Close interface.
func (e *FilterExec) Close() error {
	return e.Src.Close()
}

// SelectLockExec represents a select lock executor.
type SelectLockExec struct {
	Src  Executor
	Lock ast.SelectLockType
	ctx  context.Context
}

// Fields implements Executor Fields interface.
func (e *SelectLockExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
func (e *SelectLockExec) Next() (*Row, error) {
	row, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return nil, nil
	}
	if len(row.RowKeys) != 0 && e.Lock == ast.SelectLockForUpdate {
		forupdate.SetForUpdate(e.ctx)
		txn, err := e.ctx.GetTxn(false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, k := range row.RowKeys {
			err = txn.LockKeys([]byte(k.Key))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return row, nil
}

// Close implements Executor Close interface.
func (e *SelectLockExec) Close() error {
	return e.Src.Close()
}

// LimitExec represents limit executor
type LimitExec struct {
	Src    Executor
	Offset uint64
	Count  uint64
	Idx    uint64
}

// Fields implements Executor Fields interface.
func (e *LimitExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
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
	// Negative Limit means no limit.
	if e.Count >= 0 && e.Idx >= e.Offset+e.Count {
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

// Close implements Executor Close interface.
func (e *LimitExec) Close() error {
	return e.Src.Close()
}

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key []interface{}
	row *Row
}

// SortExec represents sorting executor.
type SortExec struct {
	Src     Executor
	ByItems []*ast.ByItem
	Rows    []*orderByRow
	ctx     context.Context
	Idx     int
	fetched bool
	err     error
}

// Fields implements Executor Fields interface.
func (e *SortExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
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
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := types.Compare(v1, v2)
		if err != nil {
			e.err = err
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

// Next implements Executor Next interface.
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
				key: make([]interface{}, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				orderRow.key[i], err = evaluator.Eval(e.ctx, byItem.Expr)
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

// Close implements Executor Close interface.
func (e *SortExec) Close() error {
	return e.Src.Close()
}

// For select stmt with aggregate function but without groupby clasue,
// We consider there is a single group with key singleGroup.
const singleGroup = "SingleGroup"

// AggregateExec deals with all the aggregate functions.
// It is built from Aggregate Plan. When Next() is called, it reads all the data from Src and updates all the items in AggFuncs.
// TODO: Support having.
type AggregateExec struct {
	Src               Executor
	ResultFields      []*ast.ResultField
	executed          bool
	ctx               context.Context
	finish            bool
	AggFuncs          []*ast.AggregateFuncExpr
	groupMap          map[string]bool
	groups            []string
	currentGroupIndex int
	GroupByItems      []*ast.ByItem
}

// Fields implements Executor Fields interface.
func (e *AggregateExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
func (e *AggregateExec) Next() (*Row, error) {
	// In this stage we consider all data from src as a single group.
	if !e.executed {
		e.groupMap = make(map[string]bool)
		e.groups = []string{}
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
		if (len(e.groups) == 0) && (len(e.GroupByItems) == 0) {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groups = append(e.groups, singleGroup)
		}
	}
	if e.currentGroupIndex >= len(e.groups) {
		return nil, nil
	}
	groupKey := e.groups[e.currentGroupIndex]
	for _, af := range e.AggFuncs {
		af.CurrentGroup = groupKey
	}
	e.currentGroupIndex++
	return &Row{}, nil
}

func (e *AggregateExec) getGroupKey() (string, error) {
	if len(e.GroupByItems) == 0 {
		return singleGroup, nil
	}
	vals := make([]interface{}, 0, len(e.GroupByItems))
	for _, item := range e.GroupByItems {
		v, err := evaluator.Eval(e.ctx, item.Expr)
		if err != nil {
			return "", errors.Trace(err)
		}
		vals = append(vals, v)
	}
	bs, err := codec.EncodeValue([]byte{}, vals...)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(bs), nil
}

// Fetch a single row from src and update each aggregate function.
// If the first return value is false, it means there is no more data from src.
func (e *AggregateExec) innerNext() (bool, error) {
	if e.Src != nil {
		srcRow, err := e.Src.Next()
		if err != nil {
			return false, errors.Trace(err)
		}
		if srcRow == nil {
			return false, nil
		}
	} else {
		// If Src is nil, only one row should be returned.
		if e.executed {
			return false, nil
		}
	}
	e.executed = true
	groupKey, err := e.getGroupKey()
	if err != nil {
		return false, errors.Trace(err)
	}
	if _, ok := e.groupMap[groupKey]; !ok {
		e.groupMap[groupKey] = true
		e.groups = append(e.groups, groupKey)
	}
	for _, af := range e.AggFuncs {
		for _, arg := range af.Args {
			_, err := evaluator.Eval(e.ctx, arg)
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		af.CurrentGroup = groupKey
		af.Update()
	}
	return true, nil
}

// Close implements Executor Close interface.
func (e *AggregateExec) Close() error {
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
}
