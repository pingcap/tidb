// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	hashTable    map[string][]*Row
	smallHashKey []*expression.Column
	bigHashKey   []*expression.Column
	smallExec    Executor
	bigExec      Executor
	prepared     bool
	ctx          context.Context
	smallFilter  expression.Expression
	bigFilter    expression.Expression
	otherFilter  expression.Expression
	schema       expression.Schema
	outter       bool
	leftSmall    bool
	matchedRows  []*Row
	cursor       int
	// targetTypes means the target the type that both smallHashKey and bigHashKey should convert to.
	targetTypes []*types.FieldType
}

// Close implements Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.prepared = false
	e.cursor = 0
	e.matchedRows = nil
	err := e.smallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return e.bigExec.Close()
}

func joinTwoRow(a *Row, b *Row) *Row {
	ret := &Row{
		RowKeys: make([]*RowKeyEntry, 0, len(a.RowKeys)+len(b.RowKeys)),
		Data:    make([]types.Datum, 0, len(a.Data)+len(b.Data)),
	}
	ret.RowKeys = append(ret.RowKeys, a.RowKeys...)
	ret.RowKeys = append(ret.RowKeys, b.RowKeys...)
	ret.Data = append(ret.Data, a.Data...)
	ret.Data = append(ret.Data, b.Data...)
	return ret
}

// getHashKey gets the hash key when given a row and hash columns.
// It will return a boolean value representing if the hash key has null, a byte slice representing the result hash code.
func getHashKey(exprs []*expression.Column, row *Row, targetTypes []*types.FieldType) (bool, []byte, error) {
	vals := make([]types.Datum, 0, len(exprs))
	for i, expr := range exprs {
		v, err := expr.Eval(row.Data, nil)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if v.IsNull() {
			return true, nil, nil
		}
		if targetTypes[i].Tp != expr.RetType.Tp {
			v, err = v.ConvertTo(targetTypes[i])
			if err != nil {
				return false, nil, errors.Trace(err)
			}
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	bytes, err := codec.EncodeValue([]byte{}, vals...)
	return false, bytes, errors.Trace(err)
}

// Schema implements Executor Schema interface.
func (e *HashJoinExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *HashJoinExec) Fields() []*ast.ResultField {
	return nil
}

func (e *HashJoinExec) prepare() error {
	e.hashTable = make(map[string][]*Row)
	e.cursor = 0
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched := true
		if e.smallFilter != nil {
			matched, err = expression.EvalBool(e.smallFilter, row.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		hasNull, hashcode, err := getHashKey(e.smallHashKey, row, e.targetTypes)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []*Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}

	e.prepared = true
	return nil
}

func (e *HashJoinExec) constructMatchedRows(bigRow *Row) (matchedRows []*Row, err error) {
	hasNull, hashcode, err := getHashKey(e.bigHashKey, bigRow, e.targetTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if hasNull {
		return
	}
	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		//TODO: remove result fields in order to reduce memory copy cost.
		otherMatched := true
		var matchedRow *Row
		if e.leftSmall {
			matchedRow = joinTwoRow(smallRow, bigRow)
		} else {
			matchedRow = joinTwoRow(bigRow, smallRow)
		}
		if e.otherFilter != nil {
			otherMatched, err = expression.EvalBool(e.otherFilter, matchedRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if otherMatched {
			matchedRows = append(matchedRows, matchedRow)
		}
	}

	return matchedRows, nil
}

func (e *HashJoinExec) fillNullRow(bigRow *Row) (returnRow *Row) {
	smallRow := &Row{
		RowKeys: make([]*RowKeyEntry, len(e.smallExec.Schema())),
		Data:    make([]types.Datum, len(e.smallExec.Schema())),
	}

	for _, data := range smallRow.Data {
		data.SetNull()
	}
	if e.leftSmall {
		returnRow = joinTwoRow(smallRow, bigRow)
	} else {
		returnRow = joinTwoRow(bigRow, smallRow)
	}
	return returnRow
}

func (e *HashJoinExec) returnRecord() (ret *Row, ok bool) {
	if e.cursor >= len(e.matchedRows) {
		return nil, false
	}
	e.cursor++
	return e.matchedRows[e.cursor-1], true
}

// Next implements Executor Next interface.
func (e *HashJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	row, ok := e.returnRecord()
	if ok {
		return row, nil
	}

	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if bigRow == nil {
			e.bigExec.Close()
			return nil, nil
		}

		var matchedRows []*Row
		bigMatched := true
		if e.bigFilter != nil {
			bigMatched, err = expression.EvalBool(e.bigFilter, bigRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if bigMatched {
			matchedRows, err = e.constructMatchedRows(bigRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		e.matchedRows = matchedRows
		e.cursor = 0
		row, ok := e.returnRecord()
		if ok {
			return row, nil
		} else if e.outter {
			row = e.fillNullRow(bigRow)
			return row, nil
		}
	}
}

// HashSemiJoinExec implements the hash join algorithm for semi join.
type HashSemiJoinExec struct {
	hashTable         map[string][]*Row
	smallHashKey      []*expression.Column
	bigHashKey        []*expression.Column
	smallExec         Executor
	bigExec           Executor
	prepared          bool
	ctx               context.Context
	smallFilter       expression.Expression
	bigFilter         expression.Expression
	otherFilter       expression.Expression
	schema            expression.Schema
	withAux           bool
	targetTypes       []*types.FieldType
	smallTableHasNull bool
	// If anti is true, semi join only output the unmatched row.
	anti bool
}

// Close implements Executor Close interface.
func (e *HashSemiJoinExec) Close() error {
	e.prepared = false
	e.hashTable = make(map[string][]*Row)
	e.smallTableHasNull = false
	err := e.smallExec.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return e.bigExec.Close()
}

// Schema implements Executor Schema interface.
func (e *HashSemiJoinExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *HashSemiJoinExec) Fields() []*ast.ResultField {
	return nil
}

func (e *HashSemiJoinExec) prepare() error {
	e.hashTable = make(map[string][]*Row)
	for {
		row, err := e.smallExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			e.smallExec.Close()
			break
		}

		matched := true
		if e.smallFilter != nil {
			matched, err = expression.EvalBool(e.smallFilter, row.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		hasNull, hashcode, err := getHashKey(e.smallHashKey, row, e.targetTypes)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			e.smallTableHasNull = true
			continue
		}
		if rows, ok := e.hashTable[string(hashcode)]; !ok {
			e.hashTable[string(hashcode)] = []*Row{row}
		} else {
			e.hashTable[string(hashcode)] = append(rows, row)
		}
	}

	e.prepared = true
	return nil
}

func (e *HashSemiJoinExec) rowIsMatched(bigRow *Row) (matched bool, hasNull bool, err error) {
	hasNull, hashcode, err := getHashKey(e.bigHashKey, bigRow, e.targetTypes)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	if hasNull {
		return false, true, nil
	}
	rows, ok := e.hashTable[string(hashcode)]
	if !ok {
		return
	}
	// match eq condition
	for _, smallRow := range rows {
		matched = true
		if e.otherFilter != nil {
			var matchedRow *Row
			matchedRow = joinTwoRow(bigRow, smallRow)
			matched, err = expression.EvalBool(e.otherFilter, matchedRow.Data, e.ctx)
			if err != nil {
				return false, false, errors.Trace(err)
			}
		}
		if matched {
			return
		}
	}
	return
}

// Next implements Executor Next interface.
func (e *HashSemiJoinExec) Next() (*Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for {
		bigRow, err := e.bigExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if bigRow == nil {
			e.bigExec.Close()
			return nil, nil
		}

		matched := true
		if e.bigFilter != nil {
			matched, err = expression.EvalBool(e.bigFilter, bigRow.Data, e.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		isNull := false
		if matched {
			matched, isNull, err = e.rowIsMatched(bigRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if !matched && e.smallTableHasNull {
			isNull = true
		}
		if e.anti && !isNull {
			matched = !matched
		}
		if e.withAux {
			if isNull {
				bigRow.Data = append(bigRow.Data, types.NewDatum(nil))
			} else {
				bigRow.Data = append(bigRow.Data, types.NewDatum(matched))
			}
			return bigRow, nil
		} else if matched {
			return bigRow, nil
		}
	}
}

// AggregationExec deals with all the aggregate functions.
// It is built from Aggregate Plan. When Next() is called, it reads all the data from Src and updates all the items in AggFuncs.
type AggregationExec struct {
	Src               Executor
	schema            expression.Schema
	ResultFields      []*ast.ResultField
	executed          bool
	ctx               context.Context
	AggFuncs          []expression.AggregationFunction
	groupMap          map[string]bool
	groups            [][]byte
	currentGroupIndex int
	GroupByItems      []expression.Expression
}

// Close implements Executor Close interface.
func (e *AggregationExec) Close() error {
	e.executed = false
	e.groups = nil
	e.currentGroupIndex = 0
	for _, agg := range e.AggFuncs {
		agg.Clear()
	}
	return e.Src.Close()
}

// Schema implements Executor Schema interface.
func (e *AggregationExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *AggregationExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
func (e *AggregationExec) Next() (*Row, error) {
	// In this stage we consider all data from src as a single group.
	if !e.executed {
		e.groupMap = make(map[string]bool)
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
			e.groups = append(e.groups, []byte{})
		}
	}
	if e.currentGroupIndex >= len(e.groups) {
		return nil, nil
	}
	retRow := &Row{Data: make([]types.Datum, 0, len(e.AggFuncs))}
	groupKey := e.groups[e.currentGroupIndex]
	for _, af := range e.AggFuncs {
		retRow.Data = append(retRow.Data, af.GetGroupResult(groupKey))
	}
	e.currentGroupIndex++
	return retRow, nil
}

func (e *AggregationExec) getGroupKey(row *Row) ([]byte, error) {
	if len(e.GroupByItems) == 0 {
		return []byte{}, nil
	}
	vals := make([]types.Datum, 0, len(e.GroupByItems))
	for _, item := range e.GroupByItems {
		v, err := item.Eval(row.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	bs, err := codec.EncodeValue([]byte{}, vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return bs, nil
}

// Fetch a single row from src and update each aggregate function.
// If the first return value is false, it means there is no more data from src.
func (e *AggregationExec) innerNext() (ret bool, err error) {
	var srcRow *Row
	if e.Src != nil {
		srcRow, err = e.Src.Next()
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
	groupKey, err := e.getGroupKey(srcRow)
	if err != nil {
		return false, errors.Trace(err)
	}
	if _, ok := e.groupMap[string(groupKey)]; !ok {
		e.groupMap[string(groupKey)] = true
		e.groups = append(e.groups, groupKey)
	}
	for _, af := range e.AggFuncs {
		af.Update(srcRow.Data, groupKey, e.ctx)
	}
	return true, nil
}

// ProjectionExec represents a select fields executor.
type ProjectionExec struct {
	Src          Executor
	ResultFields []*ast.ResultField
	schema       expression.Schema
	executed     bool
	ctx          context.Context
	exprs        []expression.Expression
}

// Schema implements Executor Schema interface.
func (e *ProjectionExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *ProjectionExec) Fields() []*ast.ResultField {
	return e.ResultFields
}

// Next implements Executor Next interface.
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

// Close implements Executor Close interface.
func (e *ProjectionExec) Close() error {
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
}

// NewTableDualExec represents a dual table executor.
type NewTableDualExec struct {
	schema   expression.Schema
	executed bool
}

// Init implements NewExecutor Init interface.
func (e *NewTableDualExec) Init() {
	e.executed = false
}

// Schema implements Executor Schema interface.
func (e *NewTableDualExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *NewTableDualExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Executor Next interface.
func (e *NewTableDualExec) Next() (*Row, error) {
	if e.executed {
		return nil, nil
	}
	e.executed = true
	return &Row{}, nil
}

// Close implements Executor interface.
func (e *NewTableDualExec) Close() error {
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	Src       Executor
	Condition expression.Expression
	ctx       context.Context
	schema    expression.Schema
}

// Schema implements Executor Schema interface.
func (e *SelectionExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *SelectionExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Next implements Executor Next interface.
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

// Close implements Executor Close interface.
func (e *SelectionExec) Close() error {
	return e.Src.Close()
}

// NewTableScanExec is a table scan executor without result fields.
type NewTableScanExec struct {
	t          table.Table
	asName     *model.CIStr
	ctx        context.Context
	ranges     []plan.TableRange
	seekHandle int64
	iter       kv.Iterator
	cursor     int
	schema     expression.Schema
	columns    []*model.ColumnInfo
}

// Schema implements Executor Schema interface.
func (e *NewTableScanExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor interface.
func (e *NewTableScanExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Executor interface.
func (e *NewTableScanExec) Next() (*Row, error) {
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

// seekRange increments the range cursor to the range
// with high value greater or equal to handle.
func (e *NewTableScanExec) seekRange(handle int64) (inRange bool) {
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

func (e *NewTableScanExec) getRow(handle int64) (*Row, error) {
	row := &Row{}
	var err error

	columns := make([]*table.Column, len(e.schema))
	for i, v := range e.columns {
		columns[i] = &table.Column{ColumnInfo: *v}
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

// Close implements Executor Close interface.
func (e *NewTableScanExec) Close() error {
	e.iter = nil
	e.cursor = 0
	return nil
}

// NewSortExec represents sorting executor.
type NewSortExec struct {
	Src     Executor
	ByItems []*plan.ByItems
	Rows    []*orderByRow
	ctx     context.Context
	Limit   *plan.Limit
	Idx     int
	fetched bool
	err     error
	schema  expression.Schema
}

// Close implements Executor Close interface.
func (e *NewSortExec) Close() error {
	e.fetched = false
	e.Rows = nil
	return e.Src.Close()
}

// Schema implements Executor Schema interface.
func (e *NewSortExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *NewSortExec) Fields() []*ast.ResultField {
	return e.Src.Fields()
}

// Len returns the number of rows.
func (e *NewSortExec) Len() int {
	return len(e.Rows)
}

// Swap implements sort.Interface Swap interface.
func (e *NewSortExec) Swap(i, j int) {
	e.Rows[i], e.Rows[j] = e.Rows[j], e.Rows[i]
}

// Less implements sort.Interface Less interface.
func (e *NewSortExec) Less(i, j int) bool {
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(v2)
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
func (e *NewSortExec) Next() (*Row, error) {
	if !e.fetched {
		offset := -1
		totalCount := -1
		if e.Limit != nil {
			offset = int(e.Limit.Offset)
			totalCount = offset + int(e.Limit.Count)
		}
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
			if totalCount != -1 && e.Len() >= totalCount+SortBufferSize {
				sort.Sort(e)
				e.Rows = e.Rows[:totalCount]
			}
		}
		sort.Sort(e)
		if offset >= 0 && offset < e.Len() {
			if totalCount > e.Len() {
				e.Rows = e.Rows[offset:]
			} else {
				e.Rows = e.Rows[offset:totalCount]
			}
		} else if offset != -1 {
			e.Rows = e.Rows[:0]
		}
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

func (b *executorBuilder) newConditionExprToPBExpr(client kv.Client, exprs []expression.Expression,
	tbl *model.TableInfo) (pbExpr *tipb.Expr, remained []expression.Expression) {
	for _, expr := range exprs {
		v := b.newExprToPBExpr(client, expr, tbl)
		if v == nil {
			remained = append(remained, expr)
			continue
		}
		if pbExpr == nil {
			pbExpr = v
		} else {
			// merge multiple converted pb expression into an AND expression.
			pbExpr = &tipb.Expr{
				Tp:       tipb.ExprType_And.Enum(),
				Children: []*tipb.Expr{pbExpr, v}}
		}
	}
	return
}

func (b *executorBuilder) newGroupByItemToPB(client kv.Client, expr expression.Expression, tbl *model.TableInfo) *tipb.ByItem {
	e := b.newExprToPBExpr(client, expr, tbl)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e}
}

func (b *executorBuilder) newAggFuncToPBExpr(client kv.Client, aggFunc expression.AggregationFunction,
	tbl *model.TableInfo) *tipb.Expr {
	var tp tipb.ExprType
	switch aggFunc.GetName() {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
	case ast.AggFuncFirstRow:
		tp = tipb.ExprType_First
	case ast.AggFuncGroupConcat:
		tp = tipb.ExprType_GroupConcat
	case ast.AggFuncMax:
		tp = tipb.ExprType_Max
	case ast.AggFuncMin:
		tp = tipb.ExprType_Min
	case ast.AggFuncSum:
		tp = tipb.ExprType_Sum
	case ast.AggFuncAvg:
		tp = tipb.ExprType_Avg
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	children := make([]*tipb.Expr, 0, len(aggFunc.GetArgs()))
	for _, arg := range aggFunc.GetArgs() {
		pbArg := b.newExprToPBExpr(client, arg, tbl)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp.Enum(), Children: children}
}

// newExprToPBExpr converts an expression.Expression to a tipb.Expr, if not supported, nil will be returned.
func (b *executorBuilder) newExprToPBExpr(client kv.Client, expr expression.Expression, tbl *model.TableInfo) *tipb.Expr {
	switch x := expr.(type) {
	case *expression.Constant:
		return b.datumToPBExpr(client, expr.(*expression.Constant).Value)
	case *expression.Column:
		return b.columnToPBExpr(client, x, tbl)
	case *expression.ScalarFunction:
		return b.scalarFuncToPBExpr(client, x, tbl)
	}

	return nil
}

func (b *executorBuilder) columnToPBExpr(client kv.Client, column *expression.Column, tbl *model.TableInfo) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	switch column.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeDecimal, mysql.TypeGeometry,
		mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeYear:
		return nil
	}

	id := int64(-1)
	for _, col := range tbl.Columns {
		if tbl.Name == column.TblName && col.Name == column.ColName {
			id = col.ID
			break
		}
	}
	// Zero Column ID is not a column from table, can not support for now.
	if id == 0 {
		return nil
	}
	// TODO：If the column ID isn't in fields, it means the column is from an outer table,
	// its value is available to use.
	if id == -1 {
		return nil
	}

	return &tipb.Expr{
		Tp:  tipb.ExprType_ColumnRef.Enum(),
		Val: codec.EncodeInt(nil, id)}
}

func (b *executorBuilder) inToPBExpr(client kv.Client, expr *expression.ScalarFunction, tbl *model.TableInfo) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_In)) {
		return nil
	}

	pbExpr := b.newExprToPBExpr(client, expr.Args[0], tbl)
	if pbExpr == nil {
		return nil
	}
	listExpr := b.constListToPBExpr(client, expr.Args[1:], tbl)
	if listExpr == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tipb.ExprType_In.Enum(),
		Children: []*tipb.Expr{pbExpr, listExpr}}
}

func (b *executorBuilder) constListToPBExpr(client kv.Client, list []expression.Expression, tbl *model.TableInfo) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}

	// Only list of *expression.Constant can be push down.
	datums := make([]types.Datum, 0, len(list))
	for _, expr := range list {
		v, ok := expr.(*expression.Constant)
		if !ok {
			return nil
		}
		if b.datumToPBExpr(client, v.Value) == nil {
			return nil
		}
		datums = append(datums, v.Value)
	}
	return b.datumsToValueList(datums)
}

func (b *executorBuilder) notToPBExpr(client kv.Client, expr *expression.ScalarFunction, tbl *model.TableInfo) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_Not)) {
		return nil
	}

	child := b.newExprToPBExpr(client, expr.Args[0], tbl)
	if child == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tipb.ExprType_Not.Enum(),
		Children: []*tipb.Expr{child}}
}

func (b *executorBuilder) scalarFuncToPBExpr(client kv.Client, expr *expression.ScalarFunction,
	tbl *model.TableInfo) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.LT:
		tp = tipb.ExprType_LT
	case ast.LE:
		tp = tipb.ExprType_LE
	case ast.EQ:
		tp = tipb.ExprType_EQ
	case ast.NE:
		tp = tipb.ExprType_NE
	case ast.GE:
		tp = tipb.ExprType_GE
	case ast.GT:
		tp = tipb.ExprType_GT
	case ast.NullEQ:
		tp = tipb.ExprType_NullEQ
	case ast.And:
		tp = tipb.ExprType_And
	case ast.Or:
		tp = tipb.ExprType_Or
	case ast.UnaryNot:
		return b.notToPBExpr(client, expr, tbl)
	case ast.In:
		return b.inToPBExpr(client, expr, tbl)
	case ast.Like:
		// Only patterns like 'abc', '%abc', 'abc%', '%abc%' can be converted to *tipb.Expr for now.
		escape := expr.Args[2].(*expression.Constant).Value
		if escape.IsNull() || byte(escape.GetInt64()) != '\\' {
			return nil
		}
		pattern := expr.Args[1].(*expression.Constant).Value
		if pattern.Kind() != types.KindString {
			return nil
		}
		for i, b := range pattern.GetString() {
			switch b {
			case '\\', '_':
				return nil
			case '%':
				if i != 0 && i != len(pattern.GetString())-1 {
					return nil
				}
			}
		}
		tp = tipb.ExprType_Like
	default:
		return nil
	}

	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	expr0 := b.newExprToPBExpr(client, expr.Args[0], tbl)
	if expr0 == nil {
		return nil
	}
	expr1 := b.newExprToPBExpr(client, expr.Args[1], tbl)
	if expr1 == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tp.Enum(),
		Children: []*tipb.Expr{expr0, expr1}}
}

// ApplyExec represents apply executor.
// Apply gets one row from outer executor and gets one row from inner executor according to outer row.
type ApplyExec struct {
	schema      expression.Schema
	Src         Executor
	outerSchema expression.Schema
	innerExec   Executor
	checker     *conditionChecker
}

// conditionChecker checks if all or any of the row match this condition.
type conditionChecker struct {
	cond        expression.Expression
	trimLen     int
	ctx         context.Context
	all         bool
	dataHasNull bool
}

// Check returns finished for checking if the input row can determine the final result,
// and returns data for the eval result.
func (c *conditionChecker) Check(rowData []types.Datum) (finished bool, data types.Datum, err error) {
	data, err = c.cond.Eval(rowData, c.ctx)
	if err != nil {
		return false, data, errors.Trace(err)
	}
	var matched int64
	if data.IsNull() {
		c.dataHasNull = true
		matched = 0
	} else {
		matched, err = data.ToBool()
		if err != nil {
			return false, data, errors.Trace(err)
		}
	}
	return (matched != 0) != c.all, data, nil
}

// Schema implements Executor Schema interface.
func (e *ApplyExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *ApplyExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *ApplyExec) Close() error {
	if e.checker != nil {
		e.checker.dataHasNull = false
	}
	return e.Src.Close()
}

// Next implements Executor Next interface.
func (e *ApplyExec) Next() (*Row, error) {
	srcRow, err := e.Src.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if srcRow == nil {
		return nil, nil
	}
	for {
		for _, col := range e.outerSchema {
			idx := col.Index
			col.SetValue(&srcRow.Data[idx])
		}
		innerRow, err := e.innerExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		trimLen := len(srcRow.Data)
		if innerRow != nil {
			srcRow.Data = append(srcRow.Data, innerRow.Data...)
		}
		if e.checker == nil {
			e.innerExec.Close()
			return srcRow, nil
		}
		if innerRow == nil {
			var d types.Datum
			// If we can't determine the result until the last row comes, the all must be true and any must not be true.
			// If the any have met a null, the result will be null.
			if e.checker.dataHasNull && !e.checker.all {
				d = types.NewDatum(nil)
			} else {
				d = types.NewDatum(e.checker.all)
			}
			srcRow.Data = append(srcRow.Data, d)
			e.checker.dataHasNull = false
			e.innerExec.Close()
			return srcRow, nil
		}
		finished, data, err := e.checker.Check(srcRow.Data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		srcRow.Data = srcRow.Data[:trimLen]
		if finished {
			e.checker.dataHasNull = false
			e.innerExec.Close()
			srcRow.Data = append(srcRow.Data, data)
			return srcRow, nil
		}
	}
}

// ExistsExec represents exists executor.
type ExistsExec struct {
	schema    expression.Schema
	Src       Executor
	evaluated bool
}

// Schema implements Executor Schema interface.
func (e *ExistsExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *ExistsExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *ExistsExec) Close() error {
	e.evaluated = false
	return e.Src.Close()
}

// Next implements Executor Next interface.
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

// MaxOneRowExec checks if a query returns no more than one row.
type MaxOneRowExec struct {
	schema    expression.Schema
	Src       Executor
	evaluated bool
}

// Schema implements Executor Schema interface.
func (e *MaxOneRowExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *MaxOneRowExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *MaxOneRowExec) Close() error {
	e.evaluated = false
	return e.Src.Close()
}

// Next implements Executor Next interface.
func (e *MaxOneRowExec) Next() (*Row, error) {
	if !e.evaluated {
		e.evaluated = true
		srcRow, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow == nil {
			return &Row{Data: []types.Datum{types.NewDatum(nil)}}, nil
		}
		srcRow1, err := e.Src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if srcRow1 != nil {
			return nil, errors.New("Subquery returns more than 1 row.")
		}
		return srcRow, nil
	}
	return nil, nil
}

// TrimExec truncates src rows.
type TrimExec struct {
	schema expression.Schema
	Src    Executor
	len    int
}

// Schema implements Executor Schema interface.
func (e *TrimExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *TrimExec) Fields() []*ast.ResultField {
	return nil
}

// Close implements Executor Close interface.
func (e *TrimExec) Close() error {
	return e.Src.Close()
}

// Next implements Executor Next interface.
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

// NewUnionExec represents union executor.
type NewUnionExec struct {
	fields []*ast.ResultField
	schema expression.Schema
	Srcs   []Executor
	cursor int
}

// Schema implements Executor Schema interface.
func (e *NewUnionExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *NewUnionExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Executor Next interface.
func (e *NewUnionExec) Next() (*Row, error) {
	for {
		if e.cursor >= len(e.Srcs) {
			return nil, nil
		}
		sel := e.Srcs[e.cursor]
		row, err := sel.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			e.cursor++
			continue
		}
		if e.cursor != 0 {
			for i := range row.Data {
				// The column value should be casted as the same type of the first select statement in corresponding position.
				col := e.schema[i]
				var val types.Datum
				val, err = row.Data[i].ConvertTo(col.RetType)
				if err != nil {
					return nil, errors.Trace(err)
				}
				row.Data[i] = val
			}
		}
		return row, nil
	}
}

// Close implements Executor Close interface.
func (e *NewUnionExec) Close() error {
	e.cursor = 0
	for _, sel := range e.Srcs {
		er := sel.Close()
		if er != nil {
			return errors.Trace(er)
		}
	}
	return nil
}
