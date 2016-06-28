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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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
	"github.com/pingcap/tidb/xapi"
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
	//TODO: remove fields when abandon old plan.
	fields      []*ast.ResultField
	schema      expression.Schema
	outter      bool
	leftSmall   bool
	matchedRows []*Row
	cursor      int
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

func (e *HashJoinExec) getHashKey(exprs []*expression.Column, row *Row) ([]byte, error) {
	vals := make([]types.Datum, 0, len(exprs))
	for _, expr := range exprs {
		v, err := expr.Eval(row.Data, e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		return []byte{}, nil
	}

	return codec.EncodeValue([]byte{}, vals...)
}

// Schema implements Executor Schema interface.
func (e *HashJoinExec) Schema() expression.Schema {
	return e.schema
}

// Fields implements Executor Fields interface.
func (e *HashJoinExec) Fields() []*ast.ResultField {
	return e.fields
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
		hashcode, err := e.getHashKey(e.smallHashKey, row)
		if err != nil {
			return errors.Trace(err)
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
	hashcode, err := e.getHashKey(e.bigHashKey, bigRow)
	if err != nil {
		return nil, errors.Trace(err)
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
	tableInfo   *model.TableInfo
	table       table.Table
	asName      *model.CIStr
	ctx         context.Context
	supportDesc bool
	isMemDB     bool
	result      *xapi.SelectResult
	subResult   *xapi.SubResult
	where       *tipb.Expr
	Columns     []*model.ColumnInfo
	schema      expression.Schema
	ranges      []plan.TableRange
}

// Schema implements Executor Schema interface.
func (e *NewTableScanExec) Schema() expression.Schema {
	return e.schema
}

func (e *NewTableScanExec) doRequest() error {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	selReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selReq.StartTs = &startTs
	selReq.Where = e.where
	selReq.Ranges = tableRangesToPBRanges(e.ranges)
	columns := e.Columns
	selReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.tableInfo.ID),
	}
	selReq.TableInfo.Columns = xapi.ColumnsToProto(columns, e.tableInfo.PKIsHandle)
	e.result, err = xapi.Select(txn.GetClient(), selReq, defaultConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close implements Executor Close interface.
func (e *NewTableScanExec) Close() error {
	e.result = nil
	e.subResult = nil
	return nil
}

// Next implements Executor interface.
func (e *NewTableScanExec) Next() (*Row, error) {
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for {
		if e.subResult == nil {
			var err error
			startTs := time.Now()
			e.subResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.subResult == nil {
				return nil, nil
			}
			log.Debugf("[TIME_TABLE_SCAN] %v", time.Now().Sub(startTs))
		}
		h, rowData, err := e.subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.subResult = nil
			continue
		}
		return resultRowToRow(e.table, h, rowData, e.asName), nil
	}
}

// Fields implements Executor interface.
func (e *NewTableScanExec) Fields() []*ast.ResultField {
	return nil
}

// NewSortExec represents sorting executor.
type NewSortExec struct {
	Src     Executor
	ByItems []plan.ByItems
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

// newExprToPBExpr converts an expression.Expression to a tipb.Expr, if not supported, nil will be returned.
func (b *executorBuilder) newExprToPBExpr(client kv.Client, expr expression.Expression, tbl *model.TableInfo) *tipb.Expr {
	switch x := expr.(type) {
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
	// It's the operation for unary operator.
	case ast.UnaryNot:
		tp = tipb.ExprType_Not
	default:
		return nil
	}

	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	if len(expr.Args) == 1 {
		child := b.newExprToPBExpr(client, expr.Args[0], tbl)
		if child == nil {
			return nil
		}
		return &tipb.Expr{
			Tp:       tp.Enum(),
			Children: []*tipb.Expr{child}}
	}

	leftExpr := b.newExprToPBExpr(client, expr.Args[0], tbl)
	if leftExpr == nil {
		return nil
	}
	rightExpr := b.newExprToPBExpr(client, expr.Args[1], tbl)
	if rightExpr == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tp.Enum(),
		Children: []*tipb.Expr{leftExpr, rightExpr}}
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

type conditionChecker struct {
	cond    expression.Expression
	trimLen int
	ctx     context.Context
	all     bool
	matched bool
}

func (c *conditionChecker) Exec(row *Row, lastRow bool) (*Row, bool, error) {
	matched := false
	if !lastRow {
		var err error
		matched, err = expression.EvalBool(c.cond, row.Data, c.ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			c.matched = true
		}
	} else if c.all {
		matched = c.matched
	}
	if (matched && !c.all) || (!matched && c.all) || lastRow {
		row.Data = append(row.Data[:c.trimLen], types.NewDatum(matched))
		return row, true, nil
	}
	row.Data = row.Data[:c.trimLen]
	return nil, false, nil
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
		e.checker.matched = false
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
		outerRow, err := e.innerExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if outerRow != nil {
			srcRow.Data = append(srcRow.Data, outerRow.Data...)
		}
		if e.checker == nil {
			e.innerExec.Close()
			return srcRow, nil
		}
		resultRow, finished, err := e.checker.Exec(srcRow, outerRow == nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if finished {
			e.innerExec.Close()
			return resultRow, nil
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
