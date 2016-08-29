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
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tipb/go-tipb"
)

const defaultConcurrency int = 10

// XExecutor defines some interfaces used by dist-sql.
type XExecutor interface {
	// AddAggregate adds aggregate info into an executor.
	AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType)
	// GetTableName gets the table name of this XExecutor.
	GetTableName() *ast.TableName
}

func resultRowToRow(t table.Table, h int64, data []types.Datum, tableAsName *model.CIStr) *Row {
	entry := &RowKeyEntry{
		Handle:      h,
		Tbl:         t,
		TableAsName: tableAsName,
	}
	return &Row{Data: data, RowKeys: []*RowKeyEntry{entry}}
}

// BaseLookupTableTaskSize represents base number of handles for a lookupTableTask.
var BaseLookupTableTaskSize = 1024

// MaxLookupTableTaskSize represents max number of handles for a lookupTableTask.
var MaxLookupTableTaskSize = 1024

type lookupTableTask struct {
	handles    []int64
	rows       []*Row
	cursor     int
	done       bool
	doneCh     chan error
	indexOrder map[int64]int
}

func (task *lookupTableTask) getRow() (*Row, error) {
	if !task.done {
		err := <-task.doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
		task.done = true
	}

	if task.cursor < len(task.rows) {
		row := task.rows[task.cursor]
		task.cursor++
		return row, nil
	}

	return nil, nil
}

type rowsSorter struct {
	order map[int64]int
	rows  []*Row
}

func (s *rowsSorter) Less(i, j int) bool {
	x := s.order[s.rows[i].RowKeys[0].Handle]
	y := s.order[s.rows[j].RowKeys[0].Handle]
	return x < y
}

func (s *rowsSorter) Len() int {
	return len(s.rows)
}

func (s *rowsSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func tableRangesToPBRanges(tableRanges []plan.TableRange) []*tipb.KeyRange {
	hrs := make([]*tipb.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, tableRange.LowVal)
		hi := tableRange.HighVal
		if hi != math.MaxInt64 {
			hi++
		}
		pbRange.High = codec.EncodeInt(nil, hi)
		hrs = append(hrs, pbRange)
	}
	return hrs
}

func indexRangesToPBRanges(ranges []*plan.IndexRange, fieldTypes []*types.FieldType) ([]*tipb.KeyRange, error) {
	keyRanges := make([]*tipb.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		err := convertIndexRangeTypes(ran, fieldTypes)
		if err != nil {
			return nil, errors.Trace(err)
		}

		low, err := codec.EncodeKey(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		high, err := codec.EncodeKey(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PrefixNext())
		}
		keyRanges = append(keyRanges, &tipb.KeyRange{Low: low, High: high})
	}
	return keyRanges, nil
}

func convertIndexRangeTypes(ran *plan.IndexRange, fieldTypes []*types.FieldType) error {
	for i := range ran.LowVal {
		if ran.LowVal[i].Kind() == types.KindMinNotNull {
			ran.LowVal[i].SetBytes([]byte{})
			continue
		}
		converted, err := ran.LowVal[i].ConvertTo(fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(ran.LowVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[i] = converted
		if cmp == 0 {
			continue
		}
		if cmp < 0 && !ran.LowExclude {
			// For int column a, a >= 1.1 is converted to a > 1.
			ran.LowExclude = true
		} else if cmp > 0 && ran.LowExclude {
			// For int column a, a > 1.9 is converted to a >= 2.
			ran.LowExclude = false
		}
		// The converted value has changed, the other column values doesn't matter.
		// For equal condition, converted value changed means there will be no match.
		// For non equal condition, this column would be the last one to build the range.
		// Break here to prevent the rest columns modify LowExclude again.
		break
	}
	for i := range ran.HighVal {
		if ran.HighVal[i].Kind() == types.KindMaxValue {
			continue
		}
		converted, err := ran.HighVal[i].ConvertTo(fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(ran.HighVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.HighVal[i] = converted
		if cmp == 0 {
			continue
		}
		// For int column a, a < 1.1 is converted to a <= 1.
		if cmp < 0 && ran.HighExclude {
			ran.HighExclude = false
		}
		// For int column a, a <= 1.9 is converted to a < 2.
		if cmp > 0 && !ran.HighExclude {
			ran.HighExclude = true
		}
		break
	}
	return nil
}

// extractHandlesFromIndexResult gets some handles from SelectResult.
// It should be called in a loop until finish or error.
func extractHandlesFromIndexResult(idxResult xapi.SelectResult) (handles []int64, finish bool, err error) {
	subResult, e0 := idxResult.Next()
	if e0 != nil {
		err = errors.Trace(e0)
		return
	}
	if subResult == nil {
		finish = true
		return
	}
	subHandles, e1 := extractHandlesFromIndexSubResult(subResult)
	if e1 != nil {
		err = errors.Trace(e1)
		return
	}
	handles = subHandles
	return
}

func extractHandlesFromIndexSubResult(subResult xapi.PartialResult) ([]int64, error) {
	var handles []int64
	for {
		h, data, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		handles = append(handles, h)
	}
	return handles, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// exprToPBExpr converts an ast.ExprNode to a tipb.Expr, if not supported, nil will be returned.
func (b *executorBuilder) exprToPBExpr(client kv.Client, expr ast.ExprNode, tn *ast.TableName) *tipb.Expr {
	switch x := expr.(type) {
	case *ast.ValueExpr, *ast.ParamMarkerExpr:
		return b.datumToPBExpr(client, *expr.GetDatum())
	case *ast.ColumnNameExpr:
		return b.columnNameToPBExpr(client, x, tn)
	case *ast.BinaryOperationExpr:
		return b.binopToPBExpr(client, x, tn)
	case *ast.ParenthesesExpr:
		return b.exprToPBExpr(client, x.Expr, tn)
	case *ast.PatternLikeExpr:
		return b.likeToPBExpr(client, x, tn)
	case *ast.UnaryOperationExpr:
		return b.unaryToPBExpr(client, x, tn)
	case *ast.PatternInExpr:
		return b.patternInToPBExpr(client, x, tn)
	case *ast.SubqueryExpr:
		return b.subqueryToPBExpr(client, x)
	case *ast.AggregateFuncExpr:
		return b.aggFuncToPBExpr(client, x, tn)
	default:
		return nil
	}
}

func (b *executorBuilder) groupByItemToPB(client kv.Client, item *ast.ByItem, tn *ast.TableName) *tipb.ByItem {
	expr := b.exprToPBExpr(client, item.Expr, tn)
	if expr == nil {
		return nil
	}
	return &tipb.ByItem{Expr: expr}
}

func (b *executorBuilder) aggFuncToPBExpr(client kv.Client, af *ast.AggregateFuncExpr, tn *ast.TableName) *tipb.Expr {
	name := strings.ToLower(af.F)
	var tp tipb.ExprType
	switch name {
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
	// convert it to pb
	children := make([]*tipb.Expr, 0, len(af.Args))
	for _, arg := range af.Args {
		pbArg := b.exprToPBExpr(client, arg, tn)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp, Children: children}
}

func (b *executorBuilder) columnNameToPBExpr(client kv.Client, column *ast.ColumnNameExpr, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	// Zero Column ID is not a column from table, can not support for now.
	if column.Refer.Column.ID == 0 {
		return nil
	}
	switch column.Refer.Expr.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry,
		mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeYear:
		return nil
	}
	matched := false
	for _, f := range tn.GetResultFields() {
		if f.TableName == column.Refer.TableName && f.Column.ID == column.Refer.Column.ID {
			matched = true
			break
		}
	}
	if matched {
		pbExpr := new(tipb.Expr)
		pbExpr.Tp = tipb.ExprType_ColumnRef
		pbExpr.Val = codec.EncodeInt(nil, column.Refer.Column.ID)
		return pbExpr
	}
	// If the column ID isn't in fields, it means the column is from an outer table,
	// its value is available to use.
	return b.datumToPBExpr(client, *column.Refer.Expr.GetDatum())
}

func (b *executorBuilder) datumToPBExpr(client kv.Client, d types.Datum) *tipb.Expr {
	var tp tipb.ExprType
	var val []byte
	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		tp = tipb.ExprType_MysqlDecimal
		val = codec.EncodeDecimal(nil, d.GetMysqlDecimal())
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val}
}

func (b *executorBuilder) binopToPBExpr(client kv.Client, expr *ast.BinaryOperationExpr, tn *ast.TableName) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.Op {
	case opcode.LT:
		tp = tipb.ExprType_LT
	case opcode.LE:
		tp = tipb.ExprType_LE
	case opcode.EQ:
		tp = tipb.ExprType_EQ
	case opcode.NE:
		tp = tipb.ExprType_NE
	case opcode.GE:
		tp = tipb.ExprType_GE
	case opcode.GT:
		tp = tipb.ExprType_GT
	case opcode.NullEQ:
		tp = tipb.ExprType_NullEQ
	case opcode.AndAnd:
		tp = tipb.ExprType_And
	case opcode.OrOr:
		tp = tipb.ExprType_Or
	case opcode.Plus:
		tp = tipb.ExprType_Plus
	case opcode.Div:
		tp = tipb.ExprType_Div
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	leftExpr := b.exprToPBExpr(client, expr.L, tn)
	if leftExpr == nil {
		return nil
	}
	rightExpr := b.exprToPBExpr(client, expr.R, tn)
	if rightExpr == nil {
		return nil
	}
	return &tipb.Expr{Tp: tp, Children: []*tipb.Expr{leftExpr, rightExpr}}
}

// Only patterns like 'abc', '%abc', 'abc%', '%abc%' can be converted to *tipb.Expr for now.
func (b *executorBuilder) likeToPBExpr(client kv.Client, expr *ast.PatternLikeExpr, tn *ast.TableName) *tipb.Expr {
	if expr.Escape != '\\' {
		return nil
	}
	patternDatum := expr.Pattern.GetDatum()
	if patternDatum.Kind() != types.KindString {
		return nil
	}
	patternStr := patternDatum.GetString()
	for i, r := range patternStr {
		switch r {
		case '\\', '_':
			return nil
		case '%':
			if i != 0 && i != len(patternStr)-1 {
				return nil
			}
		}
	}
	patternExpr := b.exprToPBExpr(client, expr.Pattern, tn)
	if patternExpr == nil {
		return nil
	}
	targetExpr := b.exprToPBExpr(client, expr.Expr, tn)
	if targetExpr == nil {
		return nil
	}
	likeExpr := &tipb.Expr{Tp: tipb.ExprType_Like, Children: []*tipb.Expr{targetExpr, patternExpr}}
	if !expr.Not {
		return likeExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{likeExpr}}
}

func (b *executorBuilder) unaryToPBExpr(client kv.Client, expr *ast.UnaryOperationExpr, tn *ast.TableName) *tipb.Expr {
	switch expr.Op {
	case opcode.Not:
		if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_Not)) {
			return nil
		}
	default:
		return nil
	}
	child := b.exprToPBExpr(client, expr.V, tn)
	if child == nil {
		return nil
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{child}}
}

func (b *executorBuilder) subqueryToPBExpr(client kv.Client, expr *ast.SubqueryExpr) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}
	if expr.Correlated || len(expr.Query.GetResultFields()) != 1 {
		// We only push down evaluated non-correlated subquery which has only one field.
		return nil
	}
	err := evaluator.EvalSubquery(b.ctx, expr)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	if expr.Datum.Kind() != types.KindRow {
		// Do not push down datum kind is not row.
		return nil
	}
	return b.datumsToValueList(expr.Datum.GetRow())
}

func (b *executorBuilder) patternInToPBExpr(client kv.Client, expr *ast.PatternInExpr, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_In)) {
		return nil
	}
	pbExpr := b.exprToPBExpr(client, expr.Expr, tn)
	if pbExpr == nil {
		return nil
	}
	var listExpr *tipb.Expr
	if expr.Sel != nil {
		listExpr = b.exprToPBExpr(client, expr.Sel, tn)
	} else {
		listExpr = b.exprListToPBExpr(client, expr.List, tn)
	}
	if listExpr == nil {
		return nil
	}
	inExpr := &tipb.Expr{Tp: tipb.ExprType_In, Children: []*tipb.Expr{pbExpr, listExpr}}
	if !expr.Not {
		return inExpr
	}
	return &tipb.Expr{Tp: tipb.ExprType_Not, Children: []*tipb.Expr{inExpr}}
}

func (b *executorBuilder) exprListToPBExpr(client kv.Client, list []ast.ExprNode, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}
	// Only list of *ast.ValueExpr can be push down.
	datums := make([]types.Datum, 0, len(list))
	for _, v := range list {
		x, ok := v.(*ast.ValueExpr)
		if !ok {
			return nil
		}
		if b.datumToPBExpr(client, x.Datum) == nil {
			return nil
		}
		datums = append(datums, x.Datum)
	}
	return b.datumsToValueList(datums)
}

func (b *executorBuilder) datumsToValueList(datums []types.Datum) *tipb.Expr {
	// Don't push value list that has different datum kind.
	prevKind := types.KindNull
	for _, d := range datums {
		if prevKind == types.KindNull {
			prevKind = d.Kind()
		}
		if !d.IsNull() && d.Kind() != prevKind {
			return nil
		}
	}
	err := types.SortDatums(datums)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	val, err := codec.EncodeValue(nil, datums...)
	if err != nil {
		b.err = errors.Trace(err)
		return nil
	}
	return &tipb.Expr{Tp: tipb.ExprType_ValueList, Val: val}
}
