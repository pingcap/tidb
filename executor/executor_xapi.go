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
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tipb/go-tipb"
)

// XSelectTableExec represents XAPI select executor.
type XSelectTableExec struct {
	table     table.Table
	tablePlan *plan.TableScan
	where     *tipb.Expr
	ctx       context.Context
	result    *xapi.SelectResult
	subResult *xapi.SubResult
}

// Next implements Executor Next interface.
func (e *XSelectTableExec) Next() (*Row, error) {
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for {
		if e.subResult == nil {
			var err error
			e.subResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.subResult == nil {
				return nil, nil
			}
		}
		h, rowData, err := e.subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.subResult = nil
			continue
		}
		fullRowData := make([]types.Datum, len(e.tablePlan.Fields()))
		var j int
		for i, field := range e.tablePlan.Fields() {
			if field.Referenced {
				fullRowData[i] = rowData[j]
				field.Expr.SetDatum(rowData[j])
				j++
			}
		}
		return resultRowToRow(e.table, h, fullRowData), nil
	}
}

// Fields implements Executor Fields interface.
func (e *XSelectTableExec) Fields() []*ast.ResultField {
	return e.tablePlan.Fields()
}

// Close implements Executor Close interface.
func (e *XSelectTableExec) Close() error {
	if e.result != nil {
		e.result.Close()
	}
	return nil
}

func resultRowToRow(t table.Table, h int64, data []types.Datum) *Row {
	entry := &RowKeyEntry{Handle: h, Tbl: t}
	return &Row{Data: data, RowKeys: []*RowKeyEntry{entry}}
}

func (e *XSelectTableExec) doRequest() error {
	// TODO: add offset and limit.
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	selReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selReq.StartTs = &startTs
	selReq.Fields = resultFieldsToPBExpression(e.tablePlan.Fields())
	selReq.Where = e.where
	selReq.Ranges = tableRangesToPBRanges(e.tablePlan.Ranges)

	columns := make([]*model.ColumnInfo, 0, len(e.tablePlan.Fields()))
	for _, v := range e.tablePlan.Fields() {
		if v.Referenced {
			columns = append(columns, v.Column)
		}
	}
	selReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.table.Meta().ID),
	}
	selReq.TableInfo.Columns = tablecodec.ColumnsToProto(columns, e.table.Meta().PKIsHandle)
	e.result, err = xapi.Select(txn.GetClient(), selReq, 1)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// XSelectIndexExec represents XAPI select index executor.
type XSelectIndexExec struct {
	table     table.Table
	indexPlan *plan.IndexScan
	ctx       context.Context
	where     *tipb.Expr
	rows      []*Row
	cursor    int
}

// Fields implements Executor Fields interface.
func (e *XSelectIndexExec) Fields() []*ast.ResultField {
	return e.indexPlan.Fields()
}

// Next implements Executor Next interface.
func (e *XSelectIndexExec) Next() (*Row, error) {
	if e.rows == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	for i, field := range e.indexPlan.Fields() {
		field.Expr.SetDatum(row.Data[i])
	}
	e.cursor++
	return row, nil
}

// Close implements Executor Close interface.
func (e *XSelectIndexExec) Close() error {
	return nil
}

func (e *XSelectIndexExec) doRequest() error {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	idxResult, err := e.doIndexRequest(txn)
	if err != nil {
		return errors.Trace(err)
	}
	handles, err := extractHandlesFromIndexResult(idxResult)
	if err != nil {
		return errors.Trace(err)
	}
	if len(handles) == 0 {
		return nil
	}

	var indexOrder map[int64]int
	if !e.indexPlan.OutOfOrder {
		// Save the index order.
		indexOrder = make(map[int64]int)
		for i, h := range handles {
			indexOrder[h] = i
		}
	}

	sort.Sort(int64Slice(handles))
	tblResult, err := e.doTableRequest(txn, handles)
	if err != nil {
		return errors.Trace(err)
	}
	rows, err := e.extractRowsFromTableResult(e.table, tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	if !e.indexPlan.OutOfOrder {
		// Restore the index order.
		sorter := &rowsSorter{order: indexOrder, rows: rows}
		sort.Sort(sorter)
	}
	e.rows = rows
	return nil
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

func (e *XSelectIndexExec) doIndexRequest(txn kv.Transaction) (*xapi.SelectResult, error) {
	// TODO: add offset and limit.
	selIdxReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selIdxReq.StartTs = &startTs
	selIdxReq.IndexInfo = tablecodec.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	fieldTypes := make([]*types.FieldType, len(e.indexPlan.Index.Columns))
	for i, v := range e.indexPlan.Index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	var err error
	selIdxReq.Ranges, err = indexRangesToPBRanges(e.indexPlan.Ranges, fieldTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return xapi.Select(txn.GetClient(), selIdxReq, 1)
}

func (e *XSelectIndexExec) doTableRequest(txn kv.Transaction, handles []int64) (*xapi.SelectResult, error) {
	// TODO: add offset and limit.
	selTableReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selTableReq.StartTs = &startTs
	columns := make([]*model.ColumnInfo, 0, len(e.indexPlan.Fields()))
	for _, v := range e.indexPlan.Fields() {
		if v.Referenced {
			columns = append(columns, v.Column)
		}
	}
	selTableReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.table.Meta().ID),
	}
	selTableReq.TableInfo.Columns = tablecodec.ColumnsToProto(columns, e.table.Meta().PKIsHandle)
	selTableReq.Fields = resultFieldsToPBExpression(e.indexPlan.Fields())
	for _, h := range handles {
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			continue
		}
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, h)
		pbRange.High = kv.Key(pbRange.Low).PrefixNext()
		selTableReq.Ranges = append(selTableReq.Ranges, pbRange)
	}
	selTableReq.Where = e.where
	return xapi.Select(txn.GetClient(), selTableReq, 10)
}

// conditionsToPBExpr tries to convert filter conditions to a tipb.Expr.
// not supported conditions will be returned in remained.
func conditionsToPBExpr(client kv.Client, exprs []ast.ExprNode, tn *ast.TableName) (pbexpr *tipb.Expr,
	remained []ast.ExprNode) {
	for _, expr := range exprs {
		v := exprToPBExpr(client, expr, tn)
		if v == nil {
			remained = append(remained, expr)
		} else {
			if pbexpr == nil {
				pbexpr = v
			} else {
				// merge multiple converted pb expression into an AND expression.
				pbexpr = &tipb.Expr{Tp: tipb.ExprType_And.Enum(), Children: []*tipb.Expr{pbexpr, v}}
			}
		}
	}
	return
}

func resultFieldsToPBExpression(fields []*ast.ResultField) []*tipb.Expr {
	return nil
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

func extractHandlesFromIndexResult(idxResult *xapi.SelectResult) ([]int64, error) {
	var handles []int64
	for {
		subResult, err := idxResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if subResult == nil {
			break
		}
		subHandles, err := extractHandlesFromIndexSubResult(subResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, subHandles...)
	}
	return handles, nil
}

func extractHandlesFromIndexSubResult(subResult *xapi.SubResult) ([]int64, error) {
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

func (e *XSelectIndexExec) extractRowsFromTableResult(t table.Table, tblResult *xapi.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		subResult, err := tblResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if subResult == nil {
			break
		}
		subRows, err := e.extractRowsFromSubResult(t, subResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, subRows...)
	}
	return rows, nil
}

func (e *XSelectIndexExec) extractRowsFromSubResult(t table.Table, subResult *xapi.SubResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			break
		}
		fullRowData := make([]types.Datum, len(e.indexPlan.Fields()))
		var j int
		for i, field := range e.indexPlan.Fields() {
			if field.Referenced {
				fullRowData[i] = rowData[j]
				field.Expr.SetDatum(fullRowData[i])
				j++
			}
		}
		row := resultRowToRow(t, h, fullRowData)
		rows = append(rows, row)
	}
	return rows, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// exprToPBExpr converts an ast.ExprNode to a tipb.Expr, if not supported, nil will be returned.
func exprToPBExpr(client kv.Client, expr ast.ExprNode, tn *ast.TableName) *tipb.Expr {
	switch x := expr.(type) {
	case *ast.ValueExpr, *ast.ParamMarkerExpr:
		return datumToPBExpr(client, *expr.GetDatum())
	case *ast.ColumnNameExpr:
		return columnNameToPBExpr(client, x, tn)
	case *ast.BinaryOperationExpr:
		return binopToPBExpr(client, x, tn)
	case *ast.ParenthesesExpr:
		return exprToPBExpr(client, x.Expr, tn)
	default:
		return nil
	}
}

func columnNameToPBExpr(client kv.Client, column *ast.ColumnNameExpr, tn *ast.TableName) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	// Zero Column ID is not a column from table, can not support for now.
	if column.Refer.Column.ID == 0 {
		return nil
	}
	switch column.Refer.Expr.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeGeometry,
		mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp, mysql.TypeYear:
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
		pbExpr.Tp = tipb.ExprType_ColumnRef.Enum()
		pbExpr.Val = codec.EncodeInt(nil, column.Refer.Column.ID)
		return pbExpr
	}
	// If the column ID is in fields, it means the column is from an outer table,
	// its value is available to use.
	return datumToPBExpr(client, *column.Refer.Expr.GetDatum())
}

func datumToPBExpr(client kv.Client, d types.Datum) *tipb.Expr {
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
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp.Enum(), Val: val}
}

func binopToPBExpr(client kv.Client, expr *ast.BinaryOperationExpr, tn *ast.TableName) *tipb.Expr {
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
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	leftExpr := exprToPBExpr(client, expr.L, tn)
	if leftExpr == nil {
		return nil
	}
	rightExpr := exprToPBExpr(client, expr.R, tn)
	if rightExpr == nil {
		return nil
	}
	return &tipb.Expr{Tp: tp.Enum(), Children: []*tipb.Expr{leftExpr, rightExpr}}
}
