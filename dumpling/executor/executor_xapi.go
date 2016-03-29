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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
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
		for i, field := range e.tablePlan.Fields() {
			field.Expr.SetDatum(rowData[i])
		}
		return resultRowToRow(e.table, h, rowData), nil
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
	selReq.Where = conditionsToPBExpression(e.tablePlan.FilterConditions...)
	selReq.Ranges = tableRangesToPBRanges(e.tablePlan.Ranges)
	selReq.TableInfo = tablecodec.TableToProto(e.tablePlan.Table)
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
	rows, err := extractRowsFromTableResult(e.table, tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) < len(handles) {
		return errors.Errorf("got %d rows with %d handles", len(rows), len(handles))
	}
	if !e.indexPlan.OutOfOrder {
		// Restore the index order.
		orderedRows := make([]*Row, len(handles))
		for i, h := range handles {
			oi := indexOrder[h]
			orderedRows[oi] = rows[i]
		}
		rows = orderedRows
	}
	e.rows = rows
	return nil
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
	selTableReq.TableInfo = tablecodec.TableToProto(e.indexPlan.Table)
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
	selTableReq.Where = conditionsToPBExpression(e.indexPlan.FilterConditions...)
	return xapi.Select(txn.GetClient(), selTableReq, 10)
}

func conditionsToPBExpression(expr ...ast.ExprNode) *tipb.Expr {
	return nil
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

func extractRowsFromTableResult(t table.Table, tblResult *xapi.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		subResult, err := tblResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if subResult == nil {
			break
		}
		subRows, err := extractRowsFromSubResult(t, subResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, subRows...)
	}
	return rows, nil
}

func extractRowsFromSubResult(t table.Table, subResult *xapi.SubResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			break
		}
		row := resultRowToRow(t, h, rowData)
		rows = append(rows, row)
	}
	return rows, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
