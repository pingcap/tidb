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
	indexOrder := make(map[int64]int)
	for i, h := range handles {
		indexOrder[h] = i
	}
	sort.Sort(int64Slice(handles))
	tblResult, err := e.doTableRequest(txn, handles)
	unorderedRows, err := extractRowsFromTableResult(e.table, tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	// Restore the original index order.
	rows := make([]*Row, len(handles))
	for i, h := range handles {
		oi := indexOrder[h]
		rows[oi] = unorderedRows[i]
	}
	e.rows = rows
	return nil
}

func (e *XSelectIndexExec) doIndexRequest(txn kv.Transaction) (*xapi.SelectResult, error) {
	selIdxReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selIdxReq.StartTs = &startTs
	selIdxReq.IndexInfo = tablecodec.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	var err error
	selIdxReq.Ranges, err = indexRangesToPBRanges(e.indexPlan.Ranges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return xapi.Select(txn.GetClient(), selIdxReq, 1)
}

func (e *XSelectIndexExec) doTableRequest(txn kv.Transaction, handles []int64) (*xapi.SelectResult, error) {
	selTableReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selTableReq.StartTs = &startTs
	selTableReq.TableInfo = tablecodec.TableToProto(e.indexPlan.Table)
	selTableReq.Fields = resultFieldsToPBExpression(e.indexPlan.Fields())
	for _, h := range handles {
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, h)
		if h != math.MaxInt64 {
			h++
		}
		pbRange.High = codec.EncodeInt(nil, h)
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

func indexRangesToPBRanges(ranges []*plan.IndexRange) ([]*tipb.KeyRange, error) {
	keyRanges := make([]*tipb.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		low, err := codec.EncodeValue(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PartialNext())
		}
		high, err := codec.EncodeValue(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PartialNext())
		}
		keyRanges = append(keyRanges, &tipb.KeyRange{Low: low, High: high})
	}
	return keyRanges, nil
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
