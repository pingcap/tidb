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
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tipb"
)

// XSelectTableExec represents XAPI select executor.
type XSelectTableExec struct {
	table     table.Table
	tablePlan *plan.TableScan
	where     *tipb.Expression
	ctx       context.Context
	result    *xapi.SelectResult
}

// Next implements Executor Next interface.
func (e *XSelectTableExec) Next() (*Row, error) {
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	h, rowData, err := e.result.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rowData == nil {
		return nil, nil
	}
	for i, field := range e.tablePlan.Fields() {
		field.Expr.SetValue(rowData[i].GetValue())
	}
	return resultRowToRow(e.table, h, rowData), nil
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
	selReq.Ranges, selReq.Points = tableRangeToPBRangesAndPoints(e.tablePlan.Ranges)
	selReq.TableInfo = xapi.TableToProto(e.tablePlan.Table)
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
	where     *tipb.Expression
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
	selIdxReq := new(tipb.IndexRequest)
	startTs := txn.StartTS()
	selIdxReq.StartTs = &startTs
	selIdxReq.IndexInfo = xapi.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	selIdxReq.Ranges, selIdxReq.Points = indexRangesToPBRangesAndPoints(e.indexPlan.Ranges)
	return xapi.SelectIndex(txn.GetClient(), selIdxReq, 1)
}

func (e *XSelectIndexExec) doTableRequest(txn kv.Transaction, handles []int64) (*xapi.SelectResult, error) {
	selTableReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selTableReq.StartTs = &startTs
	selTableReq.TableInfo = xapi.TableToProto(e.indexPlan.Table)
	selTableReq.Fields = resultFieldsToPBExpression(e.indexPlan.Fields())
	selTableReq.Points = handles
	selTableReq.Where = conditionsToPBExpression(e.indexPlan.FilterConditions...)
	return xapi.Select(txn.GetClient(), selTableReq, 10)
}

func conditionsToPBExpression(expr ...ast.ExprNode) *tipb.Expression {
	return nil
}

func resultFieldsToPBExpression(fields []*ast.ResultField) []*tipb.Expression {
	return nil
}

func tableRangeToPBRangesAndPoints(tableRanges []plan.TableRange) ([]*tipb.HandleRange, []int64) {
	hrs := make([]*tipb.HandleRange, 0, len(tableRanges))
	var handlePoints []int64
	for _, ran := range tableRanges {
		if ran.LowVal == ran.HighVal {
			handlePoints = append(handlePoints, ran.LowVal)
		} else {
			hr := new(tipb.HandleRange)
			lowVal := ran.LowVal
			hr.Low = &lowVal
			hiVal := ran.HighVal
			if hiVal != math.MaxInt64 {
				hiVal++
			}
			hr.High = &hiVal
			hrs = append(hrs, hr)
		}
	}
	return hrs, handlePoints
}

func indexRangesToPBRangesAndPoints(ranges []*plan.IndexRange) ([]*tipb.KeyRange, [][]byte) {
	return nil, nil
}

func extractHandlesFromIndexResult(idxResult *xapi.SelectResult) ([]int64, error) {
	return nil, nil
}

func extractRowsFromTableResult(t table.Table, tblResult *xapi.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := tblResult.Next()
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
