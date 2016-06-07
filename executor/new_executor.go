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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tablecodec"
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

// Close implements Executor Close interface.
func (e *HashJoinExec) Close() error {
	e.hashTable = nil
	e.matchedRows = nil
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
	finish            bool
	AggFuncs          []expression.AggregationFunction
	groupMap          map[string]bool
	groups            [][]byte
	currentGroupIndex int
	GroupByItems      []expression.Expression
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

// Close implements Executor Close interface.
func (e *AggregationExec) Close() error {
	for _, af := range e.AggFuncs {
		af.Clear()
	}
	if e.Src != nil {
		return e.Src.Close()
	}
	return nil
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
	result      *xapi.SelectResult
	subResult   *xapi.SubResult
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
	selReq.Ranges = tableRangesToPBRanges(e.ranges)
	columns := e.Columns
	selReq.TableInfo = &tipb.TableInfo{
		TableId: proto.Int64(e.tableInfo.ID),
	}
	selReq.TableInfo.Columns = tablecodec.ColumnsToProto(columns, e.tableInfo.PKIsHandle)
	e.result, err = xapi.Select(txn.GetClient(), selReq, defaultConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
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

// Close implements Executor interface.
func (e *NewTableScanExec) Close() error {
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

// Close implements Executor Close interface.
func (e *NewSortExec) Close() error {
	return e.Src.Close()
}
