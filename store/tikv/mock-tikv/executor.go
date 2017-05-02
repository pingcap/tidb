// Copyright 2017 PingCAP, Inc.
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

package mocktikv

import (
	"bytes"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

// Error instances.
var (
	errInvalid = terror.ClassMockTikv.New(codeInvalid, "invalid operation")
)

// Error codes.
const (
	codeInvalid = 1
)

type executor interface {
	SetSrcExec(executor)
	Next() (int64, [][]byte, error)
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs    map[int64]int
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore *MvccStore
	cursor    int
	seekKey   []byte

	src executor
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *tableScanExec) Next() (handle int64, value [][]byte, err error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			handle, value, err = e.getRowFromPoint(ran)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			e.seekKey = nil
			e.cursor++
			return handle, value, nil
		}

		handle, value, err = e.getRowFromRange(ran)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if value == nil {
			e.seekKey = nil
			e.cursor++
			continue
		}
		return handle, value, nil
	}

	return 0, nil, nil
}

func (e *tableScanExec) getRowFromPoint(ran kv.KeyRange) (int64, [][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS)
	if len(val) == 0 {
		return 0, nil, nil
	} else if err != nil {
		return 0, nil, errors.Trace(err)
	}
	handle, err := tablecodec.DecodeRowKey(kv.Key(ran.StartKey))
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, val)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) (int64, [][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return 0, nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return 0, nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return 0, nil, nil
		}
		e.seekKey = []byte(tablecodec.TruncateToRowKeyLen(kv.Key(pair.Key)))
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return 0, nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}

	handle, err := tablecodec.DecodeRowKey(pair.Key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, pair.Value)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

type indexScanExec struct {
	*tipb.IndexScan
	colsLen   int
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore *MvccStore
	cursor    int
	seekKey   []byte
	pkCol     *tipb.ColumnInfo

	src executor
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) Next() (handle int64, value [][]byte, err error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		handle, value, err = e.getRowFromRange(ran)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if value == nil {
			e.cursor++
			e.seekKey = nil
			continue
		}
		return handle, value, nil
	}

	return 0, nil, nil
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) (int64, [][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return 0, nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return 0, nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return 0, nil, nil
		}
		e.seekKey = pair.Key
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return 0, nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}

	values, b, err := tablecodec.CutIndexKeyNew(pair.Key, e.colsLen)
	var handle int64
	if len(b) > 0 {
		var handleDatum types.Datum
		_, handleDatum, err = codec.DecodeOne(b)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		handle = handleDatum.GetInt64()
		if e.pkCol != nil {
			values = append(values, b)
		}
	} else {
		handle, err = decodeHandle(pair.Value)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if e.pkCol != nil {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(e.pkCol.GetFlag())) {
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleBytes, err := codec.EncodeValue(b, handleDatum)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			values = append(values, handleBytes)
		}
	}

	return handle, values, nil
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	evalCtx           *evalContext

	src executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, row []types.Datum, ctx *variable.StatementContext) (bool, error) {
	for _, expr := range exprs {
		data, err := expr.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) Next() (handle int64, value [][]byte, err error) {
	for {
		handle, value, err = e.src.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if value == nil {
			return 0, nil, nil
		}

		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		match, err := evalBool(e.conditions, e.row, e.evalCtx.sc)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if match {
			return handle, value, nil
		}
	}
}

type aggregateExec struct {
	evalCtx           *evalContext
	aggExprs          []expression.AggregationFunction
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	groups            map[string]struct{}
	groupKeys         [][]byte
	executed          bool
	currGroupIdx      int

	src executor
}

func (e *aggregateExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *aggregateExec) innerNext() (bool, error) {
	_, values, err := e.src.Next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if values == nil {
		return false, nil
	}
	err = e.aggregate(values)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *aggregateExec) Next() (handle int64, value [][]byte, err error) {
	if !e.executed {
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}

	if e.currGroupIdx >= len(e.groups) {
		return 0, nil, nil
	}
	gk := e.groupKeys[e.currGroupIdx]
	gkData, err := codec.EncodeValue(nil, types.NewBytesDatum(gk))
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	value = make([][]byte, 0, 1+2*len(e.aggExprs))
	// The first column is group key.
	value = append(value, gkData)
	for _, agg := range e.aggExprs {
		ds := agg.GetPartialResult(gk)
		data, err := codec.EncodeValue(nil, ds...)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		value = append(value, data)
	}
	e.currGroupIdx++

	return 0, value, nil
}

func (e *aggregateExec) getGroupKey() ([]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return []byte{}, nil
	}
	vals := make([]types.Datum, 0, len(e.groupByExprs))
	for _, item := range e.groupByExprs {
		v, err := item.Eval(e.row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	buf, err := codec.EncodeValue(nil, vals...)
	return buf, errors.Trace(err)
}

// aggregate updates aggregate functions with row.
func (e *aggregateExec) aggregate(value [][]byte) error {
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	// Get group key.
	gk, err := e.getGroupKey()
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
	}
	// Update aggregate expressions.
	for _, agg := range e.aggExprs {
		agg.Update(e.row, gk, e.evalCtx.sc)
	}
	return nil
}

type topNExec struct {
	heap              *topnHeap
	evalCtx           *evalContext
	relatedColOffsets []int
	orderByExprs      []expression.Expression
	row               []types.Datum
	cursor            int
	executed          bool

	src executor
}

func (e *topNExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *topNExec) innerNext() (bool, error) {
	handle, value, err := e.src.Next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = e.evalTopN(handle, value)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *topNExec) Next() (handle int64, value [][]byte, err error) {
	if !e.executed {
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}
	if e.cursor >= len(e.heap.rows) {
		return 0, nil, nil
	}
	sort.Sort(&e.heap.topnSorter)
	row := e.heap.rows[e.cursor]
	e.cursor++

	return row.meta.Handle, row.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(handle int64, value [][]byte) error {
	newRow := &sortRow{
		meta: tipb.RowMeta{Handle: handle},
		key:  make([]types.Datum, len(value)),
	}
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range e.orderByExprs {
		newRow.key[i], err = expr.Eval(e.row)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.heap.tryToAddRow(newRow) {
		for _, val := range value {
			newRow.data = append(newRow.data, val)
			newRow.meta.Length += int64(len(val))
		}
	}
	return errors.Trace(e.heap.err)
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) Next() (handle int64, value [][]byte, err error) {
	if e.cursor >= e.limit {
		return 0, nil, nil
	}

	handle, value, err = e.src.Next()
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if value == nil {
		return 0, nil, nil
	}
	e.cursor++
	return handle, value, nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

// getRowData decodes raw byte slice to row data.
func getRowData(columns []*tipb.ColumnInfo, colIDs map[int64]int, handle int64, value []byte) ([][]byte, error) {
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.GetColumnId()
		offset := colIDs[id]
		if col.GetPkHandle() {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasColVal(values, colIDs, id) {
			continue
		}
		if len(col.DefaultVal) > 0 {
			values[offset] = col.DefaultVal
			continue
		}
		if mysql.HasNotNullFlag(uint(col.GetFlag())) {
			return nil, errors.Errorf("Miss column %d", id)
		}

		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
}

func isDuplicated(offsets []int, offset int) bool {
	for _, idx := range offsets {
		if idx == offset {
			return true
		}
	}
	return false
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, i, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for idx, c := range columns {
			if c.GetColumnId() != i {
				continue
			}
			if !isDuplicated(collector, idx) {
				collector = append(collector, idx)
			}
			return collector, nil
		}
		return nil, errInvalid.Gen("column %d not found", i)
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}

func convertToExprs(sc *variable.StatementContext, colIDs map[int64]int, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, colIDs, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}
