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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type executor interface {
	SetSrcExec(executor)
	Next() (int64, [][]byte, error)
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs      map[int64]int
	kvRanges    []kv.KeyRange
	startTS     uint64
	mvccStore   *MvccStore
	cursor      int
	seekKey     []byte
	rawStartKey []byte // The start key of the current region.
	rawEndKey   []byte // The end key of the current region.

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
		startKey := maxStartKey(ran.StartKey, e.rawStartKey)
		endKey := minEndKey(ran.EndKey, e.rawEndKey)
		if bytes.Compare(ran.StartKey, ran.EndKey) >= 0 {
			return 0, nil, nil
		}
		if *e.Desc {
			e.seekKey = endKey
		} else {
			e.seekKey = startKey
		}
	}
	var pairs []Pair
	var pair Pair
	if *e.Desc {
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
	if *e.Desc {
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
	colsLen     int
	kvRanges    []kv.KeyRange
	startTS     uint64
	mvccStore   *MvccStore
	cursor      int
	seekKey     []byte
	rawStartKey []byte // The start key of the current region.
	rawEndKey   []byte // The end key of the current region.

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
		startKey := maxStartKey(ran.StartKey, e.rawStartKey)
		endKey := minEndKey(ran.EndKey, e.rawEndKey)
		if bytes.Compare(ran.StartKey, ran.EndKey) >= 0 {
			return 0, nil, nil
		}
		if *e.Desc {
			e.seekKey = endKey
		} else {
			e.seekKey = startKey
		}
	}
	var pairs []Pair
	var pair Pair
	if *e.Desc {
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
	if *e.Desc {
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
	} else {
		handle, err = decodeHandle(pair.Value)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	return handle, values, nil
}

type selectionExec struct {
	*tipb.Selection
	eval   *xeval.Evaluator
	colIDs map[int64]int

	src executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
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

		err = e.eval.SetRowValue(handle, value, e.colIDs)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		// TODO: Now the conditions length is 1.
		result, err := e.eval.Eval(e.Conditions[0])
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if result.IsNull() {
			continue
		}
		boolResult, err := result.ToBool(e.eval.StatementCtx)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if boolResult == 1 {
			return handle, value, nil
		}
	}
}

type aggregateExec struct {
	*tipb.Aggregation
	eval         *xeval.Evaluator
	aggFuncs     []*aggregateFuncExpr
	groups       map[string]struct{}
	groupKeys    [][]byte
	columns      map[int64]*tipb.ColumnInfo
	executed     bool
	currGroupIdx int
	colIDs       map[int64]int

	src executor
}

func (e *aggregateExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *aggregateExec) innerNext() (bool, error) {
	handle, values, err := e.src.Next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if values == nil {
		return false, nil
	}
	err = e.aggregate(handle, values)
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
	value = make([][]byte, 0, 1+2*len(e.aggFuncs))
	// The first column is group key.
	value = append(value, gkData)
	for _, agg := range e.aggFuncs {
		agg.currentGroup = gk
		ds, err := agg.toDatums(e.eval)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		data, err := codec.EncodeValue(nil, ds...)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		value = append(value, data)
	}
	e.currGroupIdx++

	return 0, value, nil
}

// aggregate updates aggregate functions with row.
func (e *aggregateExec) aggregate(handle int64, row [][]byte) error {
	// Put row data into evaluate for later evaluation.
	err := e.eval.SetRowValue(handle, row, e.colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	// Get group key.
	gk, err := getGroupKey(e.eval, e.GetGroupBy())
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
	}
	// Update aggregate funcs.
	for _, agg := range e.aggFuncs {
		agg.currentGroup = gk
		args := make([]types.Datum, 0, len(agg.expr.Children))
		// Evaluate arguments.
		for _, x := range agg.expr.Children {
			cv, err := e.eval.Eval(x)
			if err != nil {
				return errors.Trace(err)
			}
			args = append(args, cv)
		}
		agg.update(e.eval, args)
	}
	return nil
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

func extractColIDsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector map[int64]int) error {
	if expr == nil {
		return nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, i, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return errors.Trace(err)
		}
		for idx, c := range columns {
			if c.GetColumnId() == i {
				collector[i] = idx
				return nil
			}
		}
		return xeval.ErrInvalid.Gen("column %d not found", i)
	}
	for _, child := range expr.Children {
		err := extractColIDsInExpr(child, columns, collector)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
