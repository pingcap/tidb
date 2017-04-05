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
	"github.com/pingcap/tidb/sessionctx/variable"
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

func (e *tableScanExec) Next() (int64, [][]byte, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			handle, value, err := e.getRowFromPoint(ran)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			e.seekKey = nil
			e.cursor++
			return handle, value, nil
		}

		handle, value, err := e.getRowFromRange(ran)
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

func (e *indexScanExec) Next() (int64, [][]byte, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		handle, value, err := e.getRowFromRange(ran)
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
	sc     *variable.StatementContext
	eval   *xeval.Evaluator
	colIDs map[int64]int

	src executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) Next() (int64, [][]byte, error) {
	for {
		handle, row, err := e.src.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			return 0, nil, nil
		}

		err = e.eval.SetRowValue(handle, row, e.colIDs)
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
		boolResult, err := result.ToBool(e.sc)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if boolResult == 1 {
			return handle, row, nil
		}
	}
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
