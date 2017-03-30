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
	"github.com/pingcap/tidb/distsql"
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
	Next() (int64, rowData, error)
	BuildData(columns []*tipb.ColumnInfo, row rowData) []byte
}

type rowData struct {
	rawData map[int64][]byte
	data    []byte
}

func newRowData(raw map[int64][]byte, data []byte) rowData {
	return rowData{
		rawData: raw,
		data:    data,
	}
}

func (r rowData) isNull() bool {
	if r.rawData == nil && r.data == nil {
		return true
	}
	return false
}

type tableScanExec struct {
	*tipb.TableScan
	colTps      map[int64]*types.FieldType
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

func (e *tableScanExec) BuildData(columns []*tipb.ColumnInfo, row rowData) []byte {
	data := dummySlice
	for _, col := range columns {
		data = append(data, row.rawData[col.GetColumnId()]...)
	}
	return data
}

func (e *tableScanExec) Next() (int64, rowData, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			handle, value, err := e.getRowFromPoint(ran)
			if err != nil {
				return 0, rowData{}, errors.Trace(err)
			}
			e.seekKey = nil
			e.cursor++
			return handle, newRowData(value, nil), nil
		}

		handle, value, err := e.getRowFromRange(ran)
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		if value == nil {
			e.seekKey = nil
			e.cursor++
			continue
		}
		return handle, newRowData(value, nil), nil
	}

	return 0, rowData{}, nil
}

func (e *tableScanExec) getRowFromPoint(ran kv.KeyRange) (int64, map[int64][]byte, error) {
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
	row, err := handleRowData(e.Columns, e.colTps, handle, val)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) (int64, map[int64][]byte, error) {
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
	row, err := handleRowData(e.Columns, e.colTps, handle, pair.Value)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

type indexScanExec struct {
	*tipb.IndexScan
	ids         []int64
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

func (e *indexScanExec) BuildData(columns []*tipb.ColumnInfo, row rowData) []byte {
	data := dummySlice
	for _, col := range columns {
		data = append(data, row.rawData[col.GetColumnId()]...)
	}
	return data
}

func (e *indexScanExec) Next() (int64, rowData, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		handle, value, err := e.getRowFromRange(ran)
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		if value == nil {
			e.cursor++
			e.seekKey = nil
			continue
		}
		return handle, newRowData(value, nil), nil
	}

	return 0, rowData{}, nil
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) (int64, map[int64][]byte, error) {
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

	values, b, err := tablecodec.CutIndexKey(pair.Key, e.ids)
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
	eval    *xeval.Evaluator
	columns map[int64]*tipb.ColumnInfo

	src executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) BuildData(columns []*tipb.ColumnInfo, row rowData) []byte {
	data := dummySlice
	for _, col := range columns {
		data = append(data, row.rawData[col.GetColumnId()]...)
	}
	return data
}

func (e *selectionExec) Next() (int64, rowData, error) {
	for {
		handle, row, err := e.src.Next()
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		if row.isNull() {
			return 0, rowData{}, nil
		}

		err = setColumnValueToEval(e.eval, handle, row.rawData, e.columns)
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		// TODO: Now the conditions length is 1.
		result, err := e.eval.Eval(e.Conditions[0])
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		if result.IsNull() {
			continue
		}
		boolResult, err := result.ToBool(e.eval.StatementCtx)
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		if boolResult == 1 {
			return handle, newRowData(row.rawData, nil), nil
		}
	}
}

type aggContext struct {
	eval      *xeval.Evaluator
	aggFuncs  []*aggregateFuncExpr
	groups    map[string]struct{}
	groupKeys [][]byte
	columns   map[int64]*tipb.ColumnInfo
}

type aggregateExec struct {
	*tipb.Aggregation
	*aggContext
	executed     bool
	currGroupIdx int

	src executor
}

func (e *aggregateExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *aggregateExec) BuildData(columns []*tipb.ColumnInfo, row rowData) []byte {
	return row.data
}

func (e *aggregateExec) innerNext() (bool, error) {
	handle, values, err := e.src.Next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if values.isNull() {
		return false, nil
	}
	err = aggregate(e.aggContext, e.GetGroupBy(), handle, values.rawData)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *aggregateExec) Next() (int64, rowData, error) {
	if !e.executed {
		for {
			hasMore, err := e.innerNext()
			if err != nil {
				return 0, rowData{}, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}

	if e.currGroupIdx >= len(e.groups) {
		return 0, rowData{}, nil
	}
	gk := e.aggContext.groupKeys[e.currGroupIdx]
	data := make([]types.Datum, 0, 1+2*len(e.aggFuncs))
	// The first column is group key.
	data = append(data, types.NewBytesDatum(gk))
	for _, agg := range e.aggFuncs {
		agg.currentGroup = gk
		ds, err := agg.toDatums(e.aggContext)
		if err != nil {
			return 0, rowData{}, errors.Trace(err)
		}
		data = append(data, ds...)
	}
	row, err := codec.EncodeValue(nil, data...)
	if err != nil {
		return 0, rowData{}, errors.Trace(err)
	}
	e.currGroupIdx++

	return 0, newRowData(nil, row), nil
}

// handleRowData deals with raw row data:
//	1. Decodes row from raw byte slice.
func handleRowData(columns []*tipb.ColumnInfo, colTps map[int64]*types.FieldType, handle int64, value []byte) (map[int64][]byte, error) {
	values, err := getRowData(value, colTps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Fill the handle and null columns.
	for _, col := range columns {
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
			values[col.GetColumnId()] = handleData
			continue
		}
		_, ok := values[col.GetColumnId()]
		if ok {
			continue
		}
		if len(col.DefaultVal) > 0 {
			values[col.GetColumnId()] = col.DefaultVal
			continue
		}
		if mysql.HasNotNullFlag(uint(col.GetFlag())) {
			return nil, errors.New("Miss column")
		}
		values[col.GetColumnId()] = []byte{codec.NilFlag}
	}

	return values, nil
}

// setColumnValueToEval puts column values into evaluator, the values will be used for expr evaluation.
func setColumnValueToEval(eval *xeval.Evaluator, handle int64, row map[int64][]byte, cols map[int64]*tipb.ColumnInfo) error {
	for colID, col := range cols {
		if col.GetPkHandle() {
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				eval.Row[colID] = types.NewUintDatum(uint64(handle))
			} else {
				eval.Row[colID] = types.NewIntDatum(handle)
			}
		} else {
			data := row[colID]
			ft := distsql.FieldTypeFromPBColumn(col)
			datum, err := tablecodec.DecodeColumnValue(data, ft)
			if err != nil {
				return errors.Trace(err)
			}
			eval.Row[colID] = datum
		}
	}
	return nil
}
