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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

type executor interface {
	SetSrcExec(executor)
	Next() (int64, map[int64][]byte, error)
}

type tableScanExec struct {
	*tipb.TableScan
	colTps    map[int64]*types.FieldType
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore *MvccStore
	cursor    int
	newRange  bool
	seekKey   []byte

	src executor
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *tableScanExec) Close() error {
	e.cursor = 0
	return nil
}

func (e *tableScanExec) Next() (int64, map[int64][]byte, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if bytes.Compare(ran.StartKey, ran.EndKey) >= 0 {
			e.cursor++
			continue
		}
		if ran.IsPoint() {
			handle, value, err := e.getRowFromPoint(ran)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			e.cursor++
			return handle, value, nil
		}

		handle, value, err := e.getRowFromRange(ran)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if e.newRange {
			e.cursor++
			continue
		}
		return handle, value, nil
	}

	return 0, nil, nil
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
	row, err := HandleRowData(e.Columns, e.colTps, handle, val)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) (int64, map[int64][]byte, error) {
	if e.newRange {
		e.newRange = false
		if *e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
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
		e.newRange = true
		return 0, nil, nil
	}
	if *e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			e.newRange = true
			return 0, nil, nil
		}
		e.seekKey = []byte(tablecodec.TruncateToRowKeyLen(kv.Key(pair.Key)))
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			e.newRange = true
			return 0, nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}

	handle, err := tablecodec.DecodeRowKey(pair.Key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	row, err := HandleRowData(e.Columns, e.colTps, handle, pair.Value)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return handle, row, nil
}

type indexScanExec struct {
	*tipb.IndexScan
	ids       []int64
	kvRanges  []kv.KeyRange
	startTS   uint64
	mvccStore *MvccStore
	cursor    int
	newRange  bool
	seekKey   []byte

	src executor
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) Next() (int64, map[int64][]byte, error) {
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if bytes.Compare(ran.StartKey, ran.EndKey) >= 0 {
			e.cursor++
			continue
		}
		handle, value, err := e.getRowFromRange(ran)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if e.newRange {
			e.cursor++
			continue
		}
		return handle, value, nil
	}

	return 0, nil, nil
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) (int64, map[int64][]byte, error) {
	if e.newRange {
		e.newRange = false
		if *e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
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
		e.newRange = true
		return 0, nil, nil
	}
	if *e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			e.newRange = true
			return 0, nil, nil
		}
		e.seekKey = pair.Key
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			e.newRange = true
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
	sc      *variable.StatementContext
	eval    *xeval.Evaluator
	columns map[int64]*tipb.ColumnInfo

	src executor
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) Next() (int64, map[int64][]byte, error) {
	for {
		handle, row, err := e.src.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			return 0, nil, nil
		}

		err = SetColumnValueToEval(e.eval, handle, row, e.columns)
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

// HandleRowData deals with raw row data:
//	1. Decodes row from raw byte slice.
func HandleRowData(columns []*tipb.ColumnInfo, colTps map[int64]*types.FieldType, handle int64, value []byte) (map[int64][]byte, error) {
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

// SetColumnValueToEval puts column values into evaluator, the values will be used for expr evaluation.
func SetColumnValueToEval(eval *xeval.Evaluator, handle int64, row map[int64][]byte, cols map[int64]*tipb.ColumnInfo) error {
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
