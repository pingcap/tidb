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
	"context"
	hash2 "hash"
	"hash/fnv"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/bloom"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ executor = &tableScanExec{}
	_ executor = &indexScanExec{}
	_ executor = &selectionExec{}
	_ executor = &limitExec{}
	_ executor = &topNExec{}
)

type execDetail struct {
	timeProcessed   time.Duration
	numProducedRows int
	numIterations   int
}

func (e *execDetail) update(begin time.Time, row [][]byte) {
	e.timeProcessed += time.Since(begin)
	e.numIterations++
	if row != nil {
		e.numProducedRows++
	}
}

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	ResetCounts()
	Counts() []int64
	Next(ctx context.Context) ([][]byte, error)
	// Cursor returns the key gonna to be scanned by the Next() function.
	Cursor() (key []byte, desc bool)
	// ExecDetails returns its and its children's execution details.
	// The order is same as DAGRequest.Executors, which children are in front of parents.
	ExecDetails() []*execDetail
}

type tableScanExec struct {
	*tipb.TableScan
	colIDs         map[int64]int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	start          int
	counts         []int64
	execDetail     *execDetail
	rd             *rowcodec.BytesDecoder

	src executor
}

func (e *tableScanExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *tableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *tableScanExec) GetSrcExec() executor {
	return e.src
}

func (e *tableScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *tableScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *tableScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}

	if e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			return ran.StartKey, e.Desc
		}

		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}

	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *tableScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.update(begin, value)
	}(time.Now())
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
			return value, nil
		}
		value, err = e.getRowFromRange(ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			e.seekKey = nil
			e.cursor++
			continue
		}
		if e.counts != nil {
			e.counts[e.cursor]++
		}
		return value, nil
	}

	return nil, nil
}

func (e *tableScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel, e.resolvedLocks)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	handle, err := tablecodec.DecodeRowKey(ran.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, val, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

func (e *tableScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
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
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = tablecodec.TruncateToRowKeyLen(pair.Key)
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = kv.Key(pair.Key).PrefixNext()
	}

	handle, err := tablecodec.DecodeRowKey(pair.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := getRowData(e.Columns, e.colIDs, handle, pair.Value, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

type indexScanExec struct {
	*tipb.IndexScan
	colsLen        int
	kvRanges       []kv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	hdStatus       tablecodec.HandleStatus
	start          int
	counts         []int64
	execDetail     *execDetail
	colInfos       []rowcodec.ColInfo

	src executor
}

func (e *indexScanExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) GetSrcExec() executor {
	return e.src
}

func (e *indexScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *indexScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *indexScanExec) isUnique() bool {
	return e.Unique != nil && *e.Unique
}

func (e *indexScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}
	if e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			return ran.StartKey, e.Desc
		}
		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}
	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *indexScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.update(begin, value)
	}(time.Now())
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
		} else {
			value, err = e.getRowFromRange(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if value == nil {
				e.cursor++
				e.seekKey = nil
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor]++
			}
		}
		return value, nil
	}

	return nil, nil
}

// getRowFromPoint is only used for unique key.
func (e *indexScanExec) getRowFromPoint(ran kv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel, e.resolvedLocks)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	return tablecodec.DecodeIndexKV(ran.StartKey, val, e.colsLen, e.hdStatus, e.colInfos)
}

func (e *indexScanExec) getRowFromRange(ran kv.KeyRange) ([][]byte, error) {
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
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle lock error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = pair.Key
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = []byte(kv.Key(pair.Key).PrefixNext())
	}
	return tablecodec.DecodeIndexKV(pair.Key, pair.Value, e.colsLen, e.hdStatus, e.colInfos)
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	evalCtx           *evalContext
	src               executor
	execDetail        *execDetail

	bloomFilter []*bloom.Filter
	joinKeyIdx  [][]int64
}

func (e *selectionExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	return e.src
}

func (e *selectionExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *selectionExec) Counts() []int64 {
	return e.src.Counts()
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, row []types.Datum, ctx *stmtctx.StatementContext) (bool, error) {
	for _, expr := range exprs {
		data, err := expr.Eval(chunk.MutRowFromDatums(row).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		isBool, err = expression.HandleOverflowOnSelection(ctx, isBool, err)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *selectionExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.update(begin, value)
	}(time.Now())
	for {
		value, err = e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			return nil, nil
		}

		err = e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := evalBool(e.conditions, e.row, e.evalCtx.sc)

		if e.bloomFilter != nil {
			for i := range e.bloomFilter {
				ok, err := evalBoolForBloom(e.evalCtx.sc, e.bloomFilter[i], e.joinKeyIdx[i], e.row)
				if err != nil {
					return nil, err
				}
				match = match && ok
			}
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return value, nil
		}
	}
}

func evalBoolForBloom(ctx *stmtctx.StatementContext, bloom *bloom.Filter, joinKeyIdx []int64, row []types.Datum) (bool, error) {
	hash := make([]hash2.Hash64, 0, 1)
	hash = append(hash, fnv.New64())
	b := make([]byte, 1)
	isnull := make([]bool, 100)
	r := chunk.MutRowFromDatums(row).ToRow()
	for _, idx := range joinKeyIdx {
		var tp *types.FieldType
		if row[idx].Kind() == types.KindInt64 {
			tp = types.NewFieldType(mysql.TypeLonglong)
		} else {
			return true, nil
		}
		err := codec.HashChunkSelected(ctx, hash, r.Chunk(), tp, int(idx), b, isnull, nil)
		if err != nil {
			return false, err
		}
	}

	if bloom.ProbeU64(hash[0].Sum64()) {
		return true, nil
	}
	return false, nil
}

type topNExec struct {
	heap              *topNHeap
	evalCtx           *evalContext
	relatedColOffsets []int
	orderByExprs      []expression.Expression
	row               []types.Datum
	cursor            int
	executed          bool
	execDetail        *execDetail

	src executor
}

func (e *topNExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *topNExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *topNExec) GetSrcExec() executor {
	return e.src
}

func (e *topNExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *topNExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *topNExec) innerNext(ctx context.Context) (bool, error) {
	value, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = e.evalTopN(value)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *topNExec) Cursor() ([]byte, bool) {
	panic("don't not use coprocessor streaming API for topN!")
}

func (e *topNExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.update(begin, value)
	}(time.Now())
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				sort.Sort(&e.heap.topNSorter)
				break
			}
		}
		e.executed = true
	}
	if e.cursor >= len(e.heap.rows) {
		return nil, nil
	}
	row := e.heap.rows[e.cursor]
	e.cursor++

	return row.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(value [][]byte) error {
	newRow := &sortRow{
		key: make([]types.Datum, len(value)),
	}
	err := e.evalCtx.decodeRelatedColumnVals(e.relatedColOffsets, value, e.row)
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range e.orderByExprs {
		newRow.key[i], err = expr.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.heap.tryToAddRow(newRow) {
		newRow.data = append(newRow.data, value...)
	}
	return errors.Trace(e.heap.err)
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor

	execDetail *execDetail
}

func (e *limitExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	return e.src
}

func (e *limitExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *limitExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *limitExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *limitExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.update(begin, value)
	}(time.Now())
	if e.cursor >= e.limit {
		return nil, nil
	}

	value, err = e.src.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}
	e.cursor++
	return value, nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

// getRowData decodes raw byte slice to row data.
func getRowData(columns []*tipb.ColumnInfo, colIDs map[int64]int, handle int64, value []byte, rd *rowcodec.BytesDecoder) ([][]byte, error) {
	if rowcodec.IsNewFormat(value) {
		return rd.DecodeToBytes(colIDs, handle, value, nil)
	}
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
		if col.GetPkHandle() || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(uint(col.GetFlag())) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, nil, handleDatum)
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

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}
