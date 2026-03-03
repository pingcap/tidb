// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"encoding/binary"
	"math"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	transaction "github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

type memReader interface {
	getMemRowsHandle() ([]kv.Handle, error)
}

var (
	_ memReader = &memIndexReader{}
	_ memReader = &memTableReader{}
	_ memReader = &memIndexLookUpReader{}
	_ memReader = &memIndexMergeReader{}
)

type memIndexReader struct {
	ctx            sessionctx.Context
	index          *model.IndexInfo
	table          *model.TableInfo
	kvRanges       []kv.KeyRange
	conditions     []expression.Expression
	addedRows      [][]types.Datum
	addedRowsLen   int
	retFieldTypes  []*types.FieldType
	outputOffset   []int
	cacheTable     kv.MemBuffer
	indexDatumCache *tables.CachedIndexDatumData // pre-decoded index datum cache
	keepOrder      bool
	physTblIDIdx   int
	partitionIDMap map[int64]struct{}
	compareExec

	buf         [16]byte
	decodeBuff  [][]byte
	resultRows  []types.Datum
	restoredDec *tablecodec.IndexRestoredDecoder // cached restored values decoder

	hdStatus tablecodec.HandleStatus // cached, computed once per scan
	loc      *time.Location          // cached from session vars
	evalCtx  expression.EvalContext  // cached from expr context
}

func buildMemIndexReader(ctx context.Context, us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
	defer tracing.StartRegion(ctx, "buildMemIndexReader").End()
	kvRanges := idxReader.kvRanges
	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	if us.desc {
		slices.Reverse(kvRanges)
	}
	var indexDatumCache *tables.CachedIndexDatumData
	if us.indexDatumCaches != nil {
		indexDatumCache = us.indexDatumCaches[idxReader.index.ID]
	}
	return &memIndexReader{
		ctx:             us.Ctx(),
		index:           idxReader.index,
		table:           idxReader.table.Meta(),
		kvRanges:        kvRanges,
		conditions:      us.conditions,
		retFieldTypes:   exec.RetTypes(us),
		outputOffset:    outputOffset,
		cacheTable:      us.cacheTable,
		indexDatumCache: indexDatumCache,
		keepOrder:       us.keepOrder,
		compareExec:     us.compareExec,
		physTblIDIdx:    us.physTblIDIdx,
		partitionIDMap:  us.partitionIDMap,
		resultRows:      make([]types.Datum, 0, len(outputOffset)),
	}
}

func (m *memIndexReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	if m.keepOrder && m.table.GetPartitionInfo() != nil {
		data, err := m.getMemRows(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &defaultRowsIter{data: data}, nil
	}

	kvIter, err := newTxnMemBufferIter(m.ctx, m.cacheTable, m.kvRanges, m.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tps := m.getTypes()
	colInfos := tables.BuildRowcodecColInfoForIndexColumns(m.index, m.table)
	colInfos = tables.TryAppendCommonHandleRowcodecColInfos(colInfos, m.table)
	if m.evalCtx == nil {
		m.evalCtx = m.ctx.GetExprCtx().GetEvalCtx()
	}
	return &memRowsIterForIndex{
		kvIter:         kvIter,
		tps:            tps,
		mutableRow:     chunk.MutRowFromTypes(m.retFieldTypes),
		memIndexReader: m,
		colInfos:       colInfos,
	}, nil
}

func (m *memIndexReader) getTypes() []*types.FieldType {
	tps := make([]*types.FieldType, 0, len(m.index.Columns)+1)
	cols := m.table.Columns
	for _, col := range m.index.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	switch {
	case m.table.PKIsHandle:
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				tps = append(tps, &(col.FieldType))
				break
			}
		}
	case m.table.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(m.table)
		for _, pkCol := range pkIdx.Columns {
			colInfo := m.table.Columns[pkCol.Offset]
			tps = append(tps, &colInfo.FieldType)
		}
	default: // ExtraHandle Column tp.
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	return tps
}

func (m *memIndexReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	defer tracing.StartRegion(ctx, "memIndexReader.getMemRows").End()
	tps := m.getTypes()
	colInfos := tables.BuildRowcodecColInfoForIndexColumns(m.index, m.table)
	colInfos = tables.TryAppendCommonHandleRowcodecColInfos(colInfos, m.table)

	if m.evalCtx == nil {
		m.evalCtx = m.ctx.GetExprCtx().GetEvalCtx()
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, value []byte) error {
		data, err := m.decodeIndexKeyValue(key, value, tps, colInfos)
		if err != nil {
			return err
		}

		if len(m.conditions) > 0 {
			mutableRow.SetDatums(data...)
			matched, _, err := expression.EvalBool(m.evalCtx, m.conditions, mutableRow.ToRow())
			if err != nil || !matched {
				return err
			}
		}
		m.addedRows = append(m.addedRows, data)
		m.resultRows = make([]types.Datum, 0, len(data))
		return nil
	})

	if err != nil {
		return nil, err
	}

	if m.keepOrder && m.table.GetPartitionInfo() != nil {
		slices.SortFunc(m.addedRows, func(a, b []types.Datum) int {
			ret, err1 := m.compare(m.ctx.GetSessionVars().StmtCtx, a, b)
			if err1 != nil {
				err = err1
			}
			return ret
		})
		return m.addedRows, err
	}
	return m.addedRows, nil
}

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType, colInfos []rowcodec.ColInfo) ([]types.Datum, error) {
	// Lazy init cached per-scan invariants.
	if m.loc == nil {
		m.loc = m.ctx.GetSessionVars().Location()
		m.hdStatus = tablecodec.HandleDefault
		// `HandleIsUnsigned` only affects IntHandle which always has one column.
		if mysql.HasUnsignedFlag(tps[len(m.index.Columns)].GetFlag()) {
			m.hdStatus = tablecodec.HandleIsUnsigned
		}
	}

	// Fast path: use pre-decoded index cache.
	if m.indexDatumCache != nil {
		if cachedDatums, ok := m.indexDatumCache.Entries[string(key)]; ok {
			return m.projectCachedIndexDatums(cachedDatums, key), nil
		}
	}

	// Slow path: full decode.
	colsLen := len(m.index.Columns)
	if m.decodeBuff == nil {
		m.decodeBuff = make([][]byte, colsLen, colsLen+len(colInfos))
	} else {
		m.decodeBuff = m.decodeBuff[: colsLen : colsLen+len(colInfos)]
	}
	buf := m.buf[:0]
	if m.restoredDec == nil {
		m.restoredDec = tablecodec.NewIndexRestoredDecoder(colInfos[:colsLen])
	}
	values, err := tablecodec.DecodeIndexKVEx(key, value, colsLen, m.hdStatus, colInfos, buf, m.decodeBuff, m.restoredDec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	physTblIDColumnIdx := math.MaxInt64
	if m.physTblIDIdx >= 0 {
		physTblIDColumnIdx = m.outputOffset[m.physTblIDIdx]
	}

	ds := m.resultRows[:0]
	for i, offset := range m.outputOffset {
		// The `value` slice doesn't contain the value of `physTblID`, it fills by `tablecodec.DecodeKeyHead` function.
		// For example, the schema is `[a, b, physTblID, c]`, `value` is `[v_a, v_b, v_c]`, `outputOffset` is `[0, 1, 2, 3]`
		// when we want the value of `c`, we should recalculate the offset of `c` by `offset - 1`.
		if m.physTblIDIdx == i {
			tid, _, _, _ := tablecodec.DecodeKeyHead(key)
			ds = append(ds, types.NewIntDatum(tid))
			continue
		}
		if offset > physTblIDColumnIdx {
			offset = offset - 1
		}
		ds = append(ds, types.Datum{})
		if err := tablecodec.DecodeColumnValueWithDatum(values[offset], tps[offset], m.loc, &ds[len(ds)-1]); err != nil {
			return nil, err
		}
	}
	return ds, nil
}

// projectCachedIndexDatums applies outputOffset projection to cached datums,
// handles physTblIDIdx, and converts TIMESTAMP columns from UTC to session timezone.
func (m *memIndexReader) projectCachedIndexDatums(cachedDatums []types.Datum, key []byte) []types.Datum {
	physTblIDColumnIdx := math.MaxInt64
	if m.physTblIDIdx >= 0 {
		physTblIDColumnIdx = m.outputOffset[m.physTblIDIdx]
	}

	ds := m.resultRows[:0]
	for i, offset := range m.outputOffset {
		if m.physTblIDIdx == i {
			tid, _, _, _ := tablecodec.DecodeKeyHead(key)
			ds = append(ds, types.NewIntDatum(tid))
			continue
		}
		if offset > physTblIDColumnIdx {
			offset = offset - 1
		}
		d := cachedDatums[offset]
		// Convert TIMESTAMP from UTC to session timezone.
		if m.loc != time.UTC && len(m.indexDatumCache.TsColIndices) > 0 {
			for _, tsIdx := range m.indexDatumCache.TsColIndices {
				if offset == tsIdx && !d.IsNull() {
					t := d.GetMysqlTime()
					if !t.IsZero() {
						// ConvertTimeZone modifies t in place; safe because GetMysqlTime returns a copy.
						_ = t.ConvertTimeZone(time.UTC, m.loc)
						d.SetMysqlTime(t)
					}
					break
				}
			}
		}
		ds = append(ds, d)
	}
	m.resultRows = ds
	return ds
}

type memTableReader struct {
	ctx           sessionctx.Context
	table         *model.TableInfo
	columns       []*model.ColumnInfo
	kvRanges      []kv.KeyRange
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	colIDs        map[int64]int
	buffer        allocBuf
	pkColIDs      []int64
	cacheTable    kv.MemBuffer
	datumCache    *tables.CachedDatumData
	offsets       []int
	keepOrder     bool
	compareExec
}

type allocBuf struct {
	// cache for decode handle.
	handleBytes []byte
	rd          *rowcodec.BytesDecoder
	cd          *rowcodec.ChunkDecoder
}

func buildMemTableReader(ctx context.Context, us *UnionScanExec, kvRanges []kv.KeyRange) *memTableReader {
	defer tracing.StartRegion(ctx, "buildMemTableReader").End()
	colIDs := make(map[int64]int, len(us.columns))
	for i, col := range us.columns {
		colIDs[col.ID] = i
	}

	colInfo := make([]rowcodec.ColInfo, 0, len(us.columns))
	for i := range us.columns {
		col := us.columns[i]
		colInfo = append(colInfo, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: us.table.Meta().PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}

	pkColIDs := tables.TryGetCommonPkColumnIds(us.table.Meta())
	if len(pkColIDs) == 0 {
		pkColIDs = []int64{-1}
	}

	defVal := func(i int) ([]byte, error) {
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(us.Ctx().GetExprCtx(), us.columns[i])
		if err != nil {
			return nil, err
		}
		sctx := us.Ctx().GetSessionVars().StmtCtx
		buf, err := tablecodec.EncodeValue(sctx.TimeZone(), nil, d)
		return buf, sctx.HandleError(err)
	}
	cd := NewRowDecoder(us.Ctx(), us.Schema(), us.table.Meta())
	rd := rowcodec.NewByteDecoder(colInfo, pkColIDs, defVal, us.Ctx().GetSessionVars().Location())
	if us.desc {
		slices.Reverse(kvRanges)
	}
	return &memTableReader{
		ctx:           us.Ctx(),
		table:         us.table.Meta(),
		columns:       us.columns,
		kvRanges:      kvRanges,
		conditions:    us.conditions,
		retFieldTypes: exec.RetTypes(us),
		colIDs:        colIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
			cd:          cd,
		},
		pkColIDs:    pkColIDs,
		cacheTable:  us.cacheTable,
		datumCache:  us.datumCache,
		keepOrder:   us.keepOrder,
		compareExec: us.compareExec,
	}
}

// txnMemBufferIter implements a kv.Iterator, it is an iterator that combines the membuffer data and snapshot data.
type txnMemBufferIter struct {
	sctx       sessionctx.Context
	kvRanges   []kv.KeyRange
	cacheTable kv.MemBuffer
	txn        kv.Transaction
	idx        int
	curr       kv.Iterator
	reverse    bool
	err        error
}

func newTxnMemBufferIter(sctx sessionctx.Context, cacheTable kv.MemBuffer, kvRanges []kv.KeyRange, reverse bool) (*txnMemBufferIter, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &txnMemBufferIter{
		sctx:       sctx,
		txn:        txn,
		kvRanges:   kvRanges,
		cacheTable: cacheTable,
		reverse:    reverse,
	}, nil
}

func (iter *txnMemBufferIter) Valid() bool {
	if iter.curr != nil {
		if iter.curr.Valid() {
			return true
		}
		iter.idx++
	}
	for iter.idx < len(iter.kvRanges) {
		rg := iter.kvRanges[iter.idx]
		var tmp kv.Iterator
		if !iter.reverse {
			tmp = iter.txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		} else {
			tmp = iter.txn.GetMemBuffer().SnapshotIterReverse(rg.EndKey, rg.StartKey)
		}
		snapCacheIter, err := getSnapIter(iter.sctx, iter.cacheTable, rg, iter.reverse)
		if err != nil {
			iter.err = errors.Trace(err)
			return true
		}
		if snapCacheIter != nil {
			tmp, err = transaction.NewUnionIter(tmp, snapCacheIter, iter.reverse)
			if err != nil {
				iter.err = errors.Trace(err)
				return true
			}
		}
		iter.curr = tmp
		if iter.curr.Valid() {
			return true
		}
		iter.idx++
	}
	return false
}

func (iter *txnMemBufferIter) Next() error {
	if iter.err != nil {
		return errors.Trace(iter.err)
	}
	if iter.curr != nil {
		if iter.curr.Valid() {
			return iter.curr.Next()
		}
	}
	return nil
}

func (iter *txnMemBufferIter) Key() kv.Key {
	return iter.curr.Key()
}

func (iter *txnMemBufferIter) Value() []byte {
	return iter.curr.Value()
}

func (iter *txnMemBufferIter) Close() {
	if iter.curr != nil {
		iter.curr.Close()
	}
}

func (m *memTableReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	// txnMemBufferIter not supports keepOrder + partitionTable.
	if m.keepOrder && m.table.GetPartitionInfo() != nil {
		data, err := m.getMemRows(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &defaultRowsIter{data: data}, nil
	}

	m.offsets = make([]int, len(m.columns))
	for i, col := range m.columns {
		m.offsets[i] = m.colIDs[col.ID]
	}

	kvIter, err := newTxnMemBufferIter(m.ctx, m.cacheTable, m.kvRanges, m.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	intHandle := !m.table.IsCommonHandle
	if m.cacheTable != nil {
		// Try pre-decoded datum cache fast path to skip KV decode.
		if iter := m.buildDatumCacheIter(); iter != nil {
			kvIter.Close()
			return iter, nil
		}
		batchChk := chunk.New(m.retFieldTypes, cachedTableBatchSize, cachedTableBatchSize)
		return &memRowsBatchIterForTable{
			kvIter:         kvIter,
			cd:             m.buffer.cd,
			batchChk:       batchChk,
			batchIt:        chunk.NewIterator4Chunk(batchChk),
			sel:            make([]int, 0, cachedTableBatchSize),
			datumRow:       make([]types.Datum, len(m.retFieldTypes)),
			retFieldTypes:  m.retFieldTypes,
			intHandle:      intHandle,
			memTableReader: m,
		}, nil
	}
	return &memRowsIterForTable{
		kvIter:         kvIter,
		cd:             m.buffer.cd,
		chk:            chunk.New(m.retFieldTypes, 1, 1),
		datumRow:       make([]types.Datum, len(m.retFieldTypes)),
		intHandle:      intHandle,
		memTableReader: m,
	}, nil
}

func (m *memTableReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	defer tracing.StartRegion(ctx, "memTableReader.getMemRows").End()
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	resultRows := make([]types.Datum, len(m.columns))
	m.offsets = make([]int, len(m.columns))
	for i, col := range m.columns {
		m.offsets[i] = m.colIDs[col.ID]
	}
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, value []byte) error {
		var err error
		resultRows, err = m.decodeRecordKeyValue(key, value, &resultRows)
		if err != nil {
			return err
		}

		mutableRow.SetDatums(resultRows...)
		matched, _, err := expression.EvalBool(m.ctx.GetExprCtx().GetEvalCtx(), m.conditions, mutableRow.ToRow())
		if err != nil || !matched {
			return err
		}
		m.addedRows = append(m.addedRows, resultRows)
		resultRows = make([]types.Datum, len(m.columns))
		return nil
	})
	if err != nil {
		return nil, err
	}

	if m.keepOrder && m.table.GetPartitionInfo() != nil {
		slices.SortFunc(m.addedRows, func(a, b []types.Datum) int {
			ret, err1 := m.compare(m.ctx.GetSessionVars().StmtCtx, a, b)
			if err1 != nil {
				err = err1
			}
			return ret
		})
		return m.addedRows, err
	}
	return m.addedRows, nil
}

func (m *memTableReader) decodeRecordKeyValue(key, value []byte, resultRows *[]types.Datum) ([]types.Datum, error) {
	handle, err := decodeHandleFromRowKey(key, !m.table.IsCommonHandle)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m.decodeRowData(handle, value, resultRows)
}

// decodeRowData uses to decode row data value.
func (m *memTableReader) decodeRowData(handle kv.Handle, value []byte, resultRows *[]types.Datum) ([]types.Datum, error) {
	values, err := m.getRowData(handle, value)
	if err != nil {
		return nil, err
	}
	for i, col := range m.columns {
		var datum types.Datum
		err := tablecodec.DecodeColumnValueWithDatum(values[m.offsets[i]], &col.FieldType, m.ctx.GetSessionVars().Location(), &datum)
		if err != nil {
			return nil, err
		}
		(*resultRows)[i] = datum
	}
	return *resultRows, nil
}

// getRowData decodes raw byte slice to row data.
func (m *memTableReader) getRowData(handle kv.Handle, value []byte) ([][]byte, error) {
	colIDs := m.colIDs
	pkIsHandle := m.table.PKIsHandle
	buffer := &m.buffer
	ctx := m.ctx.GetSessionVars().StmtCtx
	if rowcodec.IsNewFormat(value) {
		return buffer.rd.DecodeToBytes(colIDs, handle, value, buffer.handleBytes)
	}
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range m.columns {
		id := col.ID
		offset := colIDs[id]
		if m.table.IsCommonHandle {
			for i, colID := range m.pkColIDs {
				if colID == col.ID && !types.NeedRestoredData(&col.FieldType) {
					// Only try to decode handle when there is no corresponding column in the value.
					// This is because the information in handle may be incomplete in some cases.
					// For example, prefixed clustered index like 'primary key(col1(1))' only store the leftmost 1 char in the handle.
					if values[offset] == nil {
						values[offset] = handle.EncodedCol(i)
						break
					}
				}
			}
		} else if (pkIsHandle && mysql.HasPriKeyFlag(col.GetFlag())) || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(col.GetFlag()) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle.IntValue()))
			} else {
				handleDatum = types.NewIntDatum(handle.IntValue())
			}
			handleData, err1 := codec.EncodeValue(ctx.TimeZone(), buffer.handleBytes, handleDatum)
			err1 = ctx.HandleError(err1)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasColVal(values, colIDs, id) {
			continue
		}
		// no need to fill default value.
		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
}

// getMemRowsHandle is called when memIndexMergeReader.partialPlans[i] is TableScan.
func (m *memTableReader) getMemRowsHandle() ([]kv.Handle, error) {
	handles := make([]kv.Handle, 0, 16)
	intHandle := !m.table.IsCommonHandle
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, _ []byte) error {
		handle, err := decodeHandleFromRowKey(key, intHandle)
		if err != nil {
			return err
		}
		handles = append(handles, handle)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return handles, nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

type processKVFunc func(key, value []byte) error

func iterTxnMemBuffer(ctx sessionctx.Context, cacheTable kv.MemBuffer, kvRanges []kv.KeyRange, reverse bool, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	for _, rg := range kvRanges {
		var iter kv.Iterator
		if !reverse {
			iter = txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		} else {
			iter = txn.GetMemBuffer().SnapshotIterReverse(rg.EndKey, rg.StartKey)
		}
		snapCacheIter, err := getSnapIter(ctx, cacheTable, rg, reverse)
		if err != nil {
			return err
		}
		if snapCacheIter != nil {
			iter, err = transaction.NewUnionIter(iter, snapCacheIter, reverse)
			if err != nil {
				return err
			}
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}
			err = fn(iter.Key(), iter.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getSnapIter(ctx sessionctx.Context, cacheTable kv.MemBuffer, rg kv.KeyRange, reverse bool) (snapCacheIter kv.Iterator, err error) {
	var cacheIter, snapIter kv.Iterator
	tempTableData := ctx.GetSessionVars().TemporaryTableData
	if tempTableData != nil {
		if !reverse {
			snapIter, err = tempTableData.Iter(rg.StartKey, rg.EndKey)
		} else {
			snapIter, err = tempTableData.IterReverse(rg.EndKey, rg.StartKey)
		}
		if err != nil {
			return nil, err
		}
		snapCacheIter = snapIter
	} else if cacheTable != nil {
		if !reverse {
			cacheIter, err = cacheTable.Iter(rg.StartKey, rg.EndKey)
		} else {
			cacheIter, err = cacheTable.IterReverse(rg.EndKey, rg.StartKey)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		snapCacheIter = cacheIter
	}
	return snapCacheIter, nil
}

func (m *memIndexReader) getMemRowsHandle() ([]kv.Handle, error) {
	handles := make([]kv.Handle, 0, m.addedRowsLen)
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, value []byte) error {
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(m.index.Columns))
		if err != nil {
			return err
		}
		// For https://github.com/pingcap/tidb/issues/41827,
		// When handle type is year, tablecodec.DecodeIndexHandle will convert it to IntHandle instead of CommonHandle
		if m.table.IsCommonHandle && handle.IsInt() {
			b, err := codec.EncodeKey(m.ctx.GetSessionVars().StmtCtx.TimeZone(), nil, types.NewDatum(handle.IntValue()))
			err = m.ctx.GetSessionVars().StmtCtx.HandleError(err)
			if err != nil {
				return err
			}
			handle, err = kv.NewCommonHandle(b)
			if err != nil {
				return err
			}
		}
		// filter key/value by partitition id
		if ph, ok := handle.(kv.PartitionHandle); ok {
			if _, exist := m.partitionIDMap[ph.PartitionID]; !exist {
				return nil
			}
		}
		handles = append(handles, handle)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return handles, nil
}

type memIndexLookUpReader struct {
	ctx           sessionctx.Context
	index         *model.IndexInfo
	columns       []*model.ColumnInfo
	table         table.Table
	conditions    []expression.Expression
	retFieldTypes []*types.FieldType
	schema        *expression.Schema

	idxReader *memIndexReader

	// partition mode
	partitionMode     bool                  // if this executor is accessing a local index with partition table
	partitionTables   []table.PhysicalTable // partition tables to access
	partitionKVRanges [][]kv.KeyRange       // kv ranges for these partition tables

	cacheTable kv.MemBuffer

	keepOrder bool
	compareExec
}

func buildMemIndexLookUpReader(ctx context.Context, us *UnionScanExec, idxLookUpReader *IndexLookUpExecutor) *memIndexLookUpReader {
	defer tracing.StartRegion(ctx, "buildMemIndexLookUpReader").End()

	kvRanges := idxLookUpReader.kvRanges
	outputOffset := []int{len(idxLookUpReader.index.Columns)}
	memIdxReader := &memIndexReader{
		ctx:            us.Ctx(),
		index:          idxLookUpReader.index,
		table:          idxLookUpReader.table.Meta(),
		kvRanges:       kvRanges,
		retFieldTypes:  exec.RetTypes(us),
		outputOffset:   outputOffset,
		cacheTable:     us.cacheTable,
		partitionIDMap: us.partitionIDMap,
		resultRows:     make([]types.Datum, 0, len(outputOffset)),
	}

	return &memIndexLookUpReader{
		ctx:           us.Ctx(),
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		conditions:    us.conditions,
		retFieldTypes: exec.RetTypes(us),
		schema:        us.Schema(),
		idxReader:     memIdxReader,

		partitionMode:     idxLookUpReader.partitionTableMode,
		partitionKVRanges: idxLookUpReader.partitionKVRanges,
		partitionTables:   idxLookUpReader.prunedPartitions,
		cacheTable:        us.cacheTable,

		keepOrder:   idxLookUpReader.keepOrder,
		compareExec: us.compareExec,
	}
}

func (m *memIndexLookUpReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	kvRanges := [][]kv.KeyRange{m.idxReader.kvRanges}
	tbls := []table.Table{m.table}
	if m.partitionMode {
		kvRanges = m.partitionKVRanges
		tbls = tbls[:0]
		for _, p := range m.partitionTables {
			tbls = append(tbls, p)
		}
	}

	tblKVRanges := make([]kv.KeyRange, 0, 16)
	numHandles := 0
	for i, tbl := range tbls {
		m.idxReader.kvRanges = kvRanges[i]
		handles, err := m.idxReader.getMemRowsHandle()
		if err != nil {
			return nil, err
		}
		if len(handles) == 0 {
			continue
		}
		numHandles += len(handles)
		ranges, _ := distsql.TableHandlesToKVRanges(getPhysicalTableID(tbl), handles)
		tblKVRanges = append(tblKVRanges, ranges...)
	}
	if numHandles == 0 {
		return &defaultRowsIter{}, nil
	}

	if m.desc {
		slices.Reverse(tblKVRanges)
	}

	cd := NewRowDecoder(m.ctx, m.schema, m.table.Meta())
	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.ctx, m.table, m.columns)
	memTblReader := &memTableReader{
		ctx:           m.ctx,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKVRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, numHandles),
		retFieldTypes: m.retFieldTypes,
		colIDs:        colIDs,
		pkColIDs:      pkColIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
			cd:          cd,
		},
		cacheTable:  m.cacheTable,
		keepOrder:   m.keepOrder,
		compareExec: m.compareExec,
	}

	return memTblReader.getMemRowsIter(ctx)
}

func (*memIndexLookUpReader) getMemRowsHandle() ([]kv.Handle, error) {
	return nil, errors.New("getMemRowsHandle has not been implemented for memIndexLookUpReader")
}

type memIndexMergeReader struct {
	ctx              sessionctx.Context
	columns          []*model.ColumnInfo
	table            table.Table
	conditions       []expression.Expression
	retFieldTypes    []*types.FieldType
	indexMergeReader *IndexMergeReaderExecutor
	memReaders       []memReader
	isIntersection   bool

	// partition mode
	partitionMode     bool                  // if it is accessing a partition table
	partitionTables   []table.PhysicalTable // partition tables to access
	partitionKVRanges [][][]kv.KeyRange     // kv ranges for these partition tables

	keepOrder bool
	compareExec
}

func buildMemIndexMergeReader(ctx context.Context, us *UnionScanExec, indexMergeReader *IndexMergeReaderExecutor) *memIndexMergeReader {
	defer tracing.StartRegion(ctx, "buildMemIndexMergeReader").End()
	indexCount := len(indexMergeReader.indexes)
	memReaders := make([]memReader, 0, indexCount)
	for i := 0; i < indexCount; i++ {
		if indexMergeReader.indexes[i] == nil {
			colIDs, pkColIDs, rd := getColIDAndPkColIDs(indexMergeReader.Ctx(), indexMergeReader.table, indexMergeReader.columns)
			memReaders = append(memReaders, &memTableReader{
				ctx:           us.Ctx(),
				table:         indexMergeReader.table.Meta(),
				columns:       indexMergeReader.columns,
				kvRanges:      nil,
				conditions:    us.conditions,
				addedRows:     make([][]types.Datum, 0),
				retFieldTypes: exec.RetTypes(us),
				colIDs:        colIDs,
				pkColIDs:      pkColIDs,
				buffer: allocBuf{
					handleBytes: make([]byte, 0, 16),
					rd:          rd,
				},
			})
		} else {
			outputOffset := []int{len(indexMergeReader.indexes[i].Columns)}
			memReaders = append(memReaders, &memIndexReader{
				ctx:            us.Ctx(),
				index:          indexMergeReader.indexes[i],
				table:          indexMergeReader.table.Meta(),
				kvRanges:       nil,
				compareExec:    compareExec{desc: indexMergeReader.descs[i]},
				retFieldTypes:  exec.RetTypes(us),
				outputOffset:   outputOffset,
				partitionIDMap: indexMergeReader.partitionIDMap,
				resultRows:     make([]types.Datum, 0, len(outputOffset)),
			})
		}
	}

	return &memIndexMergeReader{
		ctx:              us.Ctx(),
		table:            indexMergeReader.table,
		columns:          indexMergeReader.columns,
		conditions:       us.conditions,
		retFieldTypes:    exec.RetTypes(us),
		indexMergeReader: indexMergeReader,
		memReaders:       memReaders,
		isIntersection:   indexMergeReader.isIntersection,

		partitionMode:     indexMergeReader.partitionTableMode,
		partitionTables:   indexMergeReader.prunedPartitions,
		partitionKVRanges: indexMergeReader.partitionKeyRanges,

		keepOrder:   us.keepOrder,
		compareExec: us.compareExec,
	}
}

type memRowsIter interface {
	Next() ([]types.Datum, error)
	// Close will release the snapshot it holds, so be sure to call Close.
	Close()
}

type defaultRowsIter struct {
	data   [][]types.Datum
	cursor int
}

func (iter *defaultRowsIter) Next() ([]types.Datum, error) {
	if iter.cursor < len(iter.data) {
		ret := iter.data[iter.cursor]
		iter.cursor++
		return ret, nil
	}
	return nil, nil
}

func (*defaultRowsIter) Close() {}

// memRowsIterForTable combine a kv.Iterator and a kv decoder to get a memRowsIter.
type memRowsIterForTable struct {
	kvIter    *txnMemBufferIter // txnMemBufferIter is the kv.Iterator
	cd        *rowcodec.ChunkDecoder
	chk       *chunk.Chunk
	datumRow  []types.Datum
	intHandle bool
	*memTableReader
}

func (iter *memRowsIterForTable) Next() ([]types.Datum, error) {
	curr := iter.kvIter
	var ret []types.Datum
	for curr.Valid() {
		key := curr.Key()
		value := curr.Value()
		if err := curr.Next(); err != nil {
			return nil, errors.Trace(err)
		}

		// check whether the key was been deleted.
		if len(value) == 0 {
			continue
		}
		handle, err := decodeHandleFromRowKey(key, iter.intHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter.chk.Reset()

		if !rowcodec.IsNewFormat(value) {
			// TODO: remove the legacy code!
			// fallback to the old way.
			iter.datumRow, err = iter.memTableReader.decodeRowData(handle, value, &iter.datumRow)
			if err != nil {
				return nil, errors.Trace(err)
			}

			mutableRow := chunk.MutRowFromTypes(iter.retFieldTypes)
			mutableRow.SetDatums(iter.datumRow...)
			matched, _, err := expression.EvalBool(iter.ctx.GetExprCtx().GetEvalCtx(), iter.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !matched {
				continue
			}
			return iter.datumRow, nil
		}

		err = iter.cd.DecodeToChunk(value, handle, iter.chk)
		if err != nil {
			return nil, errors.Trace(err)
		}

		row := iter.chk.GetRow(0)
		matched, _, err := expression.EvalBool(iter.ctx.GetExprCtx().GetEvalCtx(), iter.conditions, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !matched {
			continue
		}
		ret = row.GetDatumRowWithBuffer(iter.retFieldTypes, iter.datumRow)
		break
	}
	return ret, nil
}

func (iter *memRowsIterForTable) Close() {
	if iter.kvIter != nil {
		iter.kvIter.Close()
	}
}

const cachedTableBatchSize = 64

// memRowsBatchIterForTable batch decodes cached table rows into a chunk and applies filters in vectorized mode when possible.
type memRowsBatchIterForTable struct {
	kvIter        *txnMemBufferIter
	cd            *rowcodec.ChunkDecoder
	batchChk      *chunk.Chunk
	batchIt       *chunk.Iterator4Chunk
	selected      []bool
	sel           []int
	cursor        int
	datumRow      []types.Datum
	retFieldTypes []*types.FieldType
	intHandle     bool
	*memTableReader
}

func (iter *memRowsBatchIterForTable) Next() ([]types.Datum, error) {
	curr := iter.kvIter
	evalCtx := iter.ctx.GetExprCtx().GetEvalCtx()
	vecEnabled := iter.ctx.GetSessionVars().EnableVectorizedExpression

	for {
		// Return remaining matched rows in current batch.
		if iter.cursor < len(iter.sel) {
			row := iter.batchChk.GetRow(iter.sel[iter.cursor])
			iter.cursor++
			return row.GetDatumRowWithBuffer(iter.retFieldTypes, iter.datumRow), nil
		}

		// Fill a new batch.
		iter.batchChk.Reset()
		iter.sel = iter.sel[:0]
		iter.cursor = 0

		for iter.batchChk.NumRows() < cachedTableBatchSize && curr.Valid() {
			key := curr.Key()
			value := curr.Value()

			// check whether the key was been deleted.
			if len(value) == 0 {
				if err := curr.Next(); err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}

			handle, err := decodeHandleFromRowKey(key, iter.intHandle)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if !rowcodec.IsNewFormat(value) {
				// Keep the scan order: flush current batch first.
				if iter.batchChk.NumRows() > 0 {
					break
				}

				if err := curr.Next(); err != nil {
					return nil, errors.Trace(err)
				}

				// TODO: remove the legacy code!
				iter.datumRow, err = iter.memTableReader.decodeRowData(handle, value, &iter.datumRow)
				if err != nil {
					return nil, errors.Trace(err)
				}
				mutableRow := chunk.MutRowFromTypes(iter.retFieldTypes)
				mutableRow.SetDatums(iter.datumRow...)
				matched, _, err := expression.EvalBool(evalCtx, iter.conditions, mutableRow.ToRow())
				if err != nil {
					return nil, errors.Trace(err)
				}
				if !matched {
					continue
				}
				return iter.datumRow, nil
			}

			if err := curr.Next(); err != nil {
				return nil, errors.Trace(err)
			}

			err = iter.cd.DecodeToChunk(value, handle, iter.batchChk)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		if iter.batchChk.NumRows() == 0 {
			return nil, nil
		}

		if len(iter.conditions) == 0 {
			for i := 0; i < iter.batchChk.NumRows(); i++ {
				iter.sel = append(iter.sel, i)
			}
			continue
		}

		iter.batchIt.ResetChunk(iter.batchChk)
		iter.selected = iter.selected[:0]
		var err error
		iter.selected, err = expression.VectorizedFilter(evalCtx, vecEnabled, iter.conditions, iter.batchIt, iter.selected)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := range iter.selected {
			if iter.selected[i] {
				iter.sel = append(iter.sel, i)
			}
		}
	}
}

func decodeHandleFromRowKey(key kv.Key, intHandle bool) (kv.Handle, error) {
	if !intHandle {
		return tablecodec.DecodeRowKey(key)
	}
	// Int handle record keys are fixed-length. Decode handle directly from the last 8 bytes to
	// avoid the additional checks in tablecodec.DecodeRowKey.
	if len(key) != tablecodec.RecordRowKeyLen {
		return tablecodec.DecodeRowKey(key)
	}
	u := binary.BigEndian.Uint64(key[len(key)-8:])
	return kv.IntHandle(codec.DecodeCmpUintToInt(u)), nil
}

func (iter *memRowsBatchIterForTable) Close() {
	if iter.kvIter != nil {
		iter.kvIter.Close()
	}
	iter.batchChk = nil
	iter.batchIt = nil
	iter.selected = nil
	iter.sel = nil
	iter.cursor = 0
	iter.datumRow = nil
	iter.retFieldTypes = nil
	iter.memTableReader = nil
}

// memCachedDatumIter directly iterates pre-decoded CachedDatumData, skipping KV decode.
type memCachedDatumIter struct {
	data     *tables.CachedDatumData
	chunkIdx int
	rowIdx   int
	desc     bool

	// Column projection: maps query column index to datum cache column index.
	colProjection   []int
	cacheFieldTypes []*types.FieldType
	datumRow        []types.Datum
	retFieldTypes   []*types.FieldType
	mutableRow      chunk.MutRow

	// filter
	conditions []expression.Expression
	evalCtx    expression.EvalContext

	// TIMESTAMP timezone conversion
	needTZConvert  bool
	sessionLoc     *time.Location
	tsColProjected []int // indices in projected (query) columns that are TIMESTAMP
}

func (iter *memCachedDatumIter) Next() ([]types.Datum, error) {
	for {
		// Check chunk bounds.
		if !iter.desc {
			if iter.chunkIdx >= len(iter.data.Chunks) {
				return nil, nil
			}
		} else {
			if iter.chunkIdx < 0 {
				return nil, nil
			}
		}

		chk := iter.data.Chunks[iter.chunkIdx]

		// Check row bounds and advance to next/prev chunk.
		if !iter.desc {
			if iter.rowIdx >= chk.NumRows() {
				iter.chunkIdx++
				iter.rowIdx = 0
				continue
			}
		} else {
			if iter.rowIdx < 0 {
				iter.chunkIdx--
				if iter.chunkIdx >= 0 {
					iter.rowIdx = iter.data.Chunks[iter.chunkIdx].NumRows() - 1
				}
				continue
			}
		}

		row := chk.GetRow(iter.rowIdx)
		if !iter.desc {
			iter.rowIdx++
		} else {
			iter.rowIdx--
		}

		// Project: extract only the columns the query needs.
		for i, cacheIdx := range iter.colProjection {
			iter.datumRow[i] = row.GetDatum(cacheIdx, iter.cacheFieldTypes[cacheIdx])
		}

		// TIMESTAMP timezone conversion (on projected datum copy, safe).
		// Datum cache stores TIMESTAMP in UTC; convert to session timezone on read.
		// Skip NULL values and zero timestamps (consistent with KV decode path in decoder.go).
		if iter.needTZConvert {
			for _, idx := range iter.tsColProjected {
				if !iter.datumRow[idx].IsNull() {
					t := iter.datumRow[idx].GetMysqlTime()
					if !t.IsZero() {
						if err := t.ConvertTimeZone(time.UTC, iter.sessionLoc); err != nil {
							return nil, err
						}
						iter.datumRow[idx].SetMysqlTime(t)
					}
				}
			}
		}

		// Apply filter conditions.
		if len(iter.conditions) > 0 {
			if iter.mutableRow.ToRow().Chunk() == nil {
				iter.mutableRow = chunk.MutRowFromTypes(iter.retFieldTypes)
			}
			iter.mutableRow.SetDatums(iter.datumRow...)
			matched, _, err := expression.EvalBool(iter.evalCtx, iter.conditions, iter.mutableRow.ToRow())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
		}

		return iter.datumRow, nil
	}
}

func (*memCachedDatumIter) Close() {
	// CachedDatumData is managed by cacheData; not released here.
}

// buildDatumCacheIter tries to build a memCachedDatumIter from the pre-decoded datum cache.
// Returns nil if the datum cache is not available or column projection fails.
func (m *memTableReader) buildDatumCacheIter() *memCachedDatumIter {
	if m.datumCache == nil {
		return nil
	}

	// Build column ID -> datum cache index mapping from the table's public columns.
	allCols := m.table.Cols()
	colIDToCacheIdx := make(map[int64]int, len(allCols))
	for i, col := range allCols {
		colIDToCacheIdx[col.ID] = i
	}

	// Build projection: for each query column, find its datum cache index.
	colProjection := make([]int, len(m.columns))
	for i, col := range m.columns {
		idx, ok := colIDToCacheIdx[col.ID]
		if !ok {
			// Column not found in datum cache, fallback to KV decode.
			return nil
		}
		colProjection[i] = idx
	}

	// Find TIMESTAMP columns in the projected (query) columns for TZ conversion.
	sessionLoc := m.ctx.GetSessionVars().Location()
	var tsColProjected []int
	for i, col := range m.columns {
		if col.GetType() == mysql.TypeTimestamp {
			tsColProjected = append(tsColProjected, i)
		}
	}
	needTZConvert := len(tsColProjected) > 0 && sessionLoc.String() != time.UTC.String()

	iter := &memCachedDatumIter{
		data:            m.datumCache,
		desc:            m.desc,
		colProjection:   colProjection,
		cacheFieldTypes: m.datumCache.FieldTypes,
		datumRow:        make([]types.Datum, len(m.columns)),
		retFieldTypes:   m.retFieldTypes,
		mutableRow:      chunk.MutRowFromTypes(m.retFieldTypes),
		conditions:      m.conditions,
		evalCtx:         m.ctx.GetExprCtx().GetEvalCtx(),
		needTZConvert:   needTZConvert,
		sessionLoc:      sessionLoc,
		tsColProjected:  tsColProjected,
	}

	// For descending scan, start from the last row of the last chunk.
	if m.desc {
		lastIdx := len(m.datumCache.Chunks) - 1
		iter.chunkIdx = lastIdx
		if lastIdx >= 0 {
			iter.rowIdx = m.datumCache.Chunks[lastIdx].NumRows() - 1
		}
	}

	return iter
}

type memRowsIterForIndex struct {
	kvIter     *txnMemBufferIter
	tps        []*types.FieldType
	mutableRow chunk.MutRow
	*memIndexReader
	colInfos []rowcodec.ColInfo
}

func (iter *memRowsIterForIndex) Next() ([]types.Datum, error) {
	var ret []types.Datum
	curr := iter.kvIter
	for curr.Valid() {
		key := curr.Key()
		value := curr.Value()
		if err := curr.Next(); err != nil {
			return nil, errors.Trace(err)
		}
		// check whether the key was been deleted.
		if len(value) == 0 {
			continue
		}

		// filter key/value by partitition id
		if iter.index.Global {
			_, pid, err := codec.DecodeInt(tablecodec.SplitIndexValue(value).PartitionID)
			if err != nil {
				return nil, err
			}
			if _, exists := iter.partitionIDMap[pid]; !exists {
				continue
			}
		}

		data, err := iter.memIndexReader.decodeIndexKeyValue(key, value, iter.tps, iter.colInfos)
		if err != nil {
			return nil, err
		}

		if len(iter.memIndexReader.conditions) > 0 {
			iter.mutableRow.SetDatums(data...)
			matched, _, err := expression.EvalBool(iter.memIndexReader.evalCtx, iter.memIndexReader.conditions, iter.mutableRow.ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		ret = data
		break
	}
	return ret, nil
}

func (iter *memRowsIterForIndex) Close() {
	if iter.kvIter != nil {
		iter.kvIter.Close()
	}
}

func (m *memIndexMergeReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	data, err := m.getMemRows(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &defaultRowsIter{data: data}, nil
}

func (m *memIndexMergeReader) getHandles() (handles []kv.Handle, err error) {
	hMap := kv.NewHandleMap()
	// loop each memReaders and fill handle map
	for i, reader := range m.memReaders {
		// [partitionNum][rangeNum]
		var readerKvRanges [][]kv.KeyRange
		if m.partitionMode {
			readerKvRanges = m.partitionKVRanges[i]
		} else {
			readerKvRanges = [][]kv.KeyRange{m.indexMergeReader.keyRanges[i]}
		}
		for j, kr := range readerKvRanges {
			switch r := reader.(type) {
			case *memTableReader:
				r.kvRanges = kr
			case *memIndexReader:
				r.kvRanges = kr
			default:
				return nil, errors.New("memReader have to be memTableReader or memIndexReader")
			}
			handles, err := reader.getMemRowsHandle()
			if err != nil {
				return nil, err
			}
			// Filter same row.
			for _, handle := range handles {
				if _, ok := handle.(kv.PartitionHandle); !ok && m.partitionMode {
					pid := m.partitionTables[j].GetPhysicalID()
					handle = kv.NewPartitionHandle(pid, handle)
				}
				if v, ok := hMap.Get(handle); !ok {
					cnt := 1
					hMap.Set(handle, &cnt)
				} else {
					*(v.(*int))++
				}
			}
		}
	}

	// process handle map, return handles meets the requirements (union or intersection)
	hMap.Range(func(h kv.Handle, val any) bool {
		if m.isIntersection {
			if *(val.(*int)) == len(m.memReaders) {
				handles = append(handles, h)
			}
		} else {
			handles = append(handles, h)
		}
		return true
	})

	return handles, nil
}

func (m *memIndexMergeReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	r, ctx := tracing.StartRegionEx(ctx, "memIndexMergeReader.getMemRows")
	defer r.End()

	handles, err := m.getHandles()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	var tblKVRanges []kv.KeyRange
	if m.partitionMode {
		// `tid` for partition handle is useless, so use 0 here.
		tblKVRanges, _ = distsql.TableHandlesToKVRanges(0, handles)
	} else {
		tblKVRanges, _ = distsql.TableHandlesToKVRanges(getPhysicalTableID(m.table), handles)
	}

	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.ctx, m.table, m.columns)

	memTblReader := &memTableReader{
		ctx:           m.ctx,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKVRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, len(handles)),
		retFieldTypes: m.retFieldTypes,
		colIDs:        colIDs,
		pkColIDs:      pkColIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
		},
	}

	rows, err := memTblReader.getMemRows(ctx)
	if err != nil {
		return nil, err
	}

	// Didn't set keepOrder = true for memTblReader,
	// In indexMerge, non-partitioned tables are also need reordered.
	if m.keepOrder {
		slices.SortFunc(rows, func(a, b []types.Datum) int {
			ret, err1 := m.compare(m.ctx.GetSessionVars().StmtCtx, a, b)
			if err1 != nil {
				err = err1
			}
			return ret
		})
	}

	return rows, err
}

func (*memIndexMergeReader) getMemRowsHandle() ([]kv.Handle, error) {
	return nil, errors.New("getMemRowsHandle has not been implemented for memIndexMergeReader")
}

func getColIDAndPkColIDs(ctx sessionctx.Context, tbl table.Table, columns []*model.ColumnInfo) (map[int64]int, []int64, *rowcodec.BytesDecoder) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}

	tblInfo := tbl.Meta()
	colInfos := make([]rowcodec.ColInfo, 0, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos = append(colInfos, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}
	pkColIDs := tables.TryGetCommonPkColumnIds(tblInfo)
	if len(pkColIDs) == 0 {
		pkColIDs = []int64{-1}
	}
	defVal := func(i int) ([]byte, error) {
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx.GetExprCtx(), columns[i])
		if err != nil {
			return nil, err
		}
		buf, err := tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, d)
		return buf, ctx.GetSessionVars().StmtCtx.HandleError(err)
	}
	rd := rowcodec.NewByteDecoder(colInfos, pkColIDs, defVal, ctx.GetSessionVars().Location())
	return colIDs, pkColIDs, rd
}
