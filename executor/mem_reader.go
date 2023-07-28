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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	transaction "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/exp/slices"
)

type memReader interface {
	getMemRows(ctx context.Context) ([][]types.Datum, error)
	getMemRowsHandle() ([]kv.Handle, error)
}

var (
	_ memReader = &memIndexReader{}
	_ memReader = &memTableReader{}
	_ memReader = &memIndexLookUpReader{}
	_ memReader = &memIndexMergeReader{}
)

type memIndexReader struct {
	ctx           sessionctx.Context
	index         *model.IndexInfo
	table         *model.TableInfo
	kvRanges      []kv.KeyRange
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	addedRowsLen  int
	retFieldTypes []*types.FieldType
	outputOffset  []int
	cacheTable    kv.MemBuffer
	keepOrder     bool
	compareExec
}

func buildMemIndexReader(ctx context.Context, us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
	defer tracing.StartRegion(ctx, "buildMemIndexReader").End()
	kvRanges := idxReader.kvRanges
	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	if us.desc {
		for i, j := 0, len(kvRanges)-1; i < j; i, j = i+1, j-1 {
			kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
		}
	}
	return &memIndexReader{
		ctx:           us.Ctx(),
		index:         idxReader.index,
		table:         idxReader.table.Meta(),
		kvRanges:      kvRanges,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		outputOffset:  outputOffset,
		cacheTable:    us.cacheTable,
		keepOrder:     us.keepOrder,
		compareExec:   us.compareExec,
	}
}

func (m *memIndexReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	data, err := m.getMemRows(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &defaultRowsIter{data: data}, nil
}

func (m *memIndexReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	defer tracing.StartRegion(ctx, "memIndexReader.getMemRows").End()
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

	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, value []byte) error {
		data, err := m.decodeIndexKeyValue(key, value, tps)
		if err != nil {
			return err
		}

		mutableRow.SetDatums(data...)
		matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
		if err != nil || !matched {
			return err
		}
		m.addedRows = append(m.addedRows, data)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if m.keepOrder && m.table.GetPartitionInfo() != nil {
		slices.SortFunc(m.addedRows, func(a, b []types.Datum) bool {
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

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType) ([]types.Datum, error) {
	hdStatus := tablecodec.HandleDefault
	if mysql.HasUnsignedFlag(tps[len(tps)-1].GetFlag()) {
		hdStatus = tablecodec.HandleIsUnsigned
	}
	colInfos := tables.BuildRowcodecColInfoForIndexColumns(m.index, m.table)
	colInfos = tables.TryAppendCommonHandleRowcodecColInfos(colInfos, m.table)
	values, err := tablecodec.DecodeIndexKV(key, value, len(m.index.Columns), hdStatus, colInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ds := make([]types.Datum, 0, len(m.outputOffset))
	for _, offset := range m.outputOffset {
		d, err := tablecodec.DecodeColumnValue(values[offset], tps[offset], m.ctx.GetSessionVars().Location())
		if err != nil {
			return nil, err
		}
		ds = append(ds, d)
	}
	return ds, nil
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
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(us.Ctx(), us.columns[i])
		if err != nil {
			return nil, err
		}
		return tablecodec.EncodeValue(us.Ctx().GetSessionVars().StmtCtx, nil, d)
	}
	cd := NewRowDecoder(us.Ctx(), us.Schema(), us.table.Meta())
	rd := rowcodec.NewByteDecoder(colInfo, pkColIDs, defVal, us.Ctx().GetSessionVars().Location())
	if us.desc {
		for i, j := 0, len(kvRanges)-1; i < j; i, j = i+1, j-1 {
			kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
		}
	}
	return &memTableReader{
		ctx:           us.Ctx(),
		table:         us.table.Meta(),
		columns:       us.columns,
		kvRanges:      kvRanges,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		colIDs:        colIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
			cd:          cd,
		},
		pkColIDs:    pkColIDs,
		cacheTable:  us.cacheTable,
		keepOrder:   us.keepOrder,
		compareExec: us.compareExec,
	}
}

type txnMemBufferIter struct {
	*memTableReader
	txn  kv.Transaction
	idx  int
	curr kv.Iterator

	reverse  bool
	cd       *rowcodec.ChunkDecoder
	chk      *chunk.Chunk
	datumRow []types.Datum
}

func (iter *txnMemBufferIter) Next() ([]types.Datum, error) {
	var ret []types.Datum
	for iter.idx < len(iter.kvRanges) {
		if iter.curr == nil {
			rg := iter.kvRanges[iter.idx]
			var tmp kv.Iterator
			if !iter.reverse {
				tmp = iter.txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
			} else {
				tmp = iter.txn.GetMemBuffer().SnapshotIterReverse(rg.EndKey, rg.StartKey)
			}
			snapCacheIter, err := getSnapIter(iter.ctx, iter.cacheTable, rg, iter.reverse)
			if err != nil {
				return nil, err
			}
			if snapCacheIter != nil {
				tmp, err = transaction.NewUnionIter(tmp, snapCacheIter, iter.reverse)
				if err != nil {
					return nil, err
				}
			}
			iter.curr = tmp
		} else {
			var err error
			ret, err = iter.next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if ret != nil {
				break
			}
			iter.idx++
			iter.curr = nil
		}
	}
	return ret, nil
}

func (iter *txnMemBufferIter) next() ([]types.Datum, error) {
	var err error
	curr := iter.curr
	for ; err == nil && curr.Valid(); err = curr.Next() {
		// check whether the key was been deleted.
		if len(curr.Value()) == 0 {
			continue
		}

		handle, err := tablecodec.DecodeRowKey(curr.Key())
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter.chk.Reset()

		if !rowcodec.IsNewFormat(curr.Value()) {
			// TODO: remove the legacy code!
			// fallback to the old way.
			iter.datumRow, err = iter.decodeRecordKeyValue(curr.Key(), curr.Value(), &iter.datumRow)
			if err != nil {
				return nil, errors.Trace(err)
			}

			mutableRow := chunk.MutRowFromTypes(iter.retFieldTypes)
			mutableRow.SetDatums(iter.datumRow...)
			matched, _, err := expression.EvalBool(iter.ctx, iter.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !matched {
				continue
			}
			return iter.datumRow, curr.Next()
		}

		err = iter.cd.DecodeToChunk(curr.Value(), handle, iter.chk)
		if err != nil {
			return nil, errors.Trace(err)
		}

		row := iter.chk.GetRow(0)
		matched, _, err := expression.EvalBool(iter.ctx, iter.conditions, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !matched {
			continue
		}
		ret := row.GetDatumRowWithBuffer(iter.retFieldTypes, iter.datumRow)
		return ret, curr.Next()
	}
	return nil, err
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
	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}

	return &txnMemBufferIter{
		memTableReader: m,
		txn:            txn,
		cd:             m.buffer.cd,
		chk:            chunk.New(m.retFieldTypes, 1, 1),
		datumRow:       make([]types.Datum, len(m.retFieldTypes)),
		reverse:        m.desc,
	}, nil
}

// TODO: Try to make memXXXReader lazy, There is no need to decode many rows when parent operator only need 1 row.
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
		matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
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
		slices.SortFunc(m.addedRows, func(a, b []types.Datum) bool {
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
	handle, err := tablecodec.DecodeRowKey(key)
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
			handleData, err1 := codec.EncodeValue(ctx, buffer.handleBytes, handleDatum)
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
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, m.desc, func(key, value []byte) error {
		handle, err := tablecodec.DecodeRowKey(key)
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
			b, err := codec.EncodeKey(m.ctx.GetSessionVars().StmtCtx, nil, types.NewDatum(handle.IntValue()))
			if err != nil {
				return err
			}
			handle, err = kv.NewCommonHandle(b)
			if err != nil {
				return err
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

	idxReader *memIndexReader

	// partition mode
	partitionMode     bool                  // if it is accessing a partition table
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
		ctx:           us.Ctx(),
		index:         idxLookUpReader.index,
		table:         idxLookUpReader.table.Meta(),
		kvRanges:      kvRanges,
		retFieldTypes: retTypes(us),
		outputOffset:  outputOffset,
		cacheTable:    us.cacheTable,
	}

	return &memIndexLookUpReader{
		ctx:           us.Ctx(),
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
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
	data, err := m.getMemRows(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &defaultRowsIter{data: data}, nil
}

func (m *memIndexLookUpReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	r, ctx := tracing.StartRegionEx(ctx, "memIndexLookUpReader.getMemRows")
	defer r.End()

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
		return nil, nil
	}

	if m.desc {
		for i, j := 0, len(tblKVRanges)-1; i < j; i, j = i+1, j-1 {
			tblKVRanges[i], tblKVRanges[j] = tblKVRanges[j], tblKVRanges[i]
		}
	}

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
		},
		cacheTable:  m.cacheTable,
		keepOrder:   m.keepOrder,
		compareExec: m.compareExec,
	}

	return memTblReader.getMemRows(ctx)
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
				retFieldTypes: retTypes(us),
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
				ctx:           us.Ctx(),
				index:         indexMergeReader.indexes[i],
				table:         indexMergeReader.table.Meta(),
				kvRanges:      nil,
				compareExec:   compareExec{desc: indexMergeReader.descs[i]},
				retFieldTypes: retTypes(us),
				outputOffset:  outputOffset,
			})
		}
	}

	return &memIndexMergeReader{
		ctx:              us.Ctx(),
		table:            indexMergeReader.table,
		columns:          indexMergeReader.columns,
		conditions:       us.conditions,
		retFieldTypes:    retTypes(us),
		indexMergeReader: indexMergeReader,
		memReaders:       memReaders,
		isIntersection:   indexMergeReader.isIntersection,

		partitionMode:     indexMergeReader.partitionTableMode,
		partitionTables:   indexMergeReader.prunedPartitions,
		partitionKVRanges: indexMergeReader.partitionKeyRanges,
	}
}

type memRowsIter interface {
	Next() ([]types.Datum, error)
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

func (m *memIndexMergeReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
	data, err := m.getMemRows(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &defaultRowsIter{data: data}, nil
}

func (m *memIndexMergeReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	r, ctx := tracing.StartRegionEx(ctx, "memIndexMergeReader.getMemRows")
	defer r.End()
	tbls := []table.Table{m.table}
	// [partNum][indexNum][rangeNum]
	var kvRanges [][][]kv.KeyRange
	if m.partitionMode {
		tbls = tbls[:0]
		for _, p := range m.partitionTables {
			tbls = append(tbls, p)
		}
		kvRanges = m.partitionKVRanges
	} else {
		kvRanges = append(kvRanges, m.indexMergeReader.keyRanges)
	}
	if len(kvRanges) != len(tbls) {
		return nil, errors.Errorf("length of tbls(size: %d) should be equals to length of kvRanges(size: %d)", len(tbls), len(kvRanges))
	}

	tblKVRanges := make([]kv.KeyRange, 0, 16)
	numHandles := 0
	var handles []kv.Handle
	var err error
	for i, tbl := range tbls {
		if m.isIntersection {
			handles, err = m.intersectionHandles(kvRanges[i])
		} else {
			handles, err = m.unionHandles(kvRanges[i])
		}
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
		return nil, nil
	}
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
		},
	}

	return memTblReader.getMemRows(ctx)
}

// Union all handles of all partial paths.
func (m *memIndexMergeReader) unionHandles(kvRanges [][]kv.KeyRange) (finalHandles []kv.Handle, err error) {
	if len(m.memReaders) != len(kvRanges) {
		return nil, errors.Errorf("len(kvRanges) should be equal to len(memReaders)")
	}

	hMap := kv.NewHandleMap()
	var handles []kv.Handle
	for i, reader := range m.memReaders {
		switch r := reader.(type) {
		case *memTableReader:
			r.kvRanges = kvRanges[i]
		case *memIndexReader:
			r.kvRanges = kvRanges[i]
		default:
			return nil, errors.New("memReader have to be memTableReader or memIndexReader")
		}
		if handles, err = reader.getMemRowsHandle(); err != nil {
			return nil, err
		}
		// Filter same row.
		for _, h := range handles {
			if _, ok := hMap.Get(h); !ok {
				finalHandles = append(finalHandles, h)
				hMap.Set(h, true)
			}
		}
	}
	return finalHandles, nil
}

// Intersect handles of each partial paths.
func (m *memIndexMergeReader) intersectionHandles(kvRanges [][]kv.KeyRange) (finalHandles []kv.Handle, err error) {
	if len(m.memReaders) != len(kvRanges) {
		return nil, errors.Errorf("len(kvRanges) should be equal to len(memReaders)")
	}

	hMap := kv.NewHandleMap()
	var handles []kv.Handle
	for i, reader := range m.memReaders {
		switch r := reader.(type) {
		case *memTableReader:
			r.kvRanges = kvRanges[i]
		case *memIndexReader:
			r.kvRanges = kvRanges[i]
		default:
			return nil, errors.New("memReader have to be memTableReader or memIndexReader")
		}
		if handles, err = reader.getMemRowsHandle(); err != nil {
			return nil, err
		}
		for _, h := range handles {
			if cntPtr, ok := hMap.Get(h); !ok {
				cnt := 1
				hMap.Set(h, &cnt)
			} else {
				*(cntPtr.(*int))++
			}
		}
	}
	hMap.Range(func(h kv.Handle, val interface{}) bool {
		if *(val.(*int)) == len(m.memReaders) {
			finalHandles = append(finalHandles, h)
		}
		return true
	})
	return finalHandles, nil
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
		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, columns[i])
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return nil, err
		}
		return tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx, nil, d)
	}
	rd := rowcodec.NewByteDecoder(colInfos, pkColIDs, defVal, ctx.GetSessionVars().Location())
	return colIDs, pkColIDs, rd
}
