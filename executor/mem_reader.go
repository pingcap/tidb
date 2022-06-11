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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	transaction "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
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
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	addedRowsLen  int
	retFieldTypes []*types.FieldType
	outputOffset  []int
	// belowHandleCols is the handle's position of the below scan plan.
	belowHandleCols plannercore.HandleCols
	cacheTable      kv.MemBuffer
}

func buildMemIndexReader(ctx context.Context, us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("buildMemIndexReader", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	kvRanges := idxReader.kvRanges
	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	return &memIndexReader{
		ctx:             us.ctx,
		index:           idxReader.index,
		table:           idxReader.table.Meta(),
		kvRanges:        kvRanges,
		desc:            us.desc,
		conditions:      us.conditions,
		retFieldTypes:   retTypes(us),
		outputOffset:    outputOffset,
		belowHandleCols: us.belowHandleCols,
		cacheTable:      us.cacheTable,
	}
}

func (m *memIndexReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("memIndexReader.getMemRows", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	tps := make([]*types.FieldType, 0, len(m.index.Columns)+1)
	cols := m.table.Columns
	for _, col := range m.index.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	switch {
	case m.table.PKIsHandle:
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				tps = append(tps, &col.FieldType)
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
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, func(key, value []byte) error {
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
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
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
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	colIDs        map[int64]int
	buffer        allocBuf
	pkColIDs      []int64
	cacheTable    kv.MemBuffer
	offsets       []int
}

type allocBuf struct {
	// cache for decode handle.
	handleBytes []byte
	rd          *rowcodec.BytesDecoder
}

func buildMemTableReader(ctx context.Context, us *UnionScanExec, tblReader *TableReaderExecutor) *memTableReader {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("buildMemTableReader", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
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
	rd := rowcodec.NewByteDecoder(colInfo, pkColIDs, nil, us.ctx.GetSessionVars().Location())
	return &memTableReader{
		ctx:           us.ctx,
		table:         us.table.Meta(),
		columns:       us.columns,
		kvRanges:      tblReader.kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		colIDs:        colIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
		},
		pkColIDs:   pkColIDs,
		cacheTable: us.cacheTable,
	}
}

// TODO: Try to make memXXXReader lazy, There is no need to decode many rows when parent operator only need 1 row.
func (m *memTableReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("memTableReader.getMemRows", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	resultRows := make([]types.Datum, len(m.columns))
	m.offsets = make([]int, len(m.columns))
	for i, col := range m.columns {
		m.offsets[i] = m.colIDs[col.ID]
	}
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, func(key, value []byte) error {
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

	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
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
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, func(key, value []byte) error {
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

	if m.desc {
		for i, j := 0, len(handles)-1; i < j; i, j = i+1, j-1 {
			handles[i], handles[j] = handles[j], handles[i]
		}
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

func iterTxnMemBuffer(ctx sessionctx.Context, cacheTable kv.MemBuffer, kvRanges []kv.KeyRange, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	for _, rg := range kvRanges {
		iter := txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		snapCacheIter, err := getSnapIter(ctx, cacheTable, rg)
		if err != nil {
			return err
		}
		if snapCacheIter != nil {
			iter, err = transaction.NewUnionIter(iter, snapCacheIter, false)
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

func getSnapIter(ctx sessionctx.Context, cacheTable kv.MemBuffer, rg kv.KeyRange) (kv.Iterator, error) {
	var snapCacheIter kv.Iterator
	tempTableData := ctx.GetSessionVars().TemporaryTableData
	if tempTableData != nil {
		snapIter, err := tempTableData.Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		snapCacheIter = snapIter
	} else if cacheTable != nil {
		cacheIter, err := cacheTable.Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		snapCacheIter = cacheIter
	}
	return snapCacheIter, nil
}

func reverseDatumSlice(rows [][]types.Datum) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (m *memIndexReader) getMemRowsHandle() ([]kv.Handle, error) {
	handles := make([]kv.Handle, 0, m.addedRowsLen)
	err := iterTxnMemBuffer(m.ctx, m.cacheTable, m.kvRanges, func(key, value []byte) error {
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(m.index.Columns))
		if err != nil {
			return err
		}
		handles = append(handles, handle)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if m.desc {
		for i, j := 0, len(handles)-1; i < j; i, j = i+1, j-1 {
			handles[i], handles[j] = handles[j], handles[i]
		}
	}
	return handles, nil
}

type memIndexLookUpReader struct {
	ctx           sessionctx.Context
	index         *model.IndexInfo
	columns       []*model.ColumnInfo
	table         table.Table
	desc          bool
	conditions    []expression.Expression
	retFieldTypes []*types.FieldType

	idxReader *memIndexReader

	// partition mode
	partitionMode     bool                  // if it is accessing a partition table
	partitionTables   []table.PhysicalTable // partition tables to access
	partitionKVRanges [][]kv.KeyRange       // kv ranges for these partition tables

	cacheTable kv.MemBuffer
}

func buildMemIndexLookUpReader(ctx context.Context, us *UnionScanExec, idxLookUpReader *IndexLookUpExecutor) *memIndexLookUpReader {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("buildMemIndexLookUpReader", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	kvRanges := idxLookUpReader.kvRanges
	outputOffset := []int{len(idxLookUpReader.index.Columns)}
	memIdxReader := &memIndexReader{
		ctx:             us.ctx,
		index:           idxLookUpReader.index,
		table:           idxLookUpReader.table.Meta(),
		kvRanges:        kvRanges,
		desc:            idxLookUpReader.desc,
		retFieldTypes:   retTypes(us),
		outputOffset:    outputOffset,
		belowHandleCols: us.belowHandleCols,
		cacheTable:      us.cacheTable,
	}

	return &memIndexLookUpReader{
		ctx:           us.ctx,
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		desc:          idxLookUpReader.desc,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		idxReader:     memIdxReader,

		partitionMode:     idxLookUpReader.partitionTableMode,
		partitionKVRanges: idxLookUpReader.partitionKVRanges,
		partitionTables:   idxLookUpReader.prunedPartitions,
		cacheTable:        us.cacheTable,
	}
}

func (m *memIndexLookUpReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("memIndexLookUpReader.getMemRows", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	kvRanges := [][]kv.KeyRange{m.idxReader.kvRanges}
	tbls := []table.Table{m.table}
	if m.partitionMode {
		m.idxReader.desc = false // keep-order if always false for IndexLookUp reading partitions so this parameter makes no sense
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
		tblKVRanges = append(tblKVRanges, distsql.TableHandlesToKVRanges(getPhysicalTableID(tbl), handles)...)
	}
	if numHandles == 0 {
		return nil, nil
	}

	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.table, m.columns)
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
		cacheTable: m.cacheTable,
	}

	return memTblReader.getMemRows(ctx)
}

func (m *memIndexLookUpReader) getMemRowsHandle() ([]kv.Handle, error) {
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

	// partition mode
	partitionMode     bool                  // if it is accessing a partition table
	partitionTables   []table.PhysicalTable // partition tables to access
	partitionKVRanges [][][]kv.KeyRange     // kv ranges for these partition tables
}

func buildMemIndexMergeReader(ctx context.Context, us *UnionScanExec, indexMergeReader *IndexMergeReaderExecutor) *memIndexMergeReader {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("buildMemIndexMergeReader", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	indexCount := len(indexMergeReader.indexes)
	memReaders := make([]memReader, 0, indexCount)
	for i := 0; i < indexCount; i++ {
		if indexMergeReader.indexes[i] == nil {
			colIDs, pkColIDs, rd := getColIDAndPkColIDs(indexMergeReader.table, indexMergeReader.columns)
			memReaders = append(memReaders, &memTableReader{
				ctx:           us.ctx,
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
				ctx:             us.ctx,
				index:           indexMergeReader.indexes[i],
				table:           indexMergeReader.table.Meta(),
				kvRanges:        nil,
				desc:            indexMergeReader.descs[i],
				retFieldTypes:   retTypes(us),
				outputOffset:    outputOffset,
				belowHandleCols: us.belowHandleCols,
			})
		}
	}

	return &memIndexMergeReader{
		ctx:              us.ctx,
		table:            indexMergeReader.table,
		columns:          indexMergeReader.columns,
		conditions:       us.conditions,
		retFieldTypes:    retTypes(us),
		indexMergeReader: indexMergeReader,
		memReaders:       memReaders,

		partitionMode:     indexMergeReader.partitionTableMode,
		partitionTables:   indexMergeReader.prunedPartitions,
		partitionKVRanges: indexMergeReader.partitionKeyRanges,
	}
}

func (m *memIndexMergeReader) getMemRows(ctx context.Context) ([][]types.Datum, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("memIndexMergeReader.getMemRows", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
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

	tblKVRanges := make([]kv.KeyRange, 0, 16)
	numHandles := 0
	for i, tbl := range tbls {
		handles, err := m.unionHandles(kvRanges[i])
		if err != nil {
			return nil, err
		}
		if len(handles) == 0 {
			continue
		}
		numHandles += len(handles)
		tblKVRanges = append(tblKVRanges, distsql.TableHandlesToKVRanges(getPhysicalTableID(tbl), handles)...)
	}

	if numHandles == 0 {
		return nil, nil
	}
	colIDs, pkColIDs, rd := getColIDAndPkColIDs(m.table, m.columns)

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

// Union all handles of different Indexes.
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

func (m *memIndexMergeReader) getMemRowsHandle() ([]kv.Handle, error) {
	return nil, errors.New("getMemRowsHandle has not been implemented for memIndexMergeReader")
}

func getColIDAndPkColIDs(table table.Table, columns []*model.ColumnInfo) (map[int64]int, []int64, *rowcodec.BytesDecoder) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}

	tblInfo := table.Meta()
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
	rd := rowcodec.NewByteDecoder(colInfos, pkColIDs, nil, nil)
	return colIDs, pkColIDs, rd
}
