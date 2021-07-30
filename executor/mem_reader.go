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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
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
	// belowHandleIndex is the handle's position of the below scan plan.
	belowHandleIndex int
}

func buildMemIndexReader(us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
	kvRanges := idxReader.kvRanges
	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	return &memIndexReader{
		ctx:              us.ctx,
		index:            idxReader.index,
		table:            idxReader.table.Meta(),
		kvRanges:         kvRanges,
		desc:             us.desc,
		conditions:       us.conditions,
		addedRows:        make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes:    retTypes(us),
		outputOffset:     outputOffset,
		belowHandleIndex: us.belowHandleIndex,
	}
}

func (m *memIndexReader) getMemRows() ([][]types.Datum, error) {
	tps := make([]*types.FieldType, 0, len(m.index.Columns)+1)
	cols := m.table.Columns
	for _, col := range m.index.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	if m.table.PKIsHandle {
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				tps = append(tps, &col.FieldType)
				break
			}
		}
	} else {
		// ExtraHandle Column tp.
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}

	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.kvRanges, func(key, value []byte) error {
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
	pkStatus := tablecodec.PrimaryKeyIsSigned
	if mysql.HasUnsignedFlag(tps[len(tps)-1].Flag) {
		pkStatus = tablecodec.PrimaryKeyIsUnsigned
	}
	values, err := tablecodec.DecodeIndexKV(key, value, len(m.index.Columns), pkStatus)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ds := make([]types.Datum, 0, len(m.outputOffset))
	for _, offset := range m.outputOffset {
		d, err := tablecodec.DecodeColumnValue(values[offset], tps[offset], m.ctx.GetSessionVars().TimeZone)
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
	// cache for decode handle.
	handleBytes []byte
}

func buildMemTableReader(us *UnionScanExec, tblReader *TableReaderExecutor) *memTableReader {
	colIDs := make(map[int64]int)
	for i, col := range us.columns {
		colIDs[col.ID] = i
	}

	return &memTableReader{
		ctx:           us.ctx,
		table:         us.table.Meta(),
		columns:       us.columns,
		kvRanges:      tblReader.kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: retTypes(us),
		colIDs:        colIDs,
		handleBytes:   make([]byte, 0, 16),
	}
}

// TODO: Try to make memXXXReader lazy, There is no need to decode many rows when parent operator only need 1 row.
func (m *memTableReader) getMemRows() ([][]types.Datum, error) {
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.kvRanges, func(key, value []byte) error {
		row, err := m.decodeRecordKeyValue(key, value)
		if err != nil {
			return err
		}

		mutableRow.SetDatums(row...)
		matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
		if err != nil || !matched {
			return err
		}
		m.addedRows = append(m.addedRows, row)
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

func (m *memTableReader) decodeRecordKeyValue(key, value []byte) ([]types.Datum, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decodeRowData(m.ctx, m.table, m.columns, m.colIDs, handle, m.handleBytes, value)
}

// decodeRowData uses to decode row data value.
func decodeRowData(ctx sessionctx.Context, tb *model.TableInfo, columns []*model.ColumnInfo, colIDs map[int64]int, handle int64, cacheBytes, value []byte) ([]types.Datum, error) {
	values, err := getRowData(ctx.GetSessionVars().StmtCtx, tb, columns, colIDs, handle, cacheBytes, value)
	if err != nil {
		return nil, err
	}
	ds := make([]types.Datum, 0, len(columns))
	for _, col := range columns {
		offset := colIDs[col.ID]
		d, err := tablecodec.DecodeColumnValue(values[offset], &col.FieldType, ctx.GetSessionVars().TimeZone)
		if err != nil {
			return nil, err
		}
		ds = append(ds, d)
	}
	return ds, nil
}

// getRowData decodes raw byte slice to row data.
func getRowData(ctx *stmtctx.StatementContext, tb *model.TableInfo, columns []*model.ColumnInfo, colIDs map[int64]int, handle int64, cacheBytes, value []byte) ([][]byte, error) {
	pkIsHandle := tb.PKIsHandle
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.ID
		offset := colIDs[id]
		if (pkIsHandle && mysql.HasPriKeyFlag(col.Flag)) || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(col.Flag) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(ctx, cacheBytes, handleDatum)
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

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

type processKVFunc func(key, value []byte) error

func iterTxnMemBuffer(ctx sessionctx.Context, kvRanges []kv.KeyRange, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, rg := range kvRanges {
		iter, err := txn.GetMemBufferSnapshot().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return err
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

func reverseDatumSlice(rows [][]types.Datum) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (m *memIndexReader) getMemRowsHandle() ([]int64, error) {
	pkTp := types.NewFieldType(mysql.TypeLonglong)
	if m.table.PKIsHandle {
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkTp = &col.FieldType
				break
			}
		}
	}
	handles := make([]int64, 0, m.addedRowsLen)
	err := iterTxnMemBuffer(m.ctx, m.kvRanges, func(key, value []byte) error {
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(m.index.Columns), pkTp)
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
}

func buildMemIndexLookUpReader(us *UnionScanExec, idxLookUpReader *IndexLookUpExecutor) *memIndexLookUpReader {
	kvRanges := idxLookUpReader.kvRanges
	outputOffset := []int{len(idxLookUpReader.index.Columns)}
	memIdxReader := &memIndexReader{
		ctx:              us.ctx,
		index:            idxLookUpReader.index,
		table:            idxLookUpReader.table.Meta(),
		kvRanges:         kvRanges,
		desc:             idxLookUpReader.desc,
		addedRowsLen:     len(us.dirty.addedRows),
		retFieldTypes:    retTypes(us),
		outputOffset:     outputOffset,
		belowHandleIndex: us.belowHandleIndex,
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
	}
}

func (m *memIndexLookUpReader) getMemRows() ([][]types.Datum, error) {
	handles, err := m.idxReader.getMemRowsHandle()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	tblKVRanges := distsql.TableHandlesToKVRanges(getPhysicalTableID(m.table), handles)
	colIDs := make(map[int64]int, len(m.columns))
	for i, col := range m.columns {
		colIDs[col.ID] = i
	}

	memTblReader := &memTableReader{
		ctx:           m.ctx,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKVRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, len(handles)),
		retFieldTypes: m.retFieldTypes,
		colIDs:        colIDs,
		handleBytes:   make([]byte, 0, 16),
	}

	return memTblReader.getMemRows()
}
