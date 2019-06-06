// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"encoding/binary"

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
	baseExecutor
	index         *model.IndexInfo
	table         *model.TableInfo
	kvRanges      []kv.KeyRange
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	outputOffset  []int
	// cache for decode handle.
	handleBytes   []byte
	memIdxHandles map[int64]struct{}
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
		baseExecutor:     us.baseExecutor,
		index:            idxReader.index,
		table:            idxReader.table.Meta(),
		kvRanges:         kvRanges,
		desc:             us.desc,
		conditions:       us.conditions,
		addedRows:        make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes:    us.retTypes(),
		outputOffset:     outputOffset,
		handleBytes:      make([]byte, 0, 16),
		memIdxHandles:    make(map[int64]struct{}, len(us.dirty.addedRows)),
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
		handle := data[m.belowHandleIndex].GetInt64()
		m.memIdxHandles[handle] = struct{}{}

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
	handles := make([]int64, 0, cap(m.addedRows))
	err := iterTxnMemBuffer(m.ctx, m.kvRanges, func(key, value []byte) error {
		handle, err := m.decodeIndexHandle(key, value, pkTp)
		if err != nil {
			return err
		}
		handles = append(handles, handle)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, h := range handles {
		m.memIdxHandles[h] = struct{}{}
	}
	if m.desc {
		for i, j := 0, len(handles)-1; i < j; i, j = i+1, j-1 {
			handles[i], handles[j] = handles[j], handles[i]
		}
	}
	return handles, nil
}

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType) ([]types.Datum, error) {
	// this is from indexScanExec decodeIndexKV method.
	values, b, err := tablecodec.CutIndexKeyNew(key, len(m.index.Columns))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(b) > 0 {
		values = append(values, b)
	} else if len(value) >= 8 {
		handle, err := decodeHandle(value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var handleDatum types.Datum
		if mysql.HasUnsignedFlag(tps[len(tps)-1].Flag) {
			handleDatum = types.NewUintDatum(uint64(handle))
		} else {
			handleDatum = types.NewIntDatum(handle)
		}
		m.handleBytes, err = codec.EncodeValue(m.ctx.GetSessionVars().StmtCtx, m.handleBytes[:0], handleDatum)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values = append(values, m.handleBytes)
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

func (m *memIndexReader) decodeIndexHandle(key, value []byte, pkTp *types.FieldType) (int64, error) {
	_, b, err := tablecodec.CutIndexKeyNew(key, len(m.index.Columns))
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(b) > 0 {
		d, err := tablecodec.DecodeColumnValue(b, pkTp, m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.GetInt64(), nil

	} else if len(value) >= 8 {
		return decodeHandle(value)
	}
	// Should never execute to here.
	return 0, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

type memTableReader struct {
	baseExecutor
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
	kvRanges := tblReader.kvRanges
	colIDs := make(map[int64]int)
	for i, col := range tblReader.columns {
		colIDs[col.ID] = i
	}

	return &memTableReader{
		baseExecutor:  us.baseExecutor,
		table:         tblReader.table.Meta(),
		columns:       us.columns,
		kvRanges:      kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		colIDs:        colIDs,
		handleBytes:   make([]byte, 0, 16),
	}
}

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

type memIndexLookUpReader struct {
	baseExecutor
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
		baseExecutor:     us.baseExecutor,
		index:            idxLookUpReader.index,
		table:            idxLookUpReader.table.Meta(),
		kvRanges:         kvRanges,
		desc:             idxLookUpReader.desc,
		addedRows:        make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes:    us.retTypes(),
		outputOffset:     outputOffset,
		handleBytes:      make([]byte, 0, 16),
		memIdxHandles:    make(map[int64]struct{}, len(us.dirty.addedRows)),
		belowHandleIndex: us.belowHandleIndex,
	}

	return &memIndexLookUpReader{
		baseExecutor:  us.baseExecutor,
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		desc:          idxLookUpReader.desc,
		conditions:    us.conditions,
		retFieldTypes: us.retTypes(),

		idxReader: memIdxReader,
	}
}

func (m *memIndexLookUpReader) getMemRows() ([][]types.Datum, error) {
	handles, err := m.idxReader.getMemRowsHandle()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	tblKvRanges := distsql.TableHandlesToKVRanges(getPhysicalTableID(m.table), handles)
	colIDs := make(map[int64]int)
	for i, col := range m.columns {
		colIDs[col.ID] = i
	}

	memTblReader := &memTableReader{
		baseExecutor:  m.baseExecutor,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKvRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, len(handles)),
		retFieldTypes: m.retTypes(),
		colIDs:        colIDs,
		handleBytes:   m.idxReader.handleBytes,
	}

	return memTblReader.getMemRows()
}

type processKVFunc func(key, value []byte) error

func iterTxnMemBuffer(ctx sessionctx.Context, kvRanges []kv.KeyRange, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, rg := range kvRanges {
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
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
