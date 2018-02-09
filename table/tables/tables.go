// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package tables

import (
	"encoding/binary"
	"math"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

// Table implements table.Table interface.
type Table struct {
	ID      int64
	Name    model.CIStr
	Columns []*table.Column

	publicColumns   []*table.Column
	writableColumns []*table.Column
	writableIndices []table.Index
	indices         []table.Index
	recordPrefix    kv.Key
	indexPrefix     kv.Key
	alloc           autoid.Allocator
	meta            *model.TableInfo
}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tableInfo *model.TableInfo) table.Table {
	columns := make([]*table.Column, 0, len(tableInfo.Columns))
	for _, colInfo := range tableInfo.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := newTable(tableInfo.ID, columns, nil)
	t.meta = tableInfo
	return t
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error) {
	if tblInfo.State == model.StateNone {
		return nil, table.ErrTableStateCantNone.Gen("table %s can't be in none state", tblInfo.Name)
	}

	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		if colInfo.State == model.StateNone {
			return nil, table.ErrColumnStateCantNone.Gen("column %s can't be in none state", colInfo.Name)
		}

		col := table.ToColumn(colInfo)
		if col.IsGenerated() {
			expr, err := parseExpression(colInfo.GeneratedExprString)
			if err != nil {
				return nil, errors.Trace(err)
			}
			expr, err = simpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			col.GeneratedExpr = expr
		}
		columns = append(columns, col)
	}

	t := newTable(tblInfo.ID, columns, alloc)

	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			return nil, table.ErrIndexStateCantNone.Gen("index %s can't be in none state", idxInfo.Name)
		}

		idx := NewIndex(tblInfo, idxInfo)
		t.indices = append(t.indices, idx)
	}

	t.meta = tblInfo
	return t, nil
}

// newTable constructs a Table instance.
func newTable(tableID int64, cols []*table.Column, alloc autoid.Allocator) *Table {
	t := &Table{
		ID:           tableID,
		recordPrefix: tablecodec.GenTableRecordPrefix(tableID),
		indexPrefix:  tablecodec.GenTableIndexPrefix(tableID),
		alloc:        alloc,
		Columns:      cols,
	}

	t.publicColumns = t.Cols()
	t.writableColumns = t.WritableCols()
	t.writableIndices = t.WritableIndices()
	return t
}

// Indices implements table.Table Indices interface.
func (t *Table) Indices() []table.Index {
	return t.indices
}

// WritableIndices implements table.Table WritableIndices interface.
func (t *Table) WritableIndices() []table.Index {
	if len(t.writableIndices) > 0 {
		return t.writableIndices
	}
	writable := make([]table.Index, 0, len(t.indices))
	for _, index := range t.indices {
		s := index.Meta().State
		if s != model.StateDeleteOnly && s != model.StateDeleteReorganization {
			writable = append(writable, index)
		}
	}
	return writable
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (t *Table) DeletableIndices() []table.Index {
	// All indices are deletable because we don't need to check StateNone.
	return t.indices
}

// Meta implements table.Table Meta interface.
func (t *Table) Meta() *model.TableInfo {
	return t.meta
}

// Cols implements table.Table Cols interface.
func (t *Table) Cols() []*table.Column {
	if len(t.publicColumns) > 0 {
		return t.publicColumns
	}
	publicColumns := make([]*table.Column, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		if col.State != model.StatePublic {
			continue
		}
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return publicColumns[0 : maxOffset+1]
}

// WritableCols implements table WritableCols interface.
func (t *Table) WritableCols() []*table.Column {
	if len(t.writableColumns) > 0 {
		return t.writableColumns
	}
	writableColumns := make([]*table.Column, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		writableColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return writableColumns[0 : maxOffset+1]
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (t *Table) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *Table) IndexPrefix() kv.Key {
	return t.indexPrefix
}

// RecordKey implements table.Table RecordKey interface.
func (t *Table) RecordKey(h int64) kv.Key {
	return tablecodec.EncodeRecordKey(t.recordPrefix, h)
}

// FirstKey implements table.Table FirstKey interface.
func (t *Table) FirstKey() kv.Key {
	return t.RecordKey(math.MinInt64)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *Table) UpdateRecord(ctx context.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	txn := ctx.Txn()
	// TODO: reuse bs, like AddRecord does.
	bs := kv.NewBufferStore(txn, kv.DefaultTxnMembufCap)

	// rebuild index
	err := t.rebuildIndices(ctx, bs, h, touched, oldData, newData)
	if err != nil {
		return errors.Trace(err)
	}

	var colIDs, binlogColIDs []int64
	var row, binlogOldRow, binlogNewRow []types.Datum
	colIDs = make([]int64, 0, len(newData))
	row = make([]types.Datum, 0, len(newData))
	if shouldWriteBinlog(ctx) {
		binlogColIDs = make([]int64, 0, len(newData))
		binlogOldRow = make([]types.Datum, 0, len(newData))
		binlogNewRow = make([]types.Datum, 0, len(newData))
	}

	for _, col := range t.WritableCols() {
		var value types.Datum
		if col.State != model.StatePublic {
			// If col is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the orignal data(it's changed by other TiDBs.) or the orignal default value.
			// TODO: Use newData directly.
			value = oldData[col.Offset]
		} else {
			value = newData[col.Offset]
		}
		if !t.canSkip(col, value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
		if shouldWriteBinlog(ctx) && !t.canSkipUpdateBinlog(col, value) {
			binlogColIDs = append(binlogColIDs, col.ID)
			binlogOldRow = append(binlogOldRow, oldData[col.Offset])
			binlogNewRow = append(binlogNewRow, value)
		}
	}

	key := t.RecordKey(h)
	value, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	if err = bs.Set(key, value); err != nil {
		return errors.Trace(err)
	}
	if err = bs.SaveTo(txn); err != nil {
		return errors.Trace(err)
	}
	if shouldWriteBinlog(ctx) {
		err = t.addUpdateBinlog(ctx, binlogOldRow, binlogNewRow, binlogColIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *Table) rebuildIndices(ctx context.Context, rm kv.RetrieverMutator, h int64, touched []bool, oldData []types.Datum, newData []types.Datum) error {
	for _, idx := range t.DeletableIndices() {
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if err = t.removeRowIndex(ctx.GetSessionVars().StmtCtx, rm, h, oldVs, idx); err != nil {
				return errors.Trace(err)
			}
			break
		}
	}
	for _, idx := range t.WritableIndices() {
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			newVs, err := idx.FetchValues(newData, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if err := t.buildIndexForRow(ctx, rm, h, newVs, idx); err != nil {
				return errors.Trace(err)
			}
			break
		}
	}
	return nil
}

// adjustRowValuesBuf adjust writeBufs.AddRowValues length, AddRowValues stores the inserting values that is used
// by tablecodec.EncodeRow, the encoded row format is `id1, colval, id2, colval`, so the correct length is rowLen * 2. If
// the inserting row has null value, AddRecord will skip it, so the rowLen will be different, so we need to adjust it.
func adjustRowValuesBuf(writeBufs *variable.WriteStmtBufs, rowLen int) {
	adjustLen := rowLen * 2
	if writeBufs.AddRowValues == nil || cap(writeBufs.AddRowValues) < adjustLen {
		writeBufs.AddRowValues = make([]types.Datum, adjustLen)
	}
	writeBufs.AddRowValues = writeBufs.AddRowValues[:adjustLen]
}

// getRollbackableMemStore get a rollbackable BufferStore, when we are importing data,
// Just add the kv to transaction's membuf directly.
func (t *Table) getRollbackableMemStore(ctx context.Context) kv.RetrieverMutator {
	if ctx.GetSessionVars().ImportingData {
		return ctx.Txn()
	}

	bs := ctx.GetSessionVars().GetWriteStmtBufs().BufStore
	if bs == nil {
		bs = kv.NewBufferStore(ctx.Txn(), kv.DefaultTxnMembufCap)
	} else {
		bs.Reset()
	}
	return bs
}

// AddRecord implements table.Table AddRecord interface.
func (t *Table) AddRecord(ctx context.Context, r []types.Datum, skipHandleCheck bool) (recordID int64, err error) {
	var hasRecordID bool
	for _, col := range t.Cols() {
		if col.IsPKHandleColumn(t.meta) {
			recordID = r[col.Offset].GetInt64()
			hasRecordID = true
			break
		}
	}
	if !hasRecordID {
		recordID, err = t.AllocAutoID(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	txn := ctx.Txn()
	sessVars := ctx.GetSessionVars()
	// when ImportingData or BatchCheck is true,
	// no needs to check the key constrains, so we names the variable skipCheck.
	skipCheck := sessVars.ImportingData || ctx.GetSessionVars().StmtCtx.BatchCheck
	if skipCheck {
		txn.SetOption(kv.SkipCheckForWrite, true)
	}

	rm := t.getRollbackableMemStore(ctx)
	// Insert new entries into indices.
	h, err := t.addIndices(ctx, recordID, r, rm, skipHandleCheck)
	if err != nil {
		return h, errors.Trace(err)
	}

	var colIDs, binlogColIDs []int64
	var row, binlogRow []types.Datum
	colIDs = make([]int64, 0, len(r))
	row = make([]types.Datum, 0, len(r))

	for _, col := range t.WritableCols() {
		var value types.Datum
		if col.State != model.StatePublic {
			// If col is in write only or write reorganization state, we must add it with its default value.
			value, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			value = r[col.Offset]
		}
		if !t.canSkip(col, value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
	}
	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(row))
	key := t.RecordKey(recordID)
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues)
	if err != nil {
		return 0, errors.Trace(err)
	}
	value := writeBufs.RowValBuf
	if err = txn.Set(key, value); err != nil {
		return 0, errors.Trace(err)
	}
	if !sessVars.ImportingData {
		if err = rm.(*kv.BufferStore).SaveTo(txn); err != nil {
			return 0, errors.Trace(err)
		}
	}
	if shouldWriteBinlog(ctx) {
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		err = t.addInsertBinlog(ctx, recordID, binlogRow, binlogColIDs)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	sessVars.StmtCtx.AddAffectedRows(1)
	sessVars.TxnCtx.UpdateDeltaForTable(t.ID, 1, 1)
	return recordID, nil
}

// genIndexKeyStr generates index content string representation.
func (t *Table) genIndexKeyStr(colVals []types.Datum) (string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return "", errors.Trace(err)
			}
		}
		strVals = append(strVals, cvs)
	}
	return strings.Join(strVals, "-"), nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *Table) addIndices(ctx context.Context, recordID int64, r []types.Datum, rm kv.RetrieverMutator, skipHandleCheck bool) (int64, error) {
	txn := ctx.Txn()
	// Clean up lazy check error environment
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	skipCheck := ctx.GetSessionVars().ImportingData || ctx.GetSessionVars().StmtCtx.BatchCheck
	if t.meta.PKIsHandle && !skipCheck && !skipHandleCheck {
		if err := CheckHandleExists(ctx, t, recordID); err != nil {
			return recordID, errors.Trace(err)
		}
	}

	writeBufs := ctx.GetSessionVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	for _, v := range t.WritableIndices() {
		var err2 error
		indexVals, err2 = v.FetchValues(r, indexVals)
		if err2 != nil {
			return 0, errors.Trace(err2)
		}
		var dupKeyErr error
		if !skipCheck && (v.Meta().Unique || v.Meta().Primary) {
			entryKey, err1 := t.genIndexKeyStr(indexVals)
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			dupKeyErr = kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'", entryKey, v.Meta().Name)
			txn.SetOption(kv.PresumeKeyNotExistsError, dupKeyErr)
		}
		if dupHandle, err := v.Create(ctx, rm, indexVals, recordID); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, errors.Trace(dupKeyErr)
			}
			return 0, errors.Trace(err)
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return 0, nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *Table) RowWithCols(ctx context.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := t.RecordKey(h)
	value, err := ctx.Txn().Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Decode raw row data.
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(t.meta) {
			if mysql.HasUnsignedFlag(col.Flag) {
				v[i].SetUint64(uint64(h))
			} else {
				v[i].SetInt64(h)
			}
			continue
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRow(value, colTps, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(t.meta) {
			continue
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *Table) Row(ctx context.Context, h int64) ([]types.Datum, error) {
	r, err := t.RowWithCols(ctx, h, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *Table) RemoveRecord(ctx context.Context, h int64, r []types.Datum) error {
	err := t.removeRowData(ctx, h)
	if err != nil {
		return errors.Trace(err)
	}
	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		return errors.Trace(err)
	}
	if shouldWriteBinlog(ctx) {
		colIDs := make([]int64, 0, len(t.Cols()))
		for _, col := range t.Cols() {
			colIDs = append(colIDs, col.ID)
		}
		err = t.addDeleteBinlog(ctx, r, colIDs)
	}
	return errors.Trace(err)
}

func (t *Table) addInsertBinlog(ctx context.Context, h int64, row []types.Datum, colIDs []int64) error {
	mutation := t.getMutation(ctx)
	pk, err := codec.EncodeValue(ctx.GetSessionVars().StmtCtx, nil, types.NewIntDatum(h))
	if err != nil {
		return errors.Trace(err)
	}
	value, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, row, colIDs, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *Table) addUpdateBinlog(ctx context.Context, oldRow, newRow []types.Datum, colIDs []int64) error {
	old, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, oldRow, colIDs, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	newVal, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, newRow, colIDs, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	bin := append(old, newVal...)
	mutation := t.getMutation(ctx)
	mutation.UpdatedRows = append(mutation.UpdatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Update)
	return nil
}

func (t *Table) addDeleteBinlog(ctx context.Context, r []types.Datum, colIDs []int64) error {
	data, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, r, colIDs, nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	mutation := t.getMutation(ctx)
	mutation.DeletedRows = append(mutation.DeletedRows, data)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_DeleteRow)
	return nil
}

func (t *Table) removeRowData(ctx context.Context, h int64) error {
	// Remove row data.
	err := ctx.Txn().Delete([]byte(t.RecordKey(h)))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// removeRowIndices removes all the indices of a row.
func (t *Table) removeRowIndices(ctx context.Context, h int64, rec []types.Datum) error {
	for _, v := range t.DeletableIndices() {
		vals, err := v.FetchValues(rec, nil)
		if err != nil {
			log.Infof("remove row index %v failed %v, txn %d, handle %d, data %v", v.Meta(), err, ctx.Txn().StartTS, h, rec)
			return errors.Trace(err)
		}
		if err = v.Delete(ctx.GetSessionVars().StmtCtx, ctx.Txn(), vals, h); err != nil {
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				log.Debugf("remove row index %v doesn't exist, txn %d, handle %d", v.Meta(), ctx.Txn().StartTS, h)
				continue
			}
			return errors.Trace(err)
		}
	}
	return nil
}

// removeRowIndex implements table.Table RemoveRowIndex interface.
func (t *Table) removeRowIndex(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index) error {
	if err := idx.Delete(sc, rm, vals, h); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *Table) buildIndexForRow(ctx context.Context, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index) error {
	if _, err := idx.Create(ctx, rm, vals, h); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *Table) IterRecords(ctx context.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	it, err := ctx.Txn().Seek(startKey)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	log.Debugf("startKey:%q, key:%q, value:%q", startKey, it.Key(), it.Value())

	colMap := make(map[int64]*types.FieldType)
	for _, col := range cols {
		colMap[col.ID] = &col.FieldType
	}
	prefix := t.RecordPrefix()
	defaultVals := make([]types.Datum, len(cols))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rowMap, err := tablecodec.DecodeRow(it.Value(), colMap, ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return errors.Trace(err)
		}
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.meta) {
				if mysql.HasUnsignedFlag(col.Flag) {
					data[col.Offset].SetUint64(uint64(handle))
				} else {
					data[col.Offset].SetInt64(handle)
				}
				continue
			}
			if _, ok := rowMap[col.ID]; ok {
				data[col.Offset] = rowMap[col.ID]
				continue
			}
			data[col.Offset], err = GetColDefaultValue(ctx, col, defaultVals)
			if err != nil {
				return errors.Trace(err)
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := t.RecordKey(handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx context.Context, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.OriginDefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
		return colVal, errors.New("Miss column")
	}
	if col.State != model.StatePublic {
		return colVal, nil
	}
	if defaultVals[col.Offset].IsNull() {
		colVal, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
		if err != nil {
			return colVal, errors.Trace(err)
		}
		defaultVals[col.Offset] = colVal
	} else {
		colVal = defaultVals[col.Offset]
	}

	return colVal, nil
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *Table) AllocAutoID(ctx context.Context) (int64, error) {
	rowID, err := t.Allocator(ctx).Alloc(t.ID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if t.meta.ShardRowIDBits > 0 {
		txnCtx := ctx.GetSessionVars().TxnCtx
		if txnCtx.Shard == nil {
			shard := t.calcShard(txnCtx.StartTS)
			txnCtx.Shard = &shard
		}
		rowID |= *txnCtx.Shard
	}
	return rowID, nil
}

func (t *Table) calcShard(startTS uint64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], startTS)
	hashVal := int64(murmur3.Sum32(buf[:]))
	return (hashVal & (1<<t.meta.ShardRowIDBits - 1)) << (64 - t.meta.ShardRowIDBits - 1)
}

// Allocator implements table.Table Allocator interface.
func (t *Table) Allocator(ctx context.Context) autoid.Allocator {
	if ctx != nil {
		sessAlloc := ctx.GetSessionVars().IDAllocator
		if sessAlloc != nil {
			return sessAlloc
		}
	}
	return t.alloc
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (t *Table) RebaseAutoID(ctx context.Context, newBase int64, isSetStep bool) error {
	return t.Allocator(ctx).Rebase(t.ID, newBase, isSetStep)
}

// Seek implements table.Table Seek interface.
func (t *Table) Seek(ctx context.Context, h int64) (int64, bool, error) {
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.ID, h)
	iter, err := ctx.Txn().Seek(seekKey)
	if !iter.Valid() || !iter.Key().HasPrefix(t.RecordPrefix()) {
		// No more records in the table, skip to the end.
		return 0, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	return handle, true, nil
}

// Type implements table.Table Type interface.
func (t *Table) Type() table.Type {
	return table.NormalTable
}

func shouldWriteBinlog(ctx context.Context) bool {
	if ctx.GetSessionVars().BinlogClient == nil {
		return false
	}
	return !ctx.GetSessionVars().InRestrictedSQL
}

func (t *Table) getMutation(ctx context.Context) *binlog.TableMutation {
	return ctx.StmtGetMutation(t.ID)
}

// canSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that;
// 3. the column is virtual generated.
func (t *Table) canSkip(col *table.Column, value types.Datum) bool {
	if col.IsPKHandleColumn(t.meta) {
		return true
	}
	if col.DefaultValue == nil && value.IsNull() {
		return true
	}
	if col.IsGenerated() && !col.GeneratedStored {
		return true
	}
	return false
}

// canSkipUpdateBinlog checks whether the column can be skiped or not.
func (t *Table) canSkipUpdateBinlog(col *table.Column, value types.Datum) bool {
	if col.IsGenerated() && !col.GeneratedStored {
		return true
	}
	return false
}

var (
	recordPrefixSep = []byte("_r")
)

// FindIndexByColName implements table.Table FindIndexByColName interface.
func FindIndexByColName(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		// only public index can be read.
		if idx.Meta().State != model.StatePublic {
			continue
		}

		if len(idx.Meta().Columns) == 1 && strings.EqualFold(idx.Meta().Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

// CheckHandleExists check whether recordID key exists. if not exists, return nil,
// otherwise return kv.ErrKeyExists error.
func CheckHandleExists(ctx context.Context, t table.Table, recordID int64) error {
	txn := ctx.Txn()
	// Check key exists.
	recordKey := t.RecordKey(recordID)
	e := kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", recordID)
	txn.SetOption(kv.PresumeKeyNotExistsError, e)
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	_, err := txn.Get(recordKey)
	if err == nil {
		return errors.Trace(e)
	} else if !kv.ErrNotExist.Equal(err) {
		return errors.Trace(err)
	}
	return nil
}

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
}
