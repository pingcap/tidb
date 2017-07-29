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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-binlog"
)

// Table implements table.Table interface.
type Table struct {
	ID      int64
	Name    model.CIStr
	Columns []*table.Column

	publicColumns   []*table.Column
	writableColumns []*table.Column
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
		if len(colInfo.GeneratedExprString) != 0 {
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
	return t
}

// Indices implements table.Table Indices interface.
func (t *Table) Indices() []table.Index {
	return t.indices
}

// WritableIndices implements table.Table WritableIndices interface.
func (t *Table) WritableIndices() []table.Index {
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
	return t.RecordKey(0)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *Table) UpdateRecord(ctx context.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	txn := ctx.Txn()
	bs := kv.NewBufferStore(txn)

	// rebuild index
	err := t.rebuildIndices(bs, h, touched, oldData, newData)
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
			// If col is in write only or write reorganization state
			// and the value is not default, keep the original value.
			value, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
			if err != nil {
				return errors.Trace(err)
			}
			cmp, err := oldData[col.Offset].CompareDatum(ctx.GetSessionVars().StmtCtx, value)
			if err != nil {
				return errors.Trace(err)
			}
			if cmp != 0 {
				value = oldData[col.Offset]
			}
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
	value, err := tablecodec.EncodeRow(row, colIDs, ctx.GetSessionVars().GetTimeZone())
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
		t.addUpdateBinlog(ctx, binlogOldRow, binlogNewRow, binlogColIDs)
	}
	return nil
}

func (t *Table) rebuildIndices(rm kv.RetrieverMutator, h int64, touched []bool, oldData []types.Datum, newData []types.Datum) error {
	for _, idx := range t.DeletableIndices() {
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			oldVs, err := idx.FetchValues(oldData)
			if err != nil {
				return errors.Trace(err)
			}
			if t.removeRowIndex(rm, h, oldVs, idx); err != nil {
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
			newVs, err := idx.FetchValues(newData)
			if err != nil {
				return errors.Trace(err)
			}
			if err := t.buildIndexForRow(rm, h, newVs, idx); err != nil {
				return errors.Trace(err)
			}
			break
		}
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *Table) AddRecord(ctx context.Context, r []types.Datum) (recordID int64, err error) {
	var hasRecordID bool
	for _, col := range t.Cols() {
		if col.IsPKHandleColumn(t.meta) {
			recordID = r[col.Offset].GetInt64()
			hasRecordID = true
			break
		}
	}
	if !hasRecordID {
		recordID, err = t.alloc.Alloc(t.ID)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	txn := ctx.Txn()
	bs := kv.NewBufferStore(txn)

	skipCheck := ctx.GetSessionVars().SkipConstraintCheck
	if skipCheck {
		txn.SetOption(kv.SkipCheckForWrite, true)
	}

	// Insert new entries into indices.
	h, err := t.addIndices(ctx, recordID, r, bs)
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

	key := t.RecordKey(recordID)
	value, err := tablecodec.EncodeRow(row, colIDs, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return 0, errors.Trace(err)
	}
	if err = txn.Set(key, value); err != nil {
		return 0, errors.Trace(err)
	}
	if err = bs.SaveTo(txn); err != nil {
		return 0, errors.Trace(err)
	}
	if shouldWriteBinlog(ctx) {
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		t.addInsertBinlog(ctx, recordID, binlogRow, binlogColIDs)
	}
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.ID, 1, 1)
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
func (t *Table) addIndices(ctx context.Context, recordID int64, r []types.Datum, bs *kv.BufferStore) (int64, error) {
	txn := ctx.Txn()
	// Clean up lazy check error environment
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	skipCheck := ctx.GetSessionVars().SkipConstraintCheck
	if t.meta.PKIsHandle && !skipCheck {
		// Check key exists.
		recordKey := t.RecordKey(recordID)
		e := kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", recordID)
		txn.SetOption(kv.PresumeKeyNotExistsError, e)
		_, err := txn.Get(recordKey)
		if err == nil {
			return recordID, errors.Trace(e)
		} else if !terror.ErrorEqual(err, kv.ErrNotExist) {
			return 0, errors.Trace(err)
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}

	for _, v := range t.WritableIndices() {
		colVals, err2 := v.FetchValues(r)
		if err2 != nil {
			return 0, errors.Trace(err2)
		}
		var dupKeyErr error
		if !skipCheck && (v.Meta().Unique || v.Meta().Primary) {
			entryKey, err1 := t.genIndexKeyStr(colVals)
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			dupKeyErr = kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'", entryKey, v.Meta().Name)
			txn.SetOption(kv.PresumeKeyNotExistsError, dupKeyErr)
		}
		if dupHandle, err := v.Create(bs, colVals, recordID); err != nil {
			if terror.ErrorEqual(err, kv.ErrKeyExists) {
				return dupHandle, errors.Trace(dupKeyErr)
			}
			return 0, errors.Trace(err)
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}
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
	pk, err := codec.EncodeValue(nil, types.NewIntDatum(h))
	if err != nil {
		return errors.Trace(err)
	}
	value, err := tablecodec.EncodeRow(row, colIDs, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *Table) addUpdateBinlog(ctx context.Context, oldRow, newRow []types.Datum, colIDs []int64) error {
	old, err := tablecodec.EncodeRow(oldRow, colIDs, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	new, err := tablecodec.EncodeRow(newRow, colIDs, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	bin := append(old, new...)
	mutation := t.getMutation(ctx)
	mutation.UpdatedRows = append(mutation.UpdatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Update)
	return nil
}

func (t *Table) addDeleteBinlog(ctx context.Context, r []types.Datum, colIDs []int64) error {
	data, err := tablecodec.EncodeRow(r, colIDs, ctx.GetSessionVars().GetTimeZone())
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
		vals, err := v.FetchValues(rec)
		if vals == nil {
			// TODO: check this
			continue
		}
		if err = v.Delete(ctx.Txn(), vals, h); err != nil {
			if v.Meta().State != model.StatePublic && terror.ErrorEqual(err, kv.ErrNotExist) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				continue
			}
			return errors.Trace(err)
		}
	}
	return nil
}

// removeRowIndex implements table.Table RemoveRowIndex interface.
func (t *Table) removeRowIndex(rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index) error {
	if err := idx.Delete(rm, vals, h); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *Table) buildIndexForRow(rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index) error {
	if _, err := idx.Create(rm, vals, h); err != nil {
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
func (t *Table) AllocAutoID() (int64, error) {
	return t.alloc.Alloc(t.ID)
}

// Allocator implements table.Table Allocator interface.
func (t *Table) Allocator() autoid.Allocator {
	return t.alloc
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (t *Table) RebaseAutoID(newBase int64, isSetStep bool) error {
	return t.alloc.Rebase(t.ID, newBase, isSetStep)
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

func shouldWriteBinlog(ctx context.Context) bool {
	if ctx.GetSessionVars().BinlogClient == nil {
		return false
	}
	return !ctx.GetSessionVars().InRestrictedSQL
}

func (t *Table) getMutation(ctx context.Context) *binlog.TableMutation {
	bin := binloginfo.GetPrewriteValue(ctx, true)
	for i := range bin.Mutations {
		if bin.Mutations[i].TableId == t.ID {
			return &bin.Mutations[i]
		}
	}
	idx := len(bin.Mutations)
	bin.Mutations = append(bin.Mutations, binlog.TableMutation{TableId: t.ID})
	return &bin.Mutations[idx]
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
	if len(col.GeneratedExprString) != 0 && !col.GeneratedStored {
		return true
	}
	return false
}

// canSkipUpdateBinlog checks whether the column can be skiped or not.
func (t *Table) canSkipUpdateBinlog(col *table.Column, value types.Datum) bool {
	if len(col.GeneratedExprString) != 0 && !col.GeneratedStored {
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

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
}
