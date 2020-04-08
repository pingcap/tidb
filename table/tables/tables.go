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
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
)

// TableCommon is shared by both Table and partition.
type TableCommon struct {
	tableID int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID int64
	Columns         []*table.Column
	PublicColumns   []*table.Column
	VisibleColumns  []*table.Column
	HiddenColumns   []*table.Column
	WritableColumns []*table.Column
	writableIndices []table.Index
	indices         []table.Index
	meta            *model.TableInfo
	allocs          autoid.Allocators
	sequence        *sequenceCommon

	// recordPrefix and indexPrefix are generated using physicalTableID.
	recordPrefix kv.Key
	indexPrefix  kv.Key
}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tblInfo *model.TableInfo) table.Table {
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, nil)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initTableIndices(&t); err != nil {
			return nil
		}
		return &t
	}

	ret, err := newPartitionedTable(&t, tblInfo)
	if err != nil {
		return nil
	}
	return ret
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(allocs autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error) {
	if tblInfo.State == model.StateNone {
		return nil, table.ErrTableStateCantNone.GenWithStackByArgs(tblInfo.Name)
	}

	colsLen := len(tblInfo.Columns)
	columns := make([]*table.Column, 0, colsLen)
	for i, colInfo := range tblInfo.Columns {
		if colInfo.State == model.StateNone {
			return nil, table.ErrColumnStateCantNone.GenWithStackByArgs(colInfo.Name)
		}

		// Print some information when the column's offset isn't equal to i.
		if colInfo.Offset != i {
			logutil.BgLogger().Error("wrong table schema", zap.Any("table", tblInfo), zap.Any("column", colInfo), zap.Int("index", i), zap.Int("offset", colInfo.Offset), zap.Int("columnNumber", colsLen))
		}

		col := table.ToColumn(colInfo)
		if col.IsGenerated() {
			expr, err := parseExpression(colInfo.GeneratedExprString)
			if err != nil {
				return nil, err
			}
			expr, err = simpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, err
			}
			col.GeneratedExpr = expr
		}
		// default value is expr.
		if col.DefaultIsExpr {
			expr, err := parseExpression(colInfo.DefaultValue.(string))
			if err != nil {
				return nil, err
			}
			col.DefaultExpr = expr
		}
		columns = append(columns, col)
	}

	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, allocs)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initTableIndices(&t); err != nil {
			return nil, err
		}
		return &t, nil
	}

	return newPartitionedTable(&t, tblInfo)
}

// initTableCommon initializes a TableCommon struct.
func initTableCommon(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators) {
	t.tableID = tblInfo.ID
	t.physicalTableID = physicalTableID
	t.allocs = allocs
	t.meta = tblInfo
	t.Columns = cols
	t.PublicColumns = t.Cols()
	t.VisibleColumns = t.VisibleCols()
	t.HiddenColumns = t.HiddenCols()
	t.WritableColumns = t.WritableCols()
	t.writableIndices = t.WritableIndices()
	t.recordPrefix = tablecodec.GenTableRecordPrefix(physicalTableID)
	t.indexPrefix = tablecodec.GenTableIndexPrefix(physicalTableID)
	if tblInfo.IsSequence() {
		t.sequence = &sequenceCommon{meta: tblInfo.Sequence}
	}
}

// initTableIndices initializes the indices of the TableCommon.
func initTableIndices(t *TableCommon) error {
	tblInfo := t.meta
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			return table.ErrIndexStateCantNone.GenWithStackByArgs(idxInfo.Name)
		}

		// Use partition ID for index, because TableCommon may be table or partition.
		idx := NewIndex(t.physicalTableID, tblInfo, idxInfo)
		t.indices = append(t.indices, idx)
	}
	return nil
}

func initTableCommonWithIndices(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators) error {
	initTableCommon(t, tblInfo, physicalTableID, cols, allocs)
	return initTableIndices(t)
}

// Indices implements table.Table Indices interface.
func (t *TableCommon) Indices() []table.Index {
	return t.indices
}

// WritableIndices implements table.Table WritableIndices interface.
func (t *TableCommon) WritableIndices() []table.Index {
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
func (t *TableCommon) DeletableIndices() []table.Index {
	// All indices are deletable because we don't need to check StateNone.
	return t.indices
}

// Meta implements table.Table Meta interface.
func (t *TableCommon) Meta() *model.TableInfo {
	return t.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (t *TableCommon) GetPhysicalID() int64 {
	return t.physicalTableID
}

type getColsMode int64

const (
	_ getColsMode = iota
	visible
	hidden
	full
)

func (t *TableCommon) getCols(mode getColsMode) []*table.Column {
	columns := make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State != model.StatePublic {
			continue
		}
		if (mode == visible && col.Hidden) || (mode == hidden && !col.Hidden) {
			continue
		}
		columns = append(columns, col)
	}
	return columns
}

// Cols implements table.Table Cols interface.
func (t *TableCommon) Cols() []*table.Column {
	if len(t.PublicColumns) > 0 {
		return t.PublicColumns
	}
	return t.getCols(full)
}

// VisibleCols implements table.Table VisibleCols interface.
func (t *TableCommon) VisibleCols() []*table.Column {
	if len(t.VisibleColumns) > 0 {
		return t.VisibleColumns
	}
	return t.getCols(visible)
}

// HiddenCols implements table.Table HiddenCols interface.
func (t *TableCommon) HiddenCols() []*table.Column {
	if len(t.HiddenColumns) > 0 {
		return t.HiddenColumns
	}
	return t.getCols(hidden)
}

// WritableCols implements table WritableCols interface.
func (t *TableCommon) WritableCols() []*table.Column {
	if len(t.WritableColumns) > 0 {
		return t.WritableColumns
	}
	writableColumns := make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		writableColumns = append(writableColumns, col)
	}
	return writableColumns
}

// DeletableCols implements table DeletableCols interface.
func (t *TableCommon) DeletableCols() []*table.Column {
	return t.Columns
}

// RecordPrefix implements table.Table interface.
func (t *TableCommon) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table interface.
func (t *TableCommon) IndexPrefix() kv.Key {
	return t.indexPrefix
}

// RecordKey implements table.Table interface.
func (t *TableCommon) RecordKey(h int64) kv.Key {
	return tablecodec.EncodeRecordKey(t.recordPrefix, h)
}

// FirstKey implements table.Table interface.
func (t *TableCommon) FirstKey() kv.Key {
	return t.RecordKey(math.MinInt64)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	execBuf := kv.NewStagingBufferStore(txn)
	defer execBuf.Discard()

	// rebuild index
	err = t.rebuildIndices(ctx, execBuf, h, touched, oldData, newData)
	if err != nil {
		return err
	}
	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	var colIDs, binlogColIDs []int64
	var row, binlogOldRow, binlogNewRow []types.Datum
	colIDs = make([]int64, 0, numColsCap)
	row = make([]types.Datum, 0, numColsCap)
	if shouldWriteBinlog(ctx) {
		binlogColIDs = make([]int64, 0, numColsCap)
		binlogOldRow = make([]types.Datum, 0, numColsCap)
		binlogNewRow = make([]types.Datum, 0, numColsCap)
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
	sessVars := ctx.GetSessionVars()
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, rd)
	if err != nil {
		return err
	}
	if err = execBuf.Set(key, value); err != nil {
		return err
	}
	if _, err := execBuf.Flush(); err != nil {
		return err
	}
	ctx.StmtAddDirtyTableOP(table.DirtyTableDeleteRow, t.physicalTableID, h)
	ctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, h)
	if shouldWriteBinlog(ctx) {
		if !t.meta.PKIsHandle {
			binlogColIDs = append(binlogColIDs, model.ExtraHandleID)
			binlogOldRow = append(binlogOldRow, types.NewIntDatum(h))
			binlogNewRow = append(binlogNewRow, types.NewIntDatum(h))
		}
		err = t.addUpdateBinlog(ctx, binlogOldRow, binlogNewRow, binlogColIDs)
		if err != nil {
			return err
		}
	}
	colSize := make(map[int64]int64, len(t.Cols()))
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc, newData[id])
		if err != nil {
			continue
		}
		newLen := size - 1
		size, err = codec.EstimateValueSize(sc, oldData[id])
		if err != nil {
			continue
		}
		oldLen := size - 1
		colSize[col.ID] = int64(newLen - oldLen)
	}
	sessVars.TxnCtx.UpdateDeltaForTable(t.physicalTableID, 0, 1, colSize)
	return nil
}

func (t *TableCommon) rebuildIndices(ctx sessionctx.Context, rm kv.RetrieverMutator, h int64, touched []bool, oldData []types.Datum, newData []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, idx := range t.DeletableIndices() {
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return err
			}
			if err = t.removeRowIndex(ctx.GetSessionVars().StmtCtx, rm, h, oldVs, idx, txn); err != nil {
				return err
			}
			break
		}
	}
	for _, idx := range t.WritableIndices() {
		untouched := true
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		// If txn is auto commit and index is untouched, no need to write index value.
		if untouched && !ctx.GetSessionVars().InTxn() {
			continue
		}
		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, rm, h, newVs, idx, txn, untouched); err != nil {
			return err
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

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID int64, err error) {
	var opt table.AddRecordOpt
	for _, fn := range opts {
		fn.ApplyOn(&opt)
	}
	var hasRecordID bool
	cols := t.Cols()
	// opt.IsUpdate is a flag for update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
	if len(r) > len(cols) && !opt.IsUpdate {
		// The last value is _tidb_rowid.
		recordID = r[len(r)-1].GetInt64()
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		if tblInfo.PKIsHandle {
			recordID = r[tblInfo.GetPkColInfo().Offset].GetInt64()
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if opt.ReserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			stmtCtx := ctx.GetSessionVars().StmtCtx
			stmtCtx.BaseRowID, stmtCtx.MaxRowID, err = allocHandleIDs(ctx, t, uint64(opt.ReserveAutoID))
			if err != nil {
				return 0, err
			}
		}

		recordID, err = AllocHandle(ctx, t)
		if err != nil {
			return 0, err
		}
	}

	txn, err := ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	execBuf := kv.NewStagingBufferStore(txn)
	defer execBuf.Discard()
	sessVars := ctx.GetSessionVars()

	var createIdxOpts []table.CreateIdxOptFunc
	if len(opts) > 0 {
		createIdxOpts = make([]table.CreateIdxOptFunc, 0, len(opts))
		for _, fn := range opts {
			if raw, ok := fn.(table.CreateIdxOptFunc); ok {
				createIdxOpts = append(createIdxOpts, raw)
			}
		}
	}
	// Insert new entries into indices.
	h, err := t.addIndices(ctx, recordID, r, execBuf, createIdxOpts)
	if err != nil {
		return h, err
	}

	var colIDs, binlogColIDs []int64
	var row, binlogRow []types.Datum
	colIDs = make([]int64, 0, len(r))
	row = make([]types.Datum, 0, len(r))

	for _, col := range t.WritableCols() {
		var value types.Datum
		// Update call `AddRecord` will already handle the write only column default value.
		// Only insert should add default value for write only column.
		if col.State != model.StatePublic && !opt.IsUpdate {
			// If col is in write only or write reorganization state, we must add it with its default value.
			value, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
			if err != nil {
				return 0, err
			}
			// add value to `r` for dirty db in transaction.
			// Otherwise when update will panic cause by get value of column in write only state from dirty db.
			if col.Offset < len(r) {
				r[col.Offset] = value
			} else {
				r = append(r, value)
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
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(sc, row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd)
	if err != nil {
		return 0, err
	}
	value := writeBufs.RowValBuf
	if err = execBuf.Set(key, value); err != nil {
		return 0, err
	}

	if _, err := execBuf.Flush(); err != nil {
		return 0, err
	}
	ctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, recordID)

	if shouldWriteBinlog(ctx) {
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		err = t.addInsertBinlog(ctx, recordID, binlogRow, binlogColIDs)
		if err != nil {
			return 0, err
		}
	}
	sc.AddAffectedRows(1)
	if sessVars.TxnCtx == nil {
		return recordID, nil
	}
	colSize := make(map[int64]int64, len(r))
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc, r[id])
		if err != nil {
			continue
		}
		colSize[col.ID] = int64(size) - 1
	}
	sessVars.TxnCtx.UpdateDeltaForTable(t.physicalTableID, 1, 1, colSize)
	return recordID, nil
}

// genIndexKeyStr generates index content string representation.
func (t *TableCommon) genIndexKeyStr(colVals []types.Datum) (string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return "", err
			}
		}
		strVals = append(strVals, cvs)
	}
	return strings.Join(strVals, "-"), nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *TableCommon) addIndices(sctx sessionctx.Context, recordID int64, r []types.Datum, rm kv.RetrieverMutator,
	opts []table.CreateIdxOptFunc) (int64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	// Clean up lazy check error environment
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	var opt table.CreateIdxOpt
	for _, fn := range opts {
		fn(&opt)
	}
	var ctx context.Context
	if opt.Ctx != nil {
		ctx = opt.Ctx
	} else {
		ctx = context.Background()
	}
	skipCheck := sctx.GetSessionVars().StmtCtx.BatchCheck
	if t.meta.PKIsHandle && !skipCheck && !opt.SkipHandleCheck {
		if err := CheckHandleExists(ctx, sctx, t, recordID, nil); err != nil {
			return recordID, err
		}
	}

	writeBufs := sctx.GetSessionVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	for _, v := range t.WritableIndices() {
		indexVals, err = v.FetchValues(r, indexVals)
		if err != nil {
			return 0, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			entryKey, err := t.genIndexKeyStr(indexVals)
			if err != nil {
				return 0, err
			}
			existErrInfo := kv.NewExistErrInfo(v.Meta().Name.String(), entryKey)
			txn.SetOption(kv.PresumeKeyNotExistsError, existErrInfo)
			dupErr = existErrInfo.Err()
		}
		if dupHandle, err := v.Create(sctx, rm, indexVals, recordID, opts...); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return 0, err
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return 0, nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *TableCommon) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := t.RecordKey(h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx, t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h int64, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.Flag) {
				v[i].SetUint64(uint64(h))
			} else {
				v[i].SetInt64(h)
			}
			continue
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRow(value, colTps, ctx.GetSessionVars().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			continue
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// Row implements table.Table Row interface.
func (t *TableCommon) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return t.RowWithCols(ctx, h, t.Cols())
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *TableCommon) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	err := t.removeRowData(ctx, h)
	if err != nil {
		return err
	}
	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		return err
	}

	ctx.StmtAddDirtyTableOP(table.DirtyTableDeleteRow, t.physicalTableID, h)
	if shouldWriteBinlog(ctx) {
		cols := t.Cols()
		colIDs := make([]int64, 0, len(cols)+1)
		for _, col := range cols {
			colIDs = append(colIDs, col.ID)
		}
		var binlogRow []types.Datum
		if !t.meta.PKIsHandle {
			colIDs = append(colIDs, model.ExtraHandleID)
			binlogRow = make([]types.Datum, 0, len(r)+1)
			binlogRow = append(binlogRow, r...)
			binlogRow = append(binlogRow, types.NewIntDatum(h))
		} else {
			binlogRow = r
		}
		err = t.addDeleteBinlog(ctx, binlogRow, colIDs)
	}
	colSize := make(map[int64]int64, len(t.Cols()))
	sc := ctx.GetSessionVars().StmtCtx
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc, r[id])
		if err != nil {
			continue
		}
		colSize[col.ID] = -int64(size - 1)
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.physicalTableID, -1, 1, colSize)
	return err
}

func (t *TableCommon) addInsertBinlog(ctx sessionctx.Context, h int64, row []types.Datum, colIDs []int64) error {
	mutation := t.getMutation(ctx)
	pk, err := codec.EncodeValue(ctx.GetSessionVars().StmtCtx, nil, types.NewIntDatum(h))
	if err != nil {
		return err
	}
	value, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx, row, colIDs, nil, nil)
	if err != nil {
		return err
	}
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *TableCommon) addUpdateBinlog(ctx sessionctx.Context, oldRow, newRow []types.Datum, colIDs []int64) error {
	old, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx, oldRow, colIDs, nil, nil)
	if err != nil {
		return err
	}
	newVal, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx, newRow, colIDs, nil, nil)
	if err != nil {
		return err
	}
	bin := append(old, newVal...)
	mutation := t.getMutation(ctx)
	mutation.UpdatedRows = append(mutation.UpdatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Update)
	return nil
}

func (t *TableCommon) addDeleteBinlog(ctx sessionctx.Context, r []types.Datum, colIDs []int64) error {
	data, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx, r, colIDs, nil, nil)
	if err != nil {
		return err
	}
	mutation := t.getMutation(ctx)
	mutation.DeletedRows = append(mutation.DeletedRows, data)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_DeleteRow)
	return nil
}

func writeSequenceUpdateValueBinlog(ctx sessionctx.Context, db, sequence string, end int64) error {
	// 1: when sequenceCommon update the local cache passively.
	// 2: When sequenceCommon setval to the allocator actively.
	// Both of this two case means the upper bound the sequence has changed in meta, which need to write the binlog
	// to the downstream.
	// Sequence sends `select setval(seq, num)` sql string to downstream via `setDDLBinlog`, which is mocked as a DDL binlog.
	binlogCli := ctx.GetSessionVars().BinlogClient
	sqlMode := ctx.GetSessionVars().SQLMode
	sequenceFullName := stringutil.Escape(db, sqlMode) + "." + stringutil.Escape(sequence, sqlMode)
	sql := "select setval(" + sequenceFullName + ", " + strconv.FormatInt(end, 10) + ")"

	err := kv.RunInNewTxn(ctx.GetStore(), true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		mockJobID, err := m.GenGlobalID()
		if err != nil {
			return err
		}
		binloginfo.SetDDLBinlog(binlogCli, txn, mockJobID, int32(model.StatePublic), sql)
		return nil
	})
	return err
}

func (t *TableCommon) removeRowData(ctx sessionctx.Context, h int64) error {
	// Remove row data.
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	err = txn.Delete([]byte(key))
	if err != nil {
		return err
	}
	return nil
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx sessionctx.Context, h int64, rec []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, v := range t.DeletableIndices() {
		vals, err := v.FetchValues(rec, nil)
		if err != nil {
			logutil.BgLogger().Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.Int64("handle", h), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx.GetSessionVars().StmtCtx, txn, vals, h); err != nil {
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.BgLogger().Debug("row index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.Int64("handle", h))
				continue
			}
			return err
		}
	}
	return nil
}

// removeRowIndex implements table.Table RemoveRowIndex interface.
func (t *TableCommon) removeRowIndex(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index, txn kv.Transaction) error {
	return idx.Delete(sc, rm, vals, h)
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *TableCommon) buildIndexForRow(ctx sessionctx.Context, rm kv.RetrieverMutator, h int64, vals []types.Datum, idx table.Index, txn kv.Transaction, untouched bool) error {
	var opts []table.CreateIdxOptFunc
	if untouched {
		opts = append(opts, table.IndexIsUntouched)
	}
	if _, err := idx.Create(ctx, rm, vals, h, opts...); err != nil {
		if kv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MySQL.
			entryKey, err1 := t.genIndexKeyStr(vals)
			if err1 != nil {
				// if genIndexKeyStr failed, return the original error.
				return err
			}

			return kv.ErrKeyExists.FastGenByArgs(entryKey, idx.Meta().Name)
		}
		return err
	}
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *TableCommon) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	it, err := txn.Iter(startKey, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("iterate records", zap.ByteString("startKey", startKey), zap.ByteString("key", it.Key()), zap.ByteString("value", it.Value()))

	colMap := make(map[int64]*types.FieldType, len(cols))
	for _, col := range cols {
		colMap[col.ID] = &col.FieldType
	}
	defaultVals := make([]types.Datum, len(cols))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return err
		}
		rowMap, err := tablecodec.DecodeRow(it.Value(), colMap, ctx.GetSessionVars().Location())
		if err != nil {
			return err
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
				return err
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return err
		}

		rk := t.RecordKey(handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return err
		}
	}

	return nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx sessionctx.Context, col *table.Column, defaultVals []types.Datum) (
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
			return colVal, err
		}
		defaultVals[col.Offset] = colVal
	} else {
		colVal = defaultVals[col.Offset]
	}

	return colVal, nil
}

// AllocHandle allocate a new handle.
// A statement could reserve some ID in the statement context, try those ones first.
func AllocHandle(ctx sessionctx.Context, t table.Table) (int64, error) {
	if ctx != nil {
		if stmtCtx := ctx.GetSessionVars().StmtCtx; stmtCtx != nil {
			// First try to alloc if the statement has reserved auto ID.
			if stmtCtx.BaseRowID < stmtCtx.MaxRowID {
				stmtCtx.BaseRowID += 1
				return stmtCtx.BaseRowID, nil
			}
		}
	}

	_, rowID, err := allocHandleIDs(ctx, t, 1)
	return rowID, err
}

func allocHandleIDs(ctx sessionctx.Context, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(ctx).Get(autoid.RowIDAllocType).Alloc(meta.ID, n, 1, 1)
	if err != nil {
		return 0, 0, err
	}
	if meta.ShardRowIDBits > 0 {
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(maxID, meta.MaxShardRowIDBits, autoid.RowIDBitLength) {
			// If overflow, the rowID may be duplicated. For examples,
			// t.meta.ShardRowIDBits = 4
			// rowID = 0010111111111111111111111111111111111111111111111111111111111111
			// shard = 0100000000000000000000000000000000000000000000000000000000000000
			// will be duplicated with:
			// rowID = 0100111111111111111111111111111111111111111111111111111111111111
			// shard = 0010000000000000000000000000000000000000000000000000000000000000
			return 0, 0, autoid.ErrAutoincReadFailed
		}
		txnCtx := ctx.GetSessionVars().TxnCtx
		if txnCtx.Shard == nil {
			shard := CalcShard(meta.ShardRowIDBits, txnCtx.StartTS, autoid.RowIDBitLength)
			txnCtx.Shard = &shard
		}
		base |= *txnCtx.Shard
		maxID |= *txnCtx.Shard
	}
	return base, maxID, nil
}

// OverflowShardBits checks whether the recordID overflow `1<<(typeBitsLength-shardRowIDBits-1) -1`.
func OverflowShardBits(recordID int64, shardRowIDBits uint64, typeBitsLength uint64) bool {
	mask := (1<<shardRowIDBits - 1) << (typeBitsLength - shardRowIDBits - 1)
	return recordID&int64(mask) > 0
}

// CalcShard calculates the shard prefix by hashing the startTS. Make sure OverflowShardBits is false before calling it.
func CalcShard(shardRowIDBits uint64, startTS uint64, typeBitsLength uint64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], startTS)
	hashVal := int64(murmur3.Sum32(buf[:]))
	return (hashVal & (1<<shardRowIDBits - 1)) << (typeBitsLength - shardRowIDBits - 1)
}

// Allocators implements table.Table Allocators interface.
func (t *TableCommon) Allocators(ctx sessionctx.Context) autoid.Allocators {
	if ctx == nil || ctx.GetSessionVars().IDAllocator == nil {
		return t.allocs
	}

	// Replace the row id allocator with the one in session variables.
	sessAlloc := ctx.GetSessionVars().IDAllocator
	retAllocs := make([]autoid.Allocator, 0, len(t.allocs))
	copy(retAllocs, t.allocs)

	overwritten := false
	for i, a := range retAllocs {
		if a.GetType() == autoid.RowIDAllocType {
			retAllocs[i] = sessAlloc
			overwritten = true
			break
		}
	}
	if !overwritten {
		retAllocs = append(retAllocs, sessAlloc)
	}
	return retAllocs
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (t *TableCommon) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return t.Allocators(ctx).Get(autoid.RowIDAllocType).Rebase(t.tableID, newBase, isSetStep)
}

// Seek implements table.Table Seek interface.
func (t *TableCommon) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return 0, false, err
	}
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.physicalTableID, h)
	iter, err := txn.Iter(seekKey, t.RecordPrefix().PrefixNext())
	if err != nil {
		return 0, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(t.RecordPrefix()) {
		// No more records in the table, skip to the end.
		return 0, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return 0, false, err
	}
	return handle, true, nil
}

// Type implements table.Table Type interface.
func (t *TableCommon) Type() table.Type {
	return table.NormalTable
}

func shouldWriteBinlog(ctx sessionctx.Context) bool {
	if ctx.GetSessionVars().BinlogClient == nil {
		return false
	}
	return !ctx.GetSessionVars().InRestrictedSQL
}

func (t *TableCommon) getMutation(ctx sessionctx.Context) *binlog.TableMutation {
	return ctx.StmtGetMutation(t.tableID)
}

func (t *TableCommon) canSkip(col *table.Column, value types.Datum) bool {
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that;
// 3. the column is virtual generated.
func CanSkip(info *model.TableInfo, col *table.Column, value types.Datum) bool {
	if col.IsPKHandleColumn(info) {
		return true
	}
	if col.GetDefaultValue() == nil && value.IsNull() {
		return true
	}
	if col.IsGenerated() && !col.GeneratedStored {
		return true
	}
	return false
}

// canSkipUpdateBinlog checks whether the column can be skipped or not.
func (t *TableCommon) canSkipUpdateBinlog(col *table.Column, value types.Datum) bool {
	if col.IsGenerated() && !col.GeneratedStored {
		return true
	}
	return false
}

// FindIndexByColName returns a public table index containing only one column named `name`.
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
func CheckHandleExists(ctx context.Context, sctx sessionctx.Context, t table.Table, recordID int64, data []types.Datum) error {
	if pt, ok := t.(*partitionedTable); ok {
		info := t.Meta().GetPartitionInfo()
		pid, err := pt.locatePartition(sctx, info, data)
		if err != nil {
			return err
		}
		t = pt.GetPartition(pid)
	}
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	// Check key exists.
	recordKey := t.RecordKey(recordID)
	existErrInfo := kv.NewExistErrInfo("PRIMARY", strconv.Itoa(int(recordID)))
	txn.SetOption(kv.PresumeKeyNotExistsError, existErrInfo)
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	_, err = txn.Get(ctx, recordKey)
	if err == nil {
		return existErrInfo.Err()
	} else if !kv.ErrNotExist.Equal(err) {
		return err
	}
	return nil
}

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
}

// sequenceCommon cache the sequence value.
// `alter sequence` will invalidate the cached range.
// `setval` will recompute the start position of cached value.
type sequenceCommon struct {
	meta *model.SequenceInfo
	// base < end when increment > 0.
	// base > end when increment < 0.
	end  int64
	base int64
	// round is used to count the cycle times.
	round int64
	mu    sync.RWMutex
}

// GetSequenceBaseEndRound is used in test.
func (s *sequenceCommon) GetSequenceBaseEndRound() (int64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.base, s.end, s.round
}

// GetSequenceNextVal implements util.SequenceTable GetSequenceNextVal interface.
// Caching the sequence value in table, we can easily be notified with the cache empty,
// and write the binlogInfo in table level rather than in allocator.
func (t *TableCommon) GetSequenceNextVal(ctx interface{}, dbName, seqName string) (nextVal int64, err error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	err = func() error {
		// Check if need to update the cache batch from storage.
		// Because seq.base is not always the last allocated value (may be set by setval()).
		// So we should try to seek the next value in cache (not just add increment to seq.base).
		var (
			updateCache bool
			offset      int64
			ok          bool
		)
		if seq.base == seq.end {
			// There is no cache yet.
			updateCache = true
		} else {
			// Seek the first valid value in cache.
			offset = seq.getOffset()
			if seq.meta.Increment > 0 {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
			} else {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
			}
			if !ok {
				updateCache = true
			}
		}
		if !updateCache {
			return nil
		}
		// Update batch alloc from kv storage.
		sequenceAlloc, err1 := getSequenceAllocator(t.allocs)
		if err1 != nil {
			return err1
		}
		var base, end, round int64
		base, end, round, err1 = sequenceAlloc.AllocSeqCache(t.tableID)
		if err1 != nil {
			return err1
		}
		// Only update local cache when alloc succeed.
		seq.base = base
		seq.end = end
		seq.round = round
		// write sequence binlog to the pumpClient.
		if ctx.(sessionctx.Context).GetSessionVars().BinlogClient != nil {
			err = writeSequenceUpdateValueBinlog(ctx.(sessionctx.Context), dbName, seqName, seq.end)
			if err != nil {
				return err
			}
		}
		// Seek the first valid value in new cache.
		// Offset may have changed cause the round is updated.
		offset = seq.getOffset()
		if seq.meta.Increment > 0 {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
		} else {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
		}
		if !ok {
			return errors.New("can't find the first value in sequence cache")
		}
		return nil
	}()
	// Sequence alloc in kv store error.
	if err != nil {
		if err == autoid.ErrAutoincReadFailed {
			return 0, table.ErrSequenceHasRunOut.GenWithStackByArgs(dbName, seqName)
		}
		return 0, err
	}
	seq.base = nextVal
	return nextVal, nil
}

// SetSequenceVal implements util.SequenceTable SetSequenceVal interface.
// The returned bool indicates the newVal is already under the base.
func (t *TableCommon) SetSequenceVal(ctx interface{}, newVal int64, dbName, seqName string) (int64, bool, error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, false, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	if seq.meta.Increment > 0 {
		if newVal <= t.sequence.base {
			return 0, true, nil
		}
		if newVal <= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	} else {
		if newVal >= t.sequence.base {
			return 0, true, nil
		}
		if newVal >= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	}

	// Invalid the current cache.
	t.sequence.base = t.sequence.end

	// Rebase from kv storage.
	sequenceAlloc, err := getSequenceAllocator(t.allocs)
	if err != nil {
		return 0, false, err
	}
	res, alreadySatisfied, err := sequenceAlloc.RebaseSeq(t.tableID, newVal)
	if err != nil {
		return 0, false, err
	}
	if !alreadySatisfied {
		// Write sequence binlog to the pumpClient.
		if ctx.(sessionctx.Context).GetSessionVars().BinlogClient != nil {
			err = writeSequenceUpdateValueBinlog(ctx.(sessionctx.Context), dbName, seqName, seq.end)
			if err != nil {
				return 0, false, err
			}
		}
	}
	// Record the current end after setval succeed.
	// Consider the following case.
	// create sequence seq
	// setval(seq, 100) setval(seq, 50)
	// Because no cache (base, end keep 0), so the second setval won't return NULL.
	t.sequence.base, t.sequence.end = newVal, newVal
	return res, alreadySatisfied, nil
}

// getOffset is used in under GetSequenceNextVal & SetSequenceVal, which mu is locked.
func (s *sequenceCommon) getOffset() int64 {
	offset := s.meta.Start
	if s.meta.Cycle && s.round > 0 {
		if s.meta.Increment > 0 {
			offset = s.meta.MinValue
		} else {
			offset = s.meta.MaxValue
		}
	}
	return offset
}

// GetSequenceID implements util.SequenceTable GetSequenceID interface.
func (t *TableCommon) GetSequenceID() int64 {
	return t.tableID
}

// GetSequenceCommon is used in test to get sequenceCommon.
func (t *TableCommon) GetSequenceCommon() *sequenceCommon {
	return t.sequence
}

func getSequenceAllocator(allocs autoid.Allocators) (autoid.Allocator, error) {
	for _, alloc := range allocs {
		if alloc.GetType() == autoid.SequenceType {
			return alloc, nil
		}
	}
	// TODO: refine the error.
	return nil, errors.New("sequence allocator is nil")
}
