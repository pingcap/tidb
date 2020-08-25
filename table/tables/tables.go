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
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

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
	"github.com/pingcap/tidb/util/generatedexpr"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// TableCommon is shared by both Table and partition.
type TableCommon struct {
	tableID int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID                 int64
	Columns                         []*table.Column
	PublicColumns                   []*table.Column
	VisibleColumns                  []*table.Column
	HiddenColumns                   []*table.Column
	WritableColumns                 []*table.Column
	FullHiddenColsAndVisibleColumns []*table.Column
	writableIndices                 []table.Index
	indices                         []table.Index
	meta                            *model.TableInfo
	allocs                          autoid.Allocators
	sequence                        *sequenceCommon

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
			expr, err := generatedexpr.ParseExpression(colInfo.GeneratedExprString)
			if err != nil {
				return nil, err
			}
			expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, err
			}
			col.GeneratedExpr = expr
		}
		// default value is expr.
		if col.DefaultIsExpr {
			expr, err := generatedexpr.ParseExpression(colInfo.DefaultValue.(string))
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
	t.FullHiddenColsAndVisibleColumns = t.FullHiddenColsAndVisibleCols()
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
	t.writableIndices = t.WritableIndices()
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

// GetWritableIndexByName gets the index meta from the table by the index name.
func GetWritableIndexByName(idxName string, t table.Table) table.Index {
	indices := t.WritableIndices()
	for _, idx := range indices {
		if idxName == idx.Meta().Name.L {
			return idx
		}
	}
	return nil
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

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (t *TableCommon) FullHiddenColsAndVisibleCols() []*table.Column {
	if len(t.FullHiddenColsAndVisibleColumns) > 0 {
		return t.FullHiddenColsAndVisibleColumns
	}

	cols := make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.Hidden || col.State == model.StatePublic {
			cols = append(cols, col)
		}
	}
	return cols
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
func (t *TableCommon) RecordKey(h kv.Handle) kv.Key {
	return tablecodec.EncodeRecordKey(t.recordPrefix, h)
}

// FirstKey implements table.Table interface.
func (t *TableCommon) FirstKey() kv.Key {
	return t.RecordKey(kv.IntHandle(math.MinInt64))
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, oldData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	// rebuild index
	err = t.rebuildIndices(sctx, txn, h, touched, oldData, newData, table.WithCtx(ctx))
	if err != nil {
		return err
	}
	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	var colIDs, binlogColIDs []int64
	var row, binlogOldRow, binlogNewRow []types.Datum
	colIDs = make([]int64, 0, numColsCap)
	row = make([]types.Datum, 0, numColsCap)
	if shouldWriteBinlog(sctx) {
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
		if !t.canSkip(col, &value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
		if shouldWriteBinlog(sctx) && !t.canSkipUpdateBinlog(col, value) {
			binlogColIDs = append(binlogColIDs, col.ID)
			binlogOldRow = append(binlogOldRow, oldData[col.Offset])
			binlogNewRow = append(binlogNewRow, value)
		}
	}

	key := t.RecordKey(h)
	sessVars := sctx.GetSessionVars()
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, rd)
	if err != nil {
		return err
	}
	if err = memBuffer.Set(key, value); err != nil {
		return err
	}
	memBuffer.Release(sh)
	sctx.StmtAddDirtyTableOP(table.DirtyTableDeleteRow, t.physicalTableID, h)
	sctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, h)
	if shouldWriteBinlog(sctx) {
		if !t.meta.PKIsHandle {
			binlogColIDs = append(binlogColIDs, model.ExtraHandleID)
			binlogOldRow = append(binlogOldRow, types.NewIntDatum(h.IntValue()))
			binlogNewRow = append(binlogNewRow, types.NewIntDatum(h.IntValue()))
		}
		err = t.addUpdateBinlog(sctx, binlogOldRow, binlogNewRow, binlogColIDs)
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

func (t *TableCommon) rebuildIndices(ctx sessionctx.Context, txn kv.Transaction, h kv.Handle, touched []bool, oldData []types.Datum, newData []types.Datum, opts ...table.CreateIdxOptFunc) error {
	for _, idx := range t.DeletableIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return err
			}
			if err = t.removeRowIndex(ctx.GetSessionVars().StmtCtx, h, oldVs, idx, txn); err != nil {
				return err
			}
			break
		}
	}
	for _, idx := range t.WritableIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
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
		if err := t.buildIndexForRow(ctx, h, newVs, idx, txn, untouched, opts...); err != nil {
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

// FindPrimaryIndex uses to find primary index in tableInfo.
func FindPrimaryIndex(tblInfo *model.TableInfo) *model.IndexInfo {
	var pkIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	return pkIdx
}

// CommonAddRecordCtx is used in `AddRecord` to avoid memory malloc for some temp slices.
// This is useful in lightning parse row data to key-values pairs. This can gain upto 5%  performance
// improvement in lightning's local mode.
type CommonAddRecordCtx struct {
	colIDs []int64
	row    []types.Datum
}

// commonAddRecordKey is used as key in `sessionctx.Context.Value(key)`
type commonAddRecordKey struct{}

// String implement `stringer.String` for CommonAddRecordKey
func (c commonAddRecordKey) String() string {
	return "_common_add_record_context_key"
}

// addRecordCtxKey is key in `sessionctx.Context` for CommonAddRecordCtx
var addRecordCtxKey = commonAddRecordKey{}

// SetAddRecordCtx set a CommonAddRecordCtx to session context
func SetAddRecordCtx(ctx sessionctx.Context, r *CommonAddRecordCtx) {
	ctx.SetValue(addRecordCtxKey, r)
}

// ClearAddRecordCtx remove `CommonAddRecordCtx` from session context
func ClearAddRecordCtx(ctx sessionctx.Context) {
	ctx.ClearValue(addRecordCtxKey)
}

// NewCommonAddRecordCtx create a context used for `AddRecord`
func NewCommonAddRecordCtx(size int) *CommonAddRecordCtx {
	return &CommonAddRecordCtx{
		colIDs: make([]int64, 0, size),
		row:    make([]types.Datum, 0, size),
	}
}

// TryGetCommonPkColumnIds get the IDs of primary key column if the table has common handle.
func TryGetCommonPkColumnIds(tbl *model.TableInfo) []int64 {
	var pkColIds []int64
	if !tbl.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl)
	for _, idxCol := range pkIdx.Columns {
		pkColIds = append(pkColIds, tbl.Columns[idxCol.Offset].ID)
	}
	return pkColIds
}

// TryGetCommonPkColumns get the primary key columns if the table has common handle.
func TryGetCommonPkColumns(tbl table.Table) []*table.Column {
	var pkCols []*table.Column
	if !tbl.Meta().IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl.Meta())
	cols := tbl.Cols()
	for _, idxCol := range pkIdx.Columns {
		pkCols = append(pkCols, cols[idxCol.Offset])
	}
	return pkCols
}

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(sctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
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
		recordID = kv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		if tblInfo.PKIsHandle {
			recordID = kv.IntHandle(r[tblInfo.GetPkColInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				pkDts = append(pkDts, r[tblInfo.Columns[idxCol.Offset].Offset])
			}
			tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, pkDts...)
			if err != nil {
				return
			}
			recordID, err = kv.NewCommonHandle(handleBytes)
			if err != nil {
				return
			}
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if opt.ReserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			stmtCtx := sctx.GetSessionVars().StmtCtx
			stmtCtx.BaseRowID, stmtCtx.MaxRowID, err = allocHandleIDs(sctx, t, uint64(opt.ReserveAutoID))
			if err != nil {
				return nil, err
			}
		}

		recordID, err = AllocHandle(sctx, t)
		if err != nil {
			return nil, err
		}
	}

	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, err
	}
	var colIDs, binlogColIDs []int64
	var row, binlogRow []types.Datum
	if recordCtx, ok := sctx.Value(addRecordCtxKey).(*CommonAddRecordCtx); ok {
		colIDs = recordCtx.colIDs[:0]
		row = recordCtx.row[:0]
	} else {
		colIDs = make([]int64, 0, len(r))
		row = make([]types.Datum, 0, len(r))
	}
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	sessVars := sctx.GetSessionVars()

	for _, col := range t.WritableCols() {
		var value types.Datum
		// Update call `AddRecord` will already handle the write only column default value.
		// Only insert should add default value for write only column.
		if col.State != model.StatePublic && !opt.IsUpdate {
			// If col is in write only or write reorganization state, we must add it with its default value.
			value, err = table.GetColOriginDefaultValue(sctx, col.ToInfo())
			if err != nil {
				return nil, err
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
		if !t.canSkip(col, &value) {
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
		return nil, err
	}
	value := writeBufs.RowValBuf

	var setPresume bool
	var ctx context.Context
	if opt.Ctx != nil {
		ctx = opt.Ctx
	} else {
		ctx = context.Background()
	}
	skipCheck := sctx.GetSessionVars().StmtCtx.BatchCheck
	if (t.meta.IsCommonHandle || t.meta.PKIsHandle) && !skipCheck && !opt.SkipHandleCheck {
		if sctx.GetSessionVars().LazyCheckKeyNotExists() {
			var v []byte
			v, err = txn.GetMemBuffer().Get(ctx, key)
			if err != nil {
				setPresume = true
			}
			if err == nil && len(v) == 0 {
				err = kv.ErrNotExist
			}
		} else {
			_, err = txn.Get(ctx, key)
		}
		if err == nil {
			return recordID, kv.ErrKeyExists.FastGenByArgs(recordID.String(), "PRIMARY")
		} else if !kv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	if setPresume {
		err = memBuffer.SetWithFlags(key, value, kv.SetPresumeKeyNotExists)
	} else {
		err = memBuffer.Set(key, value)
	}
	if err != nil {
		return nil, err
	}

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
	h, err := t.addIndices(sctx, recordID, r, txn, createIdxOpts)
	if err != nil {
		return h, err
	}

	memBuffer.Release(sh)
	sctx.StmtAddDirtyTableOP(table.DirtyTableAddRow, t.physicalTableID, recordID)

	if shouldWriteBinlog(sctx) {
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		err = t.addInsertBinlog(sctx, recordID, binlogRow, binlogColIDs)
		if err != nil {
			return nil, err
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
func (t *TableCommon) addIndices(sctx sessionctx.Context, recordID kv.Handle, r []types.Datum, txn kv.Transaction, opts []table.CreateIdxOptFunc) (kv.Handle, error) {
	writeBufs := sctx.GetSessionVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := sctx.GetSessionVars().StmtCtx.BatchCheck
	for _, v := range t.WritableIndices() {
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}
		indexVals, err := v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			entryKey, err := t.genIndexKeyStr(indexVals)
			if err != nil {
				return nil, err
			}
			idxMeta := v.Meta()
			dupErr = kv.ErrKeyExists.FastGenByArgs(entryKey, idxMeta.Name.String())
		}
		if dupHandle, err := v.Create(sctx, txn.GetUnionStore(), indexVals, recordID, opts...); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return nil, err
		}
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return nil, nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *TableCommon) RowWithCols(ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
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
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.Flag) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if col.IsCommonHandleColumn(meta) {
			pkIdx := FindPrimaryIndex(meta)
			var idxOfIdx int
			for i, idxCol := range pkIdx.Columns {
				if meta.Columns[idxCol.Offset].ID == col.ID {
					idxOfIdx = i
					break
				}
			}
			dtBytes := h.EncodedCol(idxOfIdx)
			_, dt, err := codec.DecodeOne(dtBytes)
			if err != nil {
				return nil, nil, err
			}
			dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetSessionVars().Location())
			if err != nil {
				return nil, nil, err
			}
			v[i] = dt
			continue
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetSessionVars().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) || col.IsCommonHandleColumn(meta) {
			continue
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		if col.IsGenerated() && !col.GeneratedStored {
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
func (t *TableCommon) Row(ctx sessionctx.Context, h kv.Handle) ([]types.Datum, error) {
	return t.RowWithCols(ctx, h, t.Cols())
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *TableCommon) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
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
			binlogRow = append(binlogRow, types.NewIntDatum(h.IntValue()))
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

func (t *TableCommon) addInsertBinlog(ctx sessionctx.Context, h kv.Handle, row []types.Datum, colIDs []int64) error {
	mutation := t.getMutation(ctx)
	pk, err := codec.EncodeValue(ctx.GetSessionVars().StmtCtx, nil, types.NewIntDatum(h.IntValue()))
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

func (t *TableCommon) removeRowData(ctx sessionctx.Context, h kv.Handle) error {
	// Remove row data.
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	err = txn.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx sessionctx.Context, h kv.Handle, rec []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, v := range t.DeletableIndices() {
		vals, err := v.FetchValues(rec, nil)
		if err != nil {
			logutil.BgLogger().Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx.GetSessionVars().StmtCtx, txn, vals, h); err != nil {
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.BgLogger().Debug("row index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()))
				continue
			}
			return err
		}
	}
	return nil
}

// removeRowIndex implements table.Table RemoveRowIndex interface.
func (t *TableCommon) removeRowIndex(sc *stmtctx.StatementContext, h kv.Handle, vals []types.Datum, idx table.Index, txn kv.Transaction) error {
	return idx.Delete(sc, txn, vals, h)
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *TableCommon) buildIndexForRow(ctx sessionctx.Context, h kv.Handle, vals []types.Datum, idx table.Index, txn kv.Transaction, untouched bool, popts ...table.CreateIdxOptFunc) error {
	var opts []table.CreateIdxOptFunc
	opts = append(opts, popts...)
	if untouched {
		opts = append(opts, table.IndexIsUntouched)
	}
	if _, err := idx.Create(ctx, txn.GetUnionStore(), vals, h, opts...); err != nil {
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
		rowMap, err := tablecodec.DecodeRowToDatumMap(it.Value(), colMap, ctx.GetSessionVars().Location())
		if err != nil {
			return err
		}
		pkIds, decodeLoc := TryGetCommonPkColumnIds(t.meta), ctx.GetSessionVars().Location()
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.meta) {
				if mysql.HasUnsignedFlag(col.Flag) {
					data[col.Offset].SetUint64(uint64(handle.IntValue()))
				} else {
					data[col.Offset].SetInt64(handle.IntValue())
				}
				continue
			} else if mysql.HasPriKeyFlag(col.Flag) {
				data[col.Offset], err = tryDecodeColumnFromCommonHandle(col, handle, pkIds, decodeLoc)
				if err != nil {
					return err
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

func tryDecodeColumnFromCommonHandle(col *table.Column, handle kv.Handle, pkIds []int64, decodeLoc *time.Location) (types.Datum, error) {
	for i, hid := range pkIds {
		if hid != col.ID {
			continue
		}
		_, d, err := codec.DecodeOne(handle.EncodedCol(i))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		if d, err = tablecodec.Unflatten(d, &col.FieldType, decodeLoc); err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	return types.Datum{}, nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx sessionctx.Context, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(col.Flag) {
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
func AllocHandle(ctx sessionctx.Context, t table.Table) (kv.Handle, error) {
	if ctx != nil {
		if stmtCtx := ctx.GetSessionVars().StmtCtx; stmtCtx != nil {
			// First try to alloc if the statement has reserved auto ID.
			if stmtCtx.BaseRowID < stmtCtx.MaxRowID {
				stmtCtx.BaseRowID += 1
				return kv.IntHandle(stmtCtx.BaseRowID), nil
			}
		}
	}

	_, rowID, err := allocHandleIDs(ctx, t, 1)
	return kv.IntHandle(rowID), err
}

func allocHandleIDs(ctx sessionctx.Context, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(ctx).Get(autoid.RowIDAllocType).Alloc(meta.ID, n, 1, 1)
	if err != nil {
		return 0, 0, err
	}
	if meta.ShardRowIDBits > 0 {
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(maxID, meta.MaxShardRowIDBits, autoid.RowIDBitLength, true) {
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
		shard := txnCtx.GetShard(meta.ShardRowIDBits, autoid.RowIDBitLength, true, int(n))
		base |= shard
		maxID |= shard
	}
	return base, maxID, nil
}

// OverflowShardBits checks whether the recordID overflow `1<<(typeBitsLength-shardRowIDBits-1) -1`.
func OverflowShardBits(recordID int64, shardRowIDBits uint64, typeBitsLength uint64, reservedSignBit bool) bool {
	var signBit uint64
	if reservedSignBit {
		signBit = 1
	}
	mask := (1<<shardRowIDBits - 1) << (typeBitsLength - shardRowIDBits - signBit)
	return recordID&int64(mask) > 0
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
// Both auto-increment and auto-random can use this function to do rebase on explicit newBase value (without shadow bits).
func (t *TableCommon) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool, tp autoid.AllocatorType) error {
	return t.Allocators(ctx).Get(tp).Rebase(t.tableID, newBase, isSetStep)
}

// Seek implements table.Table Seek interface.
func (t *TableCommon) Seek(ctx sessionctx.Context, h kv.Handle) (kv.Handle, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, false, err
	}
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.physicalTableID, h)
	iter, err := txn.Iter(seekKey, t.RecordPrefix().PrefixNext())
	if err != nil {
		return nil, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(t.RecordPrefix()) {
		// No more records in the table, skip to the end.
		return nil, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return nil, false, err
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

func (t *TableCommon) canSkip(col *table.Column, value *types.Datum) bool {
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that;
// 3. the column is virtual generated.
func CanSkip(info *model.TableInfo, col *table.Column, value *types.Datum) bool {
	if col.IsPKHandleColumn(info) {
		return true
	}
	if col.IsCommonHandleColumn(info) {
		pkIdx := FindPrimaryIndex(info)
		for _, idxCol := range pkIdx.Columns {
			if info.Columns[idxCol.Offset].ID != col.ID {
				continue
			}
			return idxCol.Length == types.UnspecifiedLength
		}
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
func CheckHandleExists(ctx context.Context, sctx sessionctx.Context, t table.Table, recordID kv.Handle, data []types.Datum) error {
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
	_, err = txn.Get(ctx, recordKey)
	if err == nil {
		return kv.ErrKeyExists.FastGenByArgs(recordID.String(), "PRIMARY")
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

// BuildTableScanFromInfos build tipb.TableScan with *model.TableInfo and *model.ColumnInfo.
func BuildTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo) *tipb.TableScan {
	pkColIds := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.TableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle),
		PrimaryColumnIds: pkColIds,
	}
	return tsExec
}
