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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package tables

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// TableCommon is shared by both Table and partition.
// NOTE: when copying this struct, use Copy() to clear its columns cache.
type TableCommon struct {
	// TODO: Why do we need tableID, when it is already in meta.ID ?
	tableID int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID int64
	Columns         []*table.Column

	// column caches
	// They are pointers to support copying TableCommon to CachedTable and PartitionedTable
	publicColumns                   []*table.Column
	visibleColumns                  []*table.Column
	hiddenColumns                   []*table.Column
	writableColumns                 []*table.Column
	fullHiddenColsAndVisibleColumns []*table.Column
	writableConstraints             []*table.Constraint

	indices     []table.Index
	meta        *model.TableInfo
	allocs      autoid.Allocators
	sequence    *sequenceCommon
	Constraints []*table.Constraint

	// recordPrefix and indexPrefix are generated using physicalTableID.
	recordPrefix kv.Key
	indexPrefix  kv.Key

	// skipAssert is used for partitions that are in WriteOnly/DeleteOnly state.
	skipAssert bool
}

// ResetColumnsCache implements testingKnob interface.
func (t *TableCommon) ResetColumnsCache() {
	t.publicColumns = t.getCols(full)
	t.visibleColumns = t.getCols(visible)
	t.hiddenColumns = t.getCols(hidden)

	t.writableColumns = make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		t.writableColumns = append(t.writableColumns, col)
	}

	t.fullHiddenColsAndVisibleColumns = make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.Hidden || col.State == model.StatePublic {
			t.fullHiddenColsAndVisibleColumns = append(t.fullHiddenColsAndVisibleColumns, col)
		}
	}

	if t.Constraints != nil {
		t.writableConstraints = make([]*table.Constraint, 0, len(t.Constraints))
		for _, con := range t.Constraints {
			if !con.Enforced {
				continue
			}
			if con.State == model.StateDeleteOnly || con.State == model.StateDeleteReorganization {
				continue
			}
			t.writableConstraints = append(t.writableConstraints, con)
		}
	}
}

// Copy copies a TableCommon struct, and reset its column cache. This is not a deep copy.
func (t *TableCommon) Copy() TableCommon {
	newTable := *t
	return newTable
}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tblInfo *model.TableInfo) table.Table {
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil
	}
	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, autoid.NewAllocators(false), constraints)
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		ret, err := newCachedTable(&t)
		if err != nil {
			return nil
		}
		return ret
	}
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
			genStr := colInfo.GeneratedExprString
			expr, err := buildGeneratedExpr(tblInfo, genStr)
			if err != nil {
				return nil, err
			}
			col.GeneratedExpr = table.NewClonableExprNode(func() ast.ExprNode {
				newExpr, err1 := buildGeneratedExpr(tblInfo, genStr)
				if err1 != nil {
					logutil.BgLogger().Warn("unexpected parse generated string error",
						zap.String("generatedStr", genStr),
						zap.Error(err1))
					return expr
				}
				return newExpr
			}, expr)
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

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil, err
	}
	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, allocs, constraints)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initTableIndices(&t); err != nil {
			return nil, err
		}
		if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
			return newCachedTable(&t)
		}
		return &t, nil
	}
	return newPartitionedTable(&t, tblInfo)
}

func buildGeneratedExpr(tblInfo *model.TableInfo, genExpr string) (ast.ExprNode, error) {
	expr, err := generatedexpr.ParseExpression(genExpr)
	if err != nil {
		return nil, err
	}
	expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// initTableCommon initializes a TableCommon struct.
func initTableCommon(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators, constraints []*table.Constraint) {
	t.tableID = tblInfo.ID
	t.physicalTableID = physicalTableID
	t.allocs = allocs
	t.meta = tblInfo
	t.Columns = cols
	t.Constraints = constraints
	t.recordPrefix = tablecodec.GenTableRecordPrefix(physicalTableID)
	t.indexPrefix = tablecodec.GenTableIndexPrefix(physicalTableID)
	if tblInfo.IsSequence() {
		t.sequence = &sequenceCommon{meta: tblInfo.Sequence}
	}
	t.ResetColumnsCache()
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
		intest.AssertFunc(func() bool {
			// `TableCommon.indices` is type of `[]table.Index` to implement interface method `Table.Indices`.
			// However, we have an assumption that the specific type of each element in it should always be `*index`.
			// We have this assumption because some codes access the inner method of `*index`,
			// and they use `asIndex` to cast `table.Index` to `*index`.
			_, ok := idx.(*index)
			intest.Assert(ok, "index should be type of `*index`")
			return true
		})
		t.indices = append(t.indices, idx)
	}
	return nil
}

// asIndex casts a table.Index to *index which is the actual type of index in TableCommon.
func asIndex(idx table.Index) *index {
	return idx.(*index)
}

func initTableCommonWithIndices(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators, constraints []*table.Constraint) error {
	initTableCommon(t, tblInfo, physicalTableID, cols, allocs, constraints)
	return initTableIndices(t)
}

// Indices implements table.Table Indices interface.
func (t *TableCommon) Indices() []table.Index {
	return t.indices
}

// GetWritableIndexByName gets the index meta from the table by the index name.
func GetWritableIndexByName(idxName string, t table.Table) table.Index {
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}
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

// GetPartitionedTable implements table.Table GetPhysicalID interface.
func (t *TableCommon) GetPartitionedTable() table.PartitionedTable {
	return nil
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
	return t.publicColumns
}

// VisibleCols implements table.Table VisibleCols interface.
func (t *TableCommon) VisibleCols() []*table.Column {
	return t.visibleColumns
}

// HiddenCols implements table.Table HiddenCols interface.
func (t *TableCommon) HiddenCols() []*table.Column {
	return t.hiddenColumns
}

// WritableCols implements table WritableCols interface.
func (t *TableCommon) WritableCols() []*table.Column {
	return t.writableColumns
}

// DeletableCols implements table DeletableCols interface.
func (t *TableCommon) DeletableCols() []*table.Column {
	return t.Columns
}

// WritableConstraint returns constraints of the table in writable states.
func (t *TableCommon) WritableConstraint() []*table.Constraint {
	if t.Constraints == nil {
		return nil
	}
	return t.writableConstraints
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (t *TableCommon) FullHiddenColsAndVisibleCols() []*table.Column {
	return t.fullHiddenColsAndVisibleColumns
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

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	opt := table.NewUpdateRecordOpt(opts...)
	return t.updateRecord(ctx, txn, h, oldData, newData, touched, opt)
}

func (t *TableCommon) updateRecord(sctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opt *table.UpdateRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err := checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(numColsCap)
	checkRowBuffer := mutateBuffers.GetCheckRowBufferWithCap(numColsCap)

	for _, col := range t.Columns {
		var value types.Datum
		var err error
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), oldData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					logutil.BgLogger().Info("update record cast value failed", zap.Any("col", col), zap.Uint64("txnStartTS", txn.StartTS()),
						zap.String("handle", h.String()), zap.Any("val", oldData[col.DependencyColumnOffset]), zap.Error(err))
					return err
				}
				oldData = append(oldData, value)
				touched = append(touched, touched[col.DependencyColumnOffset])
			}
			continue
		}
		if col.State != model.StatePublic {
			// If col is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the original data(it's changed by other TiDBs.) or the original default value.
			// TODO: Use newData directly.
			value = oldData[col.Offset]
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), newData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					return err
				}
				newData[col.Offset] = value
				touched[col.Offset] = touched[col.DependencyColumnOffset]
			}
		} else {
			value = newData[col.Offset]
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
		checkRowBuffer.AddColVal(value)
	}
	// check data constraint
	if constraints := t.WritableConstraint(); len(constraints) > 0 {
		if err := table.CheckRowConstraint(sctx.GetExprCtx(), constraints, checkRowBuffer.GetRowToCheck(), t.Meta()); err != nil {
			return err
		}
	}
	// rebuild index
	err := t.rebuildUpdateRecordIndices(sctx, txn, h, touched, oldData, newData, opt)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, h)
	if err != nil {
		return err
	}

	failpoint.Inject("updateRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on UpdateRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = txn.SetAssertion(key, kv.SetAssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})

	if t.skipAssert {
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
	} else {
		err = txn.SetAssertion(key, kv.SetAssertExist)
	}
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, newData, oldData, memBuffer, sh); err != nil {
			return errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 0, 1)
	}
	return nil
}

func (t *TableCommon) rebuildUpdateRecordIndices(
	ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, touched []bool, oldData []types.Datum, newData []types.Datum,
	opt *table.UpdateRecordOpt,
) error {
	for _, idx := range t.DeletableIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
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
			if err = idx.Delete(ctx, txn, oldVs, h); err != nil {
				return err
			}
			break
		}
	}
	createIdxOpt := opt.GetCreateIdxOpt()
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}
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
		if untouched && opt.SkipWriteUntouchedIndices() {
			continue
		}
		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, h, newVs, newData, asIndex(idx), txn, untouched, createIdxOpt); err != nil {
			return err
		}
	}
	return nil
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

// TryGetCommonPkColumnIds get the IDs of primary key column if the table has common handle.
func TryGetCommonPkColumnIds(tbl *model.TableInfo) []int64 {
	if !tbl.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl)
	pkColIDs := make([]int64, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkColIDs = append(pkColIDs, tbl.Columns[idxCol.Offset].ID)
	}
	return pkColIDs
}

// PrimaryPrefixColumnIDs get prefix column ids in primary key.
func PrimaryPrefixColumnIDs(tbl *model.TableInfo) (prefixCols []int64) {
	for _, idx := range tbl.Indices {
		if !idx.Primary {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length > 0 && tbl.Columns[col.Offset].GetFlen() > col.Length {
				prefixCols = append(prefixCols, tbl.Columns[col.Offset].ID)
			}
		}
	}
	return
}

// TryGetCommonPkColumns get the primary key columns if the table has common handle.
func TryGetCommonPkColumns(tbl table.Table) []*table.Column {
	if !tbl.Meta().IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl.Meta())
	cols := tbl.Cols()
	pkCols := make([]*table.Column, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkCols = append(pkCols, cols[idxCol.Offset])
	}
	return pkCols
}

func addTemporaryTable(sctx table.MutateContext, tblInfo *model.TableInfo) (tblctx.TemporaryTableHandler, int64, bool) {
	if s, ok := sctx.GetTemporaryTableSupport(); ok {
		if h, ok := s.AddTemporaryTableToTxn(tblInfo); ok {
			return h, s.GetTemporaryTableSizeLimit(), ok
		}
	}
	return tblctx.TemporaryTableHandler{}, 0, false
}

// The size of a temporary table is calculated by accumulating the transaction size delta.
func handleTempTableSize(t tblctx.TemporaryTableHandler, txnSizeBefore int, txn kv.Transaction) {
	t.UpdateTxnDeltaSize(txn.Size() - txnSizeBefore)
}

func checkTempTableSize(tmpTable tblctx.TemporaryTableHandler, sizeLimit int64) error {
	if tmpTable.GetCommittedSize()+tmpTable.GetDirtySize() > sizeLimit {
		return table.ErrTempTableFull.GenWithStackByArgs(tmpTable.Meta().Name.O)
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	// TODO: optimize the allocation (and calculation) of opt.
	opt := table.NewAddRecordOpt(opts...)
	return t.addRecord(sctx, txn, r, opt)
}

func (t *TableCommon) addRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opt *table.AddRecordOpt) (recordID kv.Handle, err error) {
	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return nil, err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	var ctx context.Context
	if ctx = opt.Ctx(); ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "table.AddRecord")
		defer r.End()
	} else {
		ctx = context.Background()
	}

	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()

	var hasRecordID bool
	cols := t.Cols()
	// opt.GenerateRecordID is used for normal update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, insert can set _tidb_rowid.
	// Update can only update _tidb_rowid during reorganize partition, to keep the generated _tidb_rowid
	// the same between the old/new sets of partitions, where possible.
	if len(r) > len(cols) && !opt.GenerateRecordID() {
		// The last value is _tidb_rowid.
		recordID = kv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		txn.CacheTableInfo(t.physicalTableID, tblInfo)
		if tblInfo.PKIsHandle {
			recordID = kv.IntHandle(r[tblInfo.GetPkColInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				pkDts = append(pkDts, r[idxCol.Offset])
			}
			tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(tc.Location(), nil, pkDts...)
			err = ec.HandleError(err)
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
		if reserveAutoID := opt.ReserveAutoID(); reserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			if reserved, ok := sctx.GetReservedRowIDAlloc(); ok {
				var baseRowID, maxRowID int64
				if baseRowID, maxRowID, err = AllocHandleIDs(ctx, sctx, t, uint64(reserveAutoID)); err != nil {
					return nil, err
				}
				reserved.Reset(baseRowID, maxRowID)
			}
		}

		recordID, err = AllocHandle(ctx, sctx, t)
		if err != nil {
			return nil, err
		}
	}

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(len(r))
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	for _, col := range t.Columns {
		var value types.Datum
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		// In column type change, since we have set the origin default value for changing col, but
		// for the new insert statement, we should use the casted value of relative column to insert.
		if col.ChangeStateInfo != nil && col.State != model.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = table.CastColumnValue(sctx.GetExprCtx(), r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(t.WritableCols()) {
				r = append(r, value)
			} else {
				r[col.Offset] = value
			}
			encodeRowBuffer.AddColVal(col.ID, value)
			continue
		}
		if col.State == model.StatePublic {
			value = r[col.Offset]
		} else {
			// col.ChangeStateInfo must be nil here.
			// because `col.State != model.StatePublic` is true here, if col.ChangeStateInfo is not nil, the col should
			// be handle by the previous if-block.

			if opt.IsUpdate() {
				// If `AddRecord` is called by an update, the default value should be handled the update.
				value = r[col.Offset]
			} else {
				// If `AddRecord` is called by an insert and the col is in write only or write reorganization state, we must
				// add it with its default value.
				value, err = table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
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
			}
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
	}
	if err = table.CheckRowConstraintWithDatum(sctx.GetExprCtx(), t.WritableConstraint(), r, t.meta); err != nil {
		return nil, err
	}
	key := t.RecordKey(recordID)
	var setPresume bool
	if opt.DupKeyCheck() != table.DupKeyCheckSkip {
		if t.meta.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			_, err = txn.Get(ctx, key)
		} else if opt.DupKeyCheck() == table.DupKeyCheckLazy {
			var v []byte
			v, err = txn.GetMemBuffer().GetLocal(ctx, key)
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
			// If Global Index and reorganization truncate/drop partition, old partition,
			// Accept and set Assertion key to kv.SetAssertUnknown for overwrite instead
			dupErr := getDuplicateError(t.Meta(), recordID, r)
			return recordID, dupErr
		} else if !kv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	var flags []kv.FlagsOp
	if setPresume {
		flags = []kv.FlagsOp{kv.SetPresumeKeyNotExists}
		if opt.PessimisticLazyDupKeyCheck() == table.DupKeyCheckInPrewrite && txn.IsPessimistic() {
			flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
		}
	}

	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, recordID, flags...)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("addRecordForceAssertExist", func() {
		// Assert the key exists while it actually doesn't. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting exist on AddRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = txn.SetAssertion(key, kv.SetAssertExist); err != nil {
				failpoint.Return(nil, err)
			}
		}
	})
	if setPresume && !txn.IsPessimistic() {
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
	} else {
		err = txn.SetAssertion(key, kv.SetAssertNotExist)
	}
	if err != nil {
		return nil, err
	}

	// Insert new entries into indices.
	h, err := t.addIndices(sctx, recordID, r, txn, opt.GetCreateIdxOpt())
	if err != nil {
		return h, err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return nil, err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, r, nil, memBuffer, sh); err != nil {
			return nil, errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 1, 1)
	}
	return recordID, nil
}

// genIndexKeyStrs generates index content strings representation.
func genIndexKeyStrs(colVals []types.Datum) ([]string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return nil, err
			}
		}
		strVals = append(strVals, cvs)
	}
	return strVals, nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *TableCommon) addIndices(sctx table.MutateContext, recordID kv.Handle, r []types.Datum, txn kv.Transaction, opt *table.CreateIdxOpt) (kv.Handle, error) {
	writeBufs := sctx.GetMutateBuffers().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := opt.DupKeyCheck() == table.DupKeyCheckSkip
	for _, v := range t.Indices() {
		if !IsIndexWritable(v) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}
		// We declared `err` here to make sure `indexVals` is assigned with `=` instead of `:=`.
		// The latter one will create a new variable that shadows the outside `indexVals` that makes `indexVals` outside
		// always nil, and we cannot reuse it.
		var err error
		indexVals, err = v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, v.Meta(), indexVals)
			colStrVals, err := genIndexKeyStrs(indexVals)
			if err != nil {
				return nil, err
			}
			dupErr = kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", v.TableMeta().Name.String(), v.Meta().Name.String()))
		}
		rsData := TryGetHandleRestoredDataWrapper(t.meta, r, nil, v.Meta())
		if dupHandle, err := asIndex(v).create(sctx, txn, indexVals, recordID, rsData, false, opt); err != nil {
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

// RowWithCols is used to get the corresponding column datum values with the given handle.
func RowWithCols(t table.Table, ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx.GetExprCtx(), t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func containFullColInHandle(meta *model.TableInfo, col *table.Column) (containFullCol bool, idxInHandle int) {
	pkIdx := FindPrimaryIndex(meta)
	for i, idxCol := range pkIdx.Columns {
		if meta.Columns[idxCol.Offset].ID == col.ID {
			idxInHandle = i
			containFullCol = idxCol.Length == types.UnspecifiedLength
			return
		}
	}
	return
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx expression.BuildContext, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	prefixCols := make(map[int64]struct{})
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.GetFlag()) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType) {
			if containFullCol, idxInHandle := containFullColInHandle(meta, col); containFullCol {
				dtBytes := h.EncodedCol(idxInHandle)
				_, dt, err := codec.DecodeOne(dtBytes)
				if err != nil {
					return nil, nil, err
				}
				dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetEvalCtx().Location())
				if err != nil {
					return nil, nil, err
				}
				v[i] = dt
				continue
			}
			prefixCols[col.ID] = struct{}{}
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetEvalCtx().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) || (col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType)) {
			if _, isPrefix := prefixCols[col.ID]; !isPrefix {
				continue
			}
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		if col.IsVirtualGenerated() {
			continue
		}
		if col.ChangeStateInfo != nil {
			v[i], _, err = GetChangingColVal(ctx, cols, col, rowMap, defaultVals)
		} else {
			v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		}
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// GetChangingColVal gets the changing column value when executing "modify/change column" statement.
// For statement like update-where, it will fetch the old row out and insert it into kv again.
// Since update statement can see the writable columns, it is responsible for the casting relative column / get the fault value here.
// old row : a-b-[nil]
// new row : a-b-[a'/default]
// Thus the writable new row is corresponding to Write-Only constraints.
func GetChangingColVal(ctx exprctx.BuildContext, cols []*table.Column, col *table.Column, rowMap map[int64]types.Datum, defaultVals []types.Datum) (_ types.Datum, isDefaultVal bool, err error) {
	relativeCol := cols[col.ChangeStateInfo.DependencyColumnOffset]
	idxColumnVal, ok := rowMap[relativeCol.ID]
	if ok {
		idxColumnVal, err = table.CastColumnValue(ctx, idxColumnVal, col.ColumnInfo, false, false)
		// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
		if err != nil {
			return idxColumnVal, false, errors.Trace(err)
		}
		return idxColumnVal, false, nil
	}

	idxColumnVal, err = GetColDefaultValue(ctx, col, defaultVals)
	if err != nil {
		return idxColumnVal, false, errors.Trace(err)
	}

	return idxColumnVal, true, nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *TableCommon) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	opt := table.NewRemoveRecordOpt(opts...)
	return t.removeRecord(ctx, txn, h, r, opt)
}

func (t *TableCommon) removeRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opt *table.RemoveRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	err := t.removeRowData(ctx, txn, h)
	if err != nil {
		return err
	}

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(ctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	// The table has non-public column and this column is doing the operation of "modify/change column".
	// DELETE will use deletable columns, which is the same as the full columns of the table.
	// INSERT and UPDATE will only use the writable columns. So they will not see columns under MODIFY/CHANGE state.
	// This if block is for the INSERT and UPDATE.
	// And, only DELETE will make opt.HasIndexesLayout() to be true currently.
	if !opt.HasIndexesLayout() && len(t.Columns) > len(r) && t.Columns[len(r)].ChangeStateInfo != nil {
		// The changing column datum derived from related column should be casted here.
		// Otherwise, the existed changing indexes will not be deleted.
		relatedColDatum := r[t.Columns[len(r)].ChangeStateInfo.DependencyColumnOffset]
		value, err := table.CastColumnValue(ctx.GetExprCtx(), relatedColDatum, t.Columns[len(r)].ColumnInfo, false, false)
		if err != nil {
			logutil.BgLogger().Info("remove record cast value failed", zap.Any("col", t.Columns[len(r)]),
				zap.String("handle", h.String()), zap.Any("val", relatedColDatum), zap.Error(err))
			return err
		}
		r = append(r, value)
	}
	err = t.removeRowIndices(ctx, txn, h, r, opt)
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	failpoint.InjectCall("duringTableCommonRemoveRecord", t.meta)

	tc := ctx.GetExprCtx().GetEvalCtx().TypeCtx()
	if ctx.EnableMutationChecker() {
		if err = checkDataConsistency(txn, tc, t, nil, r, memBuffer, sh, opt.GetIndexesLayout()); err != nil {
			return errors.Trace(err)
		}
	}
	memBuffer.Release(sh)

	if s, ok := ctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(
			t.physicalTableID, -1, 1,
		)
	}
	return err
}

func (t *TableCommon) removeRowData(ctx table.MutateContext, txn kv.Transaction, h kv.Handle) (err error) {
	// Remove row data.
	key := t.RecordKey(h)
	failpoint.Inject("removeRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if ctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on RemoveRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = txn.SetAssertion(key, kv.SetAssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})
	if t.skipAssert {
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
	} else {
		err = txn.SetAssertion(key, kv.SetAssertExist)
	}
	if err != nil {
		return err
	}
	return txn.Delete(key)
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, rec []types.Datum, opt *table.RemoveRecordOpt) (err error) {
	for _, v := range t.DeletableIndices() {
		if v.Meta().Primary && (t.Meta().IsCommonHandle || t.Meta().PKIsHandle) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		var vals []types.Datum
		if opt.HasIndexesLayout() {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, opt.GetIndexLayout(v.Meta().ID))
		} else {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, nil)
		}
		if err != nil {
			logutil.BgLogger().Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx, txn, vals, h); err != nil {
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

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *TableCommon) buildIndexForRow(ctx table.MutateContext, h kv.Handle, vals []types.Datum, newData []types.Datum, idx *index, txn kv.Transaction, untouched bool, opt *table.CreateIdxOpt) error {
	rsData := TryGetHandleRestoredDataWrapper(t.meta, newData, nil, idx.Meta())
	if _, err := idx.create(ctx, txn, vals, h, rsData, untouched, opt); err != nil {
		if kv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, idx.Meta(), vals)
			colStrVals, err1 := genIndexKeyStrs(vals)
			if err1 != nil {
				// if genIndexKeyStrs failed, return the original error.
				return err
			}

			return kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", idx.TableMeta().Name.String(), idx.Meta().Name.String()))
		}
		return err
	}
	return nil
}

// IterRecords iterates records in the table and calls fn.
func IterRecords(t table.Table, ctx sessionctx.Context, cols []*table.Column,
	fn table.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	startKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
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
		pkIds, decodeLoc := TryGetCommonPkColumnIds(t.Meta()), ctx.GetSessionVars().Location()
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.Meta()) {
				if mysql.HasUnsignedFlag(col.GetFlag()) {
					data[col.Offset].SetUint64(uint64(handle.IntValue()))
				} else {
					data[col.Offset].SetInt64(handle.IntValue())
				}
				continue
			} else if mysql.HasPriKeyFlag(col.GetFlag()) {
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
			data[col.Offset], err = GetColDefaultValue(ctx.GetExprCtx(), col, defaultVals)
			if err != nil {
				return err
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return err
		}

		rk := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
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
func GetColDefaultValue(ctx exprctx.BuildContext, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(col.GetFlag()) {
		return colVal, errors.New("Miss column")
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
func AllocHandle(ctx context.Context, mctx table.MutateContext, t table.Table) (kv.IntHandle,
	error) {
	if mctx != nil {
		if reserved, ok := mctx.GetReservedRowIDAlloc(); ok {
			// First try to alloc if the statement has reserved auto ID.
			if rowID, ok := reserved.Consume(); ok {
				return kv.IntHandle(rowID), nil
			}
		}
	}

	_, rowID, err := AllocHandleIDs(ctx, mctx, t, 1)
	return kv.IntHandle(rowID), err
}

// AllocHandleIDs allocates n handle ids (_tidb_rowid), and caches the range
// in the table.MutateContext.
func AllocHandleIDs(ctx context.Context, mctx table.MutateContext, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(mctx).Get(autoid.RowIDAllocType).Alloc(ctx, n, 1, 1)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if meta.ShardRowIDBits > 0 {
		shardFmt := autoid.NewShardIDFormat(types.NewFieldType(mysql.TypeLonglong), meta.ShardRowIDBits, autoid.RowIDBitLength)
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
		shard := mctx.GetRowIDShardGenerator().GetCurrentShard(int(n))
		base = shardFmt.Compose(shard, base)
		maxID = shardFmt.Compose(shard, maxID)
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
func (t *TableCommon) Allocators(ctx table.AllocatorContext) autoid.Allocators {
	if ctx == nil {
		return t.allocs
	}
	if alloc, ok := ctx.AlternativeAllocators(t.meta); ok {
		return alloc
	}
	return t.allocs
}

// Type implements table.Table Type interface.
func (t *TableCommon) Type() table.Type {
	return table.NormalTable
}

func (t *TableCommon) canSkip(col *table.Column, value *types.Datum) bool {
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that but has no origin default;
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
			canSkip := idxCol.Length == types.UnspecifiedLength
			canSkip = canSkip && !types.NeedRestoredData(&col.FieldType)
			return canSkip
		}
	}
	if col.GetDefaultValue() == nil && value.IsNull() && col.GetOriginDefaultValue() == nil {
		return true
	}
	if col.IsVirtualGenerated() {
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

func getDuplicateError(tblInfo *model.TableInfo, handle kv.Handle, row []types.Datum) error {
	keyName := tblInfo.Name.String() + ".PRIMARY"

	if handle.IsInt() {
		return kv.GenKeyExistsErr([]string{handle.String()}, keyName)
	}
	pkIdx := FindPrimaryIndex(tblInfo)
	if pkIdx == nil {
		handleData, err := handle.Data()
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		colStrVals, err := genIndexKeyStrs(handleData)
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		return kv.GenKeyExistsErr(colStrVals, keyName)
	}
	pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkDts = append(pkDts, row[idxCol.Offset])
	}
	tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
	colStrVals, err := genIndexKeyStrs(pkDts)
	if err != nil {
		// if genIndexKeyStrs failed, return ErrKeyExists with handle.String().
		return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
	}
	return kv.GenKeyExistsErr(colStrVals, keyName)
}

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
	tableutil.TempTableFromMeta = TempTableFromMeta
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
func (t *TableCommon) GetSequenceNextVal(ctx any, dbName, seqName string) (nextVal int64, err error) {
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
		base, end, round, err1 = sequenceAlloc.AllocSeqCache()
		if err1 != nil {
			return err1
		}
		// Only update local cache when alloc succeed.
		seq.base = base
		seq.end = end
		seq.round = round
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
func (t *TableCommon) SetSequenceVal(ctx any, newVal int64, dbName, seqName string) (int64, bool, error) {
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
	res, alreadySatisfied, err := sequenceAlloc.RebaseSeq(newVal)
	if err != nil {
		return 0, false, err
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

// TryGetHandleRestoredDataWrapper tries to get the restored data for handle if needed. The argument can be a slice or a map.
func TryGetHandleRestoredDataWrapper(tblInfo *model.TableInfo, row []types.Datum, rowMap map[int64]types.Datum, idx *model.IndexInfo) []types.Datum {
	if !collate.NewCollationEnabled() || !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return nil
	}
	rsData := make([]types.Datum, 0, 4)
	pkIdx := FindPrimaryIndex(tblInfo)
	for _, pkIdxCol := range pkIdx.Columns {
		pkCol := tblInfo.Columns[pkIdxCol.Offset]
		if !types.NeedRestoredData(&pkCol.FieldType) {
			continue
		}
		var datum types.Datum
		if len(rowMap) > 0 {
			datum = rowMap[pkCol.ID]
		} else {
			datum = row[pkCol.Offset]
		}
		TryTruncateRestoredData(&datum, pkCol, pkIdxCol, idx)
		ConvertDatumToTailSpaceCount(&datum, pkCol)
		rsData = append(rsData, datum)
	}
	return rsData
}

// TryTruncateRestoredData tries to truncate index values.
// Says that primary key(a (8)),
// For index t(a), don't truncate the value.
// For index t(a(9)), truncate to a(9).
// For index t(a(7)), truncate to a(8).
func TryTruncateRestoredData(datum *types.Datum, pkCol *model.ColumnInfo,
	pkIdxCol *model.IndexColumn, idx *model.IndexInfo) {
	truncateTargetCol := pkIdxCol
	for _, idxCol := range idx.Columns {
		if idxCol.Offset == pkIdxCol.Offset {
			truncateTargetCol = maxIndexLen(pkIdxCol, idxCol)
			break
		}
	}
	tablecodec.TruncateIndexValue(datum, truncateTargetCol, pkCol)
}

// ConvertDatumToTailSpaceCount converts a string datum to an int datum that represents the tail space count.
func ConvertDatumToTailSpaceCount(datum *types.Datum, col *model.ColumnInfo) {
	if collate.IsBinCollation(col.GetCollate()) {
		*datum = types.NewIntDatum(stringutil.GetTailSpaceCount(datum.GetString()))
	}
}

func maxIndexLen(idxA, idxB *model.IndexColumn) *model.IndexColumn {
	if idxA.Length == types.UnspecifiedLength {
		return idxA
	}
	if idxB.Length == types.UnspecifiedLength {
		return idxB
	}
	if idxA.Length > idxB.Length {
		return idxA
	}
	return idxB
}

func getSequenceAllocator(allocs autoid.Allocators) (autoid.Allocator, error) {
	for _, alloc := range allocs.Allocs {
		if alloc.GetType() == autoid.SequenceType {
			return alloc, nil
		}
	}
	// TODO: refine the error.
	return nil, errors.New("sequence allocator is nil")
}

// BuildTableScanFromInfos build tipb.TableScan with *model.TableInfo and *model.ColumnInfo.
func BuildTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, isTiFlashStore bool) *tipb.TableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.TableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, isTiFlashStore),
		PrimaryColumnIds: pkColIDs,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// BuildPartitionTableScanFromInfos build tipb.PartitonTableScan with *model.TableInfo and *model.ColumnInfo.
func BuildPartitionTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, fastScan bool) *tipb.PartitionTableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.PartitionTableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, false),
		PrimaryColumnIds: pkColIDs,
		IsFastScan:       &fastScan,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfo.
func SetPBColumnsDefaultValue(ctx expression.BuildContext, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be computed later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		evalCtx := ctx.GetEvalCtx()
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx, c)
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(evalCtx.Location(), nil, d)
		ec := evalCtx.ErrCtx()
		err = ec.HandleError(err)
		if err != nil {
			return err
		}
	}
	return nil
}

// TemporaryTable is used to store transaction-specific or session-specific information for global / local temporary tables.
// For example, stats and autoID should have their own copies of data, instead of being shared by all sessions.
type TemporaryTable struct {
	// Whether it's modified in this transaction.
	modified bool
	// The stats of this table. So far it's always pseudo stats.
	stats *statistics.Table
	// The autoID allocator of this table.
	autoIDAllocator autoid.Allocator
	// Table size.
	size int64

	meta *model.TableInfo
}

// TempTableFromMeta builds a TempTable from model.TableInfo.
func TempTableFromMeta(tblInfo *model.TableInfo) tableutil.TempTable {
	return &TemporaryTable{
		modified:        false,
		stats:           statistics.PseudoTable(tblInfo, false, false),
		autoIDAllocator: autoid.NewAllocatorFromTempTblInfo(tblInfo),
		meta:            tblInfo,
	}
}

// GetAutoIDAllocator is implemented from TempTable.GetAutoIDAllocator.
func (t *TemporaryTable) GetAutoIDAllocator() autoid.Allocator {
	return t.autoIDAllocator
}

// SetModified is implemented from TempTable.SetModified.
func (t *TemporaryTable) SetModified(modified bool) {
	t.modified = modified
}

// GetModified is implemented from TempTable.GetModified.
func (t *TemporaryTable) GetModified() bool {
	return t.modified
}

// GetStats is implemented from TempTable.GetStats.
func (t *TemporaryTable) GetStats() any {
	return t.stats
}

// GetSize gets the table size.
func (t *TemporaryTable) GetSize() int64 {
	return t.size
}

// SetSize sets the table size.
func (t *TemporaryTable) SetSize(v int64) {
	t.size = v
}

// GetMeta gets the table meta.
func (t *TemporaryTable) GetMeta() *model.TableInfo {
	return t.meta
}
