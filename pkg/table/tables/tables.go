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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-binlog"
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
	//
	// TODO: we lazily initialize them because they could possibly consume too much unnecessary
	//  memory. We can move these initialization to initTableCommon after infoschema v2 is enabled.
	//
	// sync.Once is needed to avoid data race because TableCommon may be accessed concurrently
	// They are pointers to support copying TableCommon to CachedTable and PartitionedTable
	publicColumns                       []*table.Column
	publicColumnsOnce                   *sync.Once
	visibleColumns                      []*table.Column
	visibleColumnsOnce                  *sync.Once
	hiddenColumns                       []*table.Column
	hiddenColumnsOnce                   *sync.Once
	writableColumns                     []*table.Column
	writableColumnsOnce                 *sync.Once
	fullHiddenColsAndVisibleColumns     []*table.Column
	fullHiddenColsAndVisibleColumnsOnce *sync.Once
	writableConstraints                 []*table.Constraint
	writableConstraintsOnce             *sync.Once

	indices                 []table.Index
	meta                    *model.TableInfo
	allocs                  autoid.Allocators
	sequence                *sequenceCommon
	dependencyColumnOffsets []int
	Constraints             []*table.Constraint

	// recordPrefix and indexPrefix are generated using physicalTableID.
	recordPrefix kv.Key
	indexPrefix  kv.Key
}

// ClearColumnsCache implements testingKnob interface.
func (t *TableCommon) ClearColumnsCache() {
	t.publicColumns = nil
	t.publicColumnsOnce = new(sync.Once)
	t.visibleColumns = nil
	t.visibleColumnsOnce = new(sync.Once)
	t.hiddenColumns = nil
	t.hiddenColumnsOnce = new(sync.Once)
	t.writableColumns = nil
	t.writableColumnsOnce = new(sync.Once)
	t.fullHiddenColsAndVisibleColumns = nil
	t.fullHiddenColsAndVisibleColumnsOnce = new(sync.Once)
	t.writableConstraints = nil
	t.writableConstraintsOnce = new(sync.Once)
}

// Copy copies a TableCommon struct, and reset its column cache. This is not a deep copy.
func (t *TableCommon) Copy() TableCommon {
	newTable := *t
	newTable.ClearColumnsCache()
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
	for _, col := range cols {
		if col.ChangeStateInfo != nil {
			t.dependencyColumnOffsets = append(t.dependencyColumnOffsets, col.ChangeStateInfo.DependencyColumnOffset)
		}
	}
	t.publicColumnsOnce = new(sync.Once)
	t.visibleColumnsOnce = new(sync.Once)
	t.hiddenColumnsOnce = new(sync.Once)
	t.writableColumnsOnce = new(sync.Once)
	t.fullHiddenColsAndVisibleColumnsOnce = new(sync.Once)
	t.writableConstraintsOnce = new(sync.Once)
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
		if idxName == idx.Meta().Name.L {
			return idx
		}
	}
	return nil
}

// deletableIndices implements table.Table deletableIndices interface.
func (t *TableCommon) deletableIndices() []table.Index {
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
	t.publicColumnsOnce.Do(
		func() {
			if len(t.publicColumns) == 0 {
				t.publicColumns = t.getCols(full)
			}
		},
	)
	return t.publicColumns
}

// VisibleCols implements table.Table VisibleCols interface.
func (t *TableCommon) VisibleCols() []*table.Column {
	t.visibleColumnsOnce.Do(
		func() {
			if len(t.visibleColumns) == 0 {
				t.visibleColumns = t.getCols(visible)
			}
		},
	)
	return t.visibleColumns
}

// HiddenCols implements table.Table HiddenCols interface.
func (t *TableCommon) HiddenCols() []*table.Column {
	t.hiddenColumnsOnce.Do(
		func() {
			if len(t.hiddenColumns) == 0 {
				t.hiddenColumns = t.getCols(hidden)
			}
		},
	)
	return t.hiddenColumns
}

// WritableCols implements table WritableCols interface.
func (t *TableCommon) WritableCols() []*table.Column {
	t.writableColumnsOnce.Do(
		func() {
			t.writableColumns = make([]*table.Column, 0, len(t.Columns))
			for _, col := range t.Columns {
				if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
					continue
				}
				t.writableColumns = append(t.writableColumns, col)
			}
		},
	)
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
	t.writableConstraintsOnce.Do(
		func() {
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
		})
	return t.writableConstraints
}

// CheckRowConstraint verify row check constraints.
func (t *TableCommon) CheckRowConstraint(ctx table.MutateContext, rowToCheck []types.Datum) error {
	ectx := ctx.GetExprCtx()
	for _, constraint := range t.WritableConstraint() {
		ok, isNull, err := constraint.ConstraintExpr.EvalInt(ectx.GetEvalCtx(), chunk.MutRowFromDatums(rowToCheck).ToRow())
		if err != nil {
			return err
		}
		if ok == 0 && !isNull {
			return table.ErrCheckConstraintViolated.FastGenByArgs(constraint.Name.O)
		}
	}
	return nil
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (t *TableCommon) FullHiddenColsAndVisibleCols() []*table.Column {
	t.fullHiddenColsAndVisibleColumnsOnce.Do(func() {
		t.fullHiddenColsAndVisibleColumns = make([]*table.Column, 0, len(t.Columns))
		for _, col := range t.Columns {
			if col.Hidden || col.State == model.StatePublic {
				t.fullHiddenColsAndVisibleColumns = append(t.fullHiddenColsAndVisibleColumns, col)
			}
		}
	})
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

// shouldAssert checks if the partition should be in consistent
// state and can have assertion.
func (t *TableCommon) shouldAssert(level variable.AssertionLevel) bool {
	p := t.Meta().Partition
	if p != nil {
		// This disables asserting during Reorganize Partition.
		switch level {
		case variable.AssertionLevelFast:
			// Fast option, just skip assertion for all partitions.
			if p.DDLState != model.StateNone && p.DDLState != model.StatePublic {
				return false
			}
		case variable.AssertionLevelStrict:
			// Strict, only disable assertion for intermediate partitions.
			// If there were an easy way to get from a TableCommon back to the partitioned table...
			for i := range p.AddingDefinitions {
				if t.physicalTableID == p.AddingDefinitions[i].ID {
					return false
				}
			}
		}
	}
	return true
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx context.Context, sctx table.MutateContext, h kv.Handle, oldData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable := addTemporaryTable(sctx, m); tmpTable != nil {
			if err := checkTempTableSize(sctx, tmpTable, m); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	var colIDs, binlogColIDs []int64
	var row, binlogOldRow, binlogNewRow []types.Datum
	var checksums []uint32
	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.
	colIDs = make([]int64, 0, numColsCap)
	row = make([]types.Datum, 0, numColsCap)
	if shouldWriteBinlog(sctx.GetSessionVars(), t.meta) {
		binlogColIDs = make([]int64, 0, numColsCap)
		binlogOldRow = make([]types.Datum, 0, numColsCap)
		binlogNewRow = make([]types.Datum, 0, numColsCap)
	}
	checksumData := t.initChecksumData(sctx, h)
	needChecksum := len(checksumData) > 0
	rowToCheck := make([]types.Datum, 0, numColsCap)

	for _, col := range t.Columns {
		var value types.Datum
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
			if needChecksum {
				if col.ChangeStateInfo != nil {
					// TODO: Check overflow or ignoreTruncate.
					v, err := table.CastColumnValue(sctx.GetExprCtx(), newData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
					if err != nil {
						return err
					}
					checksumData = t.appendInChangeColForChecksum(sctx, h, checksumData, col.ToInfo(), &newData[col.DependencyColumnOffset], &v)
				} else {
					v, err := table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
					if err != nil {
						return err
					}
					checksumData = t.appendNonPublicColForChecksum(sctx, h, checksumData, col.ToInfo(), &v)
				}
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
				checksumData = t.appendInChangeColForChecksum(sctx, h, checksumData, col.ToInfo(), &newData[col.DependencyColumnOffset], &value)
			} else if needChecksum {
				checksumData = t.appendNonPublicColForChecksum(sctx, h, checksumData, col.ToInfo(), &value)
			}
		} else {
			value = newData[col.Offset]
			checksumData = t.appendPublicColForChecksum(sctx, h, checksumData, col.ToInfo(), &value)
		}
		if !t.canSkip(col, &value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
		rowToCheck = append(rowToCheck, value)
		if shouldWriteBinlog(sctx.GetSessionVars(), t.meta) && !t.canSkipUpdateBinlog(col, value) {
			binlogColIDs = append(binlogColIDs, col.ID)
			binlogOldRow = append(binlogOldRow, oldData[col.Offset])
			binlogNewRow = append(binlogNewRow, value)
		}
	}
	// check data constraint
	err = t.CheckRowConstraint(sctx, rowToCheck)
	if err != nil {
		return err
	}
	sessVars := sctx.GetSessionVars()
	// rebuild index
	if !sessVars.InTxn() {
		savePresumeKeyNotExist := sessVars.PresumeKeyNotExists
		if !sessVars.ConstraintCheckInPlace && sessVars.TxnCtx.IsPessimistic {
			sessVars.PresumeKeyNotExists = true
		}
		err = t.rebuildIndices(sctx, txn, h, touched, oldData, newData, table.WithCtx(ctx))
		sessVars.PresumeKeyNotExists = savePresumeKeyNotExist
		if err != nil {
			return err
		}
	} else {
		err = t.rebuildIndices(sctx, txn, h, touched, oldData, newData, table.WithCtx(ctx))
		if err != nil {
			return err
		}
	}

	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(row))
	key := t.RecordKey(h)
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	checksums, writeBufs.RowValBuf = t.calcChecksums(sctx, h, checksumData, writeBufs.RowValBuf)
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(sc.TimeZone(), row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd, checksums...)
	err = sc.HandleError(err)
	if err != nil {
		return err
	}
	if err = memBuffer.Set(key, writeBufs.RowValBuf); err != nil {
		return err
	}

	failpoint.Inject("updateRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.GetSessionVars().ConnectionID != 0 {
			logutil.BgLogger().Info("force asserting not exist on UpdateRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = txn.SetAssertion(key, kv.SetAssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})

	if t.shouldAssert(sessVars.AssertionLevel) {
		err = txn.SetAssertion(key, kv.SetAssertExist)
	} else {
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
	}
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	if sessVars.EnableMutationChecker {
		if err = CheckDataConsistency(txn, sessVars, t, newData, oldData, memBuffer, sh); err != nil {
			return errors.Trace(err)
		}
	}

	memBuffer.Release(sh)
	if shouldWriteBinlog(sctx.GetSessionVars(), t.meta) {
		if !t.meta.PKIsHandle && !t.meta.IsCommonHandle {
			binlogColIDs = append(binlogColIDs, model.ExtraHandleID)
			binlogOldRow = append(binlogOldRow, types.NewIntDatum(h.IntValue()))
			binlogNewRow = append(binlogNewRow, types.NewIntDatum(h.IntValue()))
		}
		err = t.addUpdateBinlog(sctx, binlogOldRow, binlogNewRow, binlogColIDs)
		if err != nil {
			return err
		}
	}
	colSize := make([]variable.ColSize, len(t.Cols()))
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc.TypeCtx(), newData[id])
		if err != nil {
			continue
		}
		newLen := size - 1
		size, err = codec.EstimateValueSize(sc.TypeCtx(), oldData[id])
		if err != nil {
			continue
		}
		oldLen := size - 1
		colSize[id] = variable.ColSize{ColID: col.ID, Size: int64(newLen - oldLen)}
	}
	sessVars.TxnCtx.UpdateDeltaForTableFromColSlice(t.physicalTableID, 0, 1, colSize)
	return nil
}

func (t *TableCommon) rebuildIndices(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, touched []bool, oldData []types.Datum, newData []types.Datum, opts ...table.CreateIdxOptFunc) error {
	for _, idx := range t.deletableIndices() {
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
			if err = t.removeRowIndex(ctx, h, oldVs, idx, txn); err != nil {
				return err
			}
			break
		}
	}
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
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
		// If txn is auto commit and index is untouched, no need to write index value.
		// If InHandleForeignKeyTrigger or ForeignKeyTriggerCtx.HasFKCascades is true indicate we may have
		// foreign key cascade need to handle later, then we still need to write index value,
		// otherwise, the later foreign cascade executor may see data-index inconsistency in txn-mem-buffer.
		sessVars := ctx.GetSessionVars()
		if untouched && !sessVars.InTxn() &&
			!sessVars.StmtCtx.InHandleForeignKeyTrigger && !sessVars.StmtCtx.ForeignKeyTriggerCtx.HasFKCascades {
			continue
		}
		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, h, newVs, newData, idx, txn, untouched, opts...); err != nil {
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

func addTemporaryTable(sctx table.MutateContext, tblInfo *model.TableInfo) tableutil.TempTable {
	tempTable := sctx.TxnRecordTempTable(tblInfo)
	tempTable.SetModified(true)
	return tempTable
}

// The size of a temporary table is calculated by accumulating the transaction size delta.
func handleTempTableSize(t tableutil.TempTable, txnSizeBefore int, txn kv.Transaction) {
	txnSizeNow := txn.Size()
	delta := txnSizeNow - txnSizeBefore

	oldSize := t.GetSize()
	newSize := oldSize + int64(delta)
	t.SetSize(newSize)
}

func checkTempTableSize(ctx table.MutateContext, tmpTable tableutil.TempTable, tblInfo *model.TableInfo) error {
	tmpTableSize := tmpTable.GetSize()
	if tempTableData := ctx.GetSessionVars().TemporaryTableData; tempTableData != nil {
		tmpTableSize += tempTableData.GetTableSize(tblInfo.ID)
	}

	if tmpTableSize > ctx.GetSessionVars().TMPTableSize {
		return table.ErrTempTableFull.GenWithStackByArgs(tblInfo.Name.O)
	}

	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(sctx table.MutateContext, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, err
	}

	var opt table.AddRecordOpt
	for _, fn := range opts {
		fn.ApplyOn(&opt)
	}

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable := addTemporaryTable(sctx, m); tmpTable != nil {
			if err := checkTempTableSize(sctx, tmpTable, m); err != nil {
				return nil, err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	var ctx context.Context
	if opt.Ctx != nil {
		ctx = opt.Ctx
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "table.AddRecord")
		defer r.End()
	} else {
		ctx = context.Background()
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
			handleBytes, err = codec.EncodeKey(sctx.GetSessionVars().StmtCtx.TimeZone(), nil, pkDts...)
			err = sctx.GetSessionVars().StmtCtx.HandleError(err)
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
			sessVars := sctx.GetSessionVars()
			stmtCtx := sessVars.StmtCtx
			stmtCtx.BaseRowID, stmtCtx.MaxRowID, err = allocHandleIDs(ctx, sctx, t, uint64(opt.ReserveAutoID))
			if err != nil {
				return nil, err
			}
		}

		recordID, err = AllocHandle(ctx, sctx, t)
		if err != nil {
			return nil, err
		}
	}

	var colIDs, binlogColIDs []int64
	var row, binlogRow []types.Datum
	var checksums []uint32
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
	checksumData := t.initChecksumData(sctx, recordID)
	needChecksum := len(checksumData) > 0

	for _, col := range t.Columns {
		var value types.Datum
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			if needChecksum {
				if col.ChangeStateInfo != nil {
					// TODO: Check overflow or ignoreTruncate.
					v, err := table.CastColumnValue(sctx.GetExprCtx(), r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
					if err != nil {
						return nil, err
					}
					checksumData = t.appendInChangeColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &r[col.DependencyColumnOffset], &v)
				} else {
					v, err := table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
					if err != nil {
						return nil, err
					}
					checksumData = t.appendNonPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &v)
				}
			}
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
			row = append(row, value)
			colIDs = append(colIDs, col.ID)
			checksumData = t.appendInChangeColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &r[col.DependencyColumnOffset], &value)
			continue
		}
		if col.State == model.StatePublic {
			value = r[col.Offset]
			checksumData = t.appendPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &value)
		} else {
			// col.ChangeStateInfo must be nil here.
			// because `col.State != model.StatePublic` is true here, if col.ChangeStateInfo is not nil, the col should
			// be handle by the previous if-block.

			if opt.IsUpdate {
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
			checksumData = t.appendNonPublicColForChecksum(sctx, recordID, checksumData, col.ToInfo(), &value)
		}
		if !t.canSkip(col, &value) {
			colIDs = append(colIDs, col.ID)
			row = append(row, value)
		}
	}
	// check data constraint
	err = t.CheckRowConstraint(sctx, r)
	if err != nil {
		return nil, err
	}
	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(row))
	key := t.RecordKey(recordID)
	logutil.BgLogger().Debug("addRecord",
		zap.Stringer("key", key))
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	checksums, writeBufs.RowValBuf = t.calcChecksums(sctx, recordID, checksumData, writeBufs.RowValBuf)
	writeBufs.RowValBuf, err = tablecodec.EncodeRow(sc.TimeZone(), row, colIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd, checksums...)
	err = sc.HandleError(err)
	if err != nil {
		return nil, err
	}
	value := writeBufs.RowValBuf

	var setPresume bool
	if !sctx.GetSessionVars().StmtCtx.BatchCheck {
		if t.meta.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			_, err = txn.Get(ctx, key)
		} else if sctx.GetSessionVars().LazyCheckKeyNotExists() || txn.IsPipelined() {
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
			handleStr := getDuplicateErrorHandleString(t.Meta(), recordID, r)
			return recordID, kv.ErrKeyExists.FastGenByArgs(handleStr, t.Meta().Name.String()+".PRIMARY")
		} else if !kv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	if setPresume {
		flags := []kv.FlagsOp{kv.SetPresumeKeyNotExists}
		if !sessVars.ConstraintCheckInPlacePessimistic && sessVars.TxnCtx.IsPessimistic && sessVars.InTxn() &&
			!sessVars.InRestrictedSQL && sessVars.ConnectionID > 0 {
			flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
		}
		err = memBuffer.SetWithFlags(key, value, flags...)
	} else {
		err = memBuffer.Set(key, value)
	}
	if err != nil {
		return nil, err
	}

	failpoint.Inject("addRecordForceAssertExist", func() {
		// Assert the key exists while it actually doesn't. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.GetSessionVars().ConnectionID != 0 {
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

	if err = injectMutationError(t, txn, sh); err != nil {
		return nil, err
	}
	if sessVars.EnableMutationChecker {
		if err = CheckDataConsistency(txn, sessVars, t, r, nil, memBuffer, sh); err != nil {
			return nil, errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if shouldWriteBinlog(sctx.GetSessionVars(), t.meta) {
		// For insert, TiDB and Binlog can use same row and schema.
		binlogRow = row
		binlogColIDs = colIDs
		err = t.addInsertBinlog(sctx, recordID, binlogRow, binlogColIDs)
		if err != nil {
			return nil, err
		}
	}

	if sessVars.TxnCtx == nil {
		return recordID, nil
	}

	if shouldIncreaseTTLMetricCount(t.meta) {
		sessVars.TxnCtx.InsertTTLRowsCount += 1
	}

	colSize := make([]variable.ColSize, len(t.Cols()))
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc.TypeCtx(), r[id])
		if err != nil {
			continue
		}
		colSize[id] = variable.ColSize{ColID: col.ID, Size: int64(size - 1)}
	}
	sessVars.TxnCtx.UpdateDeltaForTableFromColSlice(t.physicalTableID, 1, 1, colSize)
	return recordID, nil
}

// genIndexKeyStr generates index content string representation.
func genIndexKeyStr(colVals []types.Datum) (string, error) {
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
func (t *TableCommon) addIndices(sctx table.MutateContext, recordID kv.Handle, r []types.Datum, txn kv.Transaction, opts []table.CreateIdxOptFunc) (kv.Handle, error) {
	writeBufs := sctx.GetSessionVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := sctx.GetSessionVars().StmtCtx.BatchCheck
	for _, v := range t.Indices() {
		if !IsIndexWritable(v) {
			continue
		}
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}
		indexVals, err := v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, v.Meta(), indexVals)
			entryKey, err := genIndexKeyStr(indexVals)
			if err != nil {
				return nil, err
			}
			dupErr = kv.ErrKeyExists.FastGenByArgs(entryKey, fmt.Sprintf("%s.%s", v.TableMeta().Name.String(), v.Meta().Name.String()))
		}
		rsData := TryGetHandleRestoredDataWrapper(t.meta, r, nil, v.Meta())
		if dupHandle, err := v.Create(sctx, txn, indexVals, recordID, rsData, opts...); err != nil {
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
	v, _, err := DecodeRawRowData(ctx, t.Meta(), h, cols, value)
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
func DecodeRawRowData(ctx sessionctx.Context, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
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
				dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetSessionVars().Location())
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
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetSessionVars().Location())
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
			v[i], _, err = GetChangingColVal(ctx.GetExprCtx(), cols, col, rowMap, defaultVals)
		} else {
			v[i], err = GetColDefaultValue(ctx.GetExprCtx(), col, defaultVals)
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
func (t *TableCommon) RemoveRecord(ctx table.MutateContext, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	logutil.BgLogger().Debug("RemoveRecord",
		zap.Stringer("key", t.RecordKey(h)))
	err = t.removeRowData(ctx, h)
	if err != nil {
		return err
	}

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable := addTemporaryTable(ctx, m); tmpTable != nil {
			if err := checkTempTableSize(ctx, tmpTable, m); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	// The table has non-public column and this column is doing the operation of "modify/change column".
	if len(t.Columns) > len(r) && t.Columns[len(r)].ChangeStateInfo != nil {
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
	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		return err
	}

	sessVars := ctx.GetSessionVars()
	sc := sessVars.StmtCtx
	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	if sessVars.EnableMutationChecker {
		if err = CheckDataConsistency(txn, sessVars, t, nil, r, memBuffer, sh); err != nil {
			return errors.Trace(err)
		}
	}
	memBuffer.Release(sh)

	if shouldWriteBinlog(ctx.GetSessionVars(), t.meta) {
		cols := t.DeletableCols()
		colIDs := make([]int64, 0, len(cols)+1)
		for _, col := range cols {
			colIDs = append(colIDs, col.ID)
		}
		var binlogRow []types.Datum
		if !t.meta.PKIsHandle && !t.meta.IsCommonHandle {
			colIDs = append(colIDs, model.ExtraHandleID)
			binlogRow = make([]types.Datum, 0, len(r)+1)
			binlogRow = append(binlogRow, r...)
			handleData, err := h.Data()
			if err != nil {
				return err
			}
			binlogRow = append(binlogRow, handleData...)
		} else {
			binlogRow = r
		}
		err = t.addDeleteBinlog(ctx, binlogRow, colIDs)
	}
	if ctx.GetSessionVars().TxnCtx == nil {
		return nil
	}
	colSize := make([]variable.ColSize, len(t.Cols()))
	for id, col := range t.Cols() {
		size, err := codec.EstimateValueSize(sc.TypeCtx(), r[id])
		if err != nil {
			continue
		}
		colSize[id] = variable.ColSize{ColID: col.ID, Size: -int64(size - 1)}
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTableFromColSlice(t.physicalTableID, -1, 1, colSize)
	return err
}

func (t *TableCommon) addInsertBinlog(ctx table.MutateContext, h kv.Handle, row []types.Datum, colIDs []int64) error {
	mutation := t.getMutation(ctx)
	handleData, err := h.Data()
	if err != nil {
		return err
	}
	pk, err := codec.EncodeValue(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, handleData...)
	err = ctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return err
	}
	value, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx.TimeZone(), row, colIDs, nil, nil)
	err = ctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return err
	}
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *TableCommon) addUpdateBinlog(ctx table.MutateContext, oldRow, newRow []types.Datum, colIDs []int64) error {
	old, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx.TimeZone(), oldRow, colIDs, nil, nil)
	err = ctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return err
	}
	newVal, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx.TimeZone(), newRow, colIDs, nil, nil)
	err = ctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return err
	}
	bin := append(old, newVal...)
	mutation := t.getMutation(ctx)
	mutation.UpdatedRows = append(mutation.UpdatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Update)
	return nil
}

func (t *TableCommon) addDeleteBinlog(ctx table.MutateContext, r []types.Datum, colIDs []int64) error {
	data, err := tablecodec.EncodeOldRow(ctx.GetSessionVars().StmtCtx.TimeZone(), r, colIDs, nil, nil)
	err = ctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return err
	}
	mutation := t.getMutation(ctx)
	mutation.DeletedRows = append(mutation.DeletedRows, data)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_DeleteRow)
	return nil
}

func writeSequenceUpdateValueBinlog(sctx sessionctx.Context, db, sequence string, end int64) error {
	// 1: when sequenceCommon update the local cache passively.
	// 2: When sequenceCommon setval to the allocator actively.
	// Both of this two case means the upper bound the sequence has changed in meta, which need to write the binlog
	// to the downstream.
	// Sequence sends `select setval(seq, num)` sql string to downstream via `setDDLBinlog`, which is mocked as a DDL binlog.
	binlogCli := sctx.GetSessionVars().BinlogClient
	sqlMode := sctx.GetSessionVars().SQLMode
	sequenceFullName := stringutil.Escape(db, sqlMode) + "." + stringutil.Escape(sequence, sqlMode)
	sql := "select setval(" + sequenceFullName + ", " + strconv.FormatInt(end, 10) + ")"

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMeta)
	err := kv.RunInNewTxn(ctx, sctx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
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

func (t *TableCommon) removeRowData(ctx table.MutateContext, h kv.Handle) error {
	// Remove row data.
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	failpoint.Inject("removeRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if ctx.GetSessionVars().ConnectionID != 0 {
			logutil.BgLogger().Info("force asserting not exist on RemoveRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = txn.SetAssertion(key, kv.SetAssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})
	if t.shouldAssert(ctx.GetSessionVars().AssertionLevel) {
		err = txn.SetAssertion(key, kv.SetAssertExist)
	} else {
		err = txn.SetAssertion(key, kv.SetAssertUnknown)
	}
	if err != nil {
		return err
	}
	return txn.Delete(key)
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx table.MutateContext, h kv.Handle, rec []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, v := range t.deletableIndices() {
		if v.Meta().Primary && (t.Meta().IsCommonHandle || t.Meta().PKIsHandle) {
			continue
		}
		vals, err := v.FetchValues(rec, nil)
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

// removeRowIndex implements table.Table RemoveRowIndex interface.
func (t *TableCommon) removeRowIndex(ctx table.MutateContext, h kv.Handle, vals []types.Datum, idx table.Index, txn kv.Transaction) error {
	return idx.Delete(ctx, txn, vals, h)
}

// buildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *TableCommon) buildIndexForRow(ctx table.MutateContext, h kv.Handle, vals []types.Datum, newData []types.Datum, idx table.Index, txn kv.Transaction, untouched bool, popts ...table.CreateIdxOptFunc) error {
	var opts []table.CreateIdxOptFunc
	opts = append(opts, popts...)
	if untouched {
		opts = append(opts, table.IndexIsUntouched)
	}
	rsData := TryGetHandleRestoredDataWrapper(t.meta, newData, nil, idx.Meta())
	if _, err := idx.Create(ctx, txn, vals, h, rsData, opts...); err != nil {
		if kv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, idx.Meta(), vals)
			entryKey, err1 := genIndexKeyStr(vals)
			if err1 != nil {
				// if genIndexKeyStr failed, return the original error.
				return err
			}

			return kv.ErrKeyExists.FastGenByArgs(entryKey, fmt.Sprintf("%s.%s", idx.TableMeta().Name.String(), idx.Meta().Name.String()))
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
func AllocHandle(ctx context.Context, mctx table.MutateContext, t table.Table) (kv.Handle, error) {
	if mctx != nil {
		if stmtCtx := mctx.GetSessionVars().StmtCtx; stmtCtx != nil {
			// First try to alloc if the statement has reserved auto ID.
			if stmtCtx.BaseRowID < stmtCtx.MaxRowID {
				stmtCtx.BaseRowID++
				return kv.IntHandle(stmtCtx.BaseRowID), nil
			}
		}
	}

	_, rowID, err := allocHandleIDs(ctx, mctx, t, 1)
	return kv.IntHandle(rowID), err
}

func allocHandleIDs(ctx context.Context, mctx table.MutateContext, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(mctx).Get(autoid.RowIDAllocType).Alloc(ctx, n, 1, 1)
	if err != nil {
		return 0, 0, err
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
		shard := mctx.GetSessionVars().GetCurrentShard(int(n))
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

	// Use an independent allocator for global temporary tables.
	if t.meta.TempTableType == model.TempTableGlobal {
		if tbl := ctx.TxnRecordTempTable(t.meta); tbl != nil {
			if alloc := tbl.GetAutoIDAllocator(); alloc != nil {
				return autoid.NewAllocators(false, alloc)
			}
		}
		// If the session is not in a txn, for example, in "show create table", use the original allocator.
		// Otherwise the would be a nil pointer dereference.
	}
	return t.allocs
}

// Type implements table.Table Type interface.
func (t *TableCommon) Type() table.Type {
	return table.NormalTable
}

func shouldWriteBinlog(vars *variable.SessionVars, tblInfo *model.TableInfo) bool {
	failpoint.Inject("forceWriteBinlog", func() {
		// Just to cover binlog related code in this package, since the `BinlogClient` is
		// still nil, mutations won't be written to pump on commit.
		failpoint.Return(true)
	})
	if vars.BinlogClient == nil {
		return false
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return false
	}
	return !vars.InRestrictedSQL
}

func shouldIncreaseTTLMetricCount(tblInfo *model.TableInfo) bool {
	return tblInfo.TTLInfo != nil
}

func (t *TableCommon) getMutation(ctx table.MutateContext) *binlog.TableMutation {
	return ctx.StmtGetMutation(t.tableID)
}

// initChecksumData allocates data for checksum calculation, returns nil if checksum is disabled or unavailable. The
// length of returned data can be considered as the number of checksums we need to write.
func (t *TableCommon) initChecksumData(sctx table.MutateContext, h kv.Handle) [][]rowcodec.ColData {
	if !sctx.GetSessionVars().IsRowLevelChecksumEnabled() {
		return nil
	}
	numNonPubCols := len(t.Columns) - len(t.Cols())
	if numNonPubCols > 1 {
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Warn,
			"skip checksum since the number of non-public columns is greater than 1",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID), zap.Any("cols", t.meta.Columns))
		return nil
	}
	return make([][]rowcodec.ColData, 1+numNonPubCols)
}

// calcChecksums calculates the checksums of input data. The arg `buf` is used to hold the temporary encoded col data
// and it will be reset for each col, so do NOT pass a buf that contains data you may use later. If the capacity of
// `buf` is enough, it gets returned directly, otherwise a new bytes with larger capacity will be returned, and you can
// hold the returned buf for later use (to avoid memory allocation).
func (t *TableCommon) calcChecksums(sctx table.MutateContext, h kv.Handle, data [][]rowcodec.ColData, buf []byte) ([]uint32, []byte) {
	if len(data) == 0 {
		return nil, buf
	}
	checksums := make([]uint32, len(data))
	for i, cols := range data {
		row := rowcodec.RowData{
			Cols: cols,
			Data: buf,
		}
		if !sort.IsSorted(row) {
			sort.Sort(row)
		}
		checksum, err := row.Checksum(sctx.GetSessionVars().StmtCtx.TimeZone())
		buf = row.Data
		if err != nil {
			logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
				"skip checksum due to encode error",
				zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID), zap.Error(err))
			return nil, buf
		}
		checksums[i] = checksum
	}
	return checksums, buf
}

// appendPublicColForChecksum appends a public column data for checksum. If the column is in changing, that is, it's the
// old column of an on-going modify-column ddl, then skip it since it will be handle by `appendInChangeColForChecksum`.
func (t *TableCommon) appendPublicColForChecksum(
	sctx table.MutateContext, h kv.Handle, data [][]rowcodec.ColData, c *model.ColumnInfo, d *types.Datum,
) [][]rowcodec.ColData {
	if len(data) == 0 { // no need for checksum
		return nil
	}
	if c.State != model.StatePublic { // assert col is public
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
			"skip checksum due to inconsistent column state",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID), zap.Any("col", c))
		return nil
	}
	for _, offset := range t.dependencyColumnOffsets {
		if c.Offset == offset {
			// the col is in changing, skip it.
			return data
		}
	}
	// calculate the checksum with this col
	data[0] = appendColForChecksum(data[0], t, c, d)
	if len(data) > 1 {
		// calculate the extra checksum with this col
		data[1] = appendColForChecksum(data[1], t, c, d)
	}
	return data
}

// appendNonPublicColForChecksum appends a non-public (but not in-changing) column data for checksum. Two checksums are
// required because there is a non-public column. The first checksum should be calculate with the original (or default)
// value of this column. The extra checksum shall be calculated without this non-public column, thus nothing to do with
// data[1].
func (t *TableCommon) appendNonPublicColForChecksum(
	sctx table.MutateContext, h kv.Handle, data [][]rowcodec.ColData, c *model.ColumnInfo, d *types.Datum,
) [][]rowcodec.ColData {
	if size := len(data); size == 0 { // no need for checksum
		return nil
	} else if size == 1 { // assert that 2 checksums are required
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
			"skip checksum due to inconsistent length of column data",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID))
		return nil
	}
	if c.State == model.StatePublic || c.ChangeStateInfo != nil { // assert col is not public and is not in changing
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
			"skip checksum due to inconsistent column state",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID), zap.Any("col", c))
		return nil
	}
	data[0] = appendColForChecksum(data[0], t, c, d)

	return data
}

// appendInChangeColForChecksum appends an in-changing column data for checksum. Two checksums are required because
// there is a non-public column. The first checksum should be calculate with the old version of this column and the extra
// checksum should be calculated with the new version of column.
func (t *TableCommon) appendInChangeColForChecksum(
	sctx table.MutateContext, h kv.Handle, data [][]rowcodec.ColData, c *model.ColumnInfo, oldVal *types.Datum, newVal *types.Datum,
) [][]rowcodec.ColData {
	if size := len(data); size == 0 { // no need for checksum
		return nil
	} else if size == 1 { // assert that 2 checksums are required
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
			"skip checksum due to inconsistent length of column data",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID))
		return nil
	}
	if c.State == model.StatePublic || c.ChangeStateInfo == nil { // assert col is not public and is in changing
		logWithContext(sctx.GetSessionVars(), logutil.BgLogger().Error,
			"skip checksum due to inconsistent column state",
			zap.Stringer("key", t.RecordKey(h)), zap.Int64("tblID", t.meta.ID), zap.Any("col", c))
		return nil
	}
	// calculate the checksum with the old version of col
	data[0] = appendColForChecksum(data[0], t, t.meta.Columns[c.DependencyColumnOffset], oldVal)
	// calculate the extra checksum with the new version of col
	data[1] = appendColForChecksum(data[1], t, c, newVal)

	return data
}

func appendColForChecksum(dst []rowcodec.ColData, t *TableCommon, c *model.ColumnInfo, d *types.Datum) []rowcodec.ColData {
	if c.IsGenerated() && !c.GeneratedStored {
		return dst
	}
	if dst == nil {
		dst = make([]rowcodec.ColData, 0, len(t.Columns))
	}
	return append(dst, rowcodec.ColData{ColumnInfo: c, Datum: d})
}

func logWithContext(sessVars *variable.SessionVars, log func(msg string, fields ...zap.Field), msg string, fields ...zap.Field) {
	ctxFields := make([]zap.Field, 0, len(fields)+2)
	ctxFields = append(ctxFields, zap.Uint64("conn", sessVars.ConnectionID))
	if sessVars.TxnCtx != nil {
		ctxFields = append(ctxFields, zap.Uint64("txnStartTS", sessVars.TxnCtx.StartTS))
	}
	ctxFields = append(ctxFields, fields...)
	log(msg, ctxFields...)
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

// canSkipUpdateBinlog checks whether the column can be skipped or not.
func (t *TableCommon) canSkipUpdateBinlog(col *table.Column, value types.Datum) bool {
	return col.IsVirtualGenerated()
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

func getDuplicateErrorHandleString(tblInfo *model.TableInfo, handle kv.Handle, row []types.Datum) string {
	if handle.IsInt() {
		return kv.GetDuplicateErrorHandleString(handle)
	}
	pkIdx := FindPrimaryIndex(tblInfo)
	if pkIdx == nil {
		return kv.GetDuplicateErrorHandleString(handle)
	}
	pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkDts = append(pkDts, row[idxCol.Offset])
	}
	tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
	entryKey, err := genIndexKeyStr(pkDts)
	if err != nil {
		// if genIndexKeyStr failed, return DuplicateErrorHandleString.
		return kv.GetDuplicateErrorHandleString(handle)
	}
	return entryKey
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
func BuildTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo) *tipb.TableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.TableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false),
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
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false),
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
