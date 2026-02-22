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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
		idx, err := NewIndex(t.physicalTableID, tblInfo, idxInfo)
		if err != nil {
			return err
		}
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

// checkDataForModifyColumn checks if the data can be stored in the column with changingType.
// It's used to prevent illegal data being inserted if we want to skip reorg.
func checkDataForModifyColumn(row []types.Datum, col *table.Column) error {
	if col.ChangingFieldType == nil {
		return nil
	}

	data := row[col.Offset]
	value, err := table.CastColumnValueWithStrictMode(data, col.ChangingFieldType)
	if err != nil {
		return err
	}

	// For the case from VARCHAR -> CHAR
	if col.ChangingFieldType.GetType() == mysql.TypeString && value.GetString() != data.GetString() {
		return errors.New("data truncation error during modify column")
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


