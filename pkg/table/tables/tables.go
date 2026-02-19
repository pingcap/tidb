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
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
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


// RowWithCols is used to get the corresponding column datum values with the given handle.
func RowWithCols(t table.Table, ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := kv.GetValue(context.TODO(), txn, key)
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
