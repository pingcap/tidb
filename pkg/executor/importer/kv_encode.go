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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql" //nolint: goimports
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// TableKVEncoder encodes a row of data into a KV pair.
type TableKVEncoder struct {
	*kv.BaseKVEncoder
	// see import.go
	columnAssignments []expression.Expression
	fieldMappings     []*FieldMapping
	insertColumns     []*table.Column
	// Following cache use to avoid `runtime.makeslice`.
	insertColumnRowCache []types.Datum
	rowCache             []types.Datum
	hasValueCache        []bool

	// insertColToFileIdx maps each insert-column index to the
	// corresponding file-column index. -1 means no mapping (e.g., SET clause).
	insertColToFileIdx []int

	// currentSkipCast holds the per-file-column skip-cast decisions for the
	// row currently being encoded. Set at the start of each Encode call.
	currentSkipCast []bool

	// Pre-classified column indices for buildRecord.
	// Computed once in initBuildRecordMeta.
	defaultColOffsets []int  // table-column offsets for columns that get defaults (not insert, not special)
	specialColOffsets []int  // table-column offsets for auto-inc, auto-random, generated columns
	notNullFlags      []bool // per-column NOT NULL flag cache (indexed by table-column offset)
	// insertColNeedsPost indexes into insertColumns; true when a column needs
	// post-cast handling that CastColumnValue does not perform:
	// NOT-NULL enforcement (CheckNotNull/HandleBadNull), or allocator Rebase
	// for auto-inc / auto-random / generated columns. Columns where this is
	// false stay on the fast path (skip-cast or cast, then plain assign).
	insertColNeedsPost []bool
}

type simpleColAssignExprCreator interface {
	CreateColAssignSimpleExprs(expression.BuildContext) ([]expression.Expression, []contextutil.SQLWarn, error)
}

// NewTableKVEncoder creates a new TableKVEncoder.
// exported for test.
func NewTableKVEncoder(
	config *encode.EncodingConfig,
	ctrl *LoadDataController,
) (*TableKVEncoder, error) {
	return newTableKVEncoderInner(config, ctrl, ctrl.FieldMappings, ctrl.InsertColumns)
}

// NewTableKVEncoderForDupResolve creates a new TableKVEncoder for duplicate resolution.
func NewTableKVEncoderForDupResolve(
	config *encode.EncodingConfig,
	ctrl *LoadDataController,
) (*TableKVEncoder, error) {
	mappings, _ := tableVisCols2FieldMappings(ctrl.Table)
	return newTableKVEncoderInner(config, ctrl, mappings, ctrl.Table.VisibleCols())
}

func newTableKVEncoderInner(
	config *encode.EncodingConfig,
	exprCreator simpleColAssignExprCreator,
	fieldMappings []*FieldMapping,
	insertColumns []*table.Column,
) (*TableKVEncoder, error) {
	baseKVEncoder, err := kv.NewBaseKVEncoder(config)
	if err != nil {
		return nil, err
	}
	colAssignExprs, _, err := exprCreator.CreateColAssignSimpleExprs(baseKVEncoder.SessionCtx.GetExprCtx())
	if err != nil {
		return nil, err
	}

	enc := &TableKVEncoder{
		BaseKVEncoder:     baseKVEncoder,
		columnAssignments: colAssignExprs,
		fieldMappings:     fieldMappings,
		insertColumns:     insertColumns,
	}
	enc.initInsertColFileMapping()
	enc.initBuildRecordMeta()
	return enc, nil
}

// Encode encodes table row data into KV pairs.
// skipCast is aligned with row, which indicates that whether the values in row
// can be directly used without casting to column type.
func (en *TableKVEncoder) Encode(row []types.Datum, skipCast []bool, rowID int64) (*kv.Pairs, error) {
	// we ignore warnings when encoding rows now, but warnings uses the same memory as parser, since the input
	// row []types.Datum share the same underlying buf, and when doing CastValue, we're using hack.String/hack.Slice.
	// when generating error such as mysql.ErrDataOutOfRange, the data will be part of the error, causing the buf
	// unable to release. So we truncate the warnings here.
	en.currentSkipCast = skipCast
	defer func() {
		en.TruncateWarns()
		en.currentSkipCast = nil
	}()

	record, err := en.parserData2TableData(row, rowID)
	if err != nil {
		return nil, err
	}

	return en.Record2KV(record, row, rowID)
}

// todo merge with code in load_data.go
func (en *TableKVEncoder) parserData2TableData(parserData []types.Datum, rowID int64) ([]types.Datum, error) {
	if cap(en.insertColumnRowCache) < len(en.insertColumns) {
		en.insertColumnRowCache = make([]types.Datum, 0, len(en.insertColumns))
	}
	row := en.insertColumnRowCache[:0]
	setVar := func(name string, col *types.Datum) {
		// User variable names are not case-sensitive
		// https://dev.mysql.com/doc/refman/8.0/en/user-variables.html
		name = strings.ToLower(name)
		if col == nil || col.IsNull() {
			en.SessionCtx.UnsetUserVar(name)
		} else {
			en.SessionCtx.SetUserVarVal(name, *col)
		}
	}

	rowLen := len(en.Columns)
	if cap(en.rowCache) < rowLen || cap(en.hasValueCache) < rowLen {
		en.rowCache = make([]types.Datum, rowLen)
		en.hasValueCache = make([]bool, rowLen)
	} else {
		en.rowCache = en.rowCache[:0]
		en.hasValueCache = en.hasValueCache[:0]
		for range rowLen {
			en.rowCache = append(en.rowCache, types.Datum{})
			en.hasValueCache = append(en.hasValueCache, false)
		}
	}
	hasValue := en.hasValueCache
	for i := range en.insertColumns {
		offset := en.insertColumns[i].Offset
		hasValue[offset] = true
	}

	for i := range en.fieldMappings {
		col := en.fieldMappings[i].Column
		if i >= len(parserData) {
			if col == nil {
				setVar(en.fieldMappings[i].UserVar.Name, nil)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(col.GetType()) && mysql.HasNotNullFlag(col.GetFlag()) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(col.GetType())))
				continue
			}

			row = append(row, types.NewDatum(nil))
			hasValue[col.Offset] = false
			continue
		}

		if col == nil {
			setVar(en.fieldMappings[i].UserVar.Name, &parserData[i])
			continue
		}

		row = append(row, parserData[i])
	}
	for i := range en.columnAssignments {
		// eval expression of `SET` clause
		d, err := en.columnAssignments[i].Eval(en.SessionCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
		row = append(row, d)
	}

	newRow, err := en.buildRecord(row, hasValue, rowID)
	if err != nil {
		return nil, err
	}

	return newRow, nil
}

// buildRecord produces a complete table record from insert-column values,
// applying defaults for missing columns and special handling for auto-inc/
// auto-random/generated columns. It replaces the former getRow+fillRow
// two-pass approach with a single pass.
//
// TODO(optimize-encoding): For type-compatible columns, consider bypassing the Datum layer
// entirely and encoding parquet native values directly into rowcodec bytes. This would
// eliminate the setter -> Datum -> CastColumnValue -> addRecord -> AddColVal -> encodeValueDatum
// chain. See docs/superpowers/specs/2026-03-19-optimize-encoding-v4-design.md Future Work.
func (en *TableKVEncoder) buildRecord(vals []types.Datum, hasValue []bool, rowID int64) ([]types.Datum, error) {
	record := en.GetOrCreateRecord()[:len(en.Columns)]
	exprCtx := en.SessionCtx.GetExprCtx()
	errCtx := exprCtx.GetEvalCtx().ErrCtx()

	// Insert columns — apply skipCast or CastColumnValue. Columns that need
	// NOT-NULL enforcement or auto-inc / auto-random Rebase are handled via
	// ProcessColDatum (slow path); plain nullable columns stay on the fast
	// path. Precomputed in initBuildRecordMeta as insertColNeedsPost.
	for i, col := range en.insertColumns {
		offset := col.Offset
		var value types.Datum
		if en.canSkipCastColumnValue(i) {
			value = vals[i]
		} else {
			casted, err := table.CastColumnValue(exprCtx, vals[i], col.ToInfo(), false, false)
			if err != nil {
				return nil, en.LogKVConvertFailed(vals, i, col.ToInfo(), err)
			}
			value = casted
		}
		if en.insertColNeedsPost[i] {
			// needCast=false: value is already cast-or-skip. ProcessColDatum
			// runs CheckNotNull/HandleBadNull and rebases auto-inc / auto-random.
			processed, err := en.ProcessColDatum(col, rowID, &value, false)
			if err != nil {
				return nil, en.LogKVConvertFailed(vals, i, col.ToInfo(), err)
			}
			record[offset] = processed
			continue
		}
		record[offset] = value
	}

	// Default columns — not insert, not special.
	for _, i := range en.defaultColOffsets {
		col := en.Columns[i]
		if hasValue[i] {
			// Value was set by parserData2TableData (e.g., SET clause assignment).
			// Still need NOT-NULL check matching original ProcessColDatum behavior.
			value := en.rowCache[i]
			if err := col.CheckNotNull(&value, 0); err != nil {
				// Value is null for a NOT-NULL column — handle bad null.
				if err2 := col.HandleBadNull(errCtx, &value, 0); err2 != nil {
					return nil, en.LogKVConvertFailed(en.rowCache, i, col.ToInfo(), err2)
				}
			}
			record[i] = value
			continue
		}
		value, err := table.GetColDefaultValue(exprCtx, col.ToInfo())
		if err != nil {
			return nil, en.LogKVConvertFailed(en.rowCache, i, col.ToInfo(), err)
		}
		if value.IsNull() && en.notNullFlags[i] {
			if err := col.HandleBadNull(errCtx, &value, 0); err != nil {
				return nil, en.LogKVConvertFailed(en.rowCache, i, col.ToInfo(), err)
			}
		}
		record[i] = value
	}

	// Special columns — auto-inc, auto-random, generated.
	for _, i := range en.specialColOffsets {
		col := en.Columns[i]
		var theDatum *types.Datum
		if hasValue[i] {
			theDatum = &en.rowCache[i]
		}
		// needCast=false: the value (if any) was already set by parserData2TableData.
		// ProcessColDatum still does CheckNotNull and auto-value handling.
		value, err := en.ProcessColDatum(col, rowID, theDatum, false)
		if err != nil {
			return nil, en.LogKVConvertFailed(en.rowCache, i, col.ToInfo(), err)
		}
		record[i] = value
	}

	// Handle auto row ID.
	if common.TableHasAutoRowID(en.TableMeta()) {
		rowValue := rowID
		newRowID := en.AutoIDFn(rowID)
		record = append(record, types.NewIntDatum(newRowID))
		alloc := en.TableAllocators().Get(autoid.RowIDAllocType)
		if err := alloc.Rebase(context.Background(), rowValue, false); err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Evaluate generated columns.
	if len(en.GenCols) > 0 {
		if errCol, err := en.EvalGeneratedColumns(record, en.Columns); err != nil {
			return nil, en.LogEvalGenExprFailed(en.rowCache, errCol, err)
		}
	}

	return record, nil
}

func (en *TableKVEncoder) initInsertColFileMapping() {
	en.insertColToFileIdx = make([]int, len(en.insertColumns))
	insertIdx := 0
	for fileIdx, mapping := range en.fieldMappings {
		if mapping == nil || mapping.Column == nil {
			continue
		}
		if insertIdx < len(en.insertColToFileIdx) {
			intest.Assert(mapping.Column.ID == en.insertColumns[insertIdx].ID,
				"fieldMapping column ID mismatch with insertColumns")
			en.insertColToFileIdx[insertIdx] = fileIdx
		}
		insertIdx++
	}
	// SET clause columns have no file column
	for ; insertIdx < len(en.insertColToFileIdx); insertIdx++ {
		en.insertColToFileIdx[insertIdx] = -1
	}
}

func (en *TableKVEncoder) initBuildRecordMeta() {
	numCols := len(en.Columns)

	hasInsertValue := make([]bool, numCols)
	for _, col := range en.insertColumns {
		hasInsertValue[col.Offset] = true
	}

	en.notNullFlags = make([]bool, numCols)
	for i, col := range en.Columns {
		en.notNullFlags[i] = mysql.HasNotNullFlag(col.GetFlag())
	}

	en.insertColNeedsPost = make([]bool, len(en.insertColumns))
	for i, col := range en.insertColumns {
		info := col.ToInfo()
		if mysql.HasNotNullFlag(col.GetFlag()) ||
			kv.IsAutoIncCol(info) || en.IsAutoRandomCol(info) || col.IsGenerated() {
			en.insertColNeedsPost[i] = true
		}
	}

	en.defaultColOffsets = make([]int, 0)
	en.specialColOffsets = make([]int, 0)
	for i, col := range en.Columns {
		if hasInsertValue[i] {
			continue
		}
		if kv.IsAutoIncCol(col.ToInfo()) || en.IsAutoRandomCol(col.ToInfo()) || col.IsGenerated() {
			en.specialColOffsets = append(en.specialColOffsets, i)
		} else {
			en.defaultColOffsets = append(en.defaultColOffsets, i)
		}
	}
}

func (en *TableKVEncoder) canSkipCastColumnValue(insertColIdx int) bool {
	if en.currentSkipCast == nil || insertColIdx >= len(en.insertColToFileIdx) {
		return false
	}
	fileIdx := en.insertColToFileIdx[insertColIdx]
	if fileIdx < 0 || fileIdx >= len(en.currentSkipCast) {
		return false
	}
	return en.currentSkipCast[fileIdx]
}

// Close the TableKVEncoder.
func (en *TableKVEncoder) Close() error {
	en.SessionCtx.Close()
	return nil
}

// GetNumOfIndexGenKV gets the number of indices that generate index KVs.
func GetNumOfIndexGenKV(tblInfo *model.TableInfo) int {
	return len(GetIndicesGenKV(tblInfo))
}

// GenKVIndex is used to store index info that generates index KVs.
type GenKVIndex struct {
	name   string
	Unique bool
}

// GetIndicesGenKV gets all indices that generate index KVs.
func GetIndicesGenKV(tblInfo *model.TableInfo) map[int64]GenKVIndex {
	res := make(map[int64]GenKVIndex, len(tblInfo.Indices))
	for _, idxInfo := range tblInfo.Indices {
		// all public non-primary index generates index KVs
		if idxInfo.State != model.StatePublic {
			continue
		}
		if idxInfo.Primary && tblInfo.HasClusteredIndex() {
			continue
		}
		res[idxInfo.ID] = GenKVIndex{
			name:   idxInfo.Name.L,
			Unique: idxInfo.Unique,
		}
	}
	return res
}
