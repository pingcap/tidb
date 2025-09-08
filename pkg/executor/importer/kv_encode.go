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
	"github.com/pingcap/tidb/pkg/parser/mysql" //nolint: goimports
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
}

// NewTableKVEncoder creates a new TableKVEncoder.
// exported for test.
func NewTableKVEncoder(
	config *encode.EncodingConfig,
	ti *TableImporter,
) (*TableKVEncoder, error) {
	return newTableKVEncoderInner(config, ti, ti.FieldMappings, ti.InsertColumns)
}

// NewTableKVEncoderForDupResolve creates a new TableKVEncoder for duplicate resolution.
func NewTableKVEncoderForDupResolve(
	config *encode.EncodingConfig,
	ti *TableImporter,
) (*TableKVEncoder, error) {
	mappings, _ := ti.tableVisCols2FieldMappings()
	return newTableKVEncoderInner(config, ti, mappings, ti.Table.VisibleCols())
}

func newTableKVEncoderInner(
	config *encode.EncodingConfig,
	ti *TableImporter,
	fieldMappings []*FieldMapping,
	insertColumns []*table.Column,
) (*TableKVEncoder, error) {
	baseKVEncoder, err := kv.NewBaseKVEncoder(config)
	if err != nil {
		return nil, err
	}
	colAssignExprs, _, err := ti.CreateColAssignSimpleExprs(baseKVEncoder.SessionCtx.GetExprCtx())
	if err != nil {
		return nil, err
	}

	return &TableKVEncoder{
		BaseKVEncoder:     baseKVEncoder,
		columnAssignments: colAssignExprs,
		fieldMappings:     fieldMappings,
		insertColumns:     insertColumns,
	}, nil
}

// Encode table row into KVs.
func (en *TableKVEncoder) Encode(row []types.Datum, rowID int64) (*kv.Pairs, error) {
	// we ignore warnings when encoding rows now, but warnings uses the same memory as parser, since the input
	// row []types.Datum share the same underlying buf, and when doing CastValue, we're using hack.String/hack.Slice.
	// when generating error such as mysql.ErrDataOutOfRange, the data will be part of the error, causing the buf
	// unable to release. So we truncate the warnings here.
	defer en.TruncateWarns()
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

	for i := range en.fieldMappings {
		if i >= len(parserData) {
			if en.fieldMappings[i].Column == nil {
				setVar(en.fieldMappings[i].UserVar.Name, nil)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(en.fieldMappings[i].Column.GetType()) && mysql.HasNotNullFlag(en.fieldMappings[i].Column.GetFlag()) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(en.fieldMappings[i].Column.GetType())))
				continue
			}

			row = append(row, types.NewDatum(nil))
			continue
		}

		if en.fieldMappings[i].Column == nil {
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

	// a new row buffer will be allocated in getRow
	newRow, err := en.getRow(row, rowID)
	if err != nil {
		return nil, err
	}

	return newRow, nil
}

func (en *TableKVEncoder) resetRowCache() {
	rowLen := len(en.Columns)
	if cap(en.rowCache) < rowLen || cap(en.hasValueCache) < rowLen {
		en.rowCache = make([]types.Datum, rowLen)
		en.hasValueCache = make([]bool, rowLen)
	} else {
		en.rowCache = en.rowCache[:rowLen]
		en.hasValueCache = en.hasValueCache[:rowLen]
		for i := range rowLen {
			en.rowCache[i].SetNull()
			en.hasValueCache[i] = false
		}
	}
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
// copied from InsertValues
func (en *TableKVEncoder) getRow(vals []types.Datum, rowID int64) ([]types.Datum, error) {
	en.resetRowCache()
	row := en.rowCache
	hasValue := en.hasValueCache
	for i, col := range en.insertColumns {
		offset := col.Offset
		casted, err := table.CastColumnValue(en.SessionCtx.GetExprCtx(), vals[i], col.ToInfo(), false, false)
		if err != nil {
			return nil, en.LogKVConvertFailed(row, offset, col.ToInfo(), err)
		}

		row[offset] = casted
		hasValue[offset] = true
	}
	// fill value for missing columns and handle bad null value
	for i, col := range en.Columns {
		isBadNullValue := false
		if hasValue[i] && col.CheckNotNull(&row[i], 0) != nil {
			isBadNullValue = true
		}

		if !hasValue[i] || isBadNullValue {
			value, err := en.HandleSpecialValue(col, rowID, isBadNullValue)
			if err != nil {
				return nil, en.LogKVConvertFailed(row, i, col.ToInfo(), err)
			}
			row[i] = value
		}

		if err := en.RebaseAutoID(col, rowID, &row[i]); err != nil {
			return nil, en.LogKVConvertFailed(row, i, col.ToInfo(), err)
		}
	}

	if common.TableHasAutoRowID(en.TableMeta()) {
		rowValue := rowID
		newRowID := en.AutoIDFn(rowID)
		row = append(row, types.NewIntDatum(newRowID))
		alloc := en.TableAllocators().Get(autoid.RowIDAllocType)
		if err := alloc.Rebase(context.Background(), rowValue, false); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(en.GenCols) > 0 {
		if errCol, err := en.EvalGeneratedColumns(row, en.Columns); err != nil {
			return nil, en.LogEvalGenExprFailed(row, errCol, err)
		}
	}

	return row, nil
}

// Close the TableKVEncoder.
func (en *TableKVEncoder) Close() error {
	en.SessionCtx.Close()
	return nil
}
