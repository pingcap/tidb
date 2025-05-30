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

// Encode implements the KVEncoder interface.
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

	if cap(en.hasValueCache) < len(en.Columns) {
		en.hasValueCache = make([]bool, len(en.Columns))
	} else {
		en.hasValueCache = en.hasValueCache[:len(en.Columns)]
		for i := range en.hasValueCache {
			en.hasValueCache[i] = false
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
	for i := 0; i < len(en.columnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := en.columnAssignments[i].Eval(en.SessionCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := en.getRow(row, hasValue, rowID)
	if err != nil {
		return nil, err
	}

	return newRow, nil
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
// copied from InsertValues
func (en *TableKVEncoder) getRow(vals []types.Datum, hasValue []bool, rowID int64) ([]types.Datum, error) {
	rowLen := len(en.Columns)
	if cap(en.rowCache) < rowLen {
		en.rowCache = make([]types.Datum, rowLen)
	} else {
		en.rowCache = en.rowCache[:rowLen]
		for i := range en.rowCache {
			en.rowCache[i] = types.Datum{}
		}
	}
	row := en.rowCache
	for i := range en.insertColumns {
		casted, err := table.CastColumnValue(en.SessionCtx.GetExprCtx(), vals[i], en.insertColumns[i].ToInfo(), false, false)
		if err != nil {
			return nil, err
		}

		offset := en.insertColumns[i].Offset
		row[offset] = casted
	}

	return en.fillRow(row, hasValue, rowID)
}

func (en *TableKVEncoder) fillRow(row []types.Datum, hasValue []bool, rowID int64) ([]types.Datum, error) {
	var value types.Datum
	var err error

	record := en.GetOrCreateRecord()
	for i, col := range en.Columns {
		var theDatum *types.Datum
		doCast := true
		if hasValue[i] {
			theDatum = &row[i]
			doCast = false
		}
		value, err = en.ProcessColDatum(col, rowID, theDatum, doCast)
		if err != nil {
			return nil, en.LogKVConvertFailed(row, i, col.ToInfo(), err)
		}

		record = append(record, value)
	}

	if common.TableHasAutoRowID(en.TableMeta()) {
		rowValue := rowID
		newRowID := en.AutoIDFn(rowID)
		value = types.NewIntDatum(newRowID)
		record = append(record, value)
		alloc := en.TableAllocators().Get(autoid.RowIDAllocType)
		if err := alloc.Rebase(context.Background(), rowValue, false); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if len(en.GenCols) > 0 {
		if errCol, err := en.EvalGeneratedColumns(record, en.Columns); err != nil {
			return nil, en.LogEvalGenExprFailed(row, errCol, err)
		}
	}

	return record, nil
}

// Close the TableKVEncoder.
func (en *TableKVEncoder) Close() error {
	en.SessionCtx.Close()
	return nil
}
