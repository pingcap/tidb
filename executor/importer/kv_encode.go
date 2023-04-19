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
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql" //nolint: goimports
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
)

type kvEncoder interface {
	Encode(row []types.Datum, rowID int64) (*kv.Pairs, error)
	// GetLastInsertID returns the first auto-generated ID in the current encoder.
	// if there's no auto-generated id column or the column value is not auto-generated, it will be 0.
	GetLastInsertID() uint64
	io.Closer
}

// tableKVEncoder encodes a row of data into a KV pair.
type tableKVEncoder struct {
	*kv.BaseKVEncoder
	// see import.go
	columnAssignments  []*ast.Assignment
	columnsAndUserVars []*ast.ColumnNameOrUserVar
	fieldMappings      []*FieldMapping
	insertColumns      []*table.Column
}

var _ kvEncoder = &tableKVEncoder{}

func newTableKVEncoder(
	config *encode.EncodingConfig,
	columnAssignments []*ast.Assignment,
	columnsAndUserVars []*ast.ColumnNameOrUserVar,
	fieldMappings []*FieldMapping,
	insertColumns []*table.Column,
) (*tableKVEncoder, error) {
	baseKVEncoder, err := kv.NewBaseKVEncoder(config)
	if err != nil {
		return nil, err
	}
	// we need a non-nil TxnCtx to avoid panic when evaluating set clause
	baseKVEncoder.SessionCtx.Vars.TxnCtx = new(variable.TransactionContext)

	return &tableKVEncoder{
		BaseKVEncoder:      baseKVEncoder,
		columnAssignments:  columnAssignments,
		columnsAndUserVars: columnsAndUserVars,
		fieldMappings:      fieldMappings,
		insertColumns:      insertColumns,
	}, nil
}

// Encode implements the kvEncoder interface.
func (en *tableKVEncoder) Encode(row []types.Datum, rowID int64) (*kv.Pairs, error) {
	record, err := en.parserData2TableData(row, rowID)
	if err != nil {
		return nil, err
	}

	return en.Record2KV(record, row, rowID)
}

// GetLastInsertID implements the kvEncoder interface.
func (en *tableKVEncoder) GetLastInsertID() uint64 {
	return en.LastInsertID
}

// todo merge with code in load_data.go
func (en *tableKVEncoder) parserData2TableData(parserData []types.Datum, rowID int64) ([]types.Datum, error) {
	row := make([]types.Datum, 0, len(en.insertColumns))
	sessionVars := en.SessionCtx.GetSessionVars()
	setVar := func(name string, col *types.Datum) {
		// User variable names are not case-sensitive
		// https://dev.mysql.com/doc/refman/8.0/en/user-variables.html
		name = strings.ToLower(name)
		if col == nil || col.IsNull() {
			sessionVars.UnsetUserVar(name)
		} else {
			sessionVars.SetUserVarVal(name, *col)
		}
	}

	for i := 0; i < len(en.fieldMappings); i++ {
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
	for i := 0; i < len(en.columnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := expression.EvalAstExpr(en.SessionCtx, en.columnAssignments[i].Expr)
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

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
// copied from InsertValues
func (en *tableKVEncoder) getRow(vals []types.Datum, rowID int64) ([]types.Datum, error) {
	row := make([]types.Datum, len(en.Columns))
	hasValue := make([]bool, len(en.Columns))
	for i := 0; i < len(en.insertColumns); i++ {
		casted, err := table.CastValue(en.SessionCtx, vals[i], en.insertColumns[i].ToInfo(), false, false)
		if err != nil {
			return nil, err
		}

		offset := en.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return en.fillRow(row, hasValue, rowID)
}

func (en *tableKVEncoder) fillRow(row []types.Datum, hasValue []bool, rowID int64) ([]types.Datum, error) {
	var value types.Datum
	var err error

	record := en.GetOrCreateRecord()
	for i, col := range en.Columns {
		var theDatum *types.Datum = nil
		if hasValue[i] {
			theDatum = &row[i]
		}
		value, err = en.ProcessColDatum(col, rowID, theDatum)
		if err != nil {
			return nil, en.LogKVConvertFailed(row, i, col.ToInfo(), err)
		}

		record = append(record, value)
	}

	if common.TableHasAutoRowID(en.Table.Meta()) {
		// todo: we assume there's no such column in input data, will handle it later
		rowValue := rowID
		newRowID := en.AutoIDFn(rowID)
		value = types.NewIntDatum(newRowID)
		record = append(record, value)
		alloc := en.Table.Allocators(en.SessionCtx).Get(autoid.RowIDAllocType)
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

func (en *tableKVEncoder) Close() error {
	en.SessionCtx.Close()
	return nil
}
