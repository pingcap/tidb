// Copyright 2016 PingCAP, Inc.
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

package expression

import (
	"bytes"
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// CorrelatedColumn stands for a column in a correlated sub query.
type CorrelatedColumn struct {
	Column

	Data *types.Datum
}

// Clone implements Expression interface.
func (col *CorrelatedColumn) Clone() Expression {
	return col
}

// Eval implements Expression interface.
func (col *CorrelatedColumn) Eval(row []types.Datum) (types.Datum, error) {
	return *col.Data, nil
}

// EvalInt returns int representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalInt(row []types.Datum, sc *variable.StatementContext) (int64, bool, error) {
	val, isNull, err := evalExprToInt(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalReal returns real representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalReal(row []types.Datum, sc *variable.StatementContext) (float64, bool, error) {
	val, isNull, err := evalExprToReal(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalString returns string representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalString(row []types.Datum, sc *variable.StatementContext) (string, bool, error) {
	val, isNull, err := evalExprToString(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDecimal returns decimal representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDecimal(row []types.Datum, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	val, isNull, err := evalExprToDecimal(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalTime(row []types.Datum, sc *variable.StatementContext) (types.Time, bool, error) {
	val, isNull, err := evalExprToTime(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDuration returns Duration representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDuration(row []types.Datum, sc *variable.StatementContext) (types.Duration, bool, error) {
	val, isNull, err := evalExprToDuration(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// Equal implements Expression interface.
func (col *CorrelatedColumn) Equal(expr Expression, ctx context.Context) bool {
	if cc, ok := expr.(*CorrelatedColumn); ok {
		return col.Column.Equal(&cc.Column, ctx)
	}
	return false
}

// IsCorrelated implements Expression interface.
func (col *CorrelatedColumn) IsCorrelated() bool {
	return true
}

// Decorrelate implements Expression interface.
func (col *CorrelatedColumn) Decorrelate(schema *Schema) Expression {
	if !schema.Contains(&col.Column) {
		return col
	}
	return &col.Column
}

// ResolveIndices implements Expression interface.
func (col *CorrelatedColumn) ResolveIndices(_ *Schema) {
}

// Column represents a column.
type Column struct {
	FromID  string
	ColName model.CIStr
	DBName  model.CIStr
	TblName model.CIStr
	RetType *types.FieldType
	ID      int64
	// Position means the position of this column that appears in the select fields.
	// e.g. SELECT name as id , 1 - id as id , 1 + name as id, name as id from src having id = 1;
	// There are four ids in the same schema, so you can't identify the column through the FromID and ColName.
	Position int
	// IsAggOrSubq means if this column is referenced to a Aggregation column or a Subquery column.
	// If so, this column's name will be the plain sql text.
	IsAggOrSubq bool

	// Index is only used for execution.
	Index int

	hashcode []byte
}

// Equal implements Expression interface.
func (col *Column) Equal(expr Expression, _ context.Context) bool {
	if newCol, ok := expr.(*Column); ok {
		return newCol.FromID == col.FromID && newCol.Position == col.Position
	}
	return false
}

// String implements Stringer interface.
func (col *Column) String() string {
	result := col.ColName.L
	if col.TblName.L != "" {
		result = col.TblName.L + "." + result
	}
	if col.DBName.L != "" {
		result = col.DBName.L + "." + result
	}
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (col *Column) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", col))
	return buffer.Bytes(), nil
}

// GetType implements Expression interface.
func (col *Column) GetType() *types.FieldType {
	return col.RetType
}

// GetTypeClass implements Expression interface.
func (col *Column) GetTypeClass() types.TypeClass {
	return col.RetType.ToClass()
}

// Eval implements Expression interface.
func (col *Column) Eval(row []types.Datum) (types.Datum, error) {
	return row[col.Index], nil
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(row []types.Datum, sc *variable.StatementContext) (int64, bool, error) {
	val, isNull, err := evalExprToInt(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalReal returns real representation of Column.
func (col *Column) EvalReal(row []types.Datum, sc *variable.StatementContext) (float64, bool, error) {
	val, isNull, err := evalExprToReal(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(row []types.Datum, sc *variable.StatementContext) (string, bool, error) {
	val, isNull, err := evalExprToString(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDecimal returns decimal representation of Column.
func (col *Column) EvalDecimal(row []types.Datum, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	val, isNull, err := evalExprToDecimal(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Column.
func (col *Column) EvalTime(row []types.Datum, sc *variable.StatementContext) (types.Time, bool, error) {
	val, isNull, err := evalExprToTime(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// EvalDuration returns Duration representation of Column.
func (col *Column) EvalDuration(row []types.Datum, sc *variable.StatementContext) (types.Duration, bool, error) {
	val, isNull, err := evalExprToDuration(col, row, sc)
	return val, isNull, errors.Trace(err)
}

// Clone implements Expression interface.
func (col *Column) Clone() Expression {
	newCol := *col
	return &newCol
}

// IsCorrelated implements Expression interface.
func (col *Column) IsCorrelated() bool {
	return false
}

// Decorrelate implements Expression interface.
func (col *Column) Decorrelate(_ *Schema) Expression {
	return col
}

// HashCode implements Expression interface.
func (col *Column) HashCode() []byte {
	if len(col.hashcode) != 0 {
		return col.hashcode
	}
	col.hashcode, _ = codec.EncodeValue(col.hashcode, types.NewStringDatum(col.FromID), types.NewIntDatum(int64(col.Position)))
	return col.hashcode
}

// ResolveIndices implements Expression interface.
func (col *Column) ResolveIndices(schema *Schema) {
	col.Index = schema.ColumnIndex(col)
	// If col's index equals to -1, it means a internal logic error happens.
	if col.Index == -1 {
		log.Errorf("Can't find column %s in schema %s", col, schema)
	}
}

// Column2Exprs will transfer column slice to expression slice.
func Column2Exprs(cols []*Column) []Expression {
	result := make([]Expression, 0, len(cols))
	for _, col := range cols {
		result = append(result, col.Clone())
	}
	return result
}
