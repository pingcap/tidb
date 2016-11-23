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

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
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
func (col *CorrelatedColumn) Eval(row []types.Datum, _ context.Context) (types.Datum, error) {
	return *col.Data, nil
}

// Equal implements Expression interface.
func (col *CorrelatedColumn) Equal(expr Expression) bool {
	if cc, ok := expr.(*CorrelatedColumn); ok {
		return col.Column.Equal(&cc.Column)
	}
	return false
}

// IsCorrelated implements Expression interface.
func (col *CorrelatedColumn) IsCorrelated() bool {
	return true
}

// Decorrelate implements Expression interface.
func (col *CorrelatedColumn) Decorrelate(schema Schema) Expression {
	if schema.GetIndex(&col.Column) == -1 {
		return col
	}
	return &col.Column
}

// ResolveIndices implements Expression interface.
func (col *CorrelatedColumn) ResolveIndices(_ Schema) {
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

	// Only used for execution.
	Index int
}

// Equal implements Expression interface.
func (col *Column) Equal(expr Expression) bool {
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

// Eval implements Expression interface.
func (col *Column) Eval(row []types.Datum, _ context.Context) (types.Datum, error) {
	return row[col.Index], nil
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
func (col *Column) Decorrelate(_ Schema) Expression {
	return col
}

// HashCode implements Expression interface.
func (col *Column) HashCode() []byte {
	var bytes []byte
	bytes, _ = codec.EncodeValue(bytes, types.NewStringDatum(col.FromID), types.NewIntDatum(int64(col.Position)))
	return bytes
}

// ResolveIndices implements Expression interface.
func (col *Column) ResolveIndices(schema Schema) {
	col.Index = schema.GetIndex(col)
	// If col's index equals to -1, it means a internal logic error happens.
	if col.Index == -1 {
		log.Errorf("Can't find column %s in schema %s", col, schema)
	}
}
