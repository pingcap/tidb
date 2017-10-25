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

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
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
func (col *CorrelatedColumn) Eval(row types.Row) (types.Datum, error) {
	return *col.Data, nil
}

// EvalInt returns int representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalInt(row types.Row, sc *variable.StatementContext) (int64, bool, error) {
	if col.Data.IsNull() {
		return 0, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := col.Data.ToInt64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return col.Data.GetInt64(), false, nil
}

// EvalReal returns real representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalReal(row types.Row, sc *variable.StatementContext) (float64, bool, error) {
	if col.Data.IsNull() {
		return 0, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := col.Data.ToFloat64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return col.Data.GetFloat64(), false, nil
}

// EvalString returns string representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalString(row types.Row, sc *variable.StatementContext) (string, bool, error) {
	if col.Data.IsNull() {
		return "", true, nil
	}
	res, err := col.Data.ToString()
	return res, err != nil, errors.Trace(err)
}

// EvalDecimal returns decimal representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDecimal(row types.Row, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	if col.Data.IsNull() {
		return nil, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := col.Data.ToDecimal(sc)
		return res, err != nil, errors.Trace(err)
	}
	return col.Data.GetMysqlDecimal(), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalTime(row types.Row, sc *variable.StatementContext) (types.Time, bool, error) {
	if col.Data.IsNull() {
		return types.Time{}, true, nil
	}
	return col.Data.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalDuration(row types.Row, sc *variable.StatementContext) (types.Duration, bool, error) {
	if col.Data.IsNull() {
		return types.Duration{}, true, nil
	}
	return col.Data.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of CorrelatedColumn.
func (col *CorrelatedColumn) EvalJSON(row types.Row, sc *variable.StatementContext) (json.JSON, bool, error) {
	if col.Data.IsNull() {
		return json.JSON{}, true, nil
	}
	return col.Data.GetMysqlJSON(), false, nil
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
	FromID      int
	ColName     model.CIStr
	DBName      model.CIStr
	OrigTblName model.CIStr
	TblName     model.CIStr
	RetType     *types.FieldType
	ID          int64
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

// Eval implements Expression interface.
func (col *Column) Eval(row types.Row) (types.Datum, error) {
	return row.(types.DatumRow)[col.Index], nil
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(row types.Row, sc *variable.StatementContext) (int64, bool, error) {
	val := &row.(types.DatumRow)[col.Index]
	if val.IsNull() {
		return 0, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := val.ToInt64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return val.GetInt64(), false, nil
}

// EvalReal returns real representation of Column.
func (col *Column) EvalReal(row types.Row, sc *variable.StatementContext) (float64, bool, error) {
	val := &row.(types.DatumRow)[col.Index]
	if val.IsNull() {
		return 0, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := val.ToFloat64(sc)
		return res, err != nil, errors.Trace(err)
	}
	return val.GetFloat64(), false, nil
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(row types.Row, sc *variable.StatementContext) (string, bool, error) {
	val := &row.(types.DatumRow)[col.Index]
	if val.IsNull() {
		return "", true, nil
	}
	res, err := val.ToString()
	return res, err != nil, errors.Trace(err)
}

// EvalDecimal returns decimal representation of Column.
func (col *Column) EvalDecimal(row types.Row, sc *variable.StatementContext) (*types.MyDecimal, bool, error) {
	val := &row.(types.DatumRow)[col.Index]
	if val.IsNull() {
		return nil, true, nil
	}
	if col.GetType().Hybrid() {
		res, err := val.ToDecimal(sc)
		return res, err != nil, errors.Trace(err)
	}
	// We can not use val.GetMyDecimal() here directly,
	// for sql like: `select sum(1.2e2) * 0.1` may cause an panic here,
	// since we infer the result type of `SUM` as `mysql.TypeNewDecimal`,
	// but what we actually get is store as float64 in Datum.
	res, err := val.ToDecimal(sc)
	return res, false, errors.Trace(err)
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Column.
func (col *Column) EvalTime(row types.Row, sc *variable.StatementContext) (types.Time, bool, error) {
	t, isNull := row.GetTime(col.Index)
	return t, isNull, nil
}

// EvalDuration returns Duration representation of Column.
func (col *Column) EvalDuration(row types.Row, sc *variable.StatementContext) (types.Duration, bool, error) {
	dur, isNull := row.GetDuration(col.Index)
	return dur, isNull, nil
}

// EvalJSON returns JSON representation of Column.
func (col *Column) EvalJSON(row types.Row, sc *variable.StatementContext) (json.JSON, bool, error) {
	j, isNull := row.GetJSON(col.Index)
	return j, isNull, nil
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
	col.hashcode = make([]byte, 0, 16)
	col.hashcode = codec.EncodeInt(col.hashcode, int64(col.FromID))
	col.hashcode = codec.EncodeInt(col.hashcode, int64(col.Position))
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

// ColInfo2Col finds the corresponding column of the ColumnInfo in a column slice.
func ColInfo2Col(cols []*Column, col *model.ColumnInfo) *Column {
	for _, c := range cols {
		if c.ColName.L == col.Name.L {
			return c
		}
	}
	return nil
}

// indexCol2Col finds the corresponding column of the IndexColumn in a column slice.
func indexCol2Col(cols []*Column, col *model.IndexColumn) *Column {
	for _, c := range cols {
		if c.ColName.L == col.Name.L {
			return c
		}
	}
	return nil
}

// IndexInfo2Cols gets the corresponding []*Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *Column,
// the return value will be only the 1st corresponding *Column and its length.
func IndexInfo2Cols(cols []*Column, index *model.IndexInfo) ([]*Column, []int) {
	retCols := make([]*Column, 0, len(index.Columns))
	lengths := make([]int, 0, len(index.Columns))
	for _, c := range index.Columns {
		col := indexCol2Col(cols, c)
		if col == nil {
			return retCols, lengths
		}
		retCols = append(retCols, col)
		lengths = append(lengths, c.Length)
	}
	return retCols, lengths
}
