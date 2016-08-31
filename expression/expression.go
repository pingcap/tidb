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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	json.Marshaler
	// Eval evaluates an expression through a row.
	Eval(row []types.Datum, ctx context.Context) (types.Datum, error)

	// Get the expression return type.
	GetType() *types.FieldType

	// DeepCopy copies an expression totally.
	DeepCopy() Expression
}

// EvalBool evaluates expression to a boolean value.
func EvalBool(expr Expression, row []types.Datum, ctx context.Context) (bool, error) {
	data, err := expr.Eval(row, ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if data.IsNull() {
		return false, nil
	}

	i, err := data.ToBool()
	if err != nil {
		return false, errors.Trace(err)
	}
	return i != 0, nil
}

// Column represents a column.
type Column struct {
	FromID  string
	ColName model.CIStr
	DBName  model.CIStr
	TblName model.CIStr
	RetType *types.FieldType
	// Position means the position of this column that appears in the select fields.
	// e.g. SELECT name as id , 1 - id as id , 1 + name as id, name as id from src having id = 1;
	// There are four ids in the same schema, so you can't identify the column through the FromID and ColName.
	Position int
	// IsAggOrSubq means if this column is referenced to a Aggregation column or a Subquery column.
	// If so, this column's name will be the plain sql text.
	IsAggOrSubq bool

	// only used during execution
	Index      int
	Correlated bool
	data       *types.Datum
}

// Equal checks if two columns are equal
func (col *Column) Equal(expr Expression) bool {
	if newCol, ok := expr.(*Column); ok {
		return col.FromID == newCol.FromID && col.ColName == newCol.ColName
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

// SetValue sets value for correlated columns.
func (col *Column) SetValue(d *types.Datum) {
	col.data = d
}

// GetType implements Expression interface.
func (col *Column) GetType() *types.FieldType {
	return col.RetType
}

// Eval implements Expression interface.
func (col *Column) Eval(row []types.Datum, _ context.Context) (types.Datum, error) {
	if col.Correlated {
		return *col.data, nil
	}
	return row[col.Index], nil
}

// DeepCopy implements Expression interface.
func (col *Column) DeepCopy() Expression {
	if col.Correlated {
		return col
	}
	newCol := *col
	return &newCol
}

// Schema stands for the row schema get from input.
type Schema []*Column

// String implements fmt.Stringer interface.
func (s Schema) String() string {
	strs := make([]string, 0, len(s))
	for _, col := range s {
		strs = append(strs, col.String())
	}
	return "[" + strings.Join(strs, ",") + "]"
}

// DeepCopy copies the total schema.
func (s Schema) DeepCopy() Schema {
	result := make(Schema, 0, len(s))
	for _, col := range s {
		newCol := *col
		result = append(result, &newCol)
	}
	return result
}

// FindColumn finds an Column from schema for a ast.ColumnName. It compares the db/table/column names.
// If there are more than one result, it will raise ambiguous error.
func (s Schema) FindColumn(astCol *ast.ColumnName) (*Column, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s {
		if (dbName.L == "" || dbName.L == col.DBName.L) &&
			(tblName.L == "" || tblName.L == col.TblName.L) &&
			(colName.L == col.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return nil, errors.Errorf("Column %s is ambiguous", col.String())
			}
		}
	}
	if idx == -1 {
		return nil, nil
	}
	return s[idx], nil
}

// InitIndices sets indices for columns in schema.
func (s Schema) InitIndices() {
	for i, c := range s {
		c.Index = i
	}
}

// RetrieveColumn retrieves column in expression from the columns in schema.
func (s Schema) RetrieveColumn(col *Column) *Column {
	index := s.GetIndex(col)
	if index != -1 {
		return s[index]
	}
	return nil
}

// GetIndex finds the index for a column.
func (s Schema) GetIndex(col *Column) int {
	for i, c := range s {
		if c.FromID == col.FromID && c.Position == col.Position {
			return i
		}
	}
	return -1
}

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	Args     []Expression
	FuncName model.CIStr
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType   *types.FieldType
	Function  evaluator.BuiltinFunc
	ArgValues []types.Datum
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	result := sf.FuncName.L + "("
	for i, arg := range sf.Args {
		result += arg.String()
		if i+1 != len(sf.Args) {
			result += ", "
		}
	}
	result += ")"
	return result
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", sf))
	return buffer.Bytes(), nil
}

// NewFunction creates a new scalar function or constant.
func NewFunction(funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	_, canConstantFolding := evaluator.DynamicFuncs[funcName]
	canConstantFolding = !canConstantFolding

	f, ok := evaluator.Funcs[funcName]
	if !ok {
		return nil, errors.Errorf("Function %s is not implemented.", funcName)
	}

	if len(args) < f.MinArgs || (f.MaxArgs != -1 && len(args) > f.MaxArgs) {
		return nil, evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].",
			f.MinArgs, f.MaxArgs)
	}

	datums := make([]types.Datum, 0, len(args))

	for i := 0; i < len(args) && canConstantFolding; i++ {
		if v, ok := args[i].(*Constant); ok {
			datums = append(datums, types.NewDatum(v.Value.GetValue()))
		} else {
			canConstantFolding = false
		}
	}

	if canConstantFolding {
		fn := f.F
		newArgs, err := fn(datums, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &Constant{
			Value:   newArgs,
			RetType: retType,
		}, nil
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	return &ScalarFunction{
		Args:      funcArgs,
		FuncName:  model.NewCIStr(funcName),
		RetType:   retType,
		Function:  f.F,
		ArgValues: make([]types.Datum, len(funcArgs))}, nil
}

//Schema2Exprs converts []*Column to []Expression.
func Schema2Exprs(schema Schema) []Expression {
	result := make([]Expression, 0, len(schema))
	for _, col := range schema {
		result = append(result, col)
	}
	return result
}

//ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// DeepCopy implements Expression interface.
func (sf *ScalarFunction) DeepCopy() Expression {
	newFunc := &ScalarFunction{
		FuncName:  sf.FuncName,
		Function:  sf.Function,
		RetType:   sf.RetType,
		ArgValues: make([]types.Datum, len(sf.Args))}
	newFunc.Args = make([]Expression, 0, len(sf.Args))
	for _, arg := range sf.Args {
		newFunc.Args = append(newFunc.Args, arg.DeepCopy())
	}
	return newFunc
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row []types.Datum, ctx context.Context) (types.Datum, error) {
	var err error
	for i, arg := range sf.Args {
		sf.ArgValues[i], err = arg.Eval(row, ctx)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
	}
	return sf.Function(sf.ArgValues, ctx)
}

// Constant stands for a constant value.
type Constant struct {
	Value   types.Datum
	RetType *types.FieldType
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf("\"%s\"", c))
	return buffer.Bytes(), nil
}

// DeepCopy implements Expression interface.
func (c *Constant) DeepCopy() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.RetType
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ []types.Datum, _ context.Context) (types.Datum, error) {
	return c.Value, nil
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(conditions []Expression) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr, _ := NewFunction(ast.AndAnd,
		types.NewFieldType(mysql.TypeTiny),
		ComposeCNFCondition(conditions[length/2:]),
		ComposeCNFCondition(conditions[:length/2]))
	return expr
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col  *Column
	Expr Expression
}
