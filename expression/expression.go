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
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// Expression represents all scalar expression in SQL.
type Expression interface {
	// Eval evaluates an expression through a row.
	Eval(row []types.Datum, ctx context.Context) (types.Datum, error)

	// Get the expression return type.
	GetType() *types.FieldType

	// DeepCopy copies an expression totally.
	DeepCopy() Expression

	// ToString converts an expression into a string.
	ToString() string
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
	IsAgg    bool

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

// ToString implements Expression interface.
func (col *Column) ToString() string {
	result := col.ColName.L
	if col.TblName.L != "" {
		result = col.TblName.L + "." + result
	}
	if col.DBName.L != "" {
		result = col.DBName.L + "." + result
	}
	return result
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
func (col *Column) Eval(row []types.Datum, _ context.Context) (d types.Datum, err error) {
	if col.Correlated {
		return *col.data, nil
	}
	return row[col.Index], nil
}

// DeepCopy implements Expression interface.
func (col *Column) DeepCopy() Expression {
	newCol := *col
	return &newCol
}

// Schema stands for the row schema get from input.
type Schema []*Column

// ToString output the contents of schema, for debug.
func (s Schema) ToString() string {
	str := "["
	for _, col := range s {
		str += col.ToString() + ", "
	}
	str += "]"
	return str
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

// FindColumn find an Column from schema for a ast.ColumnName. It compares the db/table/column names.
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
				return nil, errors.Errorf("Column %s is ambiguous", col.ToString())
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
	RetType  *types.FieldType
	Function evaluator.BuiltinFunc
}

// ToString implements Expression interface.
func (sf *ScalarFunction) ToString() string {
	result := sf.FuncName.L + "("
	for _, arg := range sf.Args {
		result += arg.ToString()
		result += ","
	}
	result += ")"
	return result
}

// NewFunction creates a new scalar function.
func NewFunction(funcName string, retType *types.FieldType, args ...Expression) (*ScalarFunction, error) {
	f, ok := evaluator.Funcs[funcName]
	if !ok {
		log.Errorf("Function %s is not implemented.", funcName)
		return nil, nil
	}
	if len(args) < f.MinArgs || (f.MaxArgs != -1 && len(args) > f.MaxArgs) {
		return nil, evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].",
			f.MinArgs, f.MaxArgs)
	}
	funcArgs := make([]Expression, len(args))
	copy(funcArgs, args)
	return &ScalarFunction{
		Args:     funcArgs,
		FuncName: model.NewCIStr(funcName),
		RetType:  retType,
		Function: f.F}, nil
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
	newFunc := &ScalarFunction{FuncName: sf.FuncName, Function: sf.Function, RetType: sf.RetType}
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
	args := make([]types.Datum, 0, len(sf.Args))
	for _, arg := range sf.Args {
		result, err := arg.Eval(row, ctx)
		if err == nil {
			args = append(args, result)
		} else {
			return types.Datum{}, errors.Trace(err)
		}
	}
	return sf.Function(args, ctx)
}

// Constant stands for a constant value.
type Constant struct {
	Value   types.Datum
	RetType *types.FieldType
}

// ToString implements Expression interface.
func (c *Constant) ToString() string {
	return fmt.Sprintf("%v", c.Value.GetValue())
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
