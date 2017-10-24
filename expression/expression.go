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
	goJSON "encoding/json"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(expr ast.ExprNode, ctx context.Context) (types.Datum, error)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler

	// Eval evaluates an expression through a row.
	Eval(row types.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(row types.Row, sc *variable.StatementContext) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(row types.Row, sc *variable.StatementContext) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(row types.Row, sc *variable.StatementContext) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(row types.Row, sc *variable.StatementContext) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(row types.Row, sc *variable.StatementContext) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(row types.Row, sc *variable.StatementContext) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(row types.Row, sc *variable.StatementContext) (val json.JSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// HashCode create the hashcode for expression
	HashCode() []byte

	// Equal checks whether two expressions are equal.
	Equal(e Expression, ctx context.Context) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema.
	ResolveIndices(schema *Schema)

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		cnf = append(cnf, expr.Clone())
	}
	return cnf
}

// EvalBool evaluates expression list to a boolean value.
func EvalBool(exprList CNFExprs, row types.Row, ctx context.Context) (bool, error) {
	for _, expr := range exprList {
		data, err := expr.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if i == 0 {
			return false, nil
		}
	}
	return true, nil
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx context.Context, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx context.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx context.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col  *Column
	Expr Expression
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	switch v := onExpr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == funcName {
			var ret []Expression
			for _, arg := range v.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicOr)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx context.Context, schema *Schema, expr Expression) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		var err error
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i], err = EvaluateExprWithNull(ctx, schema, arg)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		newFunc, err := NewFunction(ctx, x.FuncName.L, types.NewFieldType(mysql.TypeTiny), args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return newFunc, nil
	case *Column:
		if !schema.Contains(x) {
			return x, nil
		}
		constant := &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
		return constant, nil
	case *Constant:
		if x.DeferredExpr != nil {
			newConst := FoldConstant(x)
			return newConst, nil
		}
	}
	return expr.Clone(), nil
}

// TableInfo2Schema converts table info to schema with empty DBName.
func TableInfo2Schema(tbl *model.TableInfo) *Schema {
	return TableInfo2SchemaWithDBName(model.CIStr{}, tbl)
}

// TableInfo2SchemaWithDBName converts table info to schema.
func TableInfo2SchemaWithDBName(dbName model.CIStr, tbl *model.TableInfo) *Schema {
	cols := ColumnInfos2ColumnsWithDBName(dbName, tbl.Name, tbl.Columns)
	keys := make([]KeyInfo, 0, len(tbl.Indices)+1)
	for _, idx := range tbl.Indices {
		if !idx.Unique || idx.State != model.StatePublic {
			continue
		}
		ok := true
		newKey := make([]*Column, 0, len(idx.Columns))
		for _, idxCol := range idx.Columns {
			find := false
			for i, col := range tbl.Columns {
				if idxCol.Name.L == col.Name.L {
					if !mysql.HasNotNullFlag(col.Flag) {
						break
					}
					newKey = append(newKey, cols[i])
					find = true
					break
				}
			}
			if !find {
				ok = false
				break
			}
		}
		if ok {
			keys = append(keys, newKey)
		}
	}
	if tbl.PKIsHandle {
		for i, col := range tbl.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				keys = append(keys, KeyInfo{cols[i]})
				break
			}
		}
	}
	schema := NewSchema(cols...)
	schema.SetUniqueKeys(keys)
	return schema
}

// ColumnInfos2Columns converts a slice of ColumnInfo to a slice of Column with empty DBName.
func ColumnInfos2Columns(tblName model.CIStr, colInfos []*model.ColumnInfo) []*Column {
	return ColumnInfos2ColumnsWithDBName(model.CIStr{}, tblName, colInfos)
}

// ColumnInfos2ColumnsWithDBName converts a slice of ColumnInfo to a slice of Column.
func ColumnInfos2ColumnsWithDBName(dbName, tblName model.CIStr, colInfos []*model.ColumnInfo) []*Column {
	columns := make([]*Column, 0, len(colInfos))
	for i, col := range colInfos {
		newCol := &Column{
			ColName:  col.Name,
			TblName:  tblName,
			DBName:   dbName,
			RetType:  &col.FieldType,
			Position: i,
			Index:    col.Offset,
		}
		columns = append(columns, newCol)
	}
	return columns
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(offset int, retTp *types.FieldType, ctx context.Context) *ScalarFunction {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, err := fc.getFunction(ctx, nil)
	terror.Log(errors.Trace(err))
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt,
	}
}

// IsHybridType checks whether a ClassString expression is a hybrid type value which will return different types of value in different context.
//
// For ENUM/SET which is consist of a string attribute `Name` and an int attribute `Value`,
// it will cause an error if we convert ENUM/SET to int as a string value.
//
// For BinaryLiteral/MysqlBit, we will get a wrong result if we convert it to int as a string value.
// For example, when convert `0b101` to int, the result should be 5, but we will get 101 if we regard it as a string.
func IsHybridType(expr Expression) bool {
	if expr.GetType().Hybrid() {
		return true
	}

	// For a constant, the field type will be inferred as `VARCHAR` when the kind of it is BinaryLiteral
	if con, ok := expr.(*Constant); ok {
		switch con.Value.Kind() {
		case types.KindBinaryLiteral:
			return true
		}
	}
	return false
}
