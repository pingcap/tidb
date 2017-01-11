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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	errInvalidOperation        = terror.ClassExpression.New(codeInvalidOperation, "invalid operation")
	errIncorrectParameterCount = terror.ClassExpression.New(codeIncorrectParameterCount, "Incorrect parameter count in the call to native function '%s'")
	errFunctionNotExists       = terror.ClassExpression.New(codeFunctionNotExists, "FUNCTION %s does not exist")
)

// Error codes.
const (
	codeInvalidOperation        terror.ErrCode = 1
	codeIncorrectParameterCount                = 1582
	codeFunctionNotExists                      = 1305
)

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(expr ast.ExprNode, ctx context.Context) (types.Datum, error)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	json.Marshaler
	// Eval evaluates an expression through a row.
	Eval(row []types.Datum, ctx context.Context) (types.Datum, error)

	// Get the expression return type.
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
	Decorrelate(schema Schema) Expression

	// ResolveIndices resolves indices by the given schema.
	ResolveIndices(schema Schema)
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

	i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return false, errors.Trace(err)
	}
	return i != 0, nil
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

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
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

// Equal implements Expression interface.
func (c *Constant) Equal(b Expression, ctx context.Context) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode() []byte {
	var bytes []byte
	bytes, _ = codec.EncodeValue(bytes, c.Value)
	return bytes
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ Schema) {
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr, _ := NewFunction(funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(conditions[:length/2], funcName),
		composeConditionWithBinaryOp(conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(conditions []Expression) Expression {
	return composeConditionWithBinaryOp(conditions, ast.AndAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(conditions []Expression) Expression {
	return composeConditionWithBinaryOp(conditions, ast.OrOr)
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
	return splitNormalFormItems(onExpr, ast.AndAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.OrOr)
}

// EvaluateExprWithNull sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx context.Context, schema Schema, expr Expression) (Expression, error) {
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
		newFunc, err := NewFunction(x.FuncName.L, types.NewFieldType(mysql.TypeTiny), args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return FoldConstant(ctx, newFunc), nil
	case *Column:
		if schema.GetColumnIndex(x) == -1 {
			return x, nil
		}
		constant := &Constant{Value: types.Datum{}}
		return constant, nil
	default:
		return x.Clone(), nil
	}
}

// TableInfo2Schema converts table info to schema.
func TableInfo2Schema(tbl *model.TableInfo) Schema {
	schema := NewSchema(make([]*Column, 0, len(tbl.Columns)))
	keys := make([]KeyInfo, 0, len(tbl.Indices)+1)
	for i, col := range tbl.Columns {
		newCol := &Column{
			ColName:  col.Name,
			TblName:  tbl.Name,
			RetType:  &col.FieldType,
			Position: i,
		}
		schema.Append(newCol)
	}
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
					newKey = append(newKey, schema.Columns[i])
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
				keys = append(keys, []*Column{schema.Columns[i]})
				break
			}
		}
	}
	schema.SetUniqueKeys(keys)
	return schema
}

// NewCastFunc creates a new cast function.
func NewCastFunc(tp *types.FieldType, arg Expression) (*ScalarFunction, error) {
	bt, err := CastFuncFactory(tp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ScalarFunction{
		args:      []Expression{arg},
		FuncName:  model.NewCIStr(ast.Cast),
		RetType:   tp,
		Function:  bt,
		ArgValues: make([]types.Datum, 1)}, nil
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(v *ast.ValuesExpr) *ScalarFunction {
	bt := BuiltinValuesFactory(v.Column.Refer.Column.Offset)
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  &v.Type,
		Function: bt,
	}
}

func init() {
	expressionMySQLErrCodes := map[terror.ErrCode]uint16{
		codeIncorrectParameterCount: mysql.ErrWrongParamcountToNativeFct,
		codeFunctionNotExists:       mysql.ErrSpDoesNotExist,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExpression] = expressionMySQLErrCodes
}
