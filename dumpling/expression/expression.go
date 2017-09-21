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

// Error instances.
var (
	ErrIncorrectParameterCount = terror.ClassExpression.New(codeIncorrectParameterCount, "Incorrect parameter count in the call to native function '%s'")

	errInvalidOperation    = terror.ClassExpression.New(codeInvalidOperation, "invalid operation")
	errFunctionNotExists   = terror.ClassExpression.New(codeFunctionNotExists, "FUNCTION %s does not exist")
	errZlibZData           = terror.ClassTypes.New(codeZlibZData, "ZLIB: Input data corrupted")
	errIncorrectArgs       = terror.ClassExpression.New(codeIncorrectArgs, mysql.MySQLErrName[mysql.ErrWrongArguments])
	errUnknownCharacterSet = terror.ClassExpression.New(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	ErrDivideByZero        = terror.ClassExpression.New(codeDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
)

// Error codes.
const (
	codeInvalidOperation        terror.ErrCode = 1
	codeIncorrectParameterCount                = 1582
	codeFunctionNotExists                      = 1305
	codeZlibZData                              = mysql.ErrZlibZData
	codeIncorrectArgs                          = mysql.ErrWrongArguments
	codeDivisionByZero                         = mysql.ErrDivisionByZero
)

func init() {
	expressionMySQLErrCodes := map[terror.ErrCode]uint16{
		codeIncorrectParameterCount: mysql.ErrWrongParamcountToNativeFct,
		codeFunctionNotExists:       mysql.ErrSpDoesNotExist,
		codeZlibZData:               mysql.ErrZlibZData,
		codeIncorrectArgs:           mysql.ErrWrongArguments,
		codeDivisionByZero:          mysql.ErrDivisionByZero,
	}
	terror.ErrClassToMySQLCodes[terror.ClassExpression] = expressionMySQLErrCodes
}

// TurnOnNewExprEval indicates whether turn on the new expression evaluation architecture.
var TurnOnNewExprEval int32

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(expr ast.ExprNode, ctx context.Context) (types.Datum, error)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler

	// Eval evaluates an expression through a row.
	Eval(row []types.Datum) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(row []types.Datum, sc *variable.StatementContext) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(row []types.Datum, sc *variable.StatementContext) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(row []types.Datum, sc *variable.StatementContext) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(row []types.Datum, sc *variable.StatementContext) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(row []types.Datum, sc *variable.StatementContext) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(row []types.Datum, sc *variable.StatementContext) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(row []types.Datum, sc *variable.StatementContext) (val json.JSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// GetTypeClass gets the TypeClass that the expression returns.
	GetTypeClass() types.TypeClass

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
func EvalBool(exprList CNFExprs, row []types.Datum, ctx context.Context) (bool, error) {
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

// evalExprToInt evaluates `expr` to int type.
func evalExprToInt(expr Expression, row []types.Datum, sc *variable.StatementContext) (res int64, isNull bool, err error) {
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if IsHybridType(expr) {
		res, err = val.ToInt64(sc)
		return res, false, errors.Trace(err)
	} else if expr.GetTypeClass() == types.ClassInt {
		return val.GetInt64(), false, nil
	}
	panic(fmt.Sprintf("cannot get INT result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToReal evaluates `expr` to real type.
func evalExprToReal(expr Expression, row []types.Datum, sc *variable.StatementContext) (res float64, isNull bool, err error) {
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if expr.GetTypeClass() == types.ClassReal {
		// TODO: fix this to val.GetFloat64() after all built-in functions been rewritten.
		res, err = val.ToFloat64(sc)
		return res, false, errors.Trace(err)
	} else if IsHybridType(expr) {
		res, err = val.ToFloat64(sc)
		return res, false, errors.Trace(err)
	}
	panic(fmt.Sprintf("cannot get REAL result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToDecimal evaluates `expr` to decimal type.
func evalExprToDecimal(expr Expression, row []types.Datum, sc *variable.StatementContext) (res *types.MyDecimal, isNull bool, err error) {
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if expr.GetTypeClass() == types.ClassDecimal {
		res, err = val.ToDecimal(sc)
		return res, false, errors.Trace(err)
		// TODO: We maintain two sets of type systems, one for Expression, one for Datum.
		// So there exists some situations that the two types are not corresponded.
		// For example, `select 1.1+1.1`
		// we infer the result type of the sql as `mysql.TypeNewDecimal` which is consistent with MySQL,
		// but what we actually get is store as float64 in Datum.
		// So if we wrap `CastDecimalAsInt` upon the result, we'll get <nil> when call `arg.EvalDecimal()`.
		// This will be fixed after all built-in functions be rewrite correctly.
	} else if IsHybridType(expr) {
		res, err = val.ToDecimal(sc)
		return res, false, errors.Trace(err)
	}
	panic(fmt.Sprintf("cannot get DECIMAL result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToString evaluates `expr` to string type.
func evalExprToString(expr Expression, row []types.Datum, _ *variable.StatementContext) (res string, isNull bool, err error) {
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if expr.GetTypeClass() == types.ClassString || IsHybridType(expr) {
		// We cannot use val.GetString() directly.
		// For example, `Bit` is regarded as ClassString,
		// while we can not use val.GetString() to get the value of a Bit variable,
		// because value of `Bit` is stored in Datum.i while val.GetString() get value from Datum.b.
		res, err = val.ToString()
		return res, false, errors.Trace(err)
	}
	panic(fmt.Sprintf("cannot get STRING result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToTime evaluates `expr` to TIME type.
func evalExprToTime(expr Expression, row []types.Datum, _ *variable.StatementContext) (res types.Time, isNull bool, err error) {
	if IsHybridType(expr) {
		return res, true, nil
	}
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if types.IsTypeTime(expr.GetType().Tp) {
		return val.GetMysqlTime(), false, nil
	}
	panic(fmt.Sprintf("cannot get DATE result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToDuration evaluates `expr` to DURATION type.
func evalExprToDuration(expr Expression, row []types.Datum, _ *variable.StatementContext) (res types.Duration, isNull bool, err error) {
	if IsHybridType(expr) {
		return res, true, nil
	}
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if expr.GetType().Tp == mysql.TypeDuration {
		return val.GetMysqlDuration(), false, nil
	}
	panic(fmt.Sprintf("cannot get DURATION result from %s expression", types.TypeStr(expr.GetType().Tp)))
}

// evalExprToJSON evaluates `expr` to JSON type.
func evalExprToJSON(expr Expression, row []types.Datum, _ *variable.StatementContext) (res json.JSON, isNull bool, err error) {
	if IsHybridType(expr) {
		return res, true, nil
	}
	val, err := expr.Eval(row)
	if val.IsNull() || err != nil {
		return res, val.IsNull(), errors.Trace(err)
	}
	if expr.GetType().Tp == mysql.TypeJSON {
		return val.GetMysqlJSON(), false, nil
	}
	panic(fmt.Sprintf("cannot get JSON result from %s expression", types.TypeStr(expr.GetType().Tp)))
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
	expr, _ := NewFunction(ctx, funcName,
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
	default:
		return x.Clone(), nil
	}
}

// TableInfo2Schema converts table info to schema.
func TableInfo2Schema(tbl *model.TableInfo) *Schema {
	cols := ColumnInfos2Columns(tbl.Name, tbl.Columns)
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

// ColumnInfos2Columns converts a slice of ColumnInfo to a slice of Column.
func ColumnInfos2Columns(tblName model.CIStr, colInfos []*model.ColumnInfo) []*Column {
	columns := make([]*Column, 0, len(colInfos))
	for i, col := range colInfos {
		newCol := &Column{
			ColName:  col.Name,
			TblName:  tblName,
			RetType:  &col.FieldType,
			Position: i,
		}
		columns = append(columns, newCol)
	}
	return columns
}

// NewCastFunc creates a new cast function.
func NewCastFunc(tp *types.FieldType, arg Expression, ctx context.Context) Expression {
	return FoldConstant(BuildCastFunction(arg, tp, ctx))
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(offset int, retTp *types.FieldType, ctx context.Context) *ScalarFunction {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, _ := fc.getFunction(ctx, nil)
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt.setSelf(bt),
	}
}
