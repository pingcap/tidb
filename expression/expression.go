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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/generatedexpr"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

// EvalAstExpr evaluates ast expression directly.
// Note: initialized in planner/core
// import expression and planner/core together to use EvalAstExpr
var EvalAstExpr func(sctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error)

// RewriteAstExpr rewrites ast expression directly.
// Note: initialized in planner/core
// import expression and planner/core together to use EvalAstExpr
var RewriteAstExpr func(sctx sessionctx.Context, expr ast.ExprNode, schema *Schema, names types.NameSlice) (Expression, error)

// VecExpr contains all vectorized evaluation methods.
type VecExpr interface {
	// Vectorized returns if this expression supports vectorized evaluation.
	Vectorized() bool

	// VecEvalInt evaluates this expression in a vectorized manner.
	VecEvalInt(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalReal evaluates this expression in a vectorized manner.
	VecEvalReal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalString evaluates this expression in a vectorized manner.
	VecEvalString(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDecimal evaluates this expression in a vectorized manner.
	VecEvalDecimal(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalTime evaluates this expression in a vectorized manner.
	VecEvalTime(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDuration evaluates this expression in a vectorized manner.
	VecEvalDuration(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalJSON evaluates this expression in a vectorized manner.
	VecEvalJSON(ctx sessionctx.Context, input *chunk.Chunk, result *chunk.Column) error
}

// ReverseExpr contains all resersed evaluation methods.
type ReverseExpr interface {
	// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
	SupportReverseEval() bool

	// ReverseEval evaluates the only one column value with given function result.
	ReverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (val types.Datum, err error)
}

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler
	VecExpr
	ReverseExpr
	CollationInfo

	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sessionctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sessionctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx sessionctx.Context, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx sessionctx.Context, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx sessionctx.Context, row chunk.Row) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx sessionctx.Context, row chunk.Row) (val json.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx sessionctx.Context, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no tables.
	// refers no correlated column.
	// refers no subqueries that refers any tables.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	// refers no param markers when prepare plan cache is enabled.
	ConstItem(sc *stmtctx.StatementContext) bool

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema. It will copy the original expression and return the copied one.
	ResolveIndices(schema *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schema *Schema) error

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
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

// Shallow makes a shallow copy of itself.
func (e CNFExprs) Shallow() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	cnf = append(cnf, e...)
	return cnf
}

func isColumnInOperand(c *Column) bool {
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return false
	}
	cols := make([]*Column, 0, 1)
	cols = ExtractColumnsFromExpressions(cols, sf.GetArgs(), isColumnInOperand)
	return len(cols) > 0
}

// ExprNotNull checks if an expression is possible to be null.
func ExprNotNull(expr Expression) bool {
	if c, ok := expr.(*Constant); ok {
		return !c.Value.IsNull()
	}
	// For ScalarFunction, the result would not be correct until we support maintaining
	// NotNull flag for it.
	return mysql.HasNotNullFlag(expr.GetType().Flag)
}

// HandleOverflowOnSelection handles Overflow errors when evaluating selection filters.
// We should ignore overflow errors when evaluating selection conditions:
//		INSERT INTO t VALUES ("999999999999999999");
//		SELECT * FROM t WHERE v;
func HandleOverflowOnSelection(sc *stmtctx.StatementContext, val int64, err error) (int64, error) {
	if sc.InSelectStmt && err != nil && types.ErrOverflow.Equal(err) {
		return -1, nil
	}
	return val, err
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx sessionctx.Context, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	hasNull := false
	for _, expr := range exprList {
		data, err := expr.Eval(row)
		if err != nil {
			return false, false, err
		}
		if data.IsNull() {
			// For queries like `select a in (select a from s where t.b = s.b) from t`,
			// if result of `t.a = s.a` is null, we cannot return immediately until
			// we have checked if `t.b = s.b` is null or false, because it means
			// subquery is empty, and we should return false as the result of the whole
			// exprList in that case, instead of null.
			if !IsEQCondFromIn(expr) {
				return false, false, nil
			}
			hasNull = true
			continue
		}

		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			i, err = HandleOverflowOnSelection(ctx.GetSessionVars().StmtCtx, i, err)
			if err != nil {
				return false, false, err

			}
		}
		if i == 0 {
			return false, false, nil
		}
	}
	if hasNull {
		return false, true, nil
	}
	return true, false, nil
}

var (
	defaultChunkSize = 1024
	selPool          = sync.Pool{
		New: func() interface{} {
			return make([]int, defaultChunkSize)
		},
	}
	zeroPool = sync.Pool{
		New: func() interface{} {
			return make([]int8, defaultChunkSize)
		},
	}
)

func allocSelSlice(n int) []int {
	if n > defaultChunkSize {
		return make([]int, n)
	}
	return selPool.Get().([]int)
}

func deallocateSelSlice(sel []int) {
	if cap(sel) <= defaultChunkSize {
		selPool.Put(sel)
	}
}

func allocZeroSlice(n int) []int8 {
	if n > defaultChunkSize {
		return make([]int8, n)
	}
	return zeroPool.Get().([]int8)
}

func deallocateZeroSlice(isZero []int8) {
	if cap(isZero) <= defaultChunkSize {
		zeroPool.Put(isZero)
	}
}

// VecEvalBool does the same thing as EvalBool but it works in a vectorized manner.
func VecEvalBool(ctx sessionctx.Context, exprList CNFExprs, input *chunk.Chunk, selected, nulls []bool) ([]bool, []bool, error) {
	// If input.Sel() != nil, we will call input.SetSel(nil) to clear the sel slice in input chunk.
	// After the function finished, then we reset the input.Sel().
	// The caller will handle the input.Sel() and selected slices.
	defer input.SetSel(input.Sel())
	input.SetSel(nil)

	n := input.NumRows()
	selected = selected[:0]
	nulls = nulls[:0]
	for i := 0; i < n; i++ {
		selected = append(selected, false)
		nulls = append(nulls, false)
	}

	sel := allocSelSlice(n)
	defer deallocateSelSlice(sel)
	sel = sel[:0]
	for i := 0; i < n; i++ {
		sel = append(sel, i)
	}
	input.SetSel(sel)

	// In isZero slice, -1 means Null, 0 means zero, 1 means not zero
	isZero := allocZeroSlice(n)
	defer deallocateZeroSlice(isZero)
	for _, expr := range exprList {
		tp := expr.GetType()
		eType := tp.EvalType()
		if CanImplicitEvalReal(expr) {
			eType = types.ETReal
		}
		buf, err := globalColumnAllocator.get(eType, n)
		if err != nil {
			return nil, nil, err
		}

		// Take the implicit evalReal path if possible.
		if CanImplicitEvalReal(expr) {
			if err := implicitEvalReal(ctx, expr, input, buf); err != nil {
				return nil, nil, err
			}
		} else if err := EvalExpr(ctx, expr, eType, input, buf); err != nil {
			return nil, nil, err
		}

		err = toBool(ctx.GetSessionVars().StmtCtx, tp, eType, buf, sel, isZero)
		if err != nil {
			return nil, nil, err
		}

		j := 0
		isEQCondFromIn := IsEQCondFromIn(expr)
		for i := range sel {
			if isZero[i] == -1 {
				if eType != types.ETInt && !isEQCondFromIn {
					continue
				}
				// In this case, we set this row to null and let it pass this filter.
				// The null flag may be set to false later by other expressions in some cases.
				nulls[sel[i]] = true
				sel[j] = sel[i]
				j++
				continue
			}

			if isZero[i] == 0 {
				continue
			}
			sel[j] = sel[i] // this row passes this filter
			j++
		}
		sel = sel[:j]
		input.SetSel(sel)
		globalColumnAllocator.put(buf)
	}

	for _, i := range sel {
		if !nulls[i] {
			selected[i] = true
		}
	}

	return selected, nulls, nil
}

func toBool(sc *stmtctx.StatementContext, tp *types.FieldType, eType types.EvalType, buf *chunk.Column, sel []int, isZero []int8) error {
	switch eType {
	case types.ETInt:
		i64s := buf.Int64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if i64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETReal:
		f64s := buf.Float64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if f64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDuration:
		d64s := buf.GoDurations()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		t64s := buf.Times()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if t64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETString:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				var fVal float64
				var err error
				sVal := buf.GetString(i)
				if tp.Hybrid() {
					switch tp.Tp {
					case mysql.TypeSet, mysql.TypeEnum:
						fVal = float64(len(sVal))
						if fVal == 0 {
							// The elements listed in the column specification are assigned index numbers, beginning
							// with 1. The index value of the empty string error value (distinguish from a "normal"
							// empty string) is 0. Thus we need to check whether it's an empty string error value when
							// `fVal==0`.
							for idx, elem := range tp.Elems {
								if elem == sVal {
									fVal = float64(idx + 1)
									break
								}
							}
						}
					case mysql.TypeBit:
						var bl types.BinaryLiteral = buf.GetBytes(i)
						iVal, err := bl.ToInt(sc)
						if err != nil {
							return err
						}
						fVal = float64(iVal)
					}
				} else {
					fVal, err = types.StrToFloat(sc, sVal, false)
					if err != nil {
						return err
					}
				}
				if fVal == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDecimal:
		d64s := buf.Decimals()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETJson:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if buf.GetJSON(i).IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	}
	return nil
}

func implicitEvalReal(ctx sessionctx.Context, expr Expression, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && ctx.GetSessionVars().EnableVectorizedExpression {
		err = expr.VecEvalReal(ctx, input, result)
	} else {
		ind, n := 0, input.NumRows()
		iter := chunk.NewIterator4Chunk(input)
		result.ResizeFloat64(n, false)
		f64s := result.Float64s()
		for it := iter.Begin(); it != iter.End(); it = iter.Next() {
			value, isNull, err := expr.EvalReal(ctx, it)
			if err != nil {
				return err
			}
			if isNull {
				result.SetNull(ind, isNull)
			} else {
				f64s[ind] = value
			}
			ind++
		}
	}
	return
}

// EvalExpr evaluates this expr according to its type.
// And it selects the method for evaluating expression based on
// the environment variables and whether the expression can be vectorized.
// Note: the input argument `evalType` is needed because of that when `expr` is
// of the hybrid type(ENUM/SET/BIT), we need the invoker decide the actual EvalType.
func EvalExpr(ctx sessionctx.Context, expr Expression, evalType types.EvalType, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && ctx.GetSessionVars().EnableVectorizedExpression {
		switch evalType {
		case types.ETInt:
			err = expr.VecEvalInt(ctx, input, result)
		case types.ETReal:
			err = expr.VecEvalReal(ctx, input, result)
		case types.ETDuration:
			err = expr.VecEvalDuration(ctx, input, result)
		case types.ETDatetime, types.ETTimestamp:
			err = expr.VecEvalTime(ctx, input, result)
		case types.ETString:
			err = expr.VecEvalString(ctx, input, result)
		case types.ETJson:
			err = expr.VecEvalJSON(ctx, input, result)
		case types.ETDecimal:
			err = expr.VecEvalDecimal(ctx, input, result)
		default:
			err = errors.New(fmt.Sprintf("invalid eval type %v", expr.GetType().EvalType()))
		}
	} else {
		ind, n := 0, input.NumRows()
		iter := chunk.NewIterator4Chunk(input)
		switch evalType {
		case types.ETInt:
			result.ResizeInt64(n, false)
			i64s := result.Int64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalInt(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					i64s[ind] = value
				}
				ind++
			}
		case types.ETReal:
			result.ResizeFloat64(n, false)
			f64s := result.Float64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalReal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					f64s[ind] = value
				}
				ind++
			}
		case types.ETDuration:
			result.ResizeGoDuration(n, false)
			d64s := result.GoDurations()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDuration(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = value.Duration
				}
				ind++
			}
		case types.ETDatetime, types.ETTimestamp:
			result.ResizeTime(n, false)
			t64s := result.Times()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalTime(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					t64s[ind] = value
				}
				ind++
			}
		case types.ETString:
			result.ReserveString(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalString(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendString(value)
				}
			}
		case types.ETJson:
			result.ReserveJSON(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalJSON(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendJSON(value)
				}
			}
		case types.ETDecimal:
			result.ResizeDecimal(n, false)
			d64s := result.Decimals()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDecimal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = *value
				}
				ind++
			}
		default:
			err = errors.New(fmt.Sprintf("invalid eval type %v", expr.GetType().EvalType()))
		}
	}
	return
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx sessionctx.Context, conditions []Expression, funcName string) Expression {
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
func ComposeCNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx sessionctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col *Column
	// ColName indicates its original column name in table schema. It's used for outputting helping message when executing meets some errors.
	ColName model.CIStr
	Expr    Expression
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
func EvaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	if ContainMutableConst(ctx, []Expression{expr}) {
		ctx.GetSessionVars().StmtCtx.OptimDependOnMutableConst = true
	}
	return evaluateExprWithNull(ctx, schema, expr)
}

func evaluateExprWithNull(ctx sessionctx.Context, schema *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = evaluateExprWithNull(ctx, schema, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType, args...)
	case *Column:
		if !schema.Contains(x) {
			return x
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(x)
		}
	}
	return expr
}

// TableInfo2SchemaAndNames converts the TableInfo to the schema and name slice.
func TableInfo2SchemaAndNames(ctx sessionctx.Context, dbName model.CIStr, tbl *model.TableInfo) (*Schema, []*types.FieldName, error) {
	cols, names, err := ColumnInfos2ColumnsAndNames(ctx, dbName, tbl.Name, tbl.Cols(), tbl)
	if err != nil {
		return nil, nil, err
	}
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
	return schema, names, nil
}

// ColumnInfos2ColumnsAndNames converts the ColumnInfo to the *Column and NameSlice.
func ColumnInfos2ColumnsAndNames(ctx sessionctx.Context, dbName, tblName model.CIStr, colInfos []*model.ColumnInfo, tblInfo *model.TableInfo) ([]*Column, types.NameSlice, error) {
	columns := make([]*Column, 0, len(colInfos))
	names := make([]*types.FieldName, 0, len(colInfos))
	for i, col := range colInfos {
		names = append(names, &types.FieldName{
			OrigTblName: tblName,
			OrigColName: col.Name,
			DBName:      dbName,
			TblName:     tblName,
			ColName:     col.Name,
		})
		newCol := &Column{
			RetType:  col.FieldType.Clone(),
			ID:       col.ID,
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			Index:    col.Offset,
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		columns = append(columns, newCol)
	}
	// Resolve virtual generated column.
	mockSchema := NewSchema(columns...)
	// Ignore redundant warning here.
	save := ctx.GetSessionVars().StmtCtx.IgnoreTruncate
	defer func() {
		ctx.GetSessionVars().StmtCtx.IgnoreTruncate = save
	}()
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	for i, col := range colInfos {
		if col.IsGenerated() && !col.GeneratedStored {
			expr, err := generatedexpr.ParseExpression(col.GeneratedExprString)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			e, err := RewriteAstExpr(ctx, expr, mockSchema, names)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if e != nil {
				columns[i].VirtualExpr = e.Clone()
			}
			columns[i].VirtualExpr, err = columns[i].VirtualExpr.ResolveIndices(mockSchema)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
	return columns, names, nil
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(ctx sessionctx.Context, offset int, retTp *types.FieldType) *ScalarFunction {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, err := fc.getFunction(ctx, nil)
	terror.Log(err)
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt,
	}
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	con, ok := expr.(*Constant)
	return ok && con.Value.Kind() == types.KindBinaryLiteral
}

func scalarExprSupportedByTiKV(sf *ScalarFunction) bool {
	switch sf.FuncName.L {
	case
		// op functions.
		ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot, ast.And, ast.Or, ast.Xor, ast.BitNeg, ast.LeftShift, ast.RightShift, ast.UnaryMinus,

		// compare functions.
		ast.LT, ast.LE, ast.EQ, ast.NE, ast.GE, ast.GT, ast.NullEQ, ast.In, ast.IsNull, ast.Like, ast.IsTruthWithoutNull, ast.IsTruthWithNull, ast.IsFalsity,

		// arithmetical functions.
		ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.Abs, /*ast.Mod,*/

		// math functions.
		ast.Ceil, ast.Ceiling, ast.Floor, ast.Sqrt, ast.Sign, ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Exp, ast.Pow,

		// Rust use the llvm math functions, which have different precision with Golang/MySQL(cmath)
		// open the following switchers if we implement them in coprocessor via `cmath`
		// ast.Sin, ast.Asin, ast.Cos, ast.Acos, ast.Tan, ast.Atan, ast.Atan2, ast.Cot,
		ast.Radians, ast.Degrees, ast.Conv, ast.CRC32,

		// control flow functions.
		ast.Case, ast.If, ast.Ifnull, ast.Coalesce,

		// string functions.
		ast.Length, ast.BitLength, ast.Concat, ast.ConcatWS /*ast.Locate,*/, ast.Replace, ast.ASCII, ast.Hex,
		ast.Reverse, ast.LTrim, ast.RTrim /*ast.Left,*/, ast.Strcmp, ast.Space, ast.Elt, ast.Field,

		// json functions.
		ast.JSONType, ast.JSONExtract, ast.JSONObject, ast.JSONArray, ast.JSONMerge, ast.JSONSet,
		ast.JSONInsert /*ast.JSONReplace,*/, ast.JSONRemove, ast.JSONLength,
		// FIXME: JSONUnquote is incompatible with Coprocessor
		// ast.JSONUnquote,

		// date functions.
		ast.DateFormat, ast.FromDays /*ast.ToDays,*/, ast.DayOfYear, ast.DayOfMonth, ast.Year, ast.Month,
		// FIXME: the coprocessor cannot keep the same behavior with TiDB in current compute framework
		// ast.Hour, ast.Minute, ast.Second, ast.MicroSecond, ast.DayName,
		ast.PeriodAdd, ast.PeriodDiff, /*ast.TimestampDiff, ast.DateAdd, ast.FromUnixTime,*/

		// encryption functions.
		ast.MD5, ast.SHA1, ast.UncompressedLength,

		ast.Cast,

		// misc functions.
		ast.InetNtoa, ast.InetAton, ast.Inet6Ntoa, ast.Inet6Aton, ast.IsIPv4, ast.IsIPv4Compat, ast.IsIPv4Mapped, ast.IsIPv6, ast.UUID:

		return true

	// A special case: Only push down Round by signature
	case ast.Round:
		switch sf.Function.PbCode() {
		case tipb.ScalarFuncSig_RoundReal, tipb.ScalarFuncSig_RoundInt, tipb.ScalarFuncSig_RoundDec:
			return true
		}
	case ast.Rand:
		switch sf.Function.PbCode() {
		case tipb.ScalarFuncSig_RandWithSeedFirstGen:
			return true
		}
	}
	return false
}

func scalarExprSupportedByFlash(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case
		ast.LogicOr, ast.LogicAnd, ast.UnaryNot, ast.BitNeg, ast.Xor, ast.And, ast.Or,
		ast.GE, ast.LE, ast.EQ, ast.NE, ast.LT, ast.GT, ast.In, ast.IsNull, ast.Like,
		ast.Plus, ast.Minus, ast.Div, ast.Mul, /*ast.Mod,*/
		ast.If, ast.Ifnull, ast.Case,
		ast.Month,
		ast.TimestampDiff, ast.DateFormat, ast.FromUnixTime,
		ast.JSONLength:
		return true
	case ast.Substr, ast.Substring:
		switch function.Function.PbCode() {
		case
			tipb.ScalarFuncSig_Substring2ArgsUTF8,
			tipb.ScalarFuncSig_Substring3ArgsUTF8:
			return true
		}
	case ast.Cast:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_CastIntAsInt, tipb.ScalarFuncSig_CastIntAsDecimal, tipb.ScalarFuncSig_CastIntAsString, tipb.ScalarFuncSig_CastIntAsTime,
			tipb.ScalarFuncSig_CastRealAsInt, tipb.ScalarFuncSig_CastRealAsDecimal, tipb.ScalarFuncSig_CastRealAsString, tipb.ScalarFuncSig_CastRealAsTime,
			tipb.ScalarFuncSig_CastStringAsInt, tipb.ScalarFuncSig_CastStringAsDecimal, tipb.ScalarFuncSig_CastStringAsString, tipb.ScalarFuncSig_CastStringAsTime,
			tipb.ScalarFuncSig_CastDecimalAsInt, tipb.ScalarFuncSig_CastDecimalAsDecimal, tipb.ScalarFuncSig_CastDecimalAsString, tipb.ScalarFuncSig_CastDecimalAsTime,
			tipb.ScalarFuncSig_CastTimeAsInt, tipb.ScalarFuncSig_CastTimeAsDecimal, tipb.ScalarFuncSig_CastTimeAsTime, tipb.ScalarFuncSig_CastIntAsReal, tipb.ScalarFuncSig_CastRealAsReal:
			return true
		}
	case ast.DateAdd:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_AddDateDatetimeInt, tipb.ScalarFuncSig_AddDateStringInt:
			return true
		}
	case ast.Round:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_RoundInt, tipb.ScalarFuncSig_RoundReal:
			return true
		}
	case ast.Extract:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_ExtractDatetime:
			return true
		}
	}
	return false
}

func scalarExprSupportedByTiDB(function *ScalarFunction) bool {
	// TiDB can support all functions, but TiPB may not include some functions.
	return scalarExprSupportedByTiKV(function) || scalarExprSupportedByFlash(function)
}

func canFuncBePushed(sf *ScalarFunction, storeType kv.StoreType) bool {
	// Use the failpoint to control whether to push down an expression in the integration test.
	// Push down all expression if the `failpoint expression` is `all`, otherwise, check
	// whether scalar function's name is contained in the enabled expression list (e.g.`ne,eq,lt`).
	// If neither of the above is true, switch to original logic.
	failpoint.Inject("PushDownTestSwitcher", func(val failpoint.Value) {
		enabled := val.(string)
		if enabled == "all" {
			failpoint.Return(true)
		}
		exprs := strings.Split(enabled, ",")
		for _, expr := range exprs {
			if strings.ToLower(strings.TrimSpace(expr)) == sf.FuncName.L {
				failpoint.Return(true)
			}
		}
	})

	ret := false

	switch storeType {
	case kv.TiFlash:
		ret = scalarExprSupportedByFlash(sf)
	case kv.TiKV:
		ret = scalarExprSupportedByTiKV(sf)
	case kv.TiDB:
		ret = scalarExprSupportedByTiDB(sf)
	case kv.UnSpecified:
		ret = scalarExprSupportedByTiDB(sf) || scalarExprSupportedByTiKV(sf) || scalarExprSupportedByFlash(sf)
	}

	if ret {
		ret = IsPushDownEnabled(sf.FuncName.L, storeType)
	}
	return ret
}

func storeTypeMask(storeType kv.StoreType) uint32 {
	if storeType == kv.UnSpecified {
		return 1<<kv.TiKV | 1<<kv.TiFlash | 1<<kv.TiDB
	}
	return 1 << storeType
}

// IsPushDownEnabled returns true if the input expr is not in the expr_pushdown_blacklist
func IsPushDownEnabled(name string, storeType kv.StoreType) bool {
	value, exists := DefaultExprPushDownBlacklist.Load().(map[string]uint32)[name]
	if exists {
		mask := storeTypeMask(storeType)
		return !(value&mask == mask)
	}

	if storeType != kv.TiFlash && name == ast.AggFuncApproxCountDistinct {
		// Can not push down approx_count_distinct to other store except tiflash by now.
		return false
	}

	return true
}

// DefaultExprPushDownBlacklist indicates the expressions which can not be pushed down to TiKV.
var DefaultExprPushDownBlacklist *atomic.Value

func init() {
	DefaultExprPushDownBlacklist = new(atomic.Value)
	DefaultExprPushDownBlacklist.Store(make(map[string]uint32))
}

func canScalarFuncPushDown(scalarFunc *ScalarFunction, pc PbConverter, storeType kv.StoreType) bool {
	pbCode := scalarFunc.Function.PbCode()
	if pbCode <= tipb.ScalarFuncSig_Unspecified {
		failpoint.Inject("PanicIfPbCodeUnspecified", func() {
			panic(errors.Errorf("unspecified PbCode: %T", scalarFunc.Function))
		})
		return false
	}

	// Check whether this function can be pushed.
	if !canFuncBePushed(scalarFunc, storeType) {
		if pc.sc.InExplainStmt {
			storageName := storeType.Name()
			if storeType == kv.UnSpecified {
				storageName = "storage layer"
			}
			pc.sc.AppendWarning(errors.New("Scalar function '" + scalarFunc.FuncName.L + "'(signature: " + scalarFunc.Function.PbCode().String() + ") can not be pushed to " + storageName))
		}
		return false
	}

	// Check whether all of its parameters can be pushed.
	for _, arg := range scalarFunc.GetArgs() {
		if !canExprPushDown(arg, pc, storeType) {
			return false
		}
	}

	if metadata := scalarFunc.Function.metadata(); metadata != nil {
		var err error
		_, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return false
		}
	}
	return true
}

func canExprPushDown(expr Expression, pc PbConverter, storeType kv.StoreType) bool {
	if storeType == kv.TiFlash {
		switch expr.GetType().Tp {
		case mysql.TypeDuration:
			if pc.sc.InExplainStmt {
				pc.sc.AppendWarning(errors.New("Expr '" + expr.String() + "' can not be pushed to TiFlash because it contains Duration type"))
			}
			return false
		case mysql.TypeEnum:
			if pc.sc.InExplainStmt {
				pc.sc.AppendWarning(errors.New("Expr '" + expr.String() + "' can not be pushed to TiFlash because it contains Enum type"))
			}
			return false
		default:
		}
	}
	switch x := expr.(type) {
	case *CorrelatedColumn:
		return pc.conOrCorColToPBExpr(expr) != nil && pc.columnToPBExpr(&x.Column) != nil
	case *Constant:
		return pc.conOrCorColToPBExpr(expr) != nil
	case *Column:
		return pc.columnToPBExpr(x) != nil
	case *ScalarFunction:
		return canScalarFuncPushDown(x, pc, storeType)
	}
	return false
}

// PushDownExprs split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprs(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client, storeType kv.StoreType) (pushed []Expression, remained []Expression) {
	pc := PbConverter{sc: sc, client: client}
	for _, expr := range exprs {
		if canExprPushDown(expr, pc, storeType) {
			pushed = append(pushed, expr)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

// CanExprsPushDown return true if all the expr in exprs can be pushed down
func CanExprsPushDown(sc *stmtctx.StatementContext, exprs []Expression, client kv.Client, storeType kv.StoreType) bool {
	_, remained := PushDownExprs(sc, exprs, client, storeType)
	return len(remained) == 0
}

// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
// The `wrapForInt` indicates whether we need to wrapIsTrue for non-logical Expression with int type.
// TODO: remove this function. ScalarFunction should be newed in one place.
func wrapWithIsTrue(ctx sessionctx.Context, keepNull bool, arg Expression, wrapForInt bool) (Expression, error) {
	if arg.GetType().EvalType() == types.ETInt {
		if !wrapForInt {
			return arg, nil
		}
		if child, ok := arg.(*ScalarFunction); ok {
			if _, isLogicalOp := logicalOps[child.FuncName.L]; isLogicalOp {
				return arg, nil
			}
		}
	}
	var fc *isTrueOrFalseFunctionClass
	if keepNull {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithNull, 1, 1}, opcode.IsTruth, keepNull}
	} else {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithoutNull, 1, 1}, opcode.IsTruth, keepNull}
	}
	f, err := fc.getFunction(ctx, []Expression{arg})
	if err != nil {
		return nil, err
	}
	sf := &ScalarFunction{
		FuncName: model.NewCIStr(ast.IsTruthWithoutNull),
		Function: f,
		RetType:  f.getRetTp(),
	}
	if keepNull {
		sf.FuncName = model.NewCIStr(ast.IsTruthWithNull)
	}
	return FoldConstant(sf), nil
}
