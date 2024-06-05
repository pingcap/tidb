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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	goJSON "encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/zeropool"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
	parameterFlag      byte = 4
	ScalarSubQFlag     byte = 5
)

// EvalSimpleAst evaluates a simple ast expression directly.
// This function is used to evaluate some "simple" expressions with limited context.
// See `BuildSimpleExpr` for more details about the differences.
var EvalSimpleAst func(ctx BuildContext, expr ast.ExprNode) (types.Datum, error)

// BuildOptions is used to provide optional settings to build an expression
type BuildOptions struct {
	// InputSchema is the input schema for expression to build
	InputSchema *Schema
	// InputNames is the input names for expression to build
	InputNames types.NameSlice
	// SourceTableDB is the database that the source table located
	SourceTableDB model.CIStr
	// SourceTable is used to provide some extra column info.
	SourceTable *model.TableInfo
	// AllowCastArray specifies whether to allow casting to an array type.
	AllowCastArray bool
	// TargetFieldType indicates to cast the expression to the target field type if it is not nil
	TargetFieldType *types.FieldType
}

// BuildOption is a function to apply optional settings
type BuildOption func(*BuildOptions)

// WithTableInfo specifies table meta for the expression to build.
// When this option is specified, it will use the table meta to resolve column names.
func WithTableInfo(db string, tblInfo *model.TableInfo) BuildOption {
	return func(options *BuildOptions) {
		options.SourceTableDB = model.NewCIStr(db)
		options.SourceTable = tblInfo
	}
}

// WithInputSchemaAndNames specifies the input schema and names for the expression to build.
func WithInputSchemaAndNames(schema *Schema, names types.NameSlice, table *model.TableInfo) BuildOption {
	return func(options *BuildOptions) {
		options.InputSchema = schema
		options.InputNames = names
		options.SourceTable = table
	}
}

// WithAllowCastArray specifies whether to allow casting to an array type.
func WithAllowCastArray(allow bool) BuildOption {
	return func(options *BuildOptions) {
		options.AllowCastArray = allow
	}
}

// WithCastExprTo indicates that we need to the cast the generated expression to the target type
func WithCastExprTo(targetFt *types.FieldType) BuildOption {
	return func(options *BuildOptions) {
		options.TargetFieldType = targetFt
	}
}

// BuildSimpleExpr builds a simple expression from an ast node.
// This function is used to build some "simple" expressions with limited context.
// The below expressions are not supported:
//   - Subquery
//   - Param marker (e.g. `?`)
//   - Variable (e.g. `@a`)
//   - Window functions
//   - Aggregate functions
//   - Other special functions such as `GROUPING`
//
// If you want to build a more complex expression, you should use `EvalAstExprWithPlanCtx` or `RewriteAstExprWithPlanCtx`
// in `github.com/pingcap/tidb/pkg/planner/util`. They are more powerful but need planner context to build expressions.
var BuildSimpleExpr func(ctx BuildContext, expr ast.ExprNode, opts ...BuildOption) (Expression, error)

// VecExpr contains all vectorized evaluation methods.
type VecExpr interface {
	// Vectorized returns if this expression supports vectorized evaluation.
	Vectorized() bool

	// VecEvalInt evaluates this expression in a vectorized manner.
	VecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalReal evaluates this expression in a vectorized manner.
	VecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalString evaluates this expression in a vectorized manner.
	VecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDecimal evaluates this expression in a vectorized manner.
	VecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalTime evaluates this expression in a vectorized manner.
	VecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalDuration evaluates this expression in a vectorized manner.
	VecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error

	// VecEvalJSON evaluates this expression in a vectorized manner.
	VecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error
}

// TraverseAction define the interface for action when traversing down an expression.
type TraverseAction interface {
	Transform(Expression) Expression
}

// ConstLevel indicates the const level for an expression
type ConstLevel uint

const (
	// ConstNone indicates the expression is not a constant expression.
	// The evaluation result may be different for different input rows.
	// e.g. `col_a * 2`, `substring(col_b, 5, 3)`.
	ConstNone ConstLevel = iota
	// ConstOnlyInContext indicates the expression is only a constant for a same context.
	// This is mainly for Plan Cache, e.g. `prepare st from 'select * from t where a<1+?'`, where
	// the value of `?` may change between different Contexts (executions).
	ConstOnlyInContext
	// ConstStrict indicates the expression is a constant expression.
	// The evaluation result is always the same no matter the input context or rows.
	// e.g. `1 + 2`, `substring("TiDB SQL Tutorial", 5, 3) + 'abcde'`
	ConstStrict
)

// Expression represents all scalar expression in SQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler
	VecExpr
	CollationInfo

	Traverse(TraverseAction) Expression

	// Eval evaluates an expression through a row.
	Eval(ctx EvalContext, row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx EvalContext, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx EvalContext, row chunk.Row) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx EvalContext, row chunk.Row) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType(ctx EvalContext) *types.FieldType

	// Clone copies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx EvalContext, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstLevel returns the const level of the expression.
	ConstLevel() ConstLevel

	// Decorrelate try to decorrelate the expression by schema.
	Decorrelate(schema *Schema) Expression

	// ResolveIndices resolves indices by the given schema. It will copy the original expression and return the copied one.
	ResolveIndices(schema *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schema *Schema) error

	// ResolveIndicesByVirtualExpr resolves indices by the given schema in terms of virtual expression. It will copy the original expression and return the copied one.
	ResolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) (Expression, bool)

	// resolveIndicesByVirtualExpr is called inside the `ResolveIndicesByVirtualExpr` It will perform on the expression itself.
	resolveIndicesByVirtualExpr(ctx EvalContext, schema *Schema) bool

	// RemapColumn remaps columns with provided mapping and returns new expression
	RemapColumn(map[int64]*Column) (Expression, error)

	// ExplainInfo returns operator information to be explained.
	ExplainInfo(ctx EvalContext) string

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// ExplainNormalizedInfo4InList returns operator normalized information for plan digest.
	ExplainNormalizedInfo4InList() string

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// Column: ColumnFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode() []byte

	// CanonicalHashCode creates the canonical hashcode for expression.
	// Different with `HashCode`, this method will produce the same hashcode for expressions with the same semantic.
	// For example, `a + b` and `b + a` have the same return value of this method.
	CanonicalHashCode() []byte

	// MemoryUsage return the memory usage of Expression
	MemoryUsage() int64
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
func ExprNotNull(ctx EvalContext, expr Expression) bool {
	if c, ok := expr.(*Constant); ok {
		return !c.Value.IsNull()
	}
	// For ScalarFunction, the result would not be correct until we support maintaining
	// NotNull flag for it.
	return mysql.HasNotNullFlag(expr.GetType(ctx).GetFlag())
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx EvalContext, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	hasNull := false
	tc := typeCtx(ctx)
	for _, expr := range exprList {
		data, err := expr.Eval(ctx, row)
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

		i, err := data.ToBool(tc)
		if err != nil {
			return false, false, err
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
	selPool          = zeropool.New[[]int](func() []int {
		return make([]int, defaultChunkSize)
	})
	zeroPool = zeropool.New[[]int8](func() []int8 {
		return make([]int8, defaultChunkSize)
	})
)

func allocSelSlice(n int) []int {
	if n > defaultChunkSize {
		return make([]int, n)
	}
	return selPool.Get()
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
	return zeroPool.Get()
}

func deallocateZeroSlice(isZero []int8) {
	if cap(isZero) <= defaultChunkSize {
		zeroPool.Put(isZero)
	}
}

// VecEvalBool does the same thing as EvalBool but it works in a vectorized manner.
func VecEvalBool(ctx EvalContext, vecEnabled bool, exprList CNFExprs, input *chunk.Chunk, selected, nulls []bool) ([]bool, []bool, error) {
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
		tp := expr.GetType(ctx)
		eType := tp.EvalType()
		if CanImplicitEvalReal(expr) {
			eType = types.ETReal
		}
		buf, err := globalColumnAllocator.get()
		if err != nil {
			return nil, nil, err
		}

		// Take the implicit evalReal path if possible.
		if CanImplicitEvalReal(expr) {
			if err := implicitEvalReal(ctx, vecEnabled, expr, input, buf); err != nil {
				return nil, nil, err
			}
		} else if err := EvalExpr(ctx, vecEnabled, expr, eType, input, buf); err != nil {
			return nil, nil, err
		}

		err = toBool(typeCtx(ctx), tp, eType, buf, sel, isZero)
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

func toBool(tc types.Context, tp *types.FieldType, eType types.EvalType, buf *chunk.Column, sel []int, isZero []int8) error {
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
					switch tp.GetType() {
					case mysql.TypeSet, mysql.TypeEnum:
						fVal = float64(len(sVal))
						if fVal == 0 {
							// The elements listed in the column specification are assigned index numbers, beginning
							// with 1. The index value of the empty string error value (distinguish from a "normal"
							// empty string) is 0. Thus we need to check whether it's an empty string error value when
							// `fVal==0`.
							for idx, elem := range tp.GetElems() {
								if elem == sVal {
									fVal = float64(idx + 1)
									break
								}
							}
						}
					case mysql.TypeBit:
						var bl types.BinaryLiteral = buf.GetBytes(i)
						iVal, err := bl.ToInt(tc)
						if err != nil {
							return err
						}
						fVal = float64(iVal)
					}
				} else {
					fVal, err = types.StrToFloat(tc, sVal, false)
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

func implicitEvalReal(ctx EvalContext, vecEnabled bool, expr Expression, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && vecEnabled {
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
func EvalExpr(ctx EvalContext, vecEnabled bool, expr Expression, evalType types.EvalType, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && vecEnabled {
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
			err = fmt.Errorf("invalid eval type %v", expr.GetType(ctx).EvalType())
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
			err = fmt.Errorf("invalid eval type %v", expr.GetType(ctx).EvalType())
		}
	}
	return
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx BuildContext, conditions []Expression, funcName string) Expression {
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
func ComposeCNFCondition(ctx BuildContext, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx BuildContext, conditions ...Expression) Expression {
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
	// LazyErr is used in statement like `INSERT INTO t1 (a) VALUES (1) ON DUPLICATE KEY UPDATE a= (SELECT b FROM source);`, ErrSubqueryMoreThan1Row
	// should be evaluated after the duplicate situation is detected in the executing procedure.
	LazyErr error
}

// MemoryUsage return the memory usage of Assignment
func (a *Assignment) MemoryUsage() (sum int64) {
	if a == nil {
		return
	}

	sum = size.SizeOfPointer + a.ColName.MemoryUsage() + size.SizeOfInterface*2
	if a.Expr != nil {
		sum += a.Expr.MemoryUsage()
	}
	return
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
	//nolint: revive
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
func EvaluateExprWithNull(ctx BuildContext, schema *Schema, expr Expression) Expression {
	if MaybeOverOptimized4PlanCache(ctx, []Expression{expr}) {
		ctx.SetSkipPlanCache(fmt.Sprintf("%v affects null check", expr.String()))
	}
	if ctx.IsInNullRejectCheck() {
		expr, _ = evaluateExprWithNullInNullRejectCheck(ctx, schema, expr)
		return expr
	}
	return evaluateExprWithNull(ctx, schema, expr)
}

func evaluateExprWithNull(ctx BuildContext, schema *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = evaluateExprWithNull(ctx, schema, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType.Clone(), args...)
	case *Column:
		if !schema.Contains(x) {
			return x
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(ctx, x)
		}
	}
	return expr
}

// evaluateExprWithNullInNullRejectCheck sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
// The returned bool values indicates whether the value is influenced by the Null Constant transformed from schema column
// when the value is Null Constant.
func evaluateExprWithNullInNullRejectCheck(ctx BuildContext, schema *Schema, expr Expression) (Expression, bool) {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		nullFromSets := make([]bool, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i], nullFromSets[i] = evaluateExprWithNullInNullRejectCheck(ctx, schema, arg)
		}
		allArgsNullFromSet := true
		for i := range args {
			if cons, ok := args[i].(*Constant); ok && cons.Value.IsNull() && !nullFromSets[i] {
				allArgsNullFromSet = false
				break
			}
		}

		// If one of the args of `AND` and `OR` are Null Constant from the column schema, and the another argument is Constant, then we should keep it.
		// Otherwise, we shouldn't let Null Constant which affected by the column schema participate in computing in `And` and `OR`
		// due to the result of `AND` and `OR` are uncertain if one of the arguments is NULL.
		if x.FuncName.L == ast.LogicAnd || x.FuncName.L == ast.LogicOr {
			hasNonConstantArg := false
			for _, arg := range args {
				if _, ok := arg.(*Constant); !ok {
					hasNonConstantArg = true
					break
				}
			}
			if hasNonConstantArg {
				for i := range args {
					if cons, ok := args[i].(*Constant); ok && cons.Value.IsNull() && nullFromSets[i] {
						if x.FuncName.L == ast.LogicAnd {
							args[i] = NewOne()
							break
						}
						if x.FuncName.L == ast.LogicOr {
							args[i] = NewZero()
							break
						}
					}
				}
			}
		}

		c := NewFunctionInternal(ctx, x.FuncName.L, x.RetType.Clone(), args...)
		cons, ok := c.(*Constant)
		// If the return expr is Null Constant, and all the Null Constant arguments are affected by column schema,
		// then we think the result Null Constant is also affected by the column schema
		return c, ok && cons.Value.IsNull() && allArgsNullFromSet
	case *Column:
		if !schema.Contains(x) {
			return x, false
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, true
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(ctx, x), false
		}
	}
	return expr, false
}

// TableInfo2SchemaAndNames converts the TableInfo to the schema and name slice.
func TableInfo2SchemaAndNames(ctx BuildContext, dbName model.CIStr, tbl *model.TableInfo) (*Schema, []*types.FieldName, error) {
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
					if !mysql.HasNotNullFlag(col.GetFlag()) {
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
			if mysql.HasPriKeyFlag(col.GetFlag()) {
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
func ColumnInfos2ColumnsAndNames(ctx BuildContext, dbName, tblName model.CIStr, colInfos []*model.ColumnInfo, tblInfo *model.TableInfo) ([]*Column, types.NameSlice, error) {
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
			UniqueID: ctx.AllocPlanColumnID(),
			Index:    col.Offset,
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		columns = append(columns, newCol)
	}
	// Resolve virtual generated column.
	mockSchema := NewSchema(columns...)

	truncateIgnored := false
	for i, col := range colInfos {
		if col.IsVirtualGenerated() {
			if !truncateIgnored {
				// Ignore redundant warning here.
				ctx = exprctx.CtxWithHandleTruncateErrLevel(ctx, errctx.LevelIgnore)
				truncateIgnored = true
			}

			expr, err := generatedexpr.ParseExpression(col.GeneratedExprString)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			e, err := BuildSimpleExpr(ctx, expr, WithInputSchemaAndNames(mockSchema, names, tblInfo), WithAllowCastArray(true))
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
func NewValuesFunc(ctx BuildContext, offset int, retTp *types.FieldType) *ScalarFunction {
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

// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
// The `wrapForInt` indicates whether we need to wrapIsTrue for non-logical Expression with int type.
// TODO: remove this function. ScalarFunction should be newed in one place.
func wrapWithIsTrue(ctx BuildContext, keepNull bool, arg Expression, wrapForInt bool) (Expression, error) {
	if arg.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
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
	return FoldConstant(ctx, sf), nil
}

// PropagateType propagates the type information to the `expr`.
// Note: For now, we only propagate type for the function CastDecimalAsDouble.
//
// e.g.
// > create table t(a decimal(9, 8));
// > insert into t values(5.04600000)
// > select a/36000 from t;
// Type:       NEWDECIMAL
// Length:     15
// Decimals:   12
// +------------------+
// | 5.04600000/36000 |
// +------------------+
// |   0.000140166667 |
// +------------------+
//
// > select cast(a/36000 as double) as result from t;
// Type:       DOUBLE
// Length:     23
// Decimals:   31
// +----------------------+
// | result               |
// +----------------------+
// | 0.000140166666666666 |
// +----------------------+
// The expected `decimal` and `length` of the outer cast_as_double need to be
// propagated to the inner div.
func PropagateType(ctx EvalContext, evalType types.EvalType, args ...Expression) {
	switch evalType {
	case types.ETReal:
		expr := args[0]
		oldFlen, oldDecimal := expr.GetType(ctx).GetFlen(), expr.GetType(ctx).GetDecimal()
		newFlen, newDecimal := setDataTypeDouble(expr.GetType(ctx).GetDecimal())
		// For float(M,D), double(M,D) or decimal(M,D), M must be >= D.
		if newFlen < newDecimal {
			newFlen = oldFlen - oldDecimal + newDecimal
		}
		if oldFlen != newFlen || oldDecimal != newDecimal {
			if col, ok := args[0].(*Column); ok {
				newCol := col.Clone()
				newCol.(*Column).RetType = col.RetType.Clone()
				args[0] = newCol
			}
			if col, ok := args[0].(*CorrelatedColumn); ok {
				newCol := col.Clone()
				newCol.(*CorrelatedColumn).RetType = col.RetType.Clone()
				args[0] = newCol
			}
			if args[0].GetType(ctx).GetType() == mysql.TypeNewDecimal {
				if newDecimal > mysql.MaxDecimalScale {
					newDecimal = mysql.MaxDecimalScale
				}
				if oldFlen-oldDecimal > newFlen-newDecimal {
					// the input data should never be overflow under the new type
					if newDecimal > oldDecimal {
						// if the target decimal part is larger than the original decimal part, we try to extend
						// the decimal part as much as possible while keeping the integer part big enough to hold
						// the original data. For example, original type is Decimal(50, 0), new type is Decimal(48,30), then
						// incDecimal = min(30-0, mysql.MaxDecimalWidth-50) = 15
						// the new target Decimal will be Decimal(50+15, 0+15) = Decimal(65, 15)
						incDecimal := mathutil.Min(newDecimal-oldDecimal, mysql.MaxDecimalWidth-oldFlen)
						newFlen = oldFlen + incDecimal
						newDecimal = oldDecimal + incDecimal
					} else {
						newFlen, newDecimal = oldFlen, oldDecimal
					}
				}
			}
			args[0].GetType(ctx).SetFlenUnderLimit(newFlen)
			args[0].GetType(ctx).SetDecimalUnderLimit(newDecimal)
		}
	}
}

// Args2Expressions4Test converts these values to an expression list.
// This conversion is incomplete, so only use for test.
func Args2Expressions4Test(args ...any) []Expression {
	exprs := make([]Expression, len(args))
	for i, v := range args {
		d := types.NewDatum(v)
		var ft *types.FieldType
		switch d.Kind() {
		case types.KindNull:
			ft = types.NewFieldType(mysql.TypeNull)
		case types.KindInt64:
			ft = types.NewFieldType(mysql.TypeLong)
		case types.KindUint64:
			ft = types.NewFieldType(mysql.TypeLong)
			ft.AddFlag(mysql.UnsignedFlag)
		case types.KindFloat64:
			ft = types.NewFieldType(mysql.TypeDouble)
		case types.KindString:
			ft = types.NewFieldType(mysql.TypeVarString)
		case types.KindMysqlTime:
			ft = types.NewFieldType(mysql.TypeTimestamp)
		case types.KindBytes:
			ft = types.NewFieldType(mysql.TypeBlob)
		default:
			exprs[i] = nil
			continue
		}
		exprs[i] = &Constant{Value: d, RetType: ft}
	}
	return exprs
}
