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
	"slices"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/zeropool"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
	parameterFlag      byte = 4
	ScalarSubQFlag     byte = 5
	correlatedColumn   byte = 6
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
	SourceTableDB ast.CIStr
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
		options.SourceTableDB = ast.NewCIStr(db)
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
//   - System Variables (e.g. `@tidb_enable_async_commit`)
//   - Window functions
//   - Aggregate functions
//   - Other special functions used in some specified queries such as `GROUPING`, `VALUES` ...
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

	// VecEvalBool evaluates this expression in a vectorized manner.
	VecEvalVectorFloat32(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error
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

// SafeToShareAcrossSession indicates whether the expression can be shared across different sessions.
// In Instance Plan Cache, we'll share the same Plan/Expression across different sessions, and this interface
// is used to check whether the expression is safe to share without cloning.
type SafeToShareAcrossSession interface {
	SafeToShareAcrossSession() bool
}

// Expression represents all scalar expression in SQL.
type Expression interface {
	VecExpr
	CollationInfo
	base.HashEquals
	SafeToShareAcrossSession

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

	// EvalVectorFloat32 returns the VectorFloat32 representation of expression.
	EvalVectorFloat32(ctx EvalContext, row chunk.Row) (val types.VectorFloat32, isNull bool, err error)

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

	StringerWithCtx
}

var expressionSlices = zeropool.New[[]Expression](
	func() []Expression {
		return make([]Expression, 0, 4)
	})

// GetExpressionSlices gets a slice of Expression from pool.
func GetExpressionSlices(size int) []Expression {
	result := expressionSlices.Get()
	return slices.Grow(result, size)
}

// PutExpressionSlices puts a slice of Expression back to pool.
func PutExpressionSlices(exprs []Expression) {
	exprs = exprs[:0]
	expressionSlices.Put(exprs)
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
	cols := ExtractColumnsMapFromExpressions(isColumnInOperand, sf.GetArgs()...)
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
	for range n {
		selected = append(selected, false)
		nulls = append(nulls, false)
	}

	sel := allocSelSlice(n)
	defer deallocateSelSlice(sel)
	sel = sel[:0]
	for i := range n {
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
				if eType != types.ETInt || !isEQCondFromIn {
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
				nulls[sel[i]] = false
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

