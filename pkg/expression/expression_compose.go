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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/size"
)

// Assignment represents a set assignment in Update, such as
// Update t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	Col *Column
	// ColName indicates its original column name in table schema. It's used for outputting helping message when executing meets some errors.
	ColName ast.CIStr
	Expr    Expression
	// LazyErr is used in statement like `INSERT INTO t1 (a) VALUES (1) ON DUPLICATE KEY UPDATE a= (SELECT b FROM source);`, ErrSubqueryMoreThan1Row
	// should be evaluated after the duplicate situation is detected in the executing procedure.
	LazyErr error
}

// Clone clones the Assignment.
func (a *Assignment) Clone() *Assignment {
	return &Assignment{
		Col:     a.Col.Clone().(*Column),
		ColName: a.ColName,
		Expr:    a.Expr.Clone(),
		LazyErr: a.LazyErr,
	}
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
	IsInstance  bool
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
// Set the skip cache to false when the caller will not change the logical plan tree.
// it is currently closed only by pkg/planner/core.ExtractNotNullFromConds when to extractFD.
func EvaluateExprWithNull(ctx BuildContext, schema *Schema, expr Expression, skipPlanCacheCheck bool) (Expression, error) {
	if skipPlanCacheCheck && MaybeOverOptimized4PlanCache(ctx, expr) {
		ctx.SetSkipPlanCache(fmt.Sprintf("%v affects null check", expr.StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
	}
	if ctx.IsInNullRejectCheck() {
		res, _, err := evaluateExprWithNullInNullRejectCheck(ctx, schema, expr)
		return res, err
	}
	return evaluateExprWithNull(ctx, schema, expr, skipPlanCacheCheck)
}

func evaluateExprWithNull(ctx BuildContext, schema *Schema, expr Expression, skipPlanCache bool) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			res, err := EvaluateExprWithNull(ctx, schema, arg, skipPlanCache)
			if err != nil {
				return nil, err
			}
			args[i] = res
		}
		return NewFunction(ctx, x.FuncName.L, x.RetType.Clone(), args...)
	case *Column:
		if !schema.Contains(x) {
			return x, nil
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, nil
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(ctx, x), nil
		}
	}
	return expr, nil
}

// evaluateExprWithNullInNullRejectCheck sets columns in schema as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
// The returned bool values indicates whether the value is influenced by the Null Constant transformed from schema column
// when the value is Null Constant.
func evaluateExprWithNullInNullRejectCheck(ctx BuildContext, schema *Schema, expr Expression) (Expression, bool, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		nullFromSets := make([]bool, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			res, nullFromSet, err := evaluateExprWithNullInNullRejectCheck(ctx, schema, arg)
			if err != nil {
				return nil, false, err
			}
			args[i], nullFromSets[i] = res, nullFromSet
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

		c, err := NewFunction(ctx, x.FuncName.L, x.RetType.Clone(), args...)
		if err != nil {
			return nil, false, err
		}
		cons, ok := c.(*Constant)
		// If the return expr is Null Constant, and all the Null Constant arguments are affected by column schema,
		// then we think the result Null Constant is also affected by the column schema
		return c, ok && cons.Value.IsNull() && allArgsNullFromSet, nil
	case *Column:
		if !schema.Contains(x) {
			return x, false, nil
		}
		return &Constant{Value: types.Datum{}, RetType: types.NewFieldType(mysql.TypeNull)}, true, nil
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(ctx, x), false, nil
		}
	}
	return expr, false, nil
}

// TableInfo2SchemaAndNames converts the TableInfo to the schema and name slice.
func TableInfo2SchemaAndNames(ctx BuildContext, dbName ast.CIStr, tbl *model.TableInfo) (*Schema, []*types.FieldName, error) {
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
	schema.SetKeys(keys)
	return schema, names, nil
}

// ColumnInfos2ColumnsAndNames converts the ColumnInfo to the *Column and NameSlice.
func ColumnInfos2ColumnsAndNames(ctx BuildContext, dbName, tblName ast.CIStr, colInfos []*model.ColumnInfo, tblInfo *model.TableInfo) ([]*Column, types.NameSlice, error) {
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
		FuncName: ast.NewCIStr(ast.Values),
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
		FuncName: ast.NewCIStr(ast.IsTruthWithoutNull),
		Function: f,
		RetType:  f.getRetTp(),
	}
	if keepNull {
		sf.FuncName = ast.NewCIStr(ast.IsTruthWithNull)
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
						incDecimal := min(newDecimal-oldDecimal, mysql.MaxDecimalWidth-oldFlen)
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

// StringifyExpressionsWithCtx turns a slice of expressions into string
func StringifyExpressionsWithCtx(ctx EvalContext, exprs []Expression) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, expr := range exprs {
		sb.WriteString(expr.StringWithCtx(ctx, errors.RedactLogDisable))

		if i != len(exprs)-1 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
