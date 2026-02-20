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
	"sync"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/disjointset"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

// VaildConstantPropagationExpressionFuncType is to filter the unsuitable expression when to propagate the constant.
// Currently, only the `LogicalJoin.isVaildConstantPropagationExpression` has implemented this capability.
// For more information, you can refer to the comments on this.
type VaildConstantPropagationExpressionFuncType func(Expression) bool

type basePropConstSolver struct {
	colMapper map[int64]int             // colMapper maps column to its index
	eqMapper  map[int]*Constant         // if eqMapper[i] != nil, it means col_i = eqMapper[i]
	unionSet  *disjointset.SimpleIntSet // unionSet stores the relations like col_i = col_j
	columns   []*Column                 // columns stores all columns appearing in the conditions
	ctx       exprctx.ExprContext
}

func newBasePropConstSolver() basePropConstSolver {
	return basePropConstSolver{
		colMapper: make(map[int64]int, 4),
		eqMapper:  make(map[int]*Constant, 4),
		columns:   make([]*Column, 0, 4),
		unionSet:  disjointset.NewIntSet(4),
	}
}

func (s *basePropConstSolver) Clear() {
	clear(s.colMapper)
	clear(s.eqMapper)
	s.columns = s.columns[:0]
	s.unionSet.Clear()
	s.ctx = nil
}

func (s *basePropConstSolver) getColID(col *Column) int {
	return s.colMapper[col.UniqueID]
}

func (s *basePropConstSolver) insertCols(cols map[int64]*Column) {
	for uniqueID, col := range cols {
		_, ok := s.colMapper[uniqueID]
		if !ok {
			s.colMapper[uniqueID] = len(s.colMapper)
			s.columns = append(s.columns, col)
		}
	}
}

// tryToUpdateEQList tries to update the eqMapper. When the eqMapper has store this column with a different constant, like
// a = 1 and a = 2, we set the second return value to false.
func (s *basePropConstSolver) tryToUpdateEQList(col *Column, con *Constant) (bool, bool) {
	if con.Value.IsNull() && ConstExprConsiderPlanCache(con, s.ctx.IsUseCache()) {
		return false, true
	}
	id := s.getColID(col)
	oldCon, ok := s.eqMapper[id]
	if ok {
		evalCtx := s.ctx.GetEvalCtx()
		res, err := oldCon.Value.Compare(evalCtx.TypeCtx(), &con.Value, collate.GetCollator(col.GetType(s.ctx.GetEvalCtx()).GetCollate()))
		return false, res != 0 || err != nil
	}
	s.eqMapper[id] = con
	return true, false
}

// ValidCompareConstantPredicate checks if the predicate is an expression like [column '>'|'>='|'<'|'<='|'=' constant].
// return param1: return true, if the predicate is a compare constant predicate.
// return param2: return the column side of predicate.
func ValidCompareConstantPredicate(ctx EvalContext, candidatePredicate Expression) bool {
	scalarFunction, ok := candidatePredicate.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalarFunction.FuncName.L != ast.GT && scalarFunction.FuncName.L != ast.GE &&
		scalarFunction.FuncName.L != ast.LT && scalarFunction.FuncName.L != ast.LE &&
		scalarFunction.FuncName.L != ast.EQ {
		return false
	}
	column, _ := ValidCompareConstantPredicateHelper(ctx, scalarFunction, true)
	if column == nil {
		column, _ = ValidCompareConstantPredicateHelper(ctx, scalarFunction, false)
	}
	if column == nil {
		return false
	}
	return true
}

// ValidCompareConstantPredicateHelper checks if the predicate is a compare constant predicate, like "Column xxx Constant"
func ValidCompareConstantPredicateHelper(ctx EvalContext, eq *ScalarFunction, colIsLeft bool) (*Column, *Constant) {
	var col *Column
	var con *Constant
	colOk := false
	conOk := false
	if colIsLeft {
		col, colOk = eq.GetArgs()[0].(*Column)
	} else {
		col, colOk = eq.GetArgs()[1].(*Column)
	}
	if !colOk {
		return nil, nil
	}
	if colIsLeft {
		con, conOk = eq.GetArgs()[1].(*Constant)
	} else {
		con, conOk = eq.GetArgs()[0].(*Constant)
	}
	if !conOk {
		return nil, nil
	}
	if col.GetStaticType().GetCollate() != con.GetType(ctx).GetCollate() {
		return nil, nil
	}
	return col, con
}

// validEqualCond checks if the cond is an expression like [column eq constant].
func validEqualCond(ctx EvalContext, cond Expression) (*Column, *Constant) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if eq.FuncName.L != ast.EQ {
			return nil, nil
		}
		col, con := ValidCompareConstantPredicateHelper(ctx, eq, true)
		if col == nil {
			return ValidCompareConstantPredicateHelper(ctx, eq, false)
		}
		return col, con
	}
	return nil, nil
}

// replaceEqCondtionWithTrue replaces eq condition in 'cond' by true if both 'src' and 'tgt' appear in 'eq cond'.
func replaceEqCondtionWithTrue(ctx BuildContext, src *Column, tgt *Column, cond Expression) (Expression, bool) {
	if src.RetType.GetType() != tgt.RetType.GetType() {
		return cond, false
	}
	sf, ok := cond.(*ScalarFunction)
	if !ok {
		return cond, false
	}
	replaced := false
	args := sf.GetArgs()
	evalCtx := ctx.GetEvalCtx()
	switch sf.FuncName.L {
	case ast.In:
		if src.GetType(ctx.GetEvalCtx()).EvalType() == types.ETString || tgt.GetType(ctx.GetEvalCtx()).EvalType() == types.ETString {
			// It is duo to ```CheckAndDeriveCollationFromExprs``` in the ```deriveCollation```.
			// If we have an expression a in (b,c,d) with each column which has difference collation, the expression's
			// return type is decided by ```CheckAndDeriveCollationFromExprs```. it will get diffence return type.
			// So when encountering a string type, we can just return it directly.
			return cond, false
		}
		// for 'a in (b, c, d)', if a = b or a = c or a = d, we can replace it with true
		constTrue := false
		switch {
		case args[0].Equal(ctx.GetEvalCtx(), src):
			constTrue = slices.ContainsFunc(args[1:], func(arg Expression) bool {
				return arg.Equal(ctx.GetEvalCtx(), tgt)
			})
		case args[0].Equal(ctx.GetEvalCtx(), tgt):
			constTrue = slices.ContainsFunc(args[1:], func(arg Expression) bool {
				return arg.Equal(ctx.GetEvalCtx(), src)
			})
		}
		if constTrue {
			return &Constant{
				Value:   types.NewDatum(true),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, true
		}
	case ast.EQ:
		// If it has equal condition `a=b`, we meet it again and we can replace it with true
		if (args[0].Equal(evalCtx, src) && args[1].Equal(evalCtx, tgt)) || (args[1].Equal(evalCtx, src) && args[0].Equal(evalCtx, tgt)) {
			return &Constant{
				Value:   types.NewDatum(true),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, true
		}
	case ast.LogicOr, ast.LogicAnd:
		for idx, expr := range args {
			if sf, ok := expr.(*ScalarFunction); ok {
				newExpr, subReplaced := replaceEqCondtionWithTrue(ctx, src, tgt, sf)
				if subReplaced {
					replaced = true
					args[idx] = newExpr
				}
			}
		}
		if replaced {
			return NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(ctx.GetEvalCtx()), args...), true
		}
	}
	return cond, false
}

// tryToReplaceCond aims to replace all occurrences of column 'src' and try to replace it with 'tgt' in 'cond'
// It returns
//
//	bool: if a replacement happened
//	bool: if 'cond' contains non-deterministic expression
//	Expression: the replaced expression, or original 'cond' if the replacement didn't happen
//
// For example:
//
//	for 'a, b, a < 3', it returns 'true, false, b < 3'
//	for 'a, b, sin(a) + cos(a) = 5', it returns 'true, false, returns sin(b) + cos(b) = 5'
//	for 'a, b, cast(a) < rand()', it returns 'false, true, cast(a) < rand()'
func tryToReplaceCond(ctx BuildContext, src *Column, tgt *Column, cond Expression, nullAware bool) (bool, bool, Expression) {
	if src.RetType.GetType() != tgt.RetType.GetType() {
		return false, false, cond
	}
	sf, ok := cond.(*ScalarFunction)
	if !ok {
		return false, false, cond
	}
	replaced := false
	var args []Expression
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false, true, cond
	}
	if _, ok := inequalFunctions[sf.FuncName.L]; ok {
		return false, true, cond
	}
	// See
	//	https://github.com/pingcap/tidb/issues/15782
	//  https://github.com/pingcap/tidb/issues/17817
	// The null sensitive function's result may rely on the original nullable information of the outer side column.
	// Its args cannot be replaced easily.
	// A more strict check is that after we replace the arg. We check the nullability of the new expression.
	// But we haven't maintained it yet, so don't replace the arg of the control function currently.
	if nullAware &&
		(sf.FuncName.L == ast.Ifnull ||
			sf.FuncName.L == ast.If ||
			sf.FuncName.L == ast.Case ||
			sf.FuncName.L == ast.NullEQ) {
		return false, true, cond
	}
	evalCtx := ctx.GetEvalCtx()
	for idx, expr := range sf.GetArgs() {
		if src.EqualColumn(expr) {
			_, coll := cond.CharsetAndCollation()
			if tgt.GetType(evalCtx).GetCollate() != coll {
				continue
			}
			replaced = true
			if args == nil {
				args = make([]Expression, len(sf.GetArgs()))
				copy(args, sf.GetArgs())
			}
			args[idx] = tgt
		} else {
			subReplaced, isNonDeterministic, subExpr := tryToReplaceCond(ctx, src, tgt, expr, nullAware)
			if isNonDeterministic {
				return false, true, cond
			} else if subReplaced {
				replaced = true
				if args == nil {
					args = make([]Expression, len(sf.GetArgs()))
					copy(args, sf.GetArgs())
				}
				args[idx] = subExpr
			}
		}
	}
	if replaced {
		return true, false, NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(ctx.GetEvalCtx()), args...)
	}
	return false, false, cond
}

var propConstSolverPool = sync.Pool{
	New: func() any {
		solver := &propConstSolver{
			basePropConstSolver: newBasePropConstSolver(),
			conditions:          make([]Expression, 0, 4),
		}
		return solver
	},
}

type propConstSolver struct {
	basePropConstSolver
	conditions []Expression
	// TODO: remove this func pointer for performance
	vaildExprFunc VaildConstantPropagationExpressionFuncType

	// if schema1 and schema2 are not nil, we're propagating constants for inner joins.
	// for outer joins, use propOuterJoinConstSolver instead.
	schema1 *Schema
	schema2 *Schema
}

// newPropConstSolver returns a PropagateConstantSolver.
func newPropConstSolver() PropagateConstantSolver {
	solver := propConstSolverPool.Get().(*propConstSolver)
	return solver
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func (s *propConstSolver) PropagateConstant(ctx exprctx.ExprContext,
	keepJoinKey bool, schema1, schema2 *Schema,
	vaildExprFunc VaildConstantPropagationExpressionFuncType, conditions []Expression) []Expression {
	s.ctx = ctx
	s.vaildExprFunc = vaildExprFunc
	s.schema1 = schema1
	s.schema2 = schema2
	return s.solve(keepJoinKey, conditions)
}

// Clear clears the solver and returns it to the pool.
func (s *propConstSolver) Clear() {
	s.basePropConstSolver.Clear()
	s.conditions = s.conditions[:0]
	s.vaildExprFunc = nil
	s.schema1 = nil
	s.schema2 = nil
	propConstSolverPool.Put(s)
}

// propagateConstantEQ propagates expressions like 'column = constant' by substituting the constant for column, the
// procedure repeats multiple times. An example runs as following:
// a = d & b * 2 = c & c = d + 2 & b = 1 & a = 4, we pick eq cond b = 1 and a = 4
// d = 4 & 2 = c & c = d + 2 & b = 1 & a = 4, we propagate b = 1 and a = 4 and pick eq cond c = 2 and d = 4
// d = 4 & 2 = c & false & b = 1 & a = 4, we propagate c = 2 and d = 4, and do constant folding: c = d + 2 will be folded as false.
func (s *propConstSolver) propagateConstantEQ() {
	intest.Assert(len(s.eqMapper) == 0 && s.eqMapper != nil)
	visited := make([]bool, len(s.conditions))
	cols := make([]*Column, 0, 4)
	cons := make([]Expression, 0, 4)
	for range MaxPropagateColsCnt {
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			return
		}
		cols = slices.Grow(cols, len(mapper))
		cons = slices.Grow(cons, len(mapper))
		for id, con := range mapper {
			cols = append(cols, s.columns[id])
			cons = append(cons, con)
		}
		for i, cond := range s.conditions {
			if !visited[i] {
				s.conditions[i] = ColumnSubstitute(s.ctx, cond, NewSchema(cols...), cons)
			}
		}
		cols = cols[:0]
		cons = cons[:0]
	}
}

// propagateColumnEQ propagates expressions like 'column A = column B' by adding extra filters
// 'expression(..., column B, ...)' propagated from 'expression(..., column A, ...)' as long as:
//
//  1. The expression is deterministic
//  2. The expression doesn't have any side effect
//
// e.g. For expression a = b and b = c and c = d and c < 1 , we can get extra a < 1 and b < 1 and d < 1.
// However, for a = b and a < rand(), we cannot propagate a < rand() to b < rand() because rand() is non-deterministic
//
// This propagation may bring redundancies that we need to resolve later, for example:
// for a = b and a < 3 and b < 3, we get new a < 3 and b < 3, which are redundant
// for a = b and a < 3 and 3 > b, we get new b < 3 and 3 > a, which are redundant
// for a = b and a < 3 and b < 4, we get new a < 4 and b < 3 but should expect a < 3 and b < 3
// for a = b and a in (3) and b in (4), we get b in (3) and a in (4) but should expect 'false'
//
// TODO: remove redundancies later
//
// We maintain a unionSet representing the equivalent for every two columns.
func (s *propConstSolver) propagateColumnEQ() {
	visited := make([]bool, len(s.conditions))
	if s.unionSet == nil {
		s.unionSet = disjointset.NewIntSet(len(s.columns))
	} else {
		s.unionSet.GrowNewIntSet(len(s.columns))
	}
	allVisited := true
	for i := range s.conditions {
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lCol, rCol, ok := IsColOpCol(fun)
			// TODO: Enable hybrid types in ConstantPropagate.
			if ok && lCol.GetType(s.ctx.GetEvalCtx()).GetCollate() == rCol.GetType(s.ctx.GetEvalCtx()).GetCollate() && !lCol.GetType(s.ctx.GetEvalCtx()).Hybrid() && !rCol.GetType(s.ctx.GetEvalCtx()).Hybrid() {
				lID := s.getColID(lCol)
				rID := s.getColID(rCol)
				visited[i] = true
				if s.unionSet.FindRoot(lID) != s.unionSet.FindRoot(rID) {
					// Add the equality relation to unionSet
					// if it has been added, we don't need to process it again.
					// It will be deleted in the replaceEqCondtionWithTrueitionsWithConstants
					s.unionSet.Union(lID, rID)
				} else if lID != rID {
					s.conditions[i] = &Constant{
						Value:   types.NewDatum(true),
						RetType: types.NewFieldType(mysql.TypeTiny),
					}
				}
				continue
			}
		}
		allVisited = false
	}
	if allVisited {
		return
	}
	condsLen := len(s.conditions)
	for i, coli := range s.columns {
		for j := i + 1; j < len(s.columns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				continue
			}
			colj := s.columns[j]
			for k := range condsLen {
				if visited[k] {
					// cond_k has been used to retrieve equality relation
					continue
				}
				s.conditions[k], _ = replaceEqCondtionWithTrue(s.ctx, coli, colj, s.conditions[k])
				cond := s.conditions[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, coli, colj, cond, false)
				if replaced {
					// TODO(hawkingrei): if it is the true expression, we can remvoe it.
					if isConstant(newExpr) || s.vaildExprFunc == nil || s.vaildExprFunc(newExpr) {
						s.conditions = append(s.conditions, newExpr)
					}
				}
				// Why do we need to replace colj with coli again?
				// Consider the case: col1 = col3
				// Join Schema: left schema: {col1,col2}, right schema: {col3,col4}
				// Conditions: col1 in (col3, col4)
				//
				// because the order of columns in equaility relation is not guaranteed,
				// We may replace col3 with col1 first, it cannot push down the condition to the child.
				// because two columns are from different side.
				// But if we replace col1 with col3, it can be pushed down.
				// So we need to try both directions.
				replaced, _, newExpr = tryToReplaceCond(s.ctx, colj, coli, cond, false)
				if replaced {
					// TODO(hawkingrei): if it is the true expression, we can remvoe it.
					if isConstant(newExpr) || s.vaildExprFunc == nil || s.vaildExprFunc(newExpr) {
						s.conditions = append(s.conditions, newExpr)
					}
				}
			}
		}
	}
}

// isConstant is to determine whether the expression is a constant.
func isConstant(cond Expression) bool {
	_, ok := cond.(*Constant)
	return ok
}

func (s *propConstSolver) setConds2ConstFalse() {
	if MaybeOverOptimized4PlanCache(s.ctx, s.conditions...) {
		s.ctx.SetSkipPlanCache("some parameters may be overwritten when constant propagation")
	}
	s.conditions = s.conditions[:0]
	s.conditions = append(s.conditions, &Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	})
}

// pickNewEQConds tries to pick new equal conds and puts them to retMapper.
func (s *propConstSolver) pickNewEQConds(visited []bool) (retMapper map[int]*Constant) {
	retMapper = make(map[int]*Constant)
	for i, cond := range s.conditions {
		if visited[i] {
			continue
		}
		col, con := validEqualCond(s.ctx.GetEvalCtx(), cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			con, ok = cond.(*Constant)
			if !ok {
				continue
			}
			visited[i] = true
			value, _, err := EvalBool(s.ctx.GetEvalCtx(), []Expression{con}, chunk.Row{})
			if err != nil {
				terror.Log(err)
				return nil
			}
			if !value {
				s.setConds2ConstFalse()
				return nil
			}
			continue
		}
		// TODO: Enable hybrid types in ConstantPropagate.
		if col.GetType(s.ctx.GetEvalCtx()).Hybrid() {
			continue
		}
		visited[i] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			s.setConds2ConstFalse()
			return nil
		}
		if updated {
			colType := col.GetType(s.ctx.GetEvalCtx())
			conType := con.GetType(s.ctx.GetEvalCtx())
			castedCon := con
			if !colType.Equal(conType) {
				oriWarningCnt := s.ctx.GetEvalCtx().WarningCount()
				newExpr := BuildCastFunction(s.ctx, con, colType.DeepCopy())
				s.ctx.GetEvalCtx().TruncateWarnings(oriWarningCnt)
				if newCon, ok := newExpr.(*Constant); ok {
					castedCon = newCon
				}
			}
			retMapper[s.getColID(col)] = castedCon
		}
	}
	return
}

func (s *propConstSolver) solve(keepJoinKey bool, conditions []Expression) []Expression {
	var joinKeys []Expression
	if keepJoinKey {
		// keep join keys in the results since they are crucial for join optimization like join reorder
		// and index join selection. (#63314, #60076)
		joinKeys = cloneJoinKeys(conditions, s.schema1, s.schema2)
	}
	s.conditions = slices.Grow(s.conditions, len(conditions))
	s.extractColumns(conditions)
	if len(s.columns) > MaxPropagateColsCnt {
		logutil.BgLogger().Warn("too many columns in a single CNF",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return conditions
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.conditions = propagateConstantDNF(s.ctx, s.vaildExprFunc, s.conditions...)
	s.conditions = append(s.conditions, joinKeys...)
	s.conditions = RemoveDupExprs(s.conditions)
	return slices.Clone(s.conditions)
}

func (s *propConstSolver) extractColumns(conditions []Expression) {
	mp := GetUniqueIDToColumnMap()
	defer PutUniqueIDToColumnMap(mp)
	s.conditions = s.extractColumnsInternal(mp, s.conditions, conditions)
}

// PropagateConstantForJoin propagate constants for inner joins.
func PropagateConstantForJoin(ctx exprctx.ExprContext, keepJoinKey bool, schema1, schema2 *Schema,
	filter VaildConstantPropagationExpressionFuncType, conditions ...Expression) []Expression {
	if len(conditions) == 0 {
		return conditions
	}
	solver := newPropConstSolver()
	defer func() {
		solver.Clear()
	}()
	return solver.PropagateConstant(exprctx.WithConstantPropagateCheck(ctx), keepJoinKey, schema1, schema2, filter, conditions)
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
// This is a constant propagation logic for expression list such as ['a=1', 'a=b']
func PropagateConstant(ctx exprctx.ExprContext, filter VaildConstantPropagationExpressionFuncType, conditions ...Expression) []Expression {
	if len(conditions) == 0 {
		return conditions
	}
	solver := newPropConstSolver()
	defer func() {
		solver.Clear()
	}()
	return solver.PropagateConstant(exprctx.WithConstantPropagateCheck(ctx), false, nil, nil, filter, conditions)
}

