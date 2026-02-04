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

var propOuterJoinConstSolverPool = sync.Pool{
	New: func() any {
		solver := &propOuterJoinConstSolver{
			basePropConstSolver: newBasePropConstSolver(),
			joinConds:           make([]Expression, 0, 4),
			filterConds:         make([]Expression, 0, 4),
		}
		return solver
	},
}

// propOuterJoinConstSolver is used to propagate constant values over outer join.
// Outer join predicates need special care since we can only propagate constants from the inner side to
// the outer side, otherwise nullability of the join result could be incorrectly affected.
// For example: `select * from t1 left join t2 on t1.a=t2.a where t1.a=1`, we can't propagate t1.a=1 to t2.a=1,
// since after this propagation, t2.a will never be null, which is incorrect for this outer join query.
type propOuterJoinConstSolver struct {
	basePropConstSolver
	joinConds   []Expression
	filterConds []Expression
	outerSchema *Schema
	innerSchema *Schema
	// TODO: remove this func pointer for performance
	vaildExprFunc VaildConstantPropagationExpressionFuncType

	// nullSensitive indicates if this outer join is null sensitive, if true, we cannot generate
	// additional `col is not null` condition from column equal conditions. Specifically, this value
	// is true for LeftOuterSemiJoin, AntiLeftOuterSemiJoin and AntiSemiJoin.
	nullSensitive bool
}

func newPropOuterJoinConstSolver() *propOuterJoinConstSolver {
	solver := propOuterJoinConstSolverPool.Get().(*propOuterJoinConstSolver)
	return solver
}

// clear resets the solver.
func (s *propOuterJoinConstSolver) Clear() {
	s.basePropConstSolver.Clear()
	s.joinConds = s.joinConds[:0]
	s.filterConds = s.filterConds[:0]
	s.outerSchema = nil
	s.innerSchema = nil
	s.nullSensitive = false
	s.vaildExprFunc = nil
	propOuterJoinConstSolverPool.Put(s)
}

func (s *propOuterJoinConstSolver) setConds2ConstFalse(filterConds bool) {
	s.joinConds = s.joinConds[:0]
	s.joinConds = append(s.joinConds, &Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	})
	if filterConds {
		s.filterConds = s.filterConds[:0]
		s.filterConds = append(s.filterConds, &Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		})
	}
}

func (s *basePropConstSolver) dealWithPossibleHybridType(col *Column, con *Constant) (*Constant, bool) {
	if !col.GetType(s.ctx.GetEvalCtx()).Hybrid() {
		return con, true
	}
	if col.GetType(s.ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
		d, err := con.Eval(s.ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, false
		}
		if MaybeOverOptimized4PlanCache(s.ctx, con) {
			s.ctx.SetSkipPlanCache("Skip plan cache since mutable constant is restored and propagated")
		}
		switch d.Kind() {
		case types.KindInt64:
			enum, err := types.ParseEnumValue(col.GetType(s.ctx.GetEvalCtx()).GetElems(), uint64(d.GetInt64()))
			if err != nil {
				logutil.BgLogger().Debug("Invalid Enum parsed during constant propagation")
				return nil, false
			}
			con = &Constant{
				Value:         types.NewMysqlEnumDatum(enum),
				RetType:       col.RetType.Clone(),
				collationInfo: col.collationInfo,
			}
		case types.KindString:
			enum, err := types.ParseEnumName(col.GetType(s.ctx.GetEvalCtx()).GetElems(), d.GetString(), d.Collation())
			if err != nil {
				logutil.BgLogger().Debug("Invalid Enum parsed during constant propagation")
				return nil, false
			}
			con = &Constant{
				Value:         types.NewMysqlEnumDatum(enum),
				RetType:       col.RetType.Clone(),
				collationInfo: col.collationInfo,
			}
		case types.KindMysqlEnum, types.KindMysqlSet:
			// It's already a hybrid type. Just use it.
		default:
			// We skip other cases first.
			return nil, false
		}
		return con, true
	}
	return nil, false
}

func (s *basePropConstSolver) extractColumnsInternal(mp map[int64]*Column, splitConds []Expression, conds []Expression) []Expression {
	for _, cond := range conds {
		splitConds = append(splitConds, SplitCNFItems(cond)...)
		ExtractColumnsMapFromExpressionsWithReusedMap(mp, nil, cond)
		s.insertCols(mp)
		clear(mp)
	}
	return splitConds
}

// pickEQCondsOnOuterCol picks constant equal expression from specified conditions.
func (s *propOuterJoinConstSolver) pickEQCondsOnOuterCol(retMapper map[int]*Constant, visited []bool, filterConds bool) map[int]*Constant {
	var conds []Expression
	var condsOffset int
	if filterConds {
		conds = s.filterConds
	} else {
		conds = s.joinConds
		condsOffset = len(s.filterConds)
	}
	for i, cond := range conds {
		if visited[i+condsOffset] {
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
			visited[i+condsOffset] = true
			value, _, err := EvalBool(s.ctx.GetEvalCtx(), []Expression{con}, chunk.Row{})
			if err != nil {
				terror.Log(err)
				return nil
			}
			if !value {
				s.setConds2ConstFalse(filterConds)
				return nil
			}
			continue
		}
		var valid bool
		con, valid = s.dealWithPossibleHybridType(col, con)
		if !valid {
			continue
		}
		// Only extract `outerCol = const` expressions.
		if !s.outerSchema.Contains(col) {
			continue
		}
		visited[i+condsOffset] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			s.setConds2ConstFalse(filterConds)
			return nil
		}
		if updated {
			retMapper[s.getColID(col)] = con
		}
	}
	return retMapper
}

// pickNewEQConds picks constant equal expressions from join and filter conditions.
func (s *propOuterJoinConstSolver) pickNewEQConds(visited []bool) map[int]*Constant {
	retMapper := make(map[int]*Constant)
	retMapper = s.pickEQCondsOnOuterCol(retMapper, visited, true)
	if retMapper == nil {
		// Filter is constant false or error occurred, enforce early termination.
		return nil
	}
	retMapper = s.pickEQCondsOnOuterCol(retMapper, visited, false)
	return retMapper
}

// propagateConstantEQ propagates expressions like `outerCol = const` by substituting `outerCol` in *JOIN* condition
// with `const`, the procedure repeats multiple times.
func (s *propOuterJoinConstSolver) propagateConstantEQ() {
	clear(s.eqMapper)
	lenFilters := len(s.filterConds)
	visited := make([]bool, lenFilters+len(s.joinConds))
	for range MaxPropagateColsCnt {
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			return
		}
		cols := make([]*Column, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			cols = append(cols, s.columns[id])
			cons = append(cons, con)
		}
		for i, cond := range s.joinConds {
			if !visited[i+lenFilters] {
				s.joinConds[i] = ColumnSubstitute(s.ctx, cond, NewSchema(cols...), cons)
			}
		}
	}
}

func (s *propOuterJoinConstSolver) colsFromOuterAndInner(col1, col2 *Column) (*Column, *Column) {
	if s.outerSchema.Contains(col1) && s.innerSchema.Contains(col2) {
		return col1, col2
	}
	if s.outerSchema.Contains(col2) && s.innerSchema.Contains(col1) {
		return col2, col1
	}
	return nil, nil
}

// validColEqualCond checks if expression is column equal condition that we can use for constant
// propagation over outer join. We only use expression like `outerCol = innerCol`, for expressions like
// `outerCol1 = outerCol2` or `innerCol1 = innerCol2`, they do not help deriving new inner table conditions
// which can be pushed down to children plan nodes, so we do not pick them.
func (s *propOuterJoinConstSolver) validColEqualCond(cond Expression) (*Column, *Column) {
	if fun, ok := cond.(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
		lCol, lOk := fun.GetArgs()[0].(*Column)
		rCol, rOk := fun.GetArgs()[1].(*Column)
		if lOk && rOk && lCol.GetType(s.ctx.GetEvalCtx()).GetCollate() == rCol.GetType(s.ctx.GetEvalCtx()).GetCollate() {
			return s.colsFromOuterAndInner(lCol, rCol)
		}
	}
	return nil, nil
}

// deriveConds given `outerCol = innerCol`, derive new expression for specified conditions.
func (s *propOuterJoinConstSolver) deriveConds(outerCol, innerCol *Column, schema *Schema, fCondsOffset int, visited []bool, filterConds bool) []bool {
	var offset, condsLen int
	var conds []Expression
	if filterConds {
		conds = s.filterConds
		offset = fCondsOffset
		condsLen = len(s.filterConds)
	} else {
		conds = s.joinConds
		condsLen = fCondsOffset
	}
	for k := range condsLen {
		if visited[k+offset] {
			// condition has been used to retrieve equality relation or contains column beyond children schema.
			continue
		}
		cond := conds[k]
		if !ExprFromSchema(cond, schema) {
			visited[k+offset] = true
			continue
		}
		replaced, _, newExpr := tryToReplaceCond(s.ctx, outerCol, innerCol, cond, true)
		if replaced {
			// TODO(hawkingrei): if it is the true expression, we can remvoe it.
			if !isConstant(newExpr) && s.vaildExprFunc != nil && !s.vaildExprFunc(newExpr) {
				continue
			}
			s.joinConds = append(s.joinConds, newExpr)
		}
	}
	return visited
}

// propagateColumnEQ propagates expressions like 'outerCol = innerCol' by adding extra filters
// 'expression(..., innerCol, ...)' derived from 'expression(..., outerCol, ...)' as long as
// 'expression(..., outerCol, ...)' does not reference columns outside children schemas of join node.
// Derived new expressions must be appended into join condition, not filter condition.
func (s *propOuterJoinConstSolver) propagateColumnEQ() {
	if s.nullSensitive {
		return
	}
	visited := make([]bool, 2*len(s.joinConds)+len(s.filterConds))
	if s.unionSet == nil {
		s.unionSet = disjointset.NewIntSet(len(s.columns))
	} else {
		s.unionSet.GrowNewIntSet(len(s.columns))
	}
	var outerCol, innerCol *Column
	// Only consider column equal condition in joinConds.
	// If we have column equal in filter condition, the outer join should have been simplified already.
	for i := range s.joinConds {
		outerCol, innerCol = s.validColEqualCond(s.joinConds[i])
		if outerCol != nil {
			outerID := s.getColID(outerCol)
			innerID := s.getColID(innerCol)
			s.unionSet.Union(outerID, innerID)
			visited[i] = true
			// Generate `innerCol is not null` from `outerCol = innerCol`. Note that `outerCol is not null`
			// does not hold since we are in outer join.
			// For AntiLeftOuterSemiJoin, this does not work, for example:
			// `select *, t1.a not in (select t2.b from t t2) from t t1` does not imply `t2.b is not null`.
			// For LeftOuterSemiJoin, this does not work either, for example:
			// `select *, t1.a in (select t2.b from t t2) from t t1`
			// rows with t2.b is null would impact whether LeftOuterSemiJoin should output 0 or null if there
			// is no row satisfying t2.b = t1.a
			childCol := s.innerSchema.RetrieveColumn(innerCol)
			if !mysql.HasNotNullFlag(childCol.RetType.GetFlag()) {
				notNullExpr := BuildNotNullExpr(s.ctx, childCol)
				s.joinConds = append(s.joinConds, notNullExpr)
			}
		}
	}
	lenJoinConds := len(s.joinConds)
	mergedSchema := MergeSchema(s.outerSchema, s.innerSchema)
	for i, coli := range s.columns {
		for j := i + 1; j < len(s.columns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation.
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				continue
			}
			colj := s.columns[j]
			outerCol, innerCol = s.colsFromOuterAndInner(coli, colj)
			if outerCol == nil {
				continue
			}
			visited = s.deriveConds(outerCol, innerCol, mergedSchema, lenJoinConds, visited, false)
			visited = s.deriveConds(outerCol, innerCol, mergedSchema, lenJoinConds, visited, true)
		}
	}
}

func (s *propOuterJoinConstSolver) solve(keepJoinKey bool, joinConds, filterConds []Expression) ([]Expression, []Expression) {
	var joinKeys []Expression
	if keepJoinKey {
		// keep join keys in the results since they are crucial for join optimization like join reorder
		// and index join selection. (#63314, #60076)
		joinKeys = cloneJoinKeys(joinConds, s.outerSchema, s.innerSchema)
	}
	s.extractColumns(joinConds, filterConds)
	if len(s.columns) > MaxPropagateColsCnt {
		logutil.BgLogger().Warn("too many columns",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return joinConds, filterConds
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.joinConds = propagateConstantDNF(s.ctx, s.vaildExprFunc, s.joinConds...)
	s.joinConds = RemoveDupExprs(append(s.joinConds, joinKeys...))
	s.filterConds = propagateConstantDNF(s.ctx, s.vaildExprFunc, s.filterConds...)
	return slices.Clone(s.joinConds), slices.Clone(s.filterConds)
}

func (s *propOuterJoinConstSolver) extractColumns(joinConds, filterConds []Expression) {
	mp := GetUniqueIDToColumnMap()
	defer PutUniqueIDToColumnMap(mp)
	s.joinConds = s.extractColumnsInternal(mp, s.joinConds, joinConds)
	s.filterConds = s.extractColumnsInternal(mp, s.filterConds, filterConds)
}

// propagateConstantDNF find DNF item from CNF, and propagate constant inside DNF.
func propagateConstantDNF(ctx exprctx.ExprContext, filter VaildConstantPropagationExpressionFuncType, conds ...Expression) []Expression {
	for i, cond := range conds {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(ctx, PropagateConstant(ctx, filter, item)...)
			}
			conds[i] = ComposeDNFCondition(ctx, dnfItems...)
		}
	}
	return conds
}

// PropConstForOuterJoin propagate constant equal and column equal conditions over outer join or anti semi join.
// First step is to extract `outerCol = const` from join conditions and filter conditions,
// and substitute `outerCol` in join conditions with `const`;
// Second step is to extract `outerCol = innerCol` from join conditions, and derive new join
// conditions based on this column equal condition and `outerCol` related
// expressions in join conditions and filter conditions;
func PropConstForOuterJoin(ctx exprctx.ExprContext, joinConds, filterConds []Expression,
	outerSchema, innerSchema *Schema, keepJoinKey, nullSensitive bool,
	vaildExprFunc VaildConstantPropagationExpressionFuncType) ([]Expression, []Expression) {
	solver := newPropOuterJoinConstSolver()
	defer func() {
		solver.Clear()
	}()
	solver.outerSchema = outerSchema
	solver.innerSchema = innerSchema
	solver.nullSensitive = nullSensitive
	solver.ctx = ctx
	solver.vaildExprFunc = vaildExprFunc
	return solver.solve(keepJoinKey, joinConds, filterConds)
}

// PropagateConstantSolver is a constant propagate solver.
type PropagateConstantSolver interface {
	PropagateConstant(ctx exprctx.ExprContext,
		keepJoinKey bool, schema1, schema2 *Schema,
		filter VaildConstantPropagationExpressionFuncType, conditions []Expression) []Expression
	Clear()
}

// cloneJoinKeys clones all join keys like `t1.col = t2.col` in these expressions.
// schema1 and schema2 are used to identify join keys.
func cloneJoinKeys(exprs []Expression, schema1, schema2 *Schema) (joinKeys []Expression) {
	if schema1 == nil || schema2 == nil {
		return nil
	}
	for _, expr := range exprs {
		if isJoinKey(expr, schema1, schema2) {
			joinKeys = append(joinKeys, expr.Clone())
		}
	}
	return
}

// isJoinKey returns true if this expression could be a join key like `t1.col = t2.col`.
func isJoinKey(expr Expression, schema1, schema2 *Schema) bool {
	binop, ok := expr.(*ScalarFunction)
	if !ok || binop.FuncName.L != ast.EQ {
		return false
	}
	col1, lOK := binop.GetArgs()[0].(*Column)
	col2, rOK := binop.GetArgs()[1].(*Column)
	if !lOK || !rOK {
		return false
	}
	// from different tables
	return (schema1.Contains(col1) && schema2.Contains(col2)) ||
		(schema1.Contains(col2) && schema2.Contains(col1))
}
