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
	"github.com/pingcap/tidb/pkg/util/disjointset"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
