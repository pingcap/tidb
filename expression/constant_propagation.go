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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/disjointset"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

type basePropConstSolver struct {
	colMapper map[int64]int       // colMapper maps column to its index
	eqList    []*Constant         // if eqList[i] != nil, it means col_i = eqList[i]
	unionSet  *disjointset.IntSet // unionSet stores the relations like col_i = col_j
	columns   []*Column           // columns stores all columns appearing in the conditions
	ctx       sessionctx.Context
}

func (s *basePropConstSolver) getColID(col *Column) int {
	return s.colMapper[col.UniqueID]
}

func (s *basePropConstSolver) insertCol(col *Column) {
	_, ok := s.colMapper[col.UniqueID]
	if !ok {
		s.colMapper[col.UniqueID] = len(s.colMapper)
		s.columns = append(s.columns, col)
	}
}

// tryToUpdateEQList tries to update the eqList. When the eqList has store this column with a different constant, like
// a = 1 and a = 2, we set the second return value to false.
func (s *basePropConstSolver) tryToUpdateEQList(col *Column, con *Constant) (bool, bool) {
	if con.Value.IsNull() {
		return false, true
	}
	id := s.getColID(col)
	oldCon := s.eqList[id]
	if oldCon != nil {
		return false, !oldCon.Equal(s.ctx, con)
	}
	s.eqList[id] = con
	return true, false
}

func validEqualCondHelper(ctx sessionctx.Context, eq *ScalarFunction, colIsLeft bool) (*Column, *Constant) {
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
	if ContainMutableConst(ctx, []Expression{con}) {
		return nil, nil
	}
	if !collate.CompatibleCollate(col.GetType().Collate, con.GetType().Collate) {
		return nil, nil
	}
	return col, con
}

// validEqualCond checks if the cond is an expression like [column eq constant].
func validEqualCond(ctx sessionctx.Context, cond Expression) (*Column, *Constant) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if eq.FuncName.L != ast.EQ {
			return nil, nil
		}
		col, con := validEqualCondHelper(ctx, eq, true)
		if col == nil {
			return validEqualCondHelper(ctx, eq, false)
		}
		return col, con
	}
	return nil, nil
}

// tryToReplaceCond aims to replace all occurrences of column 'src' and try to replace it with 'tgt' in 'cond'
// It returns
//  bool: if a replacement happened
//  bool: if 'cond' contains non-deterministic expression
//  Expression: the replaced expression, or original 'cond' if the replacement didn't happen
//
// For example:
//  for 'a, b, a < 3', it returns 'true, false, b < 3'
//  for 'a, b, sin(a) + cos(a) = 5', it returns 'true, false, returns sin(b) + cos(b) = 5'
//  for 'a, b, cast(a) < rand()', it returns 'false, true, cast(a) < rand()'
func tryToReplaceCond(ctx sessionctx.Context, src *Column, tgt *Column, cond Expression, rejectControl bool) (bool, bool, Expression) {
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
	// See https://github.com/pingcap/tidb/issues/15782. The control function's result may rely on the original nullable
	// information of the outer side column. Its args cannot be replaced easily.
	// A more strict check is that after we replace the arg. We check the nullability of the new expression.
	// But we haven't maintained it yet, so don't replace the arg of the control function currently.
	if rejectControl && (sf.FuncName.L == ast.Ifnull || sf.FuncName.L == ast.If || sf.FuncName.L == ast.Case) {
		return false, false, cond
	}
	for idx, expr := range sf.GetArgs() {
		if src.Equal(nil, expr) {
			_, coll := cond.CharsetAndCollation(ctx)
			if tgt.GetType().Collate != coll {
				continue
			}
			replaced = true
			if args == nil {
				args = make([]Expression, len(sf.GetArgs()))
				copy(args, sf.GetArgs())
			}
			args[idx] = tgt
		} else {
			subReplaced, isNonDeterministic, subExpr := tryToReplaceCond(ctx, src, tgt, expr, rejectControl)
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
		return true, false, NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), args...)
	}
	return false, false, cond
}

type propConstSolver struct {
	basePropConstSolver
	conditions []Expression
}

// propagateConstantEQ propagates expressions like 'column = constant' by substituting the constant for column, the
// procedure repeats multiple times. An example runs as following:
// a = d & b * 2 = c & c = d + 2 & b = 1 & a = 4, we pick eq cond b = 1 and a = 4
// d = 4 & 2 = c & c = d + 2 & b = 1 & a = 4, we propagate b = 1 and a = 4 and pick eq cond c = 2 and d = 4
// d = 4 & 2 = c & false & b = 1 & a = 4, we propagate c = 2 and d = 4, and do constant folding: c = d + 2 will be folded as false.
func (s *propConstSolver) propagateConstantEQ() {
	s.eqList = make([]*Constant, len(s.columns))
	visited := make([]bool, len(s.conditions))
	for i := 0; i < MaxPropagateColsCnt; i++ {
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
		for i, cond := range s.conditions {
			if !visited[i] {
				s.conditions[i] = ColumnSubstitute(cond, NewSchema(cols...), cons)
			}
		}
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
	s.unionSet = disjointset.NewIntSet(len(s.columns))
	for i := range s.conditions {
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lCol, lOk := fun.GetArgs()[0].(*Column)
			rCol, rOk := fun.GetArgs()[1].(*Column)
			if lOk && rOk && lCol.GetType().Collate == rCol.GetType().Collate {
				lID := s.getColID(lCol)
				rID := s.getColID(rCol)
				s.unionSet.Union(lID, rID)
				visited[i] = true
			}
		}
	}

	condsLen := len(s.conditions)
	for i, coli := range s.columns {
		for j := i + 1; j < len(s.columns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				continue
			}
			colj := s.columns[j]
			for k := 0; k < condsLen; k++ {
				if visited[k] {
					// cond_k has been used to retrieve equality relation
					continue
				}
				cond := s.conditions[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, coli, colj, cond, false)
				if replaced {
					s.conditions = append(s.conditions, newExpr)
				}
				replaced, _, newExpr = tryToReplaceCond(s.ctx, colj, coli, cond, false)
				if replaced {
					s.conditions = append(s.conditions, newExpr)
				}
			}
		}
	}
}

func (s *propConstSolver) setConds2ConstFalse() {
	s.conditions = []Expression{&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}}
}

// pickNewEQConds tries to pick new equal conds and puts them to retMapper.
func (s *propConstSolver) pickNewEQConds(visited []bool) (retMapper map[int]*Constant) {
	retMapper = make(map[int]*Constant)
	for i, cond := range s.conditions {
		if visited[i] {
			continue
		}
		col, con := validEqualCond(s.ctx, cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			con, ok = cond.(*Constant)
			if !ok {
				continue
			}
			visited[i] = true
			if ContainMutableConst(s.ctx, []Expression{con}) {
				continue
			}
			value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
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
		visited[i] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			s.setConds2ConstFalse()
			return nil
		}
		if updated {
			retMapper[s.getColID(col)] = con
		}
	}
	return
}

func (s *propConstSolver) solve(conditions []Expression) []Expression {
	cols := make([]*Column, 0, len(conditions))
	for _, cond := range conditions {
		s.conditions = append(s.conditions, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	for _, col := range cols {
		s.insertCol(col)
	}
	if len(s.columns) > MaxPropagateColsCnt {
		logutil.BgLogger().Warn("too many columns in a single CNF",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return conditions
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.conditions = propagateConstantDNF(s.ctx, s.conditions)
	return s.conditions
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	return newPropConstSolver().PropagateConstant(ctx, conditions)
}

type propOuterJoinConstSolver struct {
	basePropConstSolver
	joinConds   []Expression
	filterConds []Expression
	outerSchema *Schema
	innerSchema *Schema
	// nullSensitive indicates if this outer join is null sensitive, if true, we cannot generate
	// additional `col is not null` condition from column equal conditions. Specifically, this value
	// is true for LeftOuterSemiJoin and AntiLeftOuterSemiJoin.
	nullSensitive bool
}

func (s *propOuterJoinConstSolver) setConds2ConstFalse(filterConds bool) {
	s.joinConds = []Expression{&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}}
	if filterConds {
		s.filterConds = []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}
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
		col, con := validEqualCond(s.ctx, cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			con, ok = cond.(*Constant)
			if !ok {
				continue
			}
			visited[i+condsOffset] = true
			if ContainMutableConst(s.ctx, []Expression{con}) {
				continue
			}
			value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
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
	s.eqList = make([]*Constant, len(s.columns))
	lenFilters := len(s.filterConds)
	visited := make([]bool, lenFilters+len(s.joinConds))
	for i := 0; i < MaxPropagateColsCnt; i++ {
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
				s.joinConds[i] = ColumnSubstitute(cond, NewSchema(cols...), cons)
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
		if lOk && rOk && lCol.GetType().Collate == rCol.GetType().Collate {
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
	for k := 0; k < condsLen; k++ {
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
	visited := make([]bool, 2*len(s.joinConds)+len(s.filterConds))
	s.unionSet = disjointset.NewIntSet(len(s.columns))
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
			if s.nullSensitive {
				continue
			}
			childCol := s.innerSchema.RetrieveColumn(innerCol)
			if !mysql.HasNotNullFlag(childCol.RetType.Flag) {
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

func (s *propOuterJoinConstSolver) solve(joinConds, filterConds []Expression) ([]Expression, []Expression) {
	cols := make([]*Column, 0, len(joinConds)+len(filterConds))
	for _, cond := range joinConds {
		s.joinConds = append(s.joinConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	for _, cond := range filterConds {
		s.filterConds = append(s.filterConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	for _, col := range cols {
		s.insertCol(col)
	}
	if len(s.columns) > MaxPropagateColsCnt {
		logutil.BgLogger().Warn("too many columns",
			zap.Int("numCols", len(s.columns)),
			zap.Int("maxNumCols", MaxPropagateColsCnt),
		)
		return joinConds, filterConds
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	s.joinConds = propagateConstantDNF(s.ctx, s.joinConds)
	s.filterConds = propagateConstantDNF(s.ctx, s.filterConds)
	return s.joinConds, s.filterConds
}

// propagateConstantDNF find DNF item from CNF, and propagate constant inside DNF.
func propagateConstantDNF(ctx sessionctx.Context, conds []Expression) []Expression {
	for i, cond := range conds {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(ctx, PropagateConstant(ctx, []Expression{item})...)
			}
			conds[i] = ComposeDNFCondition(ctx, dnfItems...)
		}
	}
	return conds
}

// PropConstOverOuterJoin propagate constant equal and column equal conditions over outer join.
// First step is to extract `outerCol = const` from join conditions and filter conditions,
// and substitute `outerCol` in join conditions with `const`;
// Second step is to extract `outerCol = innerCol` from join conditions, and derive new join
// conditions based on this column equal condition and `outerCol` related
// expressions in join conditions and filter conditions;
func PropConstOverOuterJoin(ctx sessionctx.Context, joinConds, filterConds []Expression,
	outerSchema, innerSchema *Schema, nullSensitive bool) ([]Expression, []Expression) {
	solver := &propOuterJoinConstSolver{
		outerSchema:   outerSchema,
		innerSchema:   innerSchema,
		nullSensitive: nullSensitive,
	}
	solver.colMapper = make(map[int64]int)
	solver.ctx = ctx
	return solver.solve(joinConds, filterConds)
}

// PropagateConstantSolver is a constant propagate solver.
type PropagateConstantSolver interface {
	PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression
}

// newPropConstSolver returns a PropagateConstantSolver.
func newPropConstSolver() PropagateConstantSolver {
	solver := &propConstSolver{}
	solver.colMapper = make(map[int64]int)
	return solver
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func (s *propConstSolver) PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	s.ctx = ctx
	return s.solve(conditions)
}
