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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

type multiEqualSet struct {
	parent []int
}

func (m *multiEqualSet) init(l int) {
	m.parent = make([]int, l)
	for i := range m.parent {
		m.parent[i] = i
	}
}

func (m *multiEqualSet) addRelation(a int, b int) {
	m.parent[m.findRoot(a)] = m.findRoot(b)
}

func (m *multiEqualSet) findRoot(a int) int {
	if a == m.parent[a] {
		return a
	}
	m.parent[a] = m.findRoot(m.parent[a])
	return m.parent[a]
}

type basePropConstSolver struct {
	colMapper map[string]int // colMapper maps column to its index
	eqList    []*Constant    // if eqList[i] != nil, it means col_i = eqList[i]
	unionSet  *multiEqualSet // unionSet stores the relations like col_i = col_j
	columns   []*Column      // columns stores all columns appearing in the conditions
	ctx       sessionctx.Context
}

func (s *basePropConstSolver) getColID(col *Column) int {
	code := col.HashCode(nil)
	return s.colMapper[string(code)]
}

func (s *basePropConstSolver) insertCol(col *Column) {
	code := col.HashCode(nil)
	_, ok := s.colMapper[string(code)]
	if !ok {
		s.colMapper[string(code)] = len(s.colMapper)
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

// validEqualCond checks if the cond is an expression like [column eq constant].
func validEqualCond(cond Expression) (*Column, *Constant) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if eq.FuncName.L != ast.EQ {
			return nil, nil
		}
		if col, colOk := eq.GetArgs()[0].(*Column); colOk {
			if con, conOk := eq.GetArgs()[1].(*Constant); conOk {
				return col, con
			}
		}
		if col, colOk := eq.GetArgs()[1].(*Column); colOk {
			if con, conOk := eq.GetArgs()[0].(*Constant); conOk {
				return col, con
			}
		}
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
func tryToReplaceCond(ctx sessionctx.Context, src *Column, tgt *Column, cond Expression) (bool, bool, Expression) {
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
	for idx, expr := range sf.GetArgs() {
		if src.Equal(nil, expr) {
			replaced = true
			if args == nil {
				args = make([]Expression, len(sf.GetArgs()))
				copy(args, sf.GetArgs())
			}
			args[idx] = tgt
		} else {
			subReplaced, isNonDeterminisitic, subExpr := tryToReplaceCond(ctx, src, tgt, expr)
			if isNonDeterminisitic {
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
	s.unionSet = &multiEqualSet{}
	s.unionSet.init(len(s.columns))
	for i := range s.conditions {
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lCol, lOk := fun.GetArgs()[0].(*Column)
			rCol, rOk := fun.GetArgs()[1].(*Column)
			if lOk && rOk {
				lID := s.getColID(lCol)
				rID := s.getColID(rCol)
				s.unionSet.addRelation(lID, rID)
				visited[i] = true
			}
		}
	}

	condsLen := len(s.conditions)
	for i, coli := range s.columns {
		for j := i + 1; j < len(s.columns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation
			if s.unionSet.findRoot(i) != s.unionSet.findRoot(j) {
				continue
			}
			colj := s.columns[j]
			for k := 0; k < condsLen; k++ {
				if visited[k] {
					// cond_k has been used to retrieve equality relation
					continue
				}
				cond := s.conditions[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, coli, colj, cond)
				if replaced {
					s.conditions = append(s.conditions, newExpr)
				}
				replaced, _, newExpr = tryToReplaceCond(s.ctx, colj, coli, cond)
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
		col, con := validEqualCond(cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			if con, ok = cond.(*Constant); ok {
				value, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
				terror.Log(errors.Trace(err))
				if !value {
					s.setConds2ConstFalse()
					return nil
				}
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
		log.Warnf("[const_propagation]Too many columns in a single CNF: the column count is %d, the max count is %d.", len(s.columns), MaxPropagateColsCnt)
		return conditions
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	for i, cond := range s.conditions {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(s.ctx, PropagateConstant(s.ctx, []Expression{item})...)
			}
			s.conditions[i] = ComposeDNFCondition(s.ctx, dnfItems...)
		}
	}
	return s.conditions
}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	solver := &propConstSolver{}
	solver.colMapper = make(map[string]int)
	solver.ctx = ctx
	return solver.solve(conditions)
}

type propOuterJoinConstSolver struct {
	basePropConstSolver
	jConds      []Expression
	fConds      []Expression
	outerSchema *Schema
	innerSchema *Schema
}

func (s *propOuterJoinConstSolver) setConds2ConstFalse(jConds, fConds bool) {
	if jConds {
		s.jConds = []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}
	if fConds {
		s.fConds = []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}
}

func (s *propOuterJoinConstSolver) pickNewEQCondsFunc(retMapper map[int]*Constant, visited []bool, fConds bool) map[int]*Constant {
	var conds []Expression
	var condsOffset int
	if fConds {
		conds = s.fConds
	} else {
		conds = s.jConds
		condsOffset = len(s.fConds)
	}
	for i, cond := range conds {
		if visited[i+condsOffset] {
			continue
		}
		col, con := validEqualCond(cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if col == nil {
			if con, ok = cond.(*Constant); ok {
				value, err := EvalBool(s.ctx, []Expression{con}, chunk.Row{})
				terror.Log(errors.Trace(err))
				if !value {
					if fConds {
						s.setConds2ConstFalse(true, true)
					} else {
						s.setConds2ConstFalse(true, false)
					}
					return nil
				}
			}
			continue
		}
		// Only extract `outerTable = const` expressions.
		if !s.outerSchema.Contains(col) {
			continue
		}
		visited[i+condsOffset] = true
		updated, foreverFalse := s.tryToUpdateEQList(col, con)
		if foreverFalse {
			if fConds {
				s.setConds2ConstFalse(true, true)
			} else {
				s.setConds2ConstFalse(true, false)
			}
			return nil
		}
		if updated {
			retMapper[s.getColID(col)] = con
		}
	}
	return retMapper
}

func (s *propOuterJoinConstSolver) pickNewEQConds(visited []bool) map[int]*Constant {
	retMapper := make(map[int]*Constant)
	retMapper = s.pickNewEQCondsFunc(retMapper, visited, true)
	if retMapper == nil {
		return nil
	}
	retMapper = s.pickNewEQCondsFunc(retMapper, visited, false)
	return retMapper
}

func (s *propOuterJoinConstSolver) propagateConstantEQ() {
	s.eqList = make([]*Constant, len(s.columns))
	lenFilters := len(s.fConds)
	visited := make([]bool, lenFilters+len(s.jConds))
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
		// Only substitute join conditions for outer join.
		for i, cond := range s.jConds {
			if !visited[i+lenFilters] {
				s.jConds[i] = ColumnSubstitute(cond, NewSchema(cols...), cons)
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

// validColEqualCond check if expression is the column equql condition we can use for constant
// propagation over outer join. We only use expression like `t1.a = t2.a`, for expressions like
// `t1.a = t1.b` or `t2.a = t2.b`, they do not help deriving new conditions on inner table, which
// are intended to be pushed down to children plan nodes.
func (s *propOuterJoinConstSolver) validColEqualCond(cond Expression) (*Column, *Column) {
	if fun, ok := cond.(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
		lCol, lOk := fun.GetArgs()[0].(*Column)
		rCol, rOk := fun.GetArgs()[1].(*Column)
		if lOk && rOk {
			return s.colsFromOuterAndInner(lCol, rCol)
		}
	}
	return nil, nil

}

func (s *propOuterJoinConstSolver) propagateColumnEQ() {
	visited := make([]bool, len(s.jConds)+len(s.fConds))
	s.unionSet = &multiEqualSet{}
	s.unionSet.init(len(s.columns))
	var outerCol, innerCol *Column
	// Only consider column equal condition in joinConds.
	// If we have column equal in filter condition, the outer join should be simplified already.
	for i := range s.jConds {
		outerCol, innerCol = s.validColEqualCond(s.jConds[i])
		if outerCol != nil {
			outerID := s.getColID(outerCol)
			innerID := s.getColID(innerCol)
			s.unionSet.addRelation(outerID, innerID)
			visited[i] = true
		}
	}
	lenJoinConds := len(s.jConds)
	for i, coli := range s.columns {
		for j := i + 1; j < len(s.columns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate col_i = col_j relation.
			if s.unionSet.findRoot(i) != s.unionSet.findRoot(j) {
				continue
			}
			colj := s.columns[j]
			outerCol, innerCol = s.colsFromOuterAndInner(coli, colj)
			if outerCol == nil {
				continue
			}
			for k := 0; k < lenJoinConds; k++ {
				if visited[k] {
					// cond_k has been used to retrieve equality relation.
					continue
				}
				cond := s.jConds[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, outerCol, innerCol, cond)
				if replaced {
					// Must append to join condition, otherwise it is not semantially equivalent regarding outer join.
					s.jConds = append(s.jConds, newExpr)
				}
			}
			for _, cond := range s.fConds {
				replaced, _, newExpr := tryToReplaceCond(s.ctx, outerCol, innerCol, cond)
				if replaced {
					// Must append to join condition, otherwise it is not semantially equivalent regarding outer join.
					s.jConds = append(s.jConds, newExpr)
				}
			}
		}
	}
}

func (s *propOuterJoinConstSolver) solve(joinConds, filterConds []Expression) ([]Expression, []Expression) {
	cols := make([]*Column, 0, len(joinConds)+len(filterConds))
	for _, cond := range joinConds {
		s.jConds = append(s.jConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	for _, cond := range filterConds {
		s.fConds = append(s.fConds, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	for _, col := range cols {
		s.insertCol(col)
	}
	if len(s.columns) > MaxPropagateColsCnt {
		log.Warnf("[const_propagation] Too many columns: column count is %d, max count is %d.", len(s.columns), MaxPropagateColsCnt)
		return joinConds, filterConds
	}
	s.propagateConstantEQ()
	s.propagateColumnEQ()
	for i, cond := range s.jConds {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(s.ctx, PropagateConstant(s.ctx, []Expression{item})...)
			}
			s.jConds[i] = ComposeDNFCondition(s.ctx, dnfItems...)
		}
	}
	for i, cond := range s.fConds {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(s.ctx, PropagateConstant(s.ctx, []Expression{item})...)
			}
			s.fConds[i] = ComposeDNFCondition(s.ctx, dnfItems...)
		}
	}
	return s.jConds, s.fConds
}

// PropConstOverOuterJoin propagate constant equal and column equal conditions over outer join.
// First step is to extract `outerCol = const` from join conditions and filter conditions,
// and substitute `outerCol` in join conditions with `const`;
// Second step is to extract `outerCol = innerCol` from join conditions, and derive new join
// conditions based on this column equal condition and `outerCol` related
// expressions in join conditions and filter conditions;
func PropConstOverOuterJoin(ctx sessionctx.Context, joinConds, filterConds []Expression, outerSchema, innerSchema *Schema) ([]Expression, []Expression) {
	solver := &propOuterJoinConstSolver{
		outerSchema: outerSchema,
		innerSchema: innerSchema,
	}
	solver.colMapper = make(map[string]int)
	solver.ctx = ctx
	return solver.solve(joinConds, filterConds)
}
