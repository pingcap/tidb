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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

var eqFuncNameMap = map[string]bool{
	ast.EQ: true,
}

// inEqFuncNameMap stores all the in-equal operators that can be propagated.
var inEqFuncNameMap = map[string]bool{
	ast.LT: true,
	ast.GT: true,
	ast.LE: true,
	ast.GE: true,
	ast.NE: true,
}

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

type propagateConstantSolver struct {
	colMapper  map[string]int // colMapper maps column to its index
	unionSet   *multiEqualSet // unionSet stores the relations like col_i = col_j
	eqList     []*Constant    // if eqList[i] != nil, it means col_i = eqList[i]
	columns    []*Column      // columns stores all columns appearing in the conditions
	conditions []Expression
	ctx        context.Context
}

// propagateInEQ propagates all in-equal conditions.
// e.g. For expression a = b and b = c and c = d and c < 1 , we can get extra a < 1 and b < 1 and d < 1.
// We maintain a unionSet representing the equivalent for every two columns.
func (s *propagateConstantSolver) propagateInEQ() {
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
			}
		}
	}
	condsLen := len(s.conditions)
	for i := 0; i < condsLen; i++ {
		cond := s.conditions[i]
		col, con := s.validPropagateCond(cond, inEqFuncNameMap)
		if col == nil {
			continue
		}
		id := s.getColID(col)
		for j := range s.columns {
			if id != j && s.unionSet.findRoot(id) == s.unionSet.findRoot(j) {
				funName := cond.(*ScalarFunction).FuncName.L
				var newExpr Expression
				if _, ok := cond.(*ScalarFunction).GetArgs()[0].(*Column); ok {
					newExpr, _ = NewFunction(s.ctx, funName, cond.GetType(), s.columns[j], con)
				} else {
					newExpr, _ = NewFunction(s.ctx, funName, cond.GetType(), con, s.columns[j])
				}
				s.conditions = append(s.conditions, newExpr)
			}
		}
	}
}

// propagateEQ propagates equal expression multiple times. An example runs as following:
// a = d & b * 2 = c & c = d + 2 & b = 1 & a = 4, we pick eq cond b = 1 and a = 4
// d = 4 & 2 = c & c = d + 2 & b = 1 & a = 4, we propagate b = 1 and a = 4 and pick eq cond c = 2 and d = 4
// d = 4 & 2 = c & false & b = 1 & a = 4, we propagate c = 2 and d = 4, and do constant folding: c = d + 2 will be folded as false.
func (s *propagateConstantSolver) propagateEQ() {
	s.eqList = make([]*Constant, len(s.columns))
	visited := make([]bool, len(s.conditions))
	for i := 0; i < MaxPropagateColsCnt; i++ {
		mapper := s.pickNewEQConds(visited)
		if mapper == nil || len(mapper) == 0 {
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

// validPropagateCond checks if the cond is an expression like [column op constant] and op is in the funNameMap.
func (s *propagateConstantSolver) validPropagateCond(cond Expression, funNameMap map[string]bool) (*Column, *Constant) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if _, ok := funNameMap[eq.FuncName.L]; !ok {
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

func (s *propagateConstantSolver) setConds2ConstFalse() {
	s.conditions = []Expression{&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}}
}

// pickNewEQConds tries to pick new equal conds and puts them to retMapper.
func (s *propagateConstantSolver) pickNewEQConds(visited []bool) (retMapper map[int]*Constant) {
	retMapper = make(map[int]*Constant)
	for i, cond := range s.conditions {
		if visited[i] {
			continue
		}
		col, con := s.validPropagateCond(cond, eqFuncNameMap)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		ok := false
		if col == nil {
			if con, ok = cond.(*Constant); ok {
				value, _ := EvalBool([]Expression{con}, nil, s.ctx)
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

// tryToUpdateEQList tries to update the eqList. When the eqList has store this column with a different constant, like
// a = 1 and a = 2, we set the second return value to false.
func (s *propagateConstantSolver) tryToUpdateEQList(col *Column, con *Constant) (bool, bool) {
	if con.Value.IsNull() {
		return false, true
	}
	id := s.getColID(col)
	oldCon := s.eqList[id]
	if oldCon != nil {
		return false, !oldCon.Equal(con, s.ctx)
	}
	s.eqList[id] = con
	return true, false
}

func (s *propagateConstantSolver) solve(conditions []Expression) []Expression {
	var cols []*Column
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
	s.propagateEQ()
	s.propagateInEQ()
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

func (s *propagateConstantSolver) getColID(col *Column) int {
	code := col.HashCode()
	return s.colMapper[string(code)]
}

func (s *propagateConstantSolver) insertCol(col *Column) {
	code := col.HashCode()
	_, ok := s.colMapper[string(code)]
	if !ok {
		s.colMapper[string(code)] = len(s.colMapper)
		s.columns = append(s.columns, col)
	}
}

// PropagateConstant propagate constant values of equality predicates and inequality predicates in a condition.
func PropagateConstant(ctx context.Context, conditions []Expression) []Expression {
	solver := &propagateConstantSolver{
		colMapper: make(map[string]int),
		ctx:       ctx,
	}
	return solver.solve(conditions)
}
