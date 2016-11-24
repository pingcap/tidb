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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// MaxPropagateColsCnt means the max number of columns that can participate propagation.
var MaxPropagateColsCnt = 100

var eqFuncNameMap = map[string]bool{
	ast.EQ: true,
}

var inEqFuncNameMap = map[string]bool{
	ast.LT: true,
	ast.GT: true,
	ast.LE: true,
	ast.GE: true,
	ast.NE: true,
}

type propagateConstantSolver struct {
	colMapper       map[string]int
	transitiveGraph [][]bool
	eqList          []*Constant
	columns         []*Column
	conditions      []Expression
	foreverFalse    bool
}

func (s *propagateConstantSolver) propagateInEQ() {
	s.transitiveGraph = make([][]bool, len(s.columns))
	for i := range s.transitiveGraph {
		s.transitiveGraph[i] = make([]bool, len(s.columns))
	}
	for i := 0; i < len(s.conditions); i++ {
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lCol, lOk := fun.Args[0].(*Column)
			rCol, rOk := fun.Args[1].(*Column)
			if lOk && rOk {
				lID := s.getColID(lCol)
				rID := s.getColID(rCol)
				s.transitiveGraph[lID][rID] = true
				s.transitiveGraph[rID][lID] = true
			}
		}
	}
	colLen := len(s.colMapper)
	// floyed algorithm
	for k := 0; k < colLen; k++ {
		for i := 0; i < colLen; i++ {
			for j := 0; j < colLen; j++ {
				if !s.transitiveGraph[i][j] {
					s.transitiveGraph[i][j] = s.transitiveGraph[i][k] && s.transitiveGraph[k][j]
				}
			}
		}
	}
	condsLen := len(s.conditions)
	for i := 0; i < condsLen; i++ {
		cond := s.conditions[i]
		col, con := s.validPropagateCond(cond, inEqFuncNameMap)
		if col != nil {
			id := s.getColID(col)
			for to, connected := range s.transitiveGraph[id] {
				if to != id && connected {
					newFunc, _ := NewFunction(cond.(*ScalarFunction).FuncName.L, cond.GetType(), s.columns[to], con)
					s.conditions = append(s.conditions, newFunc)
				}
			}
		}
	}
}

func (s *propagateConstantSolver) propagateEQ() {
	s.eqList = make([]*Constant, len(s.columns))
	visited := make([]bool, len(s.conditions))
	for i := 0; i < 20; i++ {
		mapper := s.pickEQConds(visited)
		if s.foreverFalse || len(mapper) == 0 {
			return
		}
		cols := make(Schema, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			cols = append(cols, s.columns[id])
			cons = append(cons, con)
		}
		for i, cond := range s.conditions {
			if !visited[i] {
				s.conditions[i] = ColumnSubstitute(cond, Schema(cols), cons)
			}
		}
	}
}

func (s *propagateConstantSolver) validPropagateCond(cond Expression, funNameMap map[string]bool) (*Column, *Constant) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if _, ok := funNameMap[eq.FuncName.L]; !ok {
			return nil, nil
		}
		if col, colOk := eq.Args[0].(*Column); colOk {
			if con, conOk := eq.Args[1].(*Constant); conOk {
				return col, con
			}
		}
		if col, colOk := eq.Args[1].(*Column); colOk {
			if con, conOk := eq.Args[0].(*Constant); conOk {
				return col, con
			}
		}
	}
	return nil, nil
}

func (s *propagateConstantSolver) pickEQConds(visited []bool) (retMapper map[int]*Constant) {
	retMapper = make(map[int]*Constant)
	for i, cond := range s.conditions {
		if !visited[i] {
			col, con := s.validPropagateCond(cond, eqFuncNameMap)
			if col != nil {
				visited[i] = true
				if s.pickEQCond(col, con) {
					retMapper[s.getColID(col)] = con
				} else if s.foreverFalse {
					return
				}
			}
		}
	}
	return
}

func (s *propagateConstantSolver) pickEQCond(col *Column, con *Constant) bool {
	if con.Value.IsNull() {
		s.foreverFalse = true
		s.conditions = []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
		return false
	}
	id := s.getColID(col)
	oldCon := s.eqList[id]
	if oldCon != nil {
		log.Warnf("old %s new %s", oldCon, con)
		if !oldCon.Equal(con) {
			s.foreverFalse = true
			s.conditions = []Expression{&Constant{
				Value:   types.NewDatum(false),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}}
		}
		return false
	}
	s.eqList[id] = con
	return true
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
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.OrOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(PropagateConstant([]Expression{item}))
			}
			s.conditions[i] = ComposeDNFCondition(dnfItems)
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
func PropagateConstant(conditions []Expression) []Expression {
	solver := &propagateConstantSolver{
		colMapper: make(map[string]int),
	}
	return solver.solve(conditions)
}
