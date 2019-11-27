// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disjointset"
)

type hashPartitionPruner struct {
	unionSet    *disjointset.IntSet // unionSet stores the relations like col_i = col_j
	constantMap []*Constant
	conditions  []Expression
	colMapper   map[int64]int
	numColumn   int
	ctx         sessionctx.Context
}

func (p *hashPartitionPruner) getColID(col *Column) int {
	return p.colMapper[col.UniqueID]
}

func (p *hashPartitionPruner) insertCol(col *Column) {
	_, ok := p.colMapper[col.UniqueID]
	if !ok {
		p.numColumn += 1
		p.colMapper[col.UniqueID] = len(p.colMapper)
	}
}

func (p *hashPartitionPruner) reduceColumnEQ() bool {
	p.unionSet = disjointset.NewIntSet(p.numColumn)
	for i := range p.conditions {
		if fun, ok := p.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lCol, lOk := fun.GetArgs()[0].(*Column)
			rCol, rOk := fun.GetArgs()[1].(*Column)
			if lOk && rOk {
				lID := p.getColID(lCol)
				rID := p.getColID(rCol)
				p.unionSet.Union(lID, rID)
			}
		}
	}
	for i := 0; i < p.numColumn; i++ {
		father := p.unionSet.FindRoot(i)
		if p.constantMap[i] != nil {
			if p.constantMap[father] != nil {
				// May has conflict here.
				if !p.constantMap[father].Equal(p.ctx, p.constantMap[i]) {
					return false
				}
			} else {
				p.constantMap[father] = p.constantMap[i]
			}
		}
	}
	for i := 0; i < p.numColumn; i++ {
		father := p.unionSet.FindRoot(i)
		if p.constantMap[father] != nil && p.constantMap[i] == nil {
			p.constantMap[i] = p.constantMap[father]
		}
	}
	return true
}

func (p *hashPartitionPruner) reduceConstantEQ() {
	for _, con := range p.conditions {
		col, cond := validEqualCond(con)
		if col != nil {
			id := p.getColID(col)
			p.constantMap[id] = cond
		}
	}
}

func (p *hashPartitionPruner) tryEvalPartitionExpr(piExpr Expression) (int64, bool) {
	switch pi := piExpr.(type) {
	case *ScalarFunction:
		if pi.FuncName.L == ast.Plus || pi.FuncName.L == ast.Minus || pi.FuncName.L == ast.Mul || pi.FuncName.L == ast.Div {
			left, right := pi.GetArgs()[0], pi.GetArgs()[1]
			leftVal, ok := p.tryEvalPartitionExpr(left)
			if !ok {
				return 0, false
			}
			rightVal, ok := p.tryEvalPartitionExpr(right)
			if !ok {
				return 0, false
			}
			switch pi.FuncName.L {
			case ast.Plus:
				return rightVal + leftVal, true
			case ast.Minus:
				return rightVal - leftVal, true
			case ast.Mul:
				return rightVal * leftVal, true
			case ast.Div:
				return rightVal / leftVal, true
			}
		}
	case *Constant:
		val, err := pi.Eval(chunk.Row{})
		if err != nil {
			return 0, false
		}
		if val.Kind() == types.KindInt64 {
			return val.GetInt64(), true
		} else if val.Kind() == types.KindUint64 {
			return int64(val.GetUint64()), true
		}
	case *Column:
		// Look up map
		idx := p.getColID(pi)
		val := p.constantMap[idx]
		if val != nil {
			return p.tryEvalPartitionExpr(val)
		}
		return 0, false
	}
	return 0, false
}

func newHashPartitionPruner() *hashPartitionPruner {
	pruner := &hashPartitionPruner{}
	pruner.colMapper = make(map[int64]int)
	pruner.numColumn = 0
	return pruner
}

func (p *hashPartitionPruner) solve(ctx sessionctx.Context, conds []Expression, piExpr Expression) (int64, bool) {
	p.ctx = ctx
	cols := make([]*Column, 0, len(conds))
	for _, cond := range conds {
		p.conditions = append(p.conditions, SplitCNFItems(cond)...)
		cols = append(cols, ExtractColumns(cond)...)
	}
	cols = append(cols, ExtractColumns(piExpr)...)
	for _, col := range cols {
		p.insertCol(col)
	}
	p.constantMap = make([]*Constant, p.numColumn, p.numColumn)
	p.reduceConstantEQ()
	ok := p.reduceColumnEQ()
	if !ok {
		return 0, false
	}
	return p.tryEvalPartitionExpr(piExpr)
}

func FastLocateHashPartition2(ctx sessionctx.Context, conds []Expression, piExpr Expression) (int64, bool) {
	return newHashPartitionPruner().solve(ctx, conds, piExpr)
}
