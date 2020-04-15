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
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/disjointset"
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
					return true
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
	return false
}

func (p *hashPartitionPruner) reduceConstantEQ() bool {
	for _, con := range p.conditions {
		col, cond := validEqualCond(p.ctx, con)
		if col != nil {
			id := p.getColID(col)
			if p.constantMap[id] != nil {
				if p.constantMap[id].Equal(p.ctx, cond) {
					continue
				}
				return true
			}
			p.constantMap[id] = cond
		}
	}
	return false
}

func (p *hashPartitionPruner) tryEvalPartitionExpr(piExpr Expression) (val int64, success bool, isNil bool) {
	switch pi := piExpr.(type) {
	case *ScalarFunction:
		if pi.FuncName.L == ast.Plus || pi.FuncName.L == ast.Minus || pi.FuncName.L == ast.Mul || pi.FuncName.L == ast.Div {
			left, right := pi.GetArgs()[0], pi.GetArgs()[1]
			leftVal, ok, isNil := p.tryEvalPartitionExpr(left)
			if !ok {
				return 0, ok, isNil
			}
			rightVal, ok, isNil := p.tryEvalPartitionExpr(right)
			if !ok {
				return 0, ok, isNil
			}
			switch pi.FuncName.L {
			case ast.Plus:
				return rightVal + leftVal, true, false
			case ast.Minus:
				return rightVal - leftVal, true, false
			case ast.Mul:
				return rightVal * leftVal, true, false
			case ast.Div:
				return rightVal / leftVal, true, false
			}
		} else if pi.FuncName.L == ast.Year || pi.FuncName.L == ast.Month || pi.FuncName.L == ast.ToDays {
			col := pi.GetArgs()[0].(*Column)
			idx := p.getColID(col)
			val := p.constantMap[idx]
			if val != nil {
				pi.GetArgs()[0] = val
				ret, _, err := pi.EvalInt(p.ctx, chunk.Row{})
				if err != nil {
					return 0, false, false
				}
				return ret, true, false
			}
			return 0, false, false
		}
	case *Constant:
		val, err := pi.Eval(chunk.Row{})
		if err != nil {
			return 0, false, false
		}
		if val.IsNull() {
			return 0, false, true
		}
		if val.Kind() == types.KindInt64 {
			return val.GetInt64(), true, false
		} else if val.Kind() == types.KindUint64 {
			return int64(val.GetUint64()), true, false
		}
	case *Column:
		// Look up map
		idx := p.getColID(pi)
		val := p.constantMap[idx]
		if val != nil {
			return p.tryEvalPartitionExpr(val)
		}
		return 0, false, false
	}
	return 0, false, false
}

func newHashPartitionPruner() *hashPartitionPruner {
	pruner := &hashPartitionPruner{}
	pruner.colMapper = make(map[int64]int)
	pruner.numColumn = 0
	return pruner
}

// solve eval the hash partition expression, the first return value represent the result of partition expression. The second
// return value is whether eval success. The third return value represent whether the eval result of partition value is null.
func (p *hashPartitionPruner) solve(ctx sessionctx.Context, conds []Expression, piExpr Expression) (val int64, ok bool, isNil bool) {
	p.ctx = ctx
	for _, cond := range conds {
		p.conditions = append(p.conditions, SplitCNFItems(cond)...)
		for _, col := range ExtractColumns(cond) {
			p.insertCol(col)
		}
	}
	for _, col := range ExtractColumns(piExpr) {
		p.insertCol(col)
	}
	p.constantMap = make([]*Constant, p.numColumn)
	conflict := p.reduceConstantEQ()
	if conflict {
		return 0, false, conflict
	}
	conflict = p.reduceColumnEQ()
	if conflict {
		return 0, false, conflict
	}
	res, ok, isNil := p.tryEvalPartitionExpr(piExpr)
	return res, ok, isNil
}

// FastLocateHashPartition is used to get hash partition quickly.
func FastLocateHashPartition(ctx sessionctx.Context, conds []Expression, piExpr Expression) (int64, bool, bool) {
	return newHashPartitionPruner().solve(ctx, conds, piExpr)
}
