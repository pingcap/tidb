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
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disjointset"
)

type hashPartitionPruner struct {
	unionSet    *disjointset.IntSet // unionSet stores the relations like col_i = col_j
	constantMap []vector
	conditions  []Expression
	colMapper   map[int64]int
	numColumn   int
	ctx         sessionctx.Context
}

type vector []*Constant

func Equal(a, b vector, ctx sessionctx.Context) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if !x.Equal(ctx, b[i]) {
			return false
		}
	}
	return true
}

func Intersect(a, b vector, ctx sessionctx.Context) []*Constant {
	ret := make(vector, 0, len(a))
	for _, c := range a {
		for _, cb := range b {
			if c.Equal(ctx, cb) {
				ret = append(ret, c)
			}
		}
	}
	return ret
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
				// if !p.constantMap[father].Equal(p.ctx, p.constantMap[i]) {
				// 	return true
				// }
				// if !Equal(p.constantMap[father], p.constantMap[i], p.ctx) {
				//	return true
				// }
				p.constantMap[father] = Intersect(p.constantMap[father], p.constantMap[i], p.ctx)
				if len(p.constantMap[father]) == 0 {
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
			tmp := []*Constant{cond}
			if p.constantMap[id] != nil {
				// if p.constantMap[id].Equal(p.ctx, cond) {
				//	 continue
				// }
				tmp = Intersect(p.constantMap[id], tmp, p.ctx)
				if len(tmp) != 0 {
					p.constantMap[id] = tmp
					continue
				}
				return true
			}
			p.constantMap[id] = tmp
		} else {
			if in, ok := con.(*ScalarFunction); ok {
				fmt.Println(in)
				if in.FuncName.L == ast.In {
					tmp := make([]*Constant, 0, len(in.GetArgs()))
					col := in.GetArgs()[0].(*Column)
					id := p.getColID(col)
					flag := true
					for i := 1; i < len(in.GetArgs()); i++ {
						exp := in.GetArgs()[i]
						if cons, ok := exp.(*Constant); ok {
							tmp = append(tmp, cons)
						} else {
							flag = false
						}
					}
					if flag {
						if p.constantMap[id] != nil {
							tmp = Intersect(tmp, p.constantMap[id], p.ctx)
							if len(tmp) == 0 {
								p.constantMap[id] = nil
								return true
							}
						}
						p.constantMap[id] = tmp
					}
				}
			}
		}
	}
	return false
}

func calc(a, b []int64, op string) []int64 {
	ret := make([]int64, 0, len(a)*len(b))
	switch op {
	case ast.Plus:
		for _, x := range a {
			for _, y := range b {
				ret = append(ret, x + y)
			}
		}
	case ast.Minus:
		for _, x := range a {
			for _, y := range b {
				ret = append(ret, x - y)
			}
		}
	case ast.Mul:
		for _, x := range a {
			for _, y := range b {
				ret = append(ret, x * y)
			}
		}
	case ast.Div:
		for _, x := range a {
			for _, y := range b {
				ret = append(ret, x / y)
			}
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

func (p *hashPartitionPruner) tryEvalPartitionExpr(piExpr Expression) (val []int64, success bool, isNil bool) {
	switch pi := piExpr.(type) {
	case *ScalarFunction:
		if pi.FuncName.L == ast.Plus || pi.FuncName.L == ast.Minus || pi.FuncName.L == ast.Mul || pi.FuncName.L == ast.Div {
			left, right := pi.GetArgs()[0], pi.GetArgs()[1]
			leftVal, ok, isNil := p.tryEvalPartitionExpr(left)
			if !ok {
				return nil, ok, isNil
			}
			rightVal, ok, isNil := p.tryEvalPartitionExpr(right)
			if !ok {
				return nil, ok, isNil
			}
			return calc(rightVal, leftVal, pi.FuncName.L), true, false
		} else if pi.FuncName.L == ast.Year || pi.FuncName.L == ast.Month || pi.FuncName.L == ast.ToDays {
			col := pi.GetArgs()[0].(*Column)
			idx := p.getColID(col)
			val := p.constantMap[idx]
			if val != nil {
				ret := make([]int64, 0, len(val))
				for _, ele := range val {
					pi.GetArgs()[0] = ele
					retVal, _, err := pi.EvalInt(p.ctx, chunk.Row{})
					if err != nil {
						return nil, false, false
					}
					ret = append(ret, retVal)
				}
				if len(ret) == 0 {
					return nil, false, false
				}
				return ret, true, false
			}
			return nil, false, false
		}
	case *Constant:
		val, err := pi.Eval(chunk.Row{})
		if err != nil {
			return nil, false, false
		}
		if val.IsNull() {
			return nil, false, true
		}
		if val.Kind() == types.KindInt64 {
			return []int64{val.GetInt64()}, true, false
		} else if val.Kind() == types.KindUint64 {
			return []int64{int64(val.GetUint64())}, true, false
		}
		return nil, false, true
	case *Column:
		// Look up map
		idx := p.getColID(pi)
		val := p.constantMap[idx]
		if val != nil {
			// return p.tryEvalPartitionExpr(val)
			ret := make([]int64, 0, len(val))
			for _, ele := range val {
				v, succ, _ := evalConstantToInt(ele)
				if !succ {
					return nil, false, false
				}
				ret = append(ret, v)
			}
			return ret, true, false
		}
		return nil, false, false
	}
	return nil, false, false
}

func evalConstantToInt(pi *Constant) (ret int64, success bool, isNil bool)  {
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
	return 0, false, true
}

func newHashPartitionPruner() *hashPartitionPruner {
	pruner := &hashPartitionPruner{}
	pruner.colMapper = make(map[int64]int)
	pruner.numColumn = 0
	return pruner
}

// solve eval the hash partition expression, the first return value represent the result of partition expression. The second
// return value is whether eval success. The third return value represent whether the eval result of partition value is null.
func (p *hashPartitionPruner) solve(ctx sessionctx.Context, conds []Expression, piExpr Expression) (val []int64, ok bool, isNil bool) {
	p.ctx = ctx
	for _, cond := range conds {
		p.conditions = append(p.conditions, SplitCNFItems(cond)...)
		fmt.Println(cond)
		cols := ExtractColumns(cond)
		for _, col := range cols {
			fmt.Println(col)
			p.insertCol(col)
		}
	}
	p.constantMap = make([][]*Constant, p.numColumn)
	conflict := p.reduceConstantEQ()
	if conflict {
		return nil, false, conflict
	}
	conflict = p.reduceColumnEQ()
	if conflict {
		return nil, false, conflict
	}
	res, ok, isNil := p.tryEvalPartitionExpr(piExpr)
	fmt.Println(res)
	return res, ok, isNil
}

func intersect(a, b []int64) []int64 {
	ret := make([]int64, 0, len(a))
	for _, ea := range a {
		for _, eb := range b {
			if  ea == eb {
				ret = append(ret, ea)
				break
			}
		}
	}
	return ret
}

// FastLocateHashPartition is used to get hash partition quickly.
func FastLocateHashPartition(ctx sessionctx.Context, conds []Expression, piExpr Expression) ([]int64, bool, bool) {
	return newHashPartitionPruner().solve(ctx, conds, piExpr)
}
