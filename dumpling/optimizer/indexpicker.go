// Copyright 2015 PingCAP, Inc.
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

package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/optimizer/plan"
)

// indexPicker is the core of optimization
// for every Filter plan, try to use every suitable index,
// and calculate the cost, finally choose the cheapest one.
type indexPicker struct {
	is   infoschema.InfoSchema
	root plan.Plan
	// when changing index plan, the ordering will change,
	// we store the sort plan here to change the Bypass property.
	sort *plan.Sort
}

// Enter implements plan Visitor interface.
func (p *indexPicker) Enter(in plan.Plan) (plan.Plan, bool) {
	switch x := in.(type) {
	case *plan.Sort:
		p.sort = x
	}
	return in, false
}

// Leave implements plan Visitor interface.
func (p *indexPicker) Leave(in plan.Plan) (plan.Plan, bool) {
	switch x := in.(type) {
	case *plan.Filter:
		p.pickIndex(x)
	}
	return in, true
}

func (p *indexPicker) pickIndex(f *plan.Filter) {
	lowestCost := calculateCost(p.root)
	var bestIndexPlan *plan.IndexScan
	var bestIndexInfo *model.IndexInfo
	for _, v := range f.Conditions {
		var finder indexFinder
		v.Accept(&finder)
		if finder.invalid || finder.index == nil {
			continue
		}
		idxPlan := buildIndexPlan(finder.index, v)
		originalPlan := swapIndexPlan(idxPlan, f.Src)
		setBypass(finder.index, p.sort)
		currentCost := calculateCost(p.root)
		if currentCost < lowestCost {
			lowestCost = currentCost
			bestIndexPlan = idxPlan
			bestIndexInfo = finder.index
		}
		swapPlan(idxPlan, originalPlan, f.Src)
	}
	swapIndexPlan(bestIndexPlan, f.Src)
	setBypass(bestIndexInfo, p.sort)
}

// indexedColumnFinder is an ast Visitor that finds a suitable
// index in an expression.
type indexFinder struct {
	is      infoschema.InfoSchema
	invalid bool
	index   *model.IndexInfo
	field   *ast.ResultField
}

// Enter implements ast Visitor interface.
func (p *indexFinder) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

// Leave implements ast Visitor interface.
func (p *indexFinder) Leave(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.ColumnNameExpr:
		if p.field == nil {
			p.findIndex(x.Refer.Column)
			p.field = x.Refer
		} else if p.field != x.Refer {
			// The condition contains more than one column, not
			// suitable.
			p.invalid = true
		}
	}
	return in, true
}

func (p *indexFinder) findIndex(column *model.ColumnInfo) {
	indices, ok := p.is.ColumnIndicesByID(column.ID)
	if !ok {
		return
	}
	for _, v := range indices {
		if v.Columns[0].Name.L == column.Name.L {
			p.index = v
		}
	}
}

func buildIndexPlan(index *model.IndexInfo, condition ast.ExprNode) *plan.IndexScan {
	return nil
}

func swapIndexPlan(index *plan.IndexScan, outer plan.Plan) (tablePlan plan.Plan) {
	return nil
}

func swapPlan(p plan.Plan, with plan.Plan, outer plan.Plan) {

}

func setBypass(index *model.IndexInfo, sort *plan.Sort) {

}

func calculateCost(p plan.Plan) float64 {
	return 0
}
