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

// indexPicker is the core of optimization.
// for every Filter plan, try to use every applicable index,
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

// pickIndex picks index from conditions in a Filter plan,
// it modifies the plan tree to use the cheapest index plan.
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
	if bestIndexPlan != nil {
		swapIndexPlan(bestIndexPlan, f.Src)
		setBypass(bestIndexInfo, p.sort)
	}
}

// indexedColumnFinder is an ast Visitor that finds a applicable
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
			p.findApplicableIndex(x.Refer.Column)
			p.field = x.Refer
		} else if p.field != x.Refer {
			// The condition contains more than one column, not applicable.
			p.invalid = true
		}
	case *ast.SubqueryExpr:
		p.invalid = true
	case ast.AggregateFuncExpr:
		p.invalid = true
	}
	return in, true
}

// findApplicableIndex finds indices that contain the column at first.
// when their are more than one index applicable, we chose the one with
// more columns because it is more selective.
func (p *indexFinder) findApplicableIndex(column *model.ColumnInfo) {
	indices, ok := p.is.ColumnIndicesByID(column.ID)
	if !ok {
		return
	}
	for _, v := range indices {
		if v.Columns[0].Name.L == column.Name.L {
			if p.index == nil {
				p.index = v
			} else if len(v.Columns) > len(p.index.Columns) {
				p.index = v
			}
		}
	}
}

// buildIndexPlan builds an index plan from index infa and a condition expression.
// the condition only contains the index column and constant expressions which is insured
// by indexFinder. the condition is split from an CNF, so it is in the form of list of
// OR expressions and no AND expressions can be found.
func buildIndexPlan(index *model.IndexInfo, condition ast.ExprNode) *plan.IndexScan {
	return nil
}

// swapIndexPlan swaps an old table plan to an index plan and returns the oldPlan.
func swapIndexPlan(index *plan.IndexScan, root plan.Plan) (oldPlan plan.Plan) {
	return nil
}

// swapPlan swaps old plan with new plan in root plan.
func swapPlan(old plan.Plan, new plan.Plan, root plan.Plan) {

}

// setBypass sets Bypass property for sort plan based on the index ordering.
// If the index order is the same as the sort order, sort process should be bypassed.
func setBypass(index *model.IndexInfo, sort *plan.Sort) {

}

// calculateCost calculates the cost of the plan.
func calculateCost(p plan.Plan) float64 {
	return 0
}
