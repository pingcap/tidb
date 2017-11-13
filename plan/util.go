// Copyright 2017 PingCAP, Inc.
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

package plan

import (
	"github.com/pingcap/tidb/ast"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

// replaceChild replaces p's child with some plan else.
func replaceChild(p, child, replace Plan) {
	for i, ch := range p.Children() {
		if ch.ID() == child.ID() {
			p.Children()[i] = replace
		}
	}
}

// setParentAndChildren sets parent and children relationship.
func setParentAndChildren(parent Plan, children ...Plan) {
	if children == nil || parent == nil {
		return
	}
	for _, child := range children {
		child.SetParents(parent)
	}
	parent.SetChildren(children...)
}

// removePlan removes a plan from its parent and child.
func removePlan(p Plan) {
	parents := p.Parents()
	children := p.Children()
	if len(parents) == 0 {
		child := children[0]
		child.SetParents()
		return
	}
	parent, child := parents[0], children[0]
	replaceChild(parent, p, child)
	child.SetParents(parent)
}
