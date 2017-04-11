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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

// pushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type pushDownTopNOptimizer struct {
}

func (s *pushDownTopNOptimizer) optimize(p LogicalPlan, ctx context.Context, allocator *idAllocator) (LogicalPlan, error) {
	return p.pushDownTopN(Sort{}.init(allocator, ctx)), nil
}

func (s *baseLogicalPlan) pushDownTopN(topN *Sort) LogicalPlan {
	p := s.basePlan.self.(LogicalPlan)
	for i, child := range p.Children() {
		p.Children()[i] = child.(LogicalPlan).pushDownTopN(Sort{}.init(topN.allocator, topN.ctx))
		p.Children()[i].SetParents(p)
	}
	return topN.setChild(p)
}

func (s *Sort) isEmpty() bool {
	return s.ExecLimit == nil && len(s.ByItems) == 0
}

func (s *Sort) isLimit() bool {
	return len(s.ByItems) == 0 && s.ExecLimit != nil
}

func (s *Sort) isTopN() bool {
	return len(s.ByItems) != 0 && s.ExecLimit != nil
}

func (s *Sort) setChild(p LogicalPlan) LogicalPlan {
	if s.isEmpty() {
		return p
	} else if s.isLimit() {
		limit := Limit{Count: s.ExecLimit.Count, Offset: s.ExecLimit.Offset}.init(s.allocator, s.ctx)
		limit.SetChildren(p)
		p.SetParents(limit)
		limit.SetSchema(p.Schema().Clone())
		return limit
	}
	// Then s must be topN.
	s.SetChildren(p)
	p.SetParents(s)
	s.SetSchema(p.Schema().Clone())
	return s
}

func (s *Sort) pushDownTopN(topN *Sort) LogicalPlan {
	if topN.isLimit() {
		s.ExecLimit = topN.ExecLimit
		// If a Limit is pushed down, the Sort should be converted to topN and be pushed again.
		return s.children[0].(LogicalPlan).pushDownTopN(s)
	} else if topN.isEmpty() {
		// If nothing is pushed down, just continue to push nothing to its child.
		return s.baseLogicalPlan.pushDownTopN(topN)
	}
	// If a TopN is pushed down, this sort is useless.
	return s.children[0].(LogicalPlan).pushDownTopN(topN)
}

func (p *Limit) pushDownTopN(topN *Sort) LogicalPlan {
	child := p.children[0].(LogicalPlan).pushDownTopN(Sort{ExecLimit: p}.init(p.allocator, p.ctx))
	return topN.setChild(child)
}

func (p *Union) pushDownTopN(topN *Sort) LogicalPlan {
	for i, child := range p.children {
		newTopN := Sort{}.init(p.allocator, p.ctx)
		for _, by := range topN.ByItems {
			newExpr := expression.ColumnSubstitute(by.Expr, p.schema, expression.Column2Exprs(child.Schema().Columns))
			newTopN.ByItems = append(newTopN.ByItems, &ByItems{newExpr, by.Desc})
		}
		if !topN.isEmpty() {
			newTopN.ExecLimit = &Limit{Count: topN.ExecLimit.Count + topN.ExecLimit.Offset}
		}
		p.children[i] = child.(LogicalPlan).pushDownTopN(newTopN)
		p.children[i].SetParents(p)
	}
	return topN.setChild(p)
}

func (p *Projection) pushDownTopN(topN *Sort) LogicalPlan {
	for _, by := range topN.ByItems {
		by.Expr = expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs)
	}
	child := p.children[0].(LogicalPlan).pushDownTopN(topN)
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

func (p *LogicalJoin) pushDownTopNToChild(topN *Sort, idx int) LogicalPlan {
	canPush := true
	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		if len(p.children[1-idx].Schema().ColumnsIndices(cols)) != 0 {
			canPush = false
			break
		}
	}
	newTopN := Sort{}.init(topN.allocator, topN.ctx)
	if canPush {
		if !topN.isEmpty() {
			newTopN.ExecLimit = &Limit{Count: topN.ExecLimit.Count + topN.ExecLimit.Offset}
		}
		newTopN.ByItems = make([]*ByItems, len(topN.ByItems))
		copy(newTopN.ByItems, topN.ByItems)
	}
	return p.children[idx].(LogicalPlan).pushDownTopN(newTopN)
}

func (p *LogicalJoin) pushDownTopN(topN *Sort) LogicalPlan {
	var leftChild, rightChild LogicalPlan
	emptySort := Sort{}.init(p.allocator, p.ctx)
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin:
		leftChild = p.pushDownTopNToChild(topN, 0)
		rightChild = p.children[1].(LogicalPlan).pushDownTopN(emptySort)
	case RightOuterJoin:
		leftChild = p.children[0].(LogicalPlan).pushDownTopN(emptySort)
		rightChild = p.pushDownTopNToChild(topN, 1)
	default:
		return p.baseLogicalPlan.pushDownTopN(topN)
	}
	p.SetChildren(leftChild, rightChild)
	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	self := p.self.(LogicalPlan)
	leftChild.SetParents(self)
	rightChild.SetParents(self)
	return topN.setChild(self)
}
