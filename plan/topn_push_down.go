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
	return p.pushDownTopN(nil), nil
}

func (s *baseLogicalPlan) pushDownTopN(topN *TopN) LogicalPlan {
	p := s.basePlan.self.(LogicalPlan)
	for i, child := range p.Children() {
		p.Children()[i] = child.(LogicalPlan).pushDownTopN(nil)
		p.Children()[i].SetParents(p)
	}
	if topN != nil {
		return topN.setChild(p, false)
	}
	return p
}

// setChild set p as topn's child. If eliminable is true, this topn plan can be removed.
func (s *TopN) setChild(p LogicalPlan, eliminable bool) LogicalPlan {
	if s.partial && eliminable {
		return p
	}
	if s.isLimit() {
		limit := Limit{
			Count:   s.Count,
			Offset:  s.Offset,
			partial: s.partial,
		}.init(s.allocator, s.ctx)
		setParentAndChildren(limit, p)
		limit.SetSchema(p.Schema().Clone())
		return limit
	}
	// Then s must be topN.
	setParentAndChildren(s, p)
	s.SetSchema(p.Schema().Clone())
	return s
}

func (s *Sort) pushDownTopN(topN *TopN) LogicalPlan {
	if topN == nil {
		return s.baseLogicalPlan.pushDownTopN(nil)
	} else if topN.isLimit() {
		topN.ByItems = s.ByItems
		// If a Limit is pushed down, the Sort should be converted to topN and be pushed again.
		return s.children[0].(LogicalPlan).pushDownTopN(topN)
	}
	// If a TopN is pushed down, this sort is useless.
	return s.children[0].(LogicalPlan).pushDownTopN(topN)
}

func (p *Limit) convertToTopN() *TopN {
	return TopN{Offset: p.Offset, Count: p.Count}.init(p.allocator, p.ctx)
}

func (p *Limit) pushDownTopN(topN *TopN) LogicalPlan {
	child := p.children[0].(LogicalPlan).pushDownTopN(p.convertToTopN())
	if topN != nil {
		return topN.setChild(child, false)
	}
	return child
}

func (p *Union) pushDownTopN(topN *TopN) LogicalPlan {
	for i, child := range p.children {
		var newTopN *TopN
		if topN != nil {
			newTopN = TopN{Count: topN.Count + topN.Offset, partial: true}.init(p.allocator, p.ctx)
			for _, by := range topN.ByItems {
				newExpr := expression.ColumnSubstitute(by.Expr, p.schema, expression.Column2Exprs(child.Schema().Columns))
				newTopN.ByItems = append(newTopN.ByItems, &ByItems{newExpr, by.Desc})
			}
		}
		p.children[i] = child.(LogicalPlan).pushDownTopN(newTopN)
		p.children[i].SetParents(p)
	}
	if topN != nil {
		return topN.setChild(p, true)
	}
	return p
}

func (p *Projection) pushDownTopN(topN *TopN) LogicalPlan {
	if topN != nil {
		for _, by := range topN.ByItems {
			by.Expr = expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs)
		}
	}
	child := p.children[0].(LogicalPlan).pushDownTopN(topN)
	setParentAndChildren(p, child)
	return p
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *TopN, idx int) LogicalPlan {
	var newTopN *TopN
	if topN != nil {
		canPush := true
		for _, by := range topN.ByItems {
			cols := expression.ExtractColumns(by.Expr)
			if len(p.children[1-idx].Schema().ColumnsIndices(cols)) != 0 {
				canPush = false
				break
			}
		}
		if canPush {
			newTopN = TopN{
				Count:   topN.Count + topN.Offset,
				ByItems: make([]*ByItems, len(topN.ByItems)),
				partial: true,
			}.init(topN.allocator, topN.ctx)
			copy(newTopN.ByItems, topN.ByItems)
		}
	}
	return p.children[idx].(LogicalPlan).pushDownTopN(newTopN)
}

func (p *LogicalJoin) pushDownTopN(topN *TopN) LogicalPlan {
	var leftChild, rightChild LogicalPlan
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		leftChild = p.pushDownTopNToChild(topN, 0)
		rightChild = p.children[1].(LogicalPlan).pushDownTopN(nil)
	case RightOuterJoin:
		leftChild = p.children[0].(LogicalPlan).pushDownTopN(nil)
		rightChild = p.pushDownTopNToChild(topN, 1)
	default:
		return p.baseLogicalPlan.pushDownTopN(topN)
	}
	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	self := p.self.(LogicalPlan)
	setParentAndChildren(self, leftChild, rightChild)
	if topN != nil {
		return topN.setChild(self, true)
	}
	return self
}
