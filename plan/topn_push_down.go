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

func (s *pushDownTopNOptimizer) optimize(p LogicalPlan, ctx context.Context) (LogicalPlan, error) {
	return p.pushDownTopN(nil), nil
}

func (s *baseLogicalPlan) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		p.Children()[i] = child.(LogicalPlan).pushDownTopN(nil)
	}
	if topN != nil {
		return topN.setChild(p, false)
	}
	return p
}

// setChild set p as topn's child. If eliminable is true, this topn plan can be removed.
func (s *LogicalTopN) setChild(p LogicalPlan, eliminable bool) LogicalPlan {
	if s.partial && eliminable {
		return p
	}
	if s.isLimit() {
		limit := LogicalLimit{
			Count:   s.Count,
			Offset:  s.Offset,
			partial: s.partial,
		}.init(s.ctx)
		limit.SetChildren(p)
		limit.SetSchema(p.Schema().Clone())
		return limit
	}
	// Then s must be topN.
	s.SetChildren(p)
	s.SetSchema(p.Schema().Clone())
	return s
}

func (s *LogicalSort) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	if topN == nil {
		return s.baseLogicalPlan.pushDownTopN(nil)
	} else if topN.isLimit() {
		topN.ByItems = s.ByItems
		// If a Limit is pushed down, the LogicalSort should be converted to topN and be pushed again.
		return s.children[0].(LogicalPlan).pushDownTopN(topN)
	}
	// If a TopN is pushed down, this sort is useless.
	return s.children[0].(LogicalPlan).pushDownTopN(topN)
}

func (p *LogicalLimit) convertToTopN() *LogicalTopN {
	return LogicalTopN{Offset: p.Offset, Count: p.Count}.init(p.ctx)
}

func (p *LogicalLimit) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	child := p.children[0].(LogicalPlan).pushDownTopN(p.convertToTopN())
	if topN != nil {
		return topN.setChild(child, false)
	}
	return child
}

func (p *LogicalUnionAll) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	for i, child := range p.children {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, partial: true}.init(p.ctx)
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &ByItems{by.Expr.Clone(), by.Desc})
			}
		}
		p.children[i] = child.(LogicalPlan).pushDownTopN(newTopN)
	}
	if topN != nil {
		return topN.setChild(p, true)
	}
	return p
}

func (p *LogicalProjection) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	if topN != nil {
		for _, by := range topN.ByItems {
			by.Expr = expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs)
		}
	}
	p.children[0] = p.children[0].(LogicalPlan).pushDownTopN(topN)
	return p
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) LogicalPlan {
	var newTopN *LogicalTopN
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
			newTopN = LogicalTopN{
				Count:   topN.Count + topN.Offset,
				ByItems: make([]*ByItems, len(topN.ByItems)),
				partial: true,
			}.init(topN.ctx)
			// The old topN should be maintained upon Join,
			// so we clone TopN here and push down a newTopN.
			for i := range topN.ByItems {
				newTopN.ByItems[i] = topN.ByItems[i].Clone()
			}
		}
	}
	return p.children[idx].(LogicalPlan).pushDownTopN(newTopN)
}

func (p *LogicalJoin) pushDownTopN(topN *LogicalTopN) LogicalPlan {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		p.children[0] = p.pushDownTopNToChild(topN, 0)
		p.children[1] = p.children[1].(LogicalPlan).pushDownTopN(nil)
	case RightOuterJoin:
		p.children[0] = p.children[0].(LogicalPlan).pushDownTopN(nil)
		p.children[1] = p.pushDownTopNToChild(topN, 1)
	default:
		return p.baseLogicalPlan.pushDownTopN(topN)
	}
	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	self := p.self.(LogicalPlan)
	if topN != nil {
		return topN.setChild(self, true)
	}
	return self
}
