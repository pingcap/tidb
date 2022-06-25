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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/util/mathutil"
)

// pushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type pushDownTopNOptimizer struct {
}

func (s *pushDownTopNOptimizer) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return p.pushDownTopN(nil, opt), nil
}

func (s *baseLogicalPlan) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		p.Children()[i] = child.pushDownTopN(nil, opt)
	}
	if topN != nil {
		return topN.setChild(p, opt)
	}
	return p
}

// setChild set p as topn's child.
func (lt *LogicalTopN) setChild(p LogicalPlan, opt *logicalOptimizeOp) LogicalPlan {
	// Remove this TopN if its child is a TableDual.
	dual, isDual := p.(*LogicalTableDual)
	if isDual {
		numDualRows := uint64(dual.RowCount)
		if numDualRows < lt.Offset {
			dual.RowCount = 0
			return dual
		}
		dual.RowCount = int(mathutil.Min(numDualRows-lt.Offset, lt.Count))
		return dual
	}

	if lt.isLimit() {
		limit := LogicalLimit{
			Count:      lt.Count,
			Offset:     lt.Offset,
			limitHints: lt.limitHints,
		}.Init(lt.ctx, lt.blockOffset)
		limit.SetChildren(p)
		appendTopNPushDownTraceStep(limit, p, opt)
		return limit
	}
	// Then lt must be topN.
	lt.SetChildren(p)
	appendTopNPushDownTraceStep(lt, p, opt)
	return lt
}

func (ls *LogicalSort) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	if topN == nil {
		return ls.baseLogicalPlan.pushDownTopN(nil, opt)
	} else if topN.isLimit() {
		topN.ByItems = ls.ByItems
		appendSortPassByItemsTraceStep(ls, topN, opt)
		return ls.children[0].pushDownTopN(topN, opt)
	}
	// If a TopN is pushed down, this sort is useless.
	return ls.children[0].pushDownTopN(topN, opt)
}

func (p *LogicalLimit) convertToTopN(opt *logicalOptimizeOp) *LogicalTopN {
	topn := LogicalTopN{Offset: p.Offset, Count: p.Count, limitHints: p.limitHints}.Init(p.ctx, p.blockOffset)
	appendConvertTopNTraceStep(p, topn, opt)
	return topn
}

func (p *LogicalLimit) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	child := p.children[0].pushDownTopN(p.convertToTopN(opt), opt)
	if topN != nil {
		return topN.setChild(child, opt)
	}
	return child
}

func (p *LogicalUnionAll) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	for i, child := range p.children {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, limitHints: topN.limitHints}.Init(p.ctx, topN.blockOffset)
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &util.ByItems{Expr: by.Expr, Desc: by.Desc})
			}
			// newTopN to push down Union's child
			appendNewTopNTraceStep(topN, p, opt)
		}
		p.children[i] = child.pushDownTopN(newTopN, opt)
	}
	if topN != nil {
		return topN.setChild(p, opt)
	}
	return p
}

func (p *LogicalProjection) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return p.baseLogicalPlan.pushDownTopN(topN, opt)
		}
	}
	if topN != nil {
		for _, by := range topN.ByItems {
			by.Expr = expression.FoldConstant(expression.ColumnSubstitute(by.Expr, p.schema, p.Exprs))
		}

		// remove meaningless constant sort items.
		for i := len(topN.ByItems) - 1; i >= 0; i-- {
			switch topN.ByItems[i].Expr.(type) {
			case *expression.Constant, *expression.CorrelatedColumn:
				topN.ByItems = append(topN.ByItems[:i], topN.ByItems[i+1:]...)
			}
		}
	}
	p.children[0] = p.children[0].pushDownTopN(topN, opt)
	return p
}

func (p *LogicalLock) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	if topN != nil {
		p.children[0] = p.children[0].pushDownTopN(topN, opt)
	}
	return p.self
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int, opt *logicalOptimizeOp) LogicalPlan {
	if topN == nil {
		return p.children[idx].pushDownTopN(nil, opt)
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.children[idx].Schema().Contains(col) {
				return p.children[idx].pushDownTopN(nil, opt)
			}
		}
	}

	newTopN := LogicalTopN{
		Count:      topN.Count + topN.Offset,
		ByItems:    make([]*util.ByItems, len(topN.ByItems)),
		limitHints: topN.limitHints,
	}.Init(topN.ctx, topN.blockOffset)
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	appendTopNPushDownJoinTraceStep(p, newTopN, idx, opt)
	return p.children[idx].pushDownTopN(newTopN, opt)
}

func (p *LogicalJoin) pushDownTopN(topN *LogicalTopN, opt *logicalOptimizeOp) LogicalPlan {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		p.children[0] = p.pushDownTopNToChild(topN, 0, opt)
		p.children[1] = p.children[1].pushDownTopN(nil, opt)
	case RightOuterJoin:
		p.children[1] = p.pushDownTopNToChild(topN, 1, opt)
		p.children[0] = p.children[0].pushDownTopN(nil, opt)
	default:
		return p.baseLogicalPlan.pushDownTopN(topN, opt)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		return topN.setChild(p.self, opt)
	}
	return p.self
}

func (*pushDownTopNOptimizer) name() string {
	return "topn_push_down"
}

func appendTopNPushDownTraceStep(parent LogicalPlan, child LogicalPlan, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is added as %v_%v's parent", parent.TP(), parent.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v is pushed down", parent.TP())
	}
	opt.appendStepToCurrent(parent.ID(), parent.TP(), reason, action)
}

func appendTopNPushDownJoinTraceStep(p *LogicalJoin, topN *LogicalTopN, idx int, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v is added and pushed into %v_%v's ",
			topN.TP(), topN.ID(), p.TP(), p.ID()))
		if idx == 0 {
			buffer.WriteString("left ")
		} else {
			buffer.WriteString("right ")
		}
		buffer.WriteString("table")
		return buffer.String()
	}
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's joinType is %v, and all ByItems[", p.TP(), p.ID(), p.JoinType.String()))
		for i, item := range topN.ByItems {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.String())
		}
		buffer.WriteString("] contained in ")
		if idx == 0 {
			buffer.WriteString("left ")
		} else {
			buffer.WriteString("right ")
		}
		buffer.WriteString("table")
		return buffer.String()
	}
	opt.appendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendSortPassByItemsTraceStep(sort *LogicalSort, topN *LogicalTopN, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v passes ByItems[", sort.TP(), sort.ID()))
		for i, item := range sort.ByItems {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.String())
		}
		buffer.WriteString(fmt.Sprintf("] to %v_%v", topN.TP(), topN.ID()))
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v is Limit originally", topN.TP(), topN.ID())
	}
	opt.appendStepToCurrent(sort.ID(), sort.TP(), reason, action)
}

func appendNewTopNTraceStep(topN *LogicalTopN, union *LogicalUnionAll, opt *logicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is added and pushed down across %v_%v", topN.TP(), topN.ID(), union.TP(), union.ID())
	}
	opt.appendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}

func appendConvertTopNTraceStep(p LogicalPlan, topN *LogicalTopN, opt *logicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is converted into %v_%v", p.TP(), p.ID(), topN.TP(), topN.ID())
	}
	opt.appendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}
