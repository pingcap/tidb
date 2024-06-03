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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// pushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type pushDownTopNOptimizer struct {
}

func (*pushDownTopNOptimizer) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PushDownTopN(nil, opt), planChanged, nil
}

// pushDownTopNForBaseLogicalPlan can be moved when LogicalTopN has been moved to logicalop.
func pushDownTopNForBaseLogicalPlan(lp base.LogicalPlan, topNLogicalPlan base.LogicalPlan,
	opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	s := lp.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	p := s.Self()
	for i, child := range p.Children() {
		p.Children()[i] = child.PushDownTopN(nil, opt)
	}
	if topN != nil {
		return topN.setChild(p, opt)
	}
	return p
}

// PushDownTopN implements the LogicalPlan interface.
func (p *LogicalCTE) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		return topN.setChild(p, opt)
	}
	return p
}

// setChild set p as topn's child.
func (lt *LogicalTopN) setChild(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	// Remove this TopN if its child is a TableDual.
	dual, isDual := p.(*LogicalTableDual)
	if isDual {
		numDualRows := uint64(dual.RowCount)
		if numDualRows < lt.Offset {
			dual.RowCount = 0
			return dual
		}
		dual.RowCount = int(min(numDualRows-lt.Offset, lt.Count))
		return dual
	}

	if lt.isLimit() {
		limit := LogicalLimit{
			Count:            lt.Count,
			Offset:           lt.Offset,
			PreferLimitToCop: lt.PreferLimitToCop,
			PartitionBy:      lt.GetPartitionBy(),
		}.Init(lt.SCtx(), lt.QueryBlockOffset())
		limit.SetChildren(p)
		appendTopNPushDownTraceStep(limit, p, opt)
		return limit
	}
	// Then lt must be topN.
	lt.SetChildren(p)
	appendTopNPushDownTraceStep(lt, p, opt)
	return lt
}

// PushDownTopN implements LogicalPlan interface.
func (ls *LogicalSort) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN == nil {
		return ls.BaseLogicalPlan.PushDownTopN(nil, opt)
	} else if topN.isLimit() {
		topN.ByItems = ls.ByItems
		appendSortPassByItemsTraceStep(ls, topN, opt)
		return ls.Children()[0].PushDownTopN(topN, opt)
	}
	// If a TopN is pushed down, this sort is useless.
	return ls.Children()[0].PushDownTopN(topN, opt)
}

func (p *LogicalLimit) convertToTopN(opt *optimizetrace.LogicalOptimizeOp) *LogicalTopN {
	topn := LogicalTopN{Offset: p.Offset, Count: p.Count, PreferLimitToCop: p.PreferLimitToCop}.Init(p.SCtx(), p.QueryBlockOffset())
	appendConvertTopNTraceStep(p, topn, opt)
	return topn
}

// PushDownTopN implements the LogicalPlan interface.
func (p *LogicalLimit) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	child := p.Children()[0].PushDownTopN(p.convertToTopN(opt), opt)
	if topN != nil {
		return topN.setChild(child, opt)
	}
	return child
}

// PushDownTopN implements the LogicalPlan interface.
func (p *LogicalUnionAll) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	for i, child := range p.Children() {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, PreferLimitToCop: topN.PreferLimitToCop}.Init(p.SCtx(), topN.QueryBlockOffset())
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &util.ByItems{Expr: by.Expr, Desc: by.Desc})
			}
			// newTopN to push down Union's child
			appendNewTopNTraceStep(topN, p, opt)
		}
		p.Children()[i] = child.PushDownTopN(newTopN, opt)
	}
	if topN != nil {
		return topN.setChild(p, opt)
	}
	return p
}

// PushDownTopN implements LogicalPlan interface.
func (p *LogicalProjection) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return p.BaseLogicalPlan.PushDownTopN(topN, opt)
		}
	}
	if topN != nil {
		exprCtx := p.SCtx().GetExprCtx()
		substitutedExprs := make([]expression.Expression, 0, len(topN.ByItems))
		for _, by := range topN.ByItems {
			substituted := expression.FoldConstant(exprCtx, expression.ColumnSubstitute(exprCtx, by.Expr, p.schema, p.Exprs))
			if !expression.IsImmutableFunc(substituted) {
				// after substituting, if the order-by expression is un-deterministic like 'order by rand()', stop pushing down.
				return p.BaseLogicalPlan.PushDownTopN(topN, opt)
			}
			substitutedExprs = append(substitutedExprs, substituted)
		}
		for i, by := range topN.ByItems {
			by.Expr = substitutedExprs[i]
		}

		// remove meaningless constant sort items.
		for i := len(topN.ByItems) - 1; i >= 0; i-- {
			switch topN.ByItems[i].Expr.(type) {
			case *expression.Constant, *expression.CorrelatedColumn:
				topN.ByItems = append(topN.ByItems[:i], topN.ByItems[i+1:]...)
			}
		}

		// if topN.ByItems contains a column(with ID=0) generated by projection, projection will prevent the optimizer from pushing topN down.
		for _, by := range topN.ByItems {
			cols := expression.ExtractColumns(by.Expr)
			for _, col := range cols {
				if col.ID == 0 && p.Schema().Contains(col) {
					// check whether the column is generated by projection
					if !p.Children()[0].Schema().Contains(col) {
						p.Children()[0] = p.Children()[0].PushDownTopN(nil, opt)
						return topN.setChild(p, opt)
					}
				}
			}
		}
	}
	p.Children()[0] = p.Children()[0].PushDownTopN(topN, opt)
	return p
}

// PushDownTopN implements the LogicalPlan interface.
func (p *LogicalLock) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		p.Children()[0] = p.Children()[0].PushDownTopN(topN, opt)
	}
	return p.Self()
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	if topN == nil {
		return p.Children()[idx].PushDownTopN(nil, opt)
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.Children()[idx].Schema().Contains(col) {
				return p.Children()[idx].PushDownTopN(nil, opt)
			}
		}
	}

	newTopN := LogicalTopN{
		Count:            topN.Count + topN.Offset,
		ByItems:          make([]*util.ByItems, len(topN.ByItems)),
		PreferLimitToCop: topN.PreferLimitToCop,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	appendTopNPushDownJoinTraceStep(p, newTopN, idx, opt)
	return p.Children()[idx].PushDownTopN(newTopN, opt)
}

// PushDownTopN implements the LogicalPlan interface.
func (p *LogicalJoin) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		p.Children()[0] = p.pushDownTopNToChild(topN, 0, opt)
		p.Children()[1] = p.Children()[1].PushDownTopN(nil, opt)
	case RightOuterJoin:
		p.Children()[1] = p.pushDownTopNToChild(topN, 1, opt)
		p.Children()[0] = p.Children()[0].PushDownTopN(nil, opt)
	default:
		return p.BaseLogicalPlan.PushDownTopN(topN, opt)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		return topN.setChild(p.Self(), opt)
	}
	return p.Self()
}

func (*pushDownTopNOptimizer) name() string {
	return "topn_push_down"
}

func appendTopNPushDownTraceStep(parent base.LogicalPlan, child base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is added as %v_%v's parent", parent.TP(), parent.ID(), child.TP(), child.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v is pushed down", parent.TP())
	}
	opt.AppendStepToCurrent(parent.ID(), parent.TP(), reason, action)
}

func appendTopNPushDownJoinTraceStep(p *LogicalJoin, topN *LogicalTopN, idx int, opt *optimizetrace.LogicalOptimizeOp) {
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
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendSortPassByItemsTraceStep(sort *LogicalSort, topN *LogicalTopN, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v passes ByItems[", sort.TP(), sort.ID()))
		for i, item := range sort.ByItems {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.String())
		}
		fmt.Fprintf(buffer, "] to %v_%v", topN.TP(), topN.ID())
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v is Limit originally", topN.TP(), topN.ID())
	}
	opt.AppendStepToCurrent(sort.ID(), sort.TP(), reason, action)
}

func appendNewTopNTraceStep(topN *LogicalTopN, union *LogicalUnionAll, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is added and pushed down across %v_%v", topN.TP(), topN.ID(), union.TP(), union.ID())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}

func appendConvertTopNTraceStep(p base.LogicalPlan, topN *LogicalTopN, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is converted into %v_%v", p.TP(), p.ID(), topN.TP(), topN.ID())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}
