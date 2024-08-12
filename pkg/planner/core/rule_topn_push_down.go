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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// PushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type PushDownTopNOptimizer struct {
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (*PushDownTopNOptimizer) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PushDownTopN(nil, opt), planChanged, nil
}

// pushDownTopNForBaseLogicalPlan can be moved when LogicalTopN has been moved to logicalop.
func pushDownTopNForBaseLogicalPlan(lp base.LogicalPlan, topNLogicalPlan base.LogicalPlan,
	opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	s := lp.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	var topN *logicalop.LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*logicalop.LogicalTopN)
	}
	p := s.Self()
	for i, child := range p.Children() {
		p.Children()[i] = child.PushDownTopN(nil, opt)
	}
	if topN != nil {
		return topN.AttachChild(p, opt)
	}
	return p
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*PushDownTopNOptimizer) Name() string {
	return "topn_push_down"
}

func appendTopNPushDownJoinTraceStep(p *LogicalJoin, topN *logicalop.LogicalTopN, idx int, opt *optimizetrace.LogicalOptimizeOp) {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
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
			buffer.WriteString(item.StringWithCtx(ectx, errors.RedactLogDisable))
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

func appendNewTopNTraceStep(topN *logicalop.LogicalTopN, union *LogicalUnionAll, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string {
		return ""
	}
	action := func() string {
		return fmt.Sprintf("%v_%v is added and pushed down across %v_%v", topN.TP(), topN.ID(), union.TP(), union.ID())
	}
	opt.AppendStepToCurrent(topN.ID(), topN.TP(), reason, action)
}
