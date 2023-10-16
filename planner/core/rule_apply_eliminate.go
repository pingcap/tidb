// Copyright 2023 PingCAP, Inc.
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
)

// applyEliminator tries to eliminate apply operator.
// For SQL like `select count(*) from (select a,(select t2.b from t t2,t t3 where t2.a=t3.a and t1.a=t2.a limit 1) t from t1) t;`, we could optimize it to `select count(*) from t1;`.
// For SQL like `select count(a) from (select a,(select t2.b from t t2,t t3 where t2.a=t3.a and t1.a=t2.a limit 1) t from t1) t;`, we could optimize it to `select count(a) from t1;`.
// For SQL like `select count(t) from (select a,(select t2.b from t t2,t t3 where t2.a=t3.a and t1.a=t2.a limit 1) t from t1) t;`, we couldn't optimize it.
type applyEliminator struct {
}

// optimize implements the logicalOptRule interface.
func (pe *applyEliminator) optimize(_ context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	root := pe.eliminate(lp, opt)
	return root, nil
}

// eliminateApplyRecursively eliminates unnecessary scalar subqueries in a logical plan recursively.
func eliminateApplyRecursively(p LogicalPlan, opt *logicalOptimizeOp) LogicalPlan {
	if _, ok := p.(*LogicalCTE); ok {
		// If the node is a LogicalCTE, return it without processing.
		return p
	}

	// Recursively process the children nodes.
	children := p.Children()
	if len(children) > 0 {
		for i := range children {
			children[i] = eliminateApplyRecursively(children[i], opt)
		}

		// Handle specific node types.
		switch node := p.(type) {
		case *LogicalAggregation:
			// For LogicalAggregation node, handle it accordingly.
			aggChild := node.Children()[0]
			if proj, isProj := aggChild.(*LogicalProjection); isProj {
				// If the child node is a LogicalProjection, we need to check whether the child node contains a LogicalApply.
				projChild := proj.Children()[0]
				if apply, isApply := projChild.(*LogicalApply); isApply {
					usedlist := expression.GetUsedList(proj.Schema().Columns, apply.Children()[1].Schema())
					used := false
					for i := len(usedlist) - 1; i >= 0; i-- {
						if usedlist[i] {
							used = true
							break
						}
					}
					if !used {
						applyEliminateTraceStep(projChild, opt)
						proj.Children()[0] = apply.Children()[0]
					}
				}
			} else if _, isApply := aggChild.(*LogicalApply); isApply {
				// If the child node is a LogicalApply, we can check if the column is used by the parent node.
				usedlist := expression.GetUsedList(node.Schema().Columns, aggChild.Children()[1].Schema())
				used := false
				for i := len(usedlist) - 1; i >= 0; i-- {
					if usedlist[i] {
						used = true
						break
					}
				}
				if !used {
					applyEliminateTraceStep(aggChild, opt)
					node.Children()[0] = aggChild.Children()[0]
				}
			}
		}
	}

	return p
}

// You can call this recursive function in the original function as follows.
func (pe *applyEliminator) eliminate(p LogicalPlan, opt *logicalOptimizeOp) LogicalPlan {
	p = eliminateApplyRecursively(p, opt)
	return p
}

func (*applyEliminator) name() string {
	return "apply_eliminate"
}

func applyEliminateTraceStep(lp LogicalPlan, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated.", lp.TP(), lp.ID()))
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v can be eliminated because it hasn't been used by it's parent.", lp.TP(), lp.ID())
	}
	opt.appendStepToCurrent(lp.ID(), lp.TP(), reason, action)
}
