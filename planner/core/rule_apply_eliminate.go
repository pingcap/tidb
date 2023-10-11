// Copyright 2016 PingCAP, Inc.
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

type applyEliminator struct {
}

// optimize implements the logicalOptRule interface.
func (pe *applyEliminator) optimize(_ context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	root := pe.eliminate(lp, opt)
	return root, nil
}

// eliminate useless scalar subquery in a logical plan.
func (pe *applyEliminator) eliminate(p LogicalPlan, opt *logicalOptimizeOp) LogicalPlan {
	// LogicalCTE's logical optimization is independent.
	if _, ok := p.(*LogicalCTE); ok {
		return p
	}
	child := p.Children()[0]
	if _, isAgg := child.(*LogicalAggregation); isAgg {
		subchild := child.Children()[0]
		if _, isProj := subchild.(*LogicalProjection); isProj {
			subsubchild := subchild.Children()[0]
			if _, isApply := subsubchild.(*LogicalApply); isApply {
				usedlist := expression.GetUsedList(subchild.Schema().Columns, subsubchild.Children()[1].Schema())
				used := false
				for i := len(usedlist) - 1; i >= 0; i-- {
					if usedlist[i] {
						used = true
						break
					}
				}
				if !used {
					applyEliminateTraceStep(subsubchild, opt)
					subchild.Children()[0] = subsubchild.Children()[0]
				}
			}
		} else if _, isApply := subchild.(*LogicalApply); isApply {
			usedlist := expression.GetUsedList(child.Schema().Columns, subchild.Children()[1].Schema())
			used := false
			for i := len(usedlist) - 1; i >= 0; i-- {
				if usedlist[i] {
					used = true
					break
				}
			}
			if !used {
				applyEliminateTraceStep(subchild, opt)
				child.Children()[0] = subchild.Children()[0]
			}
		}
	}
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
