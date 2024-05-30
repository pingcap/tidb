// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/atomic"
)

func init() {
	// For code refactor init.
	utilfuncp.AddSelection = addSelection
	utilfuncp.FindBestTask = findBestTask
	utilfuncp.HasMaxOneRowUtil = HasMaxOneRow
	utilfuncp.GetTaskPlanCost = getTaskPlanCost
	utilfuncp.CanPushToCopImpl = canPushToCopImpl
	utilfuncp.AppendCandidate4PhysicalOptimizeOp = appendCandidate4PhysicalOptimizeOp
	utilfuncp.PushDownTopNForBaseLogicalPlan = pushDownTopNForBaseLogicalPlan

	// For mv index init.
	cardinality.GetTblInfoForUsedStatsByPhysicalID = getTblInfoForUsedStatsByPhysicalID
	cardinality.CollectFilters4MVIndex = collectFilters4MVIndex
	cardinality.BuildPartialPaths4MVIndex = buildPartialPaths4MVIndex
	statistics.PrepareCols4MVIndex = PrepareIdxColsAndUnwrapArrayType

	// For basic optimizer init.
	base.InvalidTask = &RootTask{} // invalid if p is nil
	expression.EvalSimpleAst = evalAstExpr
	expression.BuildSimpleExpr = buildSimpleExpr
	expression.DecodeKeyFromString = decodeKeyFromString
	plannerutil.EvalAstExprWithPlanCtx = evalAstExprWithPlanCtx
	plannerutil.RewriteAstExprWithPlanCtx = rewriteAstExprWithPlanCtx
	DefaultDisabledLogicalRulesList = new(atomic.Value)
	DefaultDisabledLogicalRulesList.Store(set.NewStringSet())
}
