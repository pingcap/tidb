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
	utilfuncp.PruneByItems = pruneByItems
	utilfuncp.GetTaskPlanCost = getTaskPlanCost
	utilfuncp.CanPushToCopImpl = canPushToCopImpl
	utilfuncp.PushDownTopNForBaseLogicalPlan = pushDownTopNForBaseLogicalPlan
	utilfuncp.FindBestTask4LogicalCTE = findBestTask4LogicalCTE
	utilfuncp.FindBestTask4LogicalShow = findBestTask4LogicalShow
	utilfuncp.FindBestTask4LogicalCTETable = findBestTask4LogicalCTETable
	utilfuncp.FindBestTask4LogicalMemTable = findBestTask4LogicalMemTable
	utilfuncp.FindBestTask4LogicalTableDual = findBestTask4LogicalTableDual
	utilfuncp.FindBestTask4LogicalDataSource = findBestTask4LogicalDataSource
	utilfuncp.FindBestTask4LogicalShowDDLJobs = findBestTask4LogicalShowDDLJobs
	utilfuncp.ExhaustPhysicalPlans4LogicalCTE = exhaustPhysicalPlans4LogicalCTE
	utilfuncp.ExhaustPhysicalPlans4LogicalSort = exhaustPhysicalPlans4LogicalSort
	utilfuncp.ExhaustPhysicalPlans4LogicalTopN = exhaustPhysicalPlans4LogicalTopN
	utilfuncp.ExhaustPhysicalPlans4LogicalLock = exhaustPhysicalPlans4LogicalLock
	utilfuncp.ExhaustPhysicalPlans4LogicalJoin = exhaustPhysicalPlans4LogicalJoin
	utilfuncp.ExhaustPhysicalPlans4LogicalApply = exhaustPhysicalPlans4LogicalApply
	utilfuncp.ExhaustPhysicalPlans4LogicalLimit = exhaustPhysicalPlans4LogicalLimit
	utilfuncp.ExhaustPhysicalPlans4LogicalWindow = exhaustPhysicalPlans4LogicalWindow
	utilfuncp.ExhaustPhysicalPlans4LogicalExpand = exhaustPhysicalPlans4LogicalExpand
	utilfuncp.ExhaustPhysicalPlans4LogicalUnionAll = exhaustPhysicalPlans4LogicalUnionAll
	utilfuncp.ExhaustPhysicalPlans4LogicalSequence = exhaustPhysicalPlans4LogicalSequence
	utilfuncp.ExhaustPhysicalPlans4LogicalSelection = exhaustPhysicalPlans4LogicalSelection
	utilfuncp.ExhaustPhysicalPlans4LogicalMaxOneRow = exhaustPhysicalPlans4LogicalMaxOneRow
	utilfuncp.ExhaustPhysicalPlans4LogicalUnionScan = exhaustPhysicalPlans4LogicalUnionScan
	utilfuncp.ExhaustPhysicalPlans4LogicalProjection = exhaustPhysicalPlans4LogicalProjection
	utilfuncp.ExhaustPhysicalPlans4LogicalAggregation = exhaustPhysicalPlans4LogicalAggregation
	utilfuncp.ExhaustPhysicalPlans4LogicalPartitionUnionAll = exhaustPhysicalPlans4LogicalPartitionUnionAll

	utilfuncp.GetActualProbeCntFromProbeParents = getActualProbeCntFromProbeParents
	utilfuncp.GetEstimatedProbeCntFromProbeParents = getEstimatedProbeCntFromProbeParents
	utilfuncp.AppendCandidate4PhysicalOptimizeOp = appendCandidate4PhysicalOptimizeOp

	utilfuncp.DoOptimize = doOptimize
	utilfuncp.IsSingleScan = isSingleScan
	utilfuncp.WindowIsTopN = windowIsTopN
	utilfuncp.AttachPlan2Task = attachPlan2Task
	utilfuncp.AddPrefix4ShardIndexes = addPrefix4ShardIndexes
	utilfuncp.DeriveStats4DataSource = deriveStats4DataSource
	utilfuncp.ApplyPredicateSimplification = applyPredicateSimplification
	utilfuncp.DeriveStats4LogicalIndexScan = deriveStats4LogicalIndexScan
	utilfuncp.DeriveStats4LogicalTableScan = deriveStats4LogicalTableScan
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
	helper := tidbCodecFuncHelper{}
	expression.DecodeKeyFromString = helper.decodeKeyFromString
	expression.EncodeRecordKeyFromRow = helper.encodeHandleFromRow
	expression.EncodeIndexKeyFromRow = helper.encodeIndexKeyFromRow
	plannerutil.EvalAstExprWithPlanCtx = evalAstExprWithPlanCtx
	plannerutil.RewriteAstExprWithPlanCtx = rewriteAstExprWithPlanCtx
	DefaultDisabledLogicalRulesList = new(atomic.Value)
	DefaultDisabledLogicalRulesList.Store(set.NewStringSet())
}
