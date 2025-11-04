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
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/atomic"
)

func init() {
	// For code refactor init.
	utilfuncp.FindBestTask4BaseLogicalPlan = findBestTask
	utilfuncp.FindBestTask4LogicalDataSource = findBestTask4LogicalDataSource
	utilfuncp.ExhaustPhysicalPlans4LogicalJoin = exhaustPhysicalPlans4LogicalJoin
	utilfuncp.ExhaustPhysicalPlans4LogicalApply = exhaustPhysicalPlans4LogicalApply

	// for physical operators.
	utilfuncp.GetActualProbeCntFromProbeParents = getActualProbeCntFromProbeParents
	utilfuncp.GetEstimatedProbeCntFromProbeParents = getEstimatedProbeCntFromProbeParents
	// for physical sort.
	utilfuncp.GetCost4PhysicalSort = getCost4PhysicalSort
	utilfuncp.Attach2Task4PhysicalSort = attach2Task4PhysicalSort
	utilfuncp.GetPlanCostVer14PhysicalSort = getPlanCostVer14PhysicalSort
	utilfuncp.GetPlanCostVer24PhysicalSort = getPlanCostVer24PhysicalSort
	// for nominal sort.
	utilfuncp.Attach2Task4NominalSort = attach2Task4NominalSort
	// for physical union all.
	utilfuncp.Attach2Task4PhysicalUnionAll = attach2Task4PhysicalUnionAll
	utilfuncp.GetPlanCostVer14PhysicalUnionAll = getPlanCostVer14PhysicalUnionAll
	utilfuncp.GetPlanCostVer24PhysicalUnionAll = getPlanCostVer24PhysicalUnionAll
	// for physical exchange receiver.
	utilfuncp.GetPlanCostVer1PhysicalExchangeReceiver = getPlanCostVer1PhysicalExchangeReceiver
	utilfuncp.GetPlanCostVer2PhysicalExchangeReceiver = getPlanCostVer2PhysicalExchangeReceiver
	// for physical limit.
	utilfuncp.ResolveIndices4PhysicalLimit = resolveIndices4PhysicalLimit
	utilfuncp.Attach2Task4PhysicalLimit = attach2Task4PhysicalLimit
	// for physical topN.
	utilfuncp.GetPlanCostVer14PhysicalTopN = getPlanCostVer14PhysicalTopN
	utilfuncp.GetPlanCostVer24PhysicalTopN = getPlanCostVer24PhysicalTopN
	utilfuncp.Attach2Task4PhysicalTopN = attach2Task4PhysicalTopN
	utilfuncp.ResolveIndices4PhysicalTopN = resolveIndices4PhysicalTopN
	// for physical selection.
	utilfuncp.Attach2Task4PhysicalSelection = attach2Task4PhysicalSelection
	utilfuncp.ResolveIndices4PhysicalSelection = resolveIndices4PhysicalSelection
	utilfuncp.GetPlanCostVer24PhysicalSelection = getPlanCostVer24PhysicalSelection
	utilfuncp.GetPlanCostVer14PhysicalSelection = getPlanCostVer14PhysicalSelection
	// for physical expand.
	utilfuncp.Attach2Task4PhysicalExpand = attach2Task4PhysicalExpand
	// for physical union scan.
	utilfuncp.Attach2Task4PhysicalUnionScan = attach2Task4PhysicalUnionScan
	utilfuncp.ResolveIndices4PhysicalUnionScan = resolveIndices4PhysicalUnionScan
	// for physical projection.
	utilfuncp.GetCost4PhysicalProjection = getCost4PhysicalProjection
	utilfuncp.Attach2Task4PhysicalProjection = attach2Task4PhysicalProjection
	utilfuncp.GetPlanCostVer14PhysicalProjection = getPlanCostVer14PhysicalProjection
	utilfuncp.GetPlanCostVer24PhysicalProjection = getPlanCostVer24PhysicalProjection
	utilfuncp.ResolveIndices4PhysicalProjection = resolveIndices4PhysicalProjection
	// for physical index join
	utilfuncp.GetCost4PhysicalIndexJoin = getCost4PhysicalIndexJoin
	utilfuncp.GetPlanCostVer14PhysicalIndexJoin = getPlanCostVer14PhysicalIndexJoin
	utilfuncp.GetIndexJoinCostVer24PhysicalIndexJoin = getIndexJoinCostVer24PhysicalIndexJoin
	utilfuncp.Attach2Task4PhysicalIndexJoin = attach2Task4PhysicalIndexJoin
	// for physical merge join
	utilfuncp.GetCost4PhysicalMergeJoin = getCost4PhysicalMergeJoin
	utilfuncp.Attach2Task4PhysicalMergeJoin = attach2Task4PhysicalMergeJoin
	utilfuncp.GetPlanCostVer14PhysicalMergeJoin = getPlanCostVer14PhysicalMergeJoin
	utilfuncp.GetPlanCostVer24PhysicalMergeJoin = getPlanCostVer24PhysicalMergeJoin
	// for physical hash join
	utilfuncp.GetCost4PhysicalHashJoin = getCost4PhysicalHashJoin
	utilfuncp.GetPlanCostVer14PhysicalHashJoin = getPlanCostVer14PhysicalHashJoin
	utilfuncp.Attach2Task4PhysicalHashJoin = attach2Task4PhysicalHashJoin
	utilfuncp.GetPlanCostVer24PhysicalHashJoin = getPlanCostVer24PhysicalHashJoin
	// for physical index hash join
	utilfuncp.GetCost4PhysicalIndexHashJoin = getCost4PhysicalIndexHashJoin
	utilfuncp.GetPlanCostVer1PhysicalIndexHashJoin = getPlanCostVer1PhysicalIndexHashJoin
	utilfuncp.Attach2Task4PhysicalIndexHashJoin = attach2Task4PhysicalIndexHashJoin
	// for physical index merge join
	utilfuncp.GetCost4PhysicalIndexMergeJoin = getCost4PhysicalIndexMergeJoin
	utilfuncp.GetPlanCostVer14PhysicalIndexMergeJoin = getPlanCostVer14PhysicalIndexMergeJoin
	utilfuncp.Attach2Task4PhysicalIndexMergeJoin = attach2Task4PhysicalIndexMergeJoin
	// for physical hash agg
	utilfuncp.GetCost4PhysicalHashAgg = getCost4PhysicalHashAgg
	utilfuncp.Attach2Task4PhysicalHashAgg = attach2Task4PhysicalHashAgg
	utilfuncp.GetPlanCostVer14PhysicalHashAgg = getPlanCostVer14PhysicalHashAgg
	utilfuncp.GetPlanCostVer24PhysicalHashAgg = getPlanCostVer24PhysicalHashAgg
	// for physical stream agg
	utilfuncp.GetCost4PhysicalStreamAgg = getCost4PhysicalStreamAgg
	utilfuncp.Attach2Task4PhysicalStreamAgg = attach2Task4PhysicalStreamAgg
	utilfuncp.GetPlanCostVer14PhysicalStreamAgg = getPlanCostVer14PhysicalStreamAgg
	utilfuncp.GetPlanCostVer24PhysicalStreamAgg = getPlanCostVer24PhysicalStreamAgg
	// for physical apply
	utilfuncp.Attach2Task4PhysicalApply = attach2Task4PhysicalApply
	utilfuncp.GetCost4PhysicalApply = getCost4PhysicalApply
	utilfuncp.GetPlanCostVer14PhysicalApply = getPlanCostVer14PhysicalApply
	utilfuncp.GetPlanCostVer24PhysicalApply = getPlanCostVer24PhysicalApply

	// for physical index look up reader
	utilfuncp.GetCost4PhysicalIndexLookUpReader = getCost4PhysicalIndexLookUpReader
	utilfuncp.GetPlanCostVer14PhysicalIndexLookUpReader = getPlanCostVer14PhysicalIndexLookUpReader
	utilfuncp.GetPlanCostVer24PhysicalIndexLookUpReader = getPlanCostVer24PhysicalIndexLookUpReader
	utilfuncp.ResolveIndices4PhysicalIndexLookUpReader = resolveIndices4PhysicalIndexLookUpReader

	// for physical window
	utilfuncp.Attach2Task4PhysicalWindow = attach2Task4PhysicalWindow
	// for physical sequence
	utilfuncp.Attach2Task4PhysicalSequence = attach2Task4PhysicalSequence
	// for physicalIndexScan.
	utilfuncp.GetPlanCostVer14PhysicalIndexScan = getPlanCostVer14PhysicalIndexScan
	utilfuncp.GetPlanCostVer24PhysicalIndexScan = getPlanCostVer24PhysicalIndexScan
	// for physical PhysicalTableScan
	utilfuncp.GetPlanCostVer14PhysicalTableScan = getPlanCostVer14PhysicalTableScan
	utilfuncp.GetPlanCostVer24PhysicalTableScan = getPlanCostVer24PhysicalTableScan
	// for physical cte
	utilfuncp.GetPlanCostVer24PhysicalCTE = getPlanCostVer24PhysicalCTE
	utilfuncp.Attach2Task4PhysicalCTEStorage = attach2Task4PhysicalCTEStorage
	// for physical index reader
	utilfuncp.GetPlanCostVer14PhysicalIndexReader = getPlanCostVer14PhysicalIndexReader
	utilfuncp.GetPlanCostVer24PhysicalIndexReader = getPlanCostVer24PhysicalIndexReader
	// for table reader
	utilfuncp.GetPlanCostVer14PhysicalTableReader = getPlanCostVer14PhysicalTableReader
	utilfuncp.GetPlanCostVer24PhysicalTableReader = getPlanCostVer24PhysicalTableReader
	// for physical point get
	utilfuncp.GetCost4PointGetPlan = getCost4PointGetPlan
	utilfuncp.GetPlanCostVer14PointGetPlan = getPlanCostVer14PointGetPlan
	utilfuncp.GetPlanCostVer24PointGetPlan = getPlanCostVer24PointGetPlan
	// for physical batch point get
	utilfuncp.GetCost4BatchPointGetPlan = getCost4BatchPointGetPlan
	utilfuncp.GetPlanCostVer14BatchPointGetPlan = getPlanCostVer14BatchPointGetPlan
	utilfuncp.GetPlanCostVer24BatchPointGetPlan = getPlanCostVer24BatchPointGetPlan

	utilfuncp.DoOptimize = doOptimize
	utilfuncp.GetPlanCost = getPlanCost
	utilfuncp.AttachPlan2Task = attachPlan2Task
	utilfuncp.GetTaskPlanCost = getTaskPlanCost
	utilfuncp.CompareTaskCost = compareTaskCost
	utilfuncp.GetPossibleAccessPaths = getPossibleAccessPaths

	utilfuncp.AddPrefix4ShardIndexes = addPrefix4ShardIndexes
	utilfuncp.DeriveStats4DataSource = deriveStats4DataSource
	utilfuncp.DeriveStats4LogicalIndexScan = deriveStats4LogicalIndexScan
	utilfuncp.DeriveStats4LogicalTableScan = deriveStats4LogicalTableScan

	// For mv index init.
	cardinality.CollectFilters4MVIndex = collectFilters4MVIndex
	cardinality.BuildPartialPaths4MVIndex = buildPartialPaths4MVIndex
	statistics.PrepareCols4MVIndex = PrepareIdxColsAndUnwrapArrayType

	// For basic optimizer init.
	base.InvalidTask = &physicalop.RootTask{} // invalid if p is nil
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

	// for physical index merge reader.
	utilfuncp.GetPlanCostVer14PhysicalIndexMergeReader = GetPlanCostVer14PhysicalIndexMergeReader
	utilfuncp.GetPlanCostVer24PhysicalIndexMergeReader = GetPlanCostVer24PhysicalIndexMergeReader
}
