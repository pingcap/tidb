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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// RuntimeFilterGenerator One plan one generator
type RuntimeFilterGenerator struct {
	rfIDGenerator                 *util.IDGenerator
	columnUniqueIDToRF            map[int64][]*RuntimeFilter
	parentPhysicalPlan            base.PhysicalPlan
	childIdxForParentPhysicalPlan int
}

// GenerateRuntimeFilter is the root method.
// It traverses the entire tree in preorder.
// It constructs RF when encountering hash join, and allocate RF when encountering table scan.
// It realizes the planning of RF in the entire plan tree.
// For example:
/*
PhysicalPlanTree:
      HashJoin
       /     \
 TableScan   ExchangeNode

1. generateRuntimeFilter
      HashJoin (with RF1)
       /     \
 TableScan   ExchangeNode

2. assignRuntimeFilter
      HashJoin (with RF1)
       /     \
 TableScan   ExchangeNode
(assign RF1)
*/
func (generator *RuntimeFilterGenerator) GenerateRuntimeFilter(plan base.PhysicalPlan) {
	switch physicalPlan := plan.(type) {
	case *PhysicalHashJoin:
		generator.generateRuntimeFilterInterval(physicalPlan)
	case *PhysicalTableScan:
		generator.assignRuntimeFilter(physicalPlan)
	case *PhysicalTableReader:
		generator.parentPhysicalPlan = plan
		generator.childIdxForParentPhysicalPlan = 0
		generator.GenerateRuntimeFilter(physicalPlan.tablePlan)
		if physicalPlan.StoreType == kv.TiFlash {
			physicalPlan.TablePlans = flattenPushDownPlan(physicalPlan.tablePlan)
		}
	}

	for i, children := range plan.Children() {
		generator.parentPhysicalPlan = plan
		generator.childIdxForParentPhysicalPlan = i
		generator.GenerateRuntimeFilter(children)
	}
}

func (generator *RuntimeFilterGenerator) generateRuntimeFilterInterval(hashJoinPlan *PhysicalHashJoin) {
	// precondition: the storage type of hash join must be TiFlash
	if hashJoinPlan.storeTp != kv.TiFlash {
		logutil.BgLogger().Warn("RF only support TiFlash compute engine while storage type of hash join node is not TiFlash",
			zap.Int("PhysicalHashJoinId", hashJoinPlan.ID()),
			zap.String("StoreTP", hashJoinPlan.storeTp.Name()))
		return
	}
	// check hash join pattern
	if !generator.matchRFJoinType(hashJoinPlan) {
		return
	}
	// check eq predicate pattern
	for _, eqPredicate := range hashJoinPlan.EqualConditions {
		if generator.matchEQPredicate(eqPredicate, hashJoinPlan.RightIsBuildSide()) {
			// construct runtime filter
			newRFList, targetColumnUniqueID := NewRuntimeFilter(generator.rfIDGenerator, eqPredicate, hashJoinPlan)
			// update generator rf list
			rfList := generator.columnUniqueIDToRF[targetColumnUniqueID]
			if rfList == nil {
				generator.columnUniqueIDToRF[targetColumnUniqueID] = newRFList
			} else {
				generator.columnUniqueIDToRF[targetColumnUniqueID] = append(generator.columnUniqueIDToRF[targetColumnUniqueID], newRFList...)
			}
		}
	}
}

func (generator *RuntimeFilterGenerator) assignRuntimeFilter(physicalTableScan *PhysicalTableScan) {
	// match rf for current scan node
	cacheBuildNodeIDToRFMode := map[int]RuntimeFilterMode{}
	var currentRFList []*RuntimeFilter
	for _, scanOutputColumn := range physicalTableScan.schema.Columns {
		currentColumnRFList := generator.columnUniqueIDToRF[scanOutputColumn.UniqueID]
		for _, runtimeFilter := range currentColumnRFList {
			// compute rf mode
			var rfMode RuntimeFilterMode
			if cacheBuildNodeIDToRFMode[runtimeFilter.buildNode.ID()] != 0 {
				rfMode = cacheBuildNodeIDToRFMode[runtimeFilter.buildNode.ID()]
			} else {
				rfMode = generator.calculateRFMode(runtimeFilter.buildNode, physicalTableScan)
				cacheBuildNodeIDToRFMode[runtimeFilter.buildNode.ID()] = rfMode
			}
			// todo support global RF
			if rfMode == variable.RFGlobal {
				logutil.BgLogger().Debug("Now we don't support global RF. Remove it",
					zap.Int("BuildNodeId", runtimeFilter.buildNode.ID()),
					zap.Int("TargetNodeId", physicalTableScan.ID()))
				continue
			}
			runtimeFilter.rfMode = rfMode

			// assign rf to current node
			runtimeFilter.assign(physicalTableScan, scanOutputColumn)
			currentRFList = append(currentRFList, runtimeFilter)
		}
	}

	if len(currentRFList) == 0 {
		return
	}

	// todo Since the feature of adding filter operators has not yet been implemented,
	// the following code for this function will not be used for now.
	// supply selection if there is no predicates above target scan node
	//if parent, ok := generator.parentPhysicalPlan.(*PhysicalSelection); !ok {
	//	// StatsInfo: Just set a placeholder value here, and this value will not be used in subsequent optimizations
	//	sel := PhysicalSelection{hasRFConditions: true}.Init(plan.SCtx(), plan.statsInfo(), plan.SelectOffset())
	//	sel.fromDataSource = true
	//	sel.SetChildren(plan)
	//	generator.parentPhysicalPlan.SetChild(generator.childIdxForParentPhysicalPlan, sel)
	//} else {
	//	parent.hasRFConditions = true
	//}

	// todo
	// filter predicate selectivity, A scan node does not need many RFs, and the same column does not need many RFs
}

func (*RuntimeFilterGenerator) matchRFJoinType(hashJoinPlan *PhysicalHashJoin) bool {
	if hashJoinPlan.RightIsBuildSide() {
		// case1: build side is on the right
		if hashJoinPlan.JoinType == LeftOuterJoin || hashJoinPlan.JoinType == AntiSemiJoin ||
			hashJoinPlan.JoinType == LeftOuterSemiJoin || hashJoinPlan.JoinType == AntiLeftOuterSemiJoin {
			logutil.BgLogger().Debug("Join type does not match RF pattern when build side is on the right",
				zap.Int32("PlanNodeId", int32(hashJoinPlan.ID())),
				zap.String("JoinType", hashJoinPlan.JoinType.String()))
			return false
		}
	} else {
		// case2: build side is on the left
		if hashJoinPlan.JoinType == RightOuterJoin {
			logutil.BgLogger().Debug("Join type does not match RF pattern when build side is on the left",
				zap.Int32("PlanNodeId", int32(hashJoinPlan.ID())),
				zap.String("JoinType", hashJoinPlan.JoinType.String()))
			return false
		}
	}
	return true
}

func (*RuntimeFilterGenerator) matchEQPredicate(eqPredicate *expression.ScalarFunction,
	rightIsBuildSide bool) bool {
	// exclude null safe equal predicate
	if eqPredicate.FuncName.L == ast.NullEQ {
		logutil.BgLogger().Debug("The runtime filter doesn't support null safe eq predicate",
			zap.String("EQPredicate", eqPredicate.String()))
		return false
	}
	var targetColumn, srcColumn *expression.Column
	if rightIsBuildSide {
		targetColumn = eqPredicate.GetArgs()[0].(*expression.Column)
		srcColumn = eqPredicate.GetArgs()[1].(*expression.Column)
	} else {
		targetColumn = eqPredicate.GetArgs()[1].(*expression.Column)
		srcColumn = eqPredicate.GetArgs()[0].(*expression.Column)
	}
	// match target column
	// condition1: the target column must be real column
	// condition2: the target column has not undergone any transformation
	// todo: cast expr in target column
	if targetColumn.IsHidden || targetColumn.OrigName == "" {
		logutil.BgLogger().Debug("Target column does not match RF pattern",
			zap.String("EQPredicate", eqPredicate.String()),
			zap.String("TargetColumn", targetColumn.String()),
			zap.Bool("IsHidden", targetColumn.IsHidden),
			zap.String("OrigName", targetColumn.OrigName))
		return false
	}
	// match data type
	srcColumnType := srcColumn.GetStaticType().GetType()
	if srcColumnType == mysql.TypeJSON || srcColumnType == mysql.TypeBlob ||
		srcColumnType == mysql.TypeLongBlob || srcColumnType == mysql.TypeMediumBlob ||
		srcColumnType == mysql.TypeTinyBlob || srcColumn.GetStaticType().Hybrid() || srcColumn.GetStaticType().IsArray() {
		logutil.BgLogger().Debug("Src column type does not match RF pattern",
			zap.String("EQPredicate", eqPredicate.String()),
			zap.String("SrcColumn", srcColumn.String()),
			zap.String("SrcColumnType", srcColumn.GetStaticType().String()))
		return false
	}
	return true
}

func (generator *RuntimeFilterGenerator) calculateRFMode(buildNode *PhysicalHashJoin, targetNode *PhysicalTableScan) variable.RuntimeFilterMode {
	if generator.belongsToSameFragment(buildNode, targetNode) {
		return variable.RFLocal
	}
	return variable.RFGlobal
}

func (generator *RuntimeFilterGenerator) belongsToSameFragment(currentNode base.PhysicalPlan, targetNode *PhysicalTableScan) bool {
	switch currentNode.(type) {
	case *PhysicalExchangeReceiver:
		// terminal traversal
		return false
	case *PhysicalTableScan:
		if currentNode.ID() == targetNode.ID() {
			return true
		}
		return false
	default:
		for _, childNode := range currentNode.Children() {
			if generator.belongsToSameFragment(childNode, targetNode) {
				return true
			}
		}
		return false
	}
}
