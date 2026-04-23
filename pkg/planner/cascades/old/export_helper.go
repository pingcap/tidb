// Copyright 2026 PingCAP, Inc.
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

package old

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannermemo "github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// Exported types for testing

// ExportedOptimizer exports Optimizer for testing
type ExportedOptimizer = Optimizer

// ExportedTransformation exports Transformation for testing
type ExportedTransformation = Transformation

// ExportedBaseRule exports baseRule for testing
type ExportedBaseRule = baseRule

// ExportedTransformationRuleBatch exports TransformationRuleBatch for testing
type ExportedTransformationRuleBatch = TransformationRuleBatch

// ExportedEnforcer exports Enforcer for testing
type ExportedEnforcer = Enforcer

// ExportedOrderEnforcer exports OrderEnforcer for testing
type ExportedOrderEnforcer = OrderEnforcer

// ExportedNewOptimizer creates a new Optimizer for testing
func ExportedNewOptimizer() *Optimizer {
	return NewOptimizer()
}

// ExportedOptimizerImplGroup calls implGroup method on Optimizer for testing
func ExportedOptimizerImplGroup(opt *Optimizer, g *plannermemo.Group, reqPhysProp *property.PhysicalProperty, costLimit float64) (plannermemo.Implementation, error) {
	return opt.implGroup(g, reqPhysProp, costLimit)
}

// ExportedOptimizerFillGroupStats calls fillGroupStats method on Optimizer for testing
func ExportedOptimizerFillGroupStats(opt *Optimizer, g *plannermemo.Group) error {
	return opt.fillGroupStats(g)
}

// ExportedOptimizerOnPhasePreprocessing calls onPhasePreprocessing method on Optimizer for testing
func ExportedOptimizerOnPhasePreprocessing(opt *Optimizer, sctx base.PlanContext, plan base.LogicalPlan) (base.LogicalPlan, error) {
	return opt.onPhasePreprocessing(sctx, plan)
}

// ExportedOptimizerOnPhaseExploration calls onPhaseExploration method on Optimizer for testing
func ExportedOptimizerOnPhaseExploration(opt *Optimizer, sctx base.PlanContext, g *plannermemo.Group) error {
	return opt.onPhaseExploration(sctx, g)
}

// ExportedNewRuleEnumeratePaths creates a new RuleEnumeratePaths for testing
func ExportedNewRuleEnumeratePaths() Transformation {
	return NewRuleEnumeratePaths()
}

// ExportedGetDefaultRuleBatches gets the DefaultRuleBatches for testing
func ExportedGetDefaultRuleBatches() []TransformationRuleBatch {
	return DefaultRuleBatches
}

// ExportedPostTransformationBatch gets the PostTransformationBatch for testing
func ExportedPostTransformationBatch() TransformationRuleBatch {
	return PostTransformationBatch
}

// ExportedNewRuleTransformLimitToTableDual creates a new TransformLimitToTableDual for testing
func ExportedNewRuleTransformLimitToTableDual() Transformation {
	return NewRuleTransformLimitToTableDual()
}

// ExportedNewRuleEliminateOuterJoinBelowAggregation creates a new EliminateOuterJoinBelowAggregation for testing
func ExportedNewRuleEliminateOuterJoinBelowAggregation() Transformation {
	return NewRuleEliminateOuterJoinBelowAggregation()
}

// ExportedNewRuleEliminateOuterJoinBelowProjection creates a new EliminateOuterJoinBelowProjection for testing
func ExportedNewRuleEliminateOuterJoinBelowProjection() Transformation {
	return NewRuleEliminateOuterJoinBelowProjection()
}

// ExportedNewRuleTransformAggregateCaseToSelection creates a new TransformAggregateCaseToSelection for testing
func ExportedNewRuleTransformAggregateCaseToSelection() Transformation {
	return NewRuleTransformAggregateCaseToSelection()
}

// ExportedNewRuleTransformAggToProj creates a new TransformAggToProj for testing
func ExportedNewRuleTransformAggToProj() Transformation {
	return NewRuleTransformAggToProj()
}

// ExportedNewRulePullSelectionUpApply creates a new PullSelectionUpApply for testing
func ExportedNewRulePullSelectionUpApply() Transformation {
	return NewRulePullSelectionUpApply()
}

// ExportedNewRuleTransformApplyToJoin creates a new TransformApplyToJoin for testing
func ExportedNewRuleTransformApplyToJoin() Transformation {
	return NewRuleTransformApplyToJoin()
}

// ExportedNewRuleInjectProjectionBelowAgg creates a new InjectProjectionBelowAgg for testing
func ExportedNewRuleInjectProjectionBelowAgg() Transformation {
	return NewRuleInjectProjectionBelowAgg()
}

// ExportedNewRuleInjectProjectionBelowTopN creates a new InjectProjectionBelowTopN for testing
func ExportedNewRuleInjectProjectionBelowTopN() Transformation {
	return NewRuleInjectProjectionBelowTopN()
}

// ExportedNewRuleMergeAdjacentWindow creates a new MergeAdjacentWindow for testing
func ExportedNewRuleMergeAdjacentWindow() Transformation {
	return NewRuleMergeAdjacentWindow()
}

// ExportedPreparePossibleProperties calls preparePossibleProperties for testing
func ExportedPreparePossibleProperties(group *plannermemo.Group, propMap map[*plannermemo.Group]*base.PossiblePropertiesInfo) *base.PossiblePropertiesInfo {
	return preparePossibleProperties(group, propMap)
}

// ExportedOptimizerResetTransformationRules calls ResetTransformationRules for testing
func ExportedOptimizerResetTransformationRules(opt *Optimizer, ruleBatches ...TransformationRuleBatch) *Optimizer {
	return opt.ResetTransformationRules(ruleBatches...)
}

// ExportedGetEnforcerRules calls GetEnforcerRules for testing
func ExportedGetEnforcerRules(g *plannermemo.Group, prop *property.PhysicalProperty) []Enforcer {
	return GetEnforcerRules(g, prop)
}

// ExportedSetBaseRulePattern sets the pattern field on a baseRule for testing
func ExportedSetBaseRulePattern(r *baseRule, p *pattern.Pattern) {
	r.pattern = p
}

// ExportedNewRulePushSelDownTiKVSingleGather creates a new PushSelDownTiKVSingleGather for testing
func ExportedNewRulePushSelDownTiKVSingleGather() Transformation {
	return NewRulePushSelDownTiKVSingleGather()
}

// ExportedNewRulePushSelDownTableScan creates a new PushSelDownTableScan for testing
func ExportedNewRulePushSelDownTableScan() Transformation {
	return NewRulePushSelDownTableScan()
}

// ExportedToString calls ToString for testing
func ExportedToString(ctx expression.EvalContext, g *plannermemo.Group) []string {
	return ToString(ctx, g)
}

// ExportedNewRulePushAggDownGather creates a new PushAggDownGather for testing
func ExportedNewRulePushAggDownGather() Transformation {
	return NewRulePushAggDownGather()
}

// ExportedNewRulePushSelDownSort creates a new PushSelDownSort for testing
func ExportedNewRulePushSelDownSort() Transformation {
	return NewRulePushSelDownSort()
}

// ExportedNewRulePushSelDownProjection creates a new PushSelDownProjection for testing
func ExportedNewRulePushSelDownProjection() Transformation {
	return NewRulePushSelDownProjection()
}

// ExportedNewRulePushSelDownAggregation creates a new PushSelDownAggregation for testing
func ExportedNewRulePushSelDownAggregation() Transformation {
	return NewRulePushSelDownAggregation()
}

// ExportedNewRulePushSelDownJoin creates a new PushSelDownJoin for testing
func ExportedNewRulePushSelDownJoin() Transformation {
	return NewRulePushSelDownJoin()
}

// ExportedNewRulePushSelDownUnionAll creates a new PushSelDownUnionAll for testing
func ExportedNewRulePushSelDownUnionAll() Transformation {
	return NewRulePushSelDownUnionAll()
}

// ExportedNewRulePushSelDownWindow creates a new PushSelDownWindow for testing
func ExportedNewRulePushSelDownWindow() Transformation {
	return NewRulePushSelDownWindow()
}

// ExportedNewRuleMergeAdjacentSelection creates a new MergeAdjacentSelection for testing
func ExportedNewRuleMergeAdjacentSelection() Transformation {
	return NewRuleMergeAdjacentSelection()
}

// ExportedNewRuleTransformJoinCondToSel creates a new TransformJoinCondToSel for testing
func ExportedNewRuleTransformJoinCondToSel() Transformation {
	return NewRuleTransformJoinCondToSel()
}

// ExportedNewRulePushSelDownIndexScan creates a new PushSelDownIndexScan for testing
func ExportedNewRulePushSelDownIndexScan() Transformation {
	return NewRulePushSelDownIndexScan()
}

// ExportedNewRuleTransformLimitToTopN creates a new TransformLimitToTopN for testing
func ExportedNewRuleTransformLimitToTopN() Transformation {
	return NewRuleTransformLimitToTopN()
}

// ExportedNewRulePushLimitDownProjection creates a new PushLimitDownProjection for testing
func ExportedNewRulePushLimitDownProjection() Transformation {
	return NewRulePushLimitDownProjection()
}

// ExportedNewRulePushLimitDownUnionAll creates a new PushLimitDownUnionAll for testing
func ExportedNewRulePushLimitDownUnionAll() Transformation {
	return NewRulePushLimitDownUnionAll()
}

// ExportedNewRulePushLimitDownOuterJoin creates a new PushLimitDownOuterJoin for testing
func ExportedNewRulePushLimitDownOuterJoin() Transformation {
	return NewRulePushLimitDownOuterJoin()
}

// ExportedNewRuleMergeAdjacentLimit creates a new MergeAdjacentLimit for testing
func ExportedNewRuleMergeAdjacentLimit() Transformation {
	return NewRuleMergeAdjacentLimit()
}

// ExportedNewRulePushTopNDownProjection creates a new PushTopNDownProjection for testing
func ExportedNewRulePushTopNDownProjection() Transformation {
	return NewRulePushTopNDownProjection()
}

// ExportedNewRulePushTopNDownOuterJoin creates a new PushTopNDownOuterJoin for testing
func ExportedNewRulePushTopNDownOuterJoin() Transformation {
	return NewRulePushTopNDownOuterJoin()
}

// ExportedNewRulePushTopNDownUnionAll creates a new PushTopNDownUnionAll for testing
func ExportedNewRulePushTopNDownUnionAll() Transformation {
	return NewRulePushTopNDownUnionAll()
}

// ExportedNewRulePushLimitDownTiKVSingleGather creates a new PushLimitDownTiKVSingleGather for testing
func ExportedNewRulePushLimitDownTiKVSingleGather() Transformation {
	return NewRulePushLimitDownTiKVSingleGather()
}

// ExportedNewRulePushTopNDownTiKVSingleGather creates a new PushTopNDownTiKVSingleGather for testing
func ExportedNewRulePushTopNDownTiKVSingleGather() Transformation {
	return NewRulePushTopNDownTiKVSingleGather()
}

// ExportedNewRuleEliminateProjection creates a new EliminateProjection for testing
func ExportedNewRuleEliminateProjection() Transformation {
	return NewRuleEliminateProjection()
}

// ExportedNewRuleMergeAdjacentProjection creates a new MergeAdjacentProjection for testing
func ExportedNewRuleMergeAdjacentProjection() Transformation {
	return NewRuleMergeAdjacentProjection()
}

// ExportedNewRuleEliminateSingleMaxMin creates a new EliminateSingleMaxMin for testing
func ExportedNewRuleEliminateSingleMaxMin() Transformation {
	return NewRuleEliminateSingleMaxMin()
}

// ExportedNewRuleMergeAggregationProjection creates a new MergeAggregationProjection for testing
func ExportedNewRuleMergeAggregationProjection() Transformation {
	return NewRuleMergeAggregationProjection()
}

// ExportedNewRuleMergeAdjacentTopN creates a new MergeAdjacentTopN for testing
func ExportedNewRuleMergeAdjacentTopN() Transformation {
	return NewRuleMergeAdjacentTopN()
}
