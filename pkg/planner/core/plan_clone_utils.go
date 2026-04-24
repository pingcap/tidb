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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
)

// FastClonePointGetForPlanCache is a fast path to clone a PointGetPlan for plan cache.
func FastClonePointGetForPlanCache(newCtx base.PlanContext, src, dst *physicalop.PointGetPlan) *physicalop.PointGetPlan {
	if dst == nil {
		dst = new(physicalop.PointGetPlan)
	}
	dst.Plan = src.Plan
	dst.Plan.SetSCtx(newCtx)
	dst.ProbeParents = src.ProbeParents
	dst.PartitionNames = src.PartitionNames
	dst.DBName = src.DBName
	dst.SetSchema(src.Schema())
	dst.TblInfo = src.TblInfo
	dst.IndexInfo = src.IndexInfo
	dst.PartitionIdx = nil // partition prune will be triggered during execution phase
	dst.Handle = nil       // handle will be set during rebuild phase
	if src.HandleConstant == nil {
		dst.HandleConstant = nil
	} else {
		if src.HandleConstant.SafeToShareAcrossSession() {
			dst.HandleConstant = src.HandleConstant
		} else {
			dst.HandleConstant = src.HandleConstant.Clone().(*expression.Constant)
		}
	}
	dst.HandleFieldType = src.HandleFieldType
	dst.HandleColOffset = src.HandleColOffset
	if len(dst.IndexValues) < len(src.IndexValues) { // actually set during rebuild phase
		dst.IndexValues = make([]types.Datum, len(src.IndexValues))
	} else {
		dst.IndexValues = dst.IndexValues[:len(src.IndexValues)]
	}
	dst.IndexConstants = utilfuncp.CloneConstantsForPlanCache(src.IndexConstants, dst.IndexConstants)
	dst.ColsFieldType = src.ColsFieldType
	dst.IdxCols = utilfuncp.CloneColumnsForPlanCache(src.IdxCols, dst.IdxCols)
	dst.IdxColLens = src.IdxColLens
	dst.AccessConditions = utilfuncp.CloneExpressionsForPlanCache(src.AccessConditions, dst.AccessConditions)
	dst.UnsignedHandle = src.UnsignedHandle
	dst.IsTableDual = src.IsTableDual
	dst.Lock = src.Lock
	dst.SetOutputNames(src.OutputNames())
	dst.LockWaitTime = src.LockWaitTime
	dst.Columns = src.Columns

	// remaining fields are unnecessary to clone:
	// cost, planCostInit, planCost, planCostVer2, accessCols
	return dst
}

// cloneLogicalSubtree creates a shallow clone of the logical plan subtree,
// ensuring each node has a fresh plan ID and independent mutable state (children,
// conditions, AllConds). Immutable data (table info, column info, etc.) is shared.
// This is used to build the Apply alternative's inner plan without modifying the
// Join's original inner subtree when PPD pushes correlated conditions down.
// Returns (clone, true) on success, or (nil, false) if an unhandled operator type
// is encountered. In the failure case, the caller must abort the correlate
// optimization to avoid corrupting the original subtree.
func cloneLogicalSubtree(p base.LogicalPlan) (base.LogicalPlan, bool) {
	switch op := p.(type) {
	case *logicalop.DataSource:
		return cloneDataSource(op), true
	case *logicalop.LogicalJoin:
		return cloneJoin(op)
	case *logicalop.LogicalSelection:
		return cloneSelection(op)
	case *logicalop.LogicalProjection:
		return cloneProjection(op)
	case *logicalop.LogicalAggregation:
		return cloneAggregation(op)
	case *logicalop.LogicalLimit:
		return cloneLimit(op)
	case *logicalop.LogicalSort:
		return cloneSort(op)
	case *logicalop.LogicalTopN:
		return cloneTopN(op)
	default:
		// Unknown operator type — cannot safely clone. Return failure
		// so the caller aborts the correlate optimization.
		return nil, false
	}
}

func cloneWithChildren(p base.LogicalPlan) ([]base.LogicalPlan, bool) {
	children := make([]base.LogicalPlan, len(p.Children()))
	for i, child := range p.Children() {
		cloned, ok := cloneLogicalSubtree(child)
		if !ok {
			return nil, false
		}
		children[i] = cloned
	}
	return children, true
}

func cloneDataSource(ds *logicalop.DataSource) *logicalop.DataSource {
	clone := *ds
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		ds.SCtx(), ds.TP(), &clone, ds.QueryBlockOffset())
	clone.SetSchema(ds.Schema().Clone())
	// Independent slices that PPD replaces.
	clone.AllConds = append([]expression.Expression(nil), ds.AllConds...)
	clone.PushedDownConds = append([]expression.Expression(nil), ds.PushedDownConds...)
	// Deep-clone AccessPaths so the Join and Apply alternatives have fully
	// independent path objects. Stats derivation (fillIndexPath, etc.) mutates
	// AccessPath fields in place; without deep cloning, costing one alternative
	// can corrupt the other and destabilize CBO.
	clone.AllPossibleAccessPaths = make([]*util.AccessPath, len(ds.AllPossibleAccessPaths))
	for i, ap := range ds.AllPossibleAccessPaths {
		clone.AllPossibleAccessPaths[i] = ap.Clone()
	}
	clone.PossibleAccessPaths = make([]*util.AccessPath, len(ds.PossibleAccessPaths))
	for i, ap := range ds.PossibleAccessPaths {
		clone.PossibleAccessPaths[i] = ap.Clone()
	}
	// Preserve original stats so DeriveStats returns early for DataSources
	// that don't receive correlated conditions. Without this, DeriveStats
	// re-runs fillIndexPath on all DataSources, which fails when conditions
	// reference columns that column pruning removed from the schema.
	if origStats := ds.StatsInfo(); origStats != nil {
		clone.SetStats(origStats)
	}
	return &clone
}

func cloneJoin(j *logicalop.LogicalJoin) (*logicalop.LogicalJoin, bool) {
	children, ok := cloneWithChildren(j)
	if !ok {
		return nil, false
	}
	clone := *j
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		j.SCtx(), j.TP(), &clone, j.QueryBlockOffset())
	clone.SetSchema(j.Schema().Clone())
	// Independent condition slices that PPD may modify.
	clone.EqualConditions = append([]*expression.ScalarFunction(nil), j.EqualConditions...)
	clone.LeftConditions = append(expression.CNFExprs(nil), j.LeftConditions...)
	clone.RightConditions = append(expression.CNFExprs(nil), j.RightConditions...)
	clone.OtherConditions = append(expression.CNFExprs(nil), j.OtherConditions...)
	// Clear PreferCorrelate on cloned inner joins to prevent CorrelateSolver
	// from processing nested semi-joins in the cloned subtree.
	clone.PreferCorrelate = false
	clone.SetChildren(children...)
	return &clone, true
}

func cloneSelection(s *logicalop.LogicalSelection) (*logicalop.LogicalSelection, bool) {
	children, ok := cloneWithChildren(s)
	if !ok {
		return nil, false
	}
	clone := *s
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		s.SCtx(), s.TP(), &clone, s.QueryBlockOffset())
	clone.Conditions = append(expression.CNFExprs(nil), s.Conditions...)
	clone.SetChildren(children...)
	return &clone, true
}

func cloneProjection(proj *logicalop.LogicalProjection) (*logicalop.LogicalProjection, bool) {
	children, ok := cloneWithChildren(proj)
	if !ok {
		return nil, false
	}
	clone := *proj
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		proj.SCtx(), proj.TP(), &clone, proj.QueryBlockOffset())
	clone.SetSchema(proj.Schema().Clone())
	clone.Exprs = append([]expression.Expression(nil), proj.Exprs...)
	clone.SetChildren(children...)
	return &clone, true
}

func cloneAggregation(agg *logicalop.LogicalAggregation) (*logicalop.LogicalAggregation, bool) {
	children, ok := cloneWithChildren(agg)
	if !ok {
		return nil, false
	}
	clone := *agg
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		agg.SCtx(), agg.TP(), &clone, agg.QueryBlockOffset())
	clone.SetSchema(agg.Schema().Clone())
	clone.AggFuncs = append([]*aggregation.AggFuncDesc(nil), agg.AggFuncs...)
	clone.GroupByItems = append([]expression.Expression(nil), agg.GroupByItems...)
	clone.SetChildren(children...)
	return &clone, true
}

func cloneLimit(lim *logicalop.LogicalLimit) (*logicalop.LogicalLimit, bool) {
	children, ok := cloneWithChildren(lim)
	if !ok {
		return nil, false
	}
	clone := *lim
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		lim.SCtx(), lim.TP(), &clone, lim.QueryBlockOffset())
	clone.SetSchema(lim.Schema().Clone())
	if len(lim.PartitionBy) > 0 {
		clone.PartitionBy = append([]property.SortItem(nil), lim.PartitionBy...)
	}
	clone.SetChildren(children...)
	return &clone, true
}

func cloneSort(s *logicalop.LogicalSort) (*logicalop.LogicalSort, bool) {
	children, ok := cloneWithChildren(s)
	if !ok {
		return nil, false
	}
	clone := *s
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		s.SCtx(), s.TP(), &clone, s.QueryBlockOffset())
	// LogicalSort embeds BaseLogicalPlan (not LogicalSchemaProducer),
	// so it inherits schema from its child — no SetSchema needed.
	clone.ByItems = append([]*util.ByItems(nil), s.ByItems...)
	clone.SetChildren(children...)
	return &clone, true
}

func cloneTopN(tn *logicalop.LogicalTopN) (*logicalop.LogicalTopN, bool) {
	children, ok := cloneWithChildren(tn)
	if !ok {
		return nil, false
	}
	clone := *tn
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		tn.SCtx(), tn.TP(), &clone, tn.QueryBlockOffset())
	clone.SetSchema(tn.Schema().Clone())
	clone.ByItems = append([]*util.ByItems(nil), tn.ByItems...)
	if len(tn.PartitionBy) > 0 {
		clone.PartitionBy = append([]property.SortItem(nil), tn.PartitionBy...)
	}
	clone.SetChildren(children...)
	return &clone, true
}

// freshAccessPath creates a new AccessPath with only the structural identity
// fields from the source path (Index, StoreType, handle flags, hint flags).
// Analysis fields (Ranges, AccessConds, IdxCols, etc.) are left at zero so
// that fillIndexPath / deriveTablePathStats start from a clean state.
//
// Index-merge fields (PartialIndexPaths, PartialAlternativeIndexPaths, etc.)
// are intentionally omitted: AllPossibleAccessPaths contains only individual
// index paths; index merge paths are synthesized later by generateIndexMergePath
// which runs as part of DeriveStats after fillIndexPath populates these fresh paths.
func freshAccessPath(src *util.AccessPath) *util.AccessPath {
	return &util.AccessPath{
		Index:                 src.Index,
		StoreType:             src.StoreType,
		IsIntHandlePath:       src.IsIntHandlePath,
		IsCommonHandlePath:    src.IsCommonHandlePath,
		Forced:                src.Forced,
		ForceKeepOrder:        src.ForceKeepOrder,
		ForceNoKeepOrder:      src.ForceNoKeepOrder,
		ForcePartialOrder:     src.ForcePartialOrder,
		IsUkShardIndexPath:    src.IsUkShardIndexPath,
		IndexLookUpPushDownBy: src.IndexLookUpPushDownBy,
		NoncacheableReason:    src.NoncacheableReason,
	}
}
