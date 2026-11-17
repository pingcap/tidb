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

package core

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// This file contains helpers for join reorder "look through Projection" optimization:
// - best-effort inlining checks for Projection on top of Join
// - derived column mapping (colExprMap) propagation and inlining safety checks

// tryInlineProjectionForJoinGroup tries to extract the join group through a Projection on top of a Join.
//
// It returns (result, true) when the Projection node is handled:
//   - inlined successfully (join group from child is returned, with updated colExprMap), or
//   - determined unsafe to inline (treat this Projection as an atomic leaf).
//
// It returns (nil, false) when the caller should continue with normal extractJoinGroupImpl logic.
func tryInlineProjectionForJoinGroup(p base.LogicalPlan, proj *logicalop.LogicalProjection) (*joinGroupResult, bool) {
	// Only inline projections whose child is a join; otherwise the projection is treated as an atomic leaf.
	child := proj.Children()[0]
	if _, isJoin := child.(*logicalop.LogicalJoin); !isJoin {
		return nil, false
	}

	// Fast safety checks first to avoid unnecessary recursion.
	if !joinorder.CanInlineProjectionBasic(proj) {
		return nil, false
	}

	childResult := extractJoinGroupImpl(child)
	// The single-leaf check needs the extracted join group and child's colExprMap.
	if !joinorder.CanInlineProjection(proj, childResult.group, childResult.colExprMap, childResult.nullExtendedCols) {
		// Cannot safely inline this projection (e.g. expression spans multiple leaves, or
		// a column is shared by multiple leaves). Treat it as an atomic leaf.
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}, true
	}

	// Only record when projection inlining actually happens (best-effort feature).
	proj.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptJoinReorderThroughProj)

	childResult.colExprMap = joinorder.BuildProjectionColumnMap(proj, childResult.colExprMap)
	return childResult, true
}
