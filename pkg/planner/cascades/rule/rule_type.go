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

package rule

// Type indicates the rule type.
type Type int

const (
	// DefaultNone indicates this is none rule.
	DefaultNone Type = iota
	// XFJoinToApply refers to join to a apply rule.
	XFJoinToApply

	// XFDeCorrelateSimpleApply try to decorate apply to a join when no corr predicates from inner side.
	XFDeCorrelateSimpleApply
	// XFPullCorrPredFromProj try to pull correlated expression from proj from inner child of an apply.
	XFPullCorrPredFromProj
	// XFPullCorrPredFromSel try to pull correlated expression from sel from inner child of an apply.
	XFPullCorrPredFromSel
	// XFPullCorrPredFromDS try to pull correlated expression from sel from inner child of an apply.
	XFPullCorrPredFromDS
	// XFPullCorrPredFromSort try to pull correlated expression from sort from inner child of an apply.
	XFPullCorrPredFromSort
	// XFPullCorrPredFromLimit try to pull correlated expression from limit from inner child of an apply.
	XFPullCorrPredFromLimit
	// XFPullCorrPredFromMax1Row try to pull correlated expression from max1Row from inner child of an apply.
	XFPullCorrPredFromMax1Row
	// XFPullCorrPredFromAgg1 try to pull correlated expression from agg from inner child of an apply.
	XFPullCorrPredFromAgg1
	// XFPullCorrPredFromAgg2 try to pull correlated expression from agg<selection> from inner child of an apply.
	XFPullCorrPredFromAgg2

	// XFMergeAdjacentProjection try to merge adjacent projection together to avoid executor open cost.
	XFMergeAdjacentProjection
	// XFEliminateProjection try to eliminate projection operator once all projected expr are columns.
	XFEliminateProjection
	// XFEliminateOuterJoinBelowProjection try to eliminate outer join below projection.
	XFEliminateOuterJoinBelowProjection

	// XFMaximumRuleLength is the maximum rule length.
	XFMaximumRuleLength
)

// String implements the fmt.Stringer interface.
func (tp *Type) String() string {
	switch *tp {
	case XFJoinToApply:
		return "join_to_apply"
	default:
		return "default_none"
	}
}
