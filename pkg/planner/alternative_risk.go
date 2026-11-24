// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"math"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
)

type alternativeRiskCandidate struct {
	plan       base.PhysicalPlan
	names      types.NameSlice
	cost       float64
	logicalCtx logicalPlanBuildCtx
}

// Keep only the cheapest eligible MPP candidate in addition to the ordinary
// CBO winner. Decide once, after all rounds, so selection is order-independent.
type alternativeRiskSelection struct {
	enabled        bool
	disabledReason string
	mpp            *alternativeRiskCandidate
}

func newAlternativeRiskSelection(sv *variable.SessionVars, node ast.Node) *alternativeRiskSelection {
	s := &alternativeRiskSelection{}
	switch {
	case !sv.AlternativePlanRiskMode:
		s.disabledReason = "mode_off"
	case !sv.EnableAlternativeLogicalPlans:
		s.disabledReason = "alternative_plans_disabled"
	case sv.IsMPPEnforced():
		s.disabledReason = "mpp_enforced"
	default:
		v := &riskHintVisitor{}
		node.Accept(v)
		if v.found {
			s.disabledReason = "explicit_plan_hint"
		} else {
			s.enabled = true
		}
	}
	return s
}

func (s *alternativeRiskSelection) consider(p base.PhysicalPlan, names types.NameSlice, cost float64, profile core.PhysicalPlanRisk, sv *variable.SessionVars) {
	if !s.enabled || profile.Risk || !finiteRiskCost(cost) || !core.IsRiskModeMPPAlternative(p) {
		return
	}
	if s.mpp == nil || cost < s.mpp.cost {
		s.mpp = &alternativeRiskCandidate{plan: p, names: names, cost: cost, logicalCtx: saveLogicalPlanBuildCtx(sv)}
	}
}

func finiteRiskCost(cost float64) bool { return cost > 0 && !math.IsInf(cost, 0) && !math.IsNaN(cost) }

func (s *alternativeRiskSelection) choose(cbo core.PhysicalPlanRisk) (*alternativeRiskCandidate, string) {
	if !s.enabled {
		return nil, s.disabledReason
	}
	if !cbo.Risk {
		return nil, "cbo_risk_not_triggered"
	}
	if s.mpp == nil {
		return nil, "no_eligible_mpp"
	}
	if !finiteRiskCost(s.mpp.cost) {
		return nil, "invalid_mpp_cost"
	}
	return s.mpp, "experimental_v3_risk_to_mpp"
}

type riskHintVisitor struct{ found bool }

func (v *riskHintVisitor) Enter(n ast.Node) (ast.Node, bool) {
	switch x := n.(type) {
	case *ast.TableOptimizerHint:
		switch x.HintName.L {
		case "set_var", "qb_name", "max_execution_time", "memory_quota", "resource_group":
		default:
			v.found = true
		}
	case *ast.TableName:
		if len(x.IndexHints) > 0 {
			v.found = true
		}
	}
	return n, v.found
}

func (*riskHintVisitor) Leave(n ast.Node) (ast.Node, bool) { return n, true }
