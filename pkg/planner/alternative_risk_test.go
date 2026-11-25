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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestAlternativeRiskSelection(t *testing.T) {
	s := &alternativeRiskSelection{enabled: true, mpp: &alternativeRiskCandidate{cost: 1e12}}
	for _, signals := range [][]core.PlanRiskSignal{nil, {{Kind: "cross_engine_index_probe"}}} {
		p, reason := s.choose(core.PhysicalPlanRisk{Signals: signals})
		require.Nil(t, p)
		require.Equal(t, "cbo_risk_not_triggered", reason)
	}
	p, reason := s.choose(core.PhysicalPlanRisk{Risk: true})
	require.Same(t, s.mpp, p) // No arbitrary CBO cost-ratio gate.
	require.Equal(t, "experimental_v3_risk_to_mpp", reason)
	for _, invalid := range []float64{0, -1, math.Inf(1), math.NaN()} {
		s.mpp.cost = invalid
		p, reason = s.choose(core.PhysicalPlanRisk{Risk: true})
		require.Nil(t, p)
		require.Equal(t, "invalid_mpp_cost", reason)
	}
	s.mpp = nil
	p, reason = s.choose(core.PhysicalPlanRisk{Risk: true})
	require.Nil(t, p)
	require.Equal(t, "no_eligible_mpp", reason)
}

func TestAlternativeRiskGates(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	sv.EnableAlternativeLogicalPlans = true
	parse := parser.New()
	node, err := parse.ParseOneStmt("select * from t", "", "")
	require.NoError(t, err)
	require.False(t, newAlternativeRiskSelection(sv, node).enabled)
	sv.AlternativePlanRiskMode = true
	require.True(t, newAlternativeRiskSelection(sv, node).enabled)
	sv.EnableAlternativeLogicalPlans = false
	require.False(t, newAlternativeRiskSelection(sv, node).enabled)
	sv.EnableAlternativeLogicalPlans = true
	for _, sql := range []string{
		"select /*+ inl_join(t) */ * from t",
		"select /*+ read_from_storage(tiflash[t]) */ * from t",
		"select * from t use index(k)",
	} {
		node, err = parse.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		s := newAlternativeRiskSelection(sv, node)
		require.False(t, s.enabled)
		require.Equal(t, "explicit_plan_hint", s.disabledReason)
	}
}

func TestAlternativeRiskStatementHints(t *testing.T) {
	sv := variable.NewSessionVars(nil)
	sv.EnableAlternativeLogicalPlans, sv.AlternativePlanRiskMode = true, true
	for _, h := range []string{"max_execution_time(100)", "memory_quota(1 MB)", "resource_group(rg)", "qb_name(main)"} {
		node, err := parser.New().ParseOneStmt("select /*+ "+h+" */ * from t", "", "")
		require.NoError(t, err)
		require.True(t, newAlternativeRiskSelection(sv, node).enabled, h)
	}
}
