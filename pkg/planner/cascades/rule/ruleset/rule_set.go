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

package ruleset

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule/apply/decorrelateapply"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// SetType is the type of rule set.
type SetType uint

const (
	// DefaultNone indicates this is none rule.
	DefaultNone SetType = iota
	// XFSetDeCorrelateApply indicates a set of sub-rules related to de-correlate an apply.
	XFSetDeCorrelateApply
)

// DefaultRuleSets indicates the all rule set.
var DefaultRuleSets = map[pattern.Operand]*OperandRules{
	// pattern.OperandApply: OperandApplyRules,
}

// OperandRules wrapper all the rules rooted from one specified operator.
type OperandRules struct {
	setMap  map[SetType][]rule.Rule
	setList []rule.Rule
}

// ListRules is a list of rules.
type ListRules []rule.Rule

// Filter mask out rules which is in mask uint64.
func (l ListRules) Filter(mask *bitset.BitSet) ListRules {
	res := make([]rule.Rule, 0, len(l))
	for _, one := range l {
		if mask.Test(one.ID()) {
			res = append(res, one)
		}
	}
	return res
}

// Filter return the specified operand rule list filter with operator's attribute.
func (ors *OperandRules) Filter(ge *memo.GroupExpression) ListRules {
	// special case for de-correlate apply.
	// for intermediate apply, we should only apply de-correlate rule for short path.
	if ge.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).Flag&logicalop.ApplyGenFromXFDeCorrelateRuleFlag > 0 {
		return ors.setMap[XFSetDeCorrelateApply]
	}
	return ors.setList
}

// OperandApplyRules is the rules rooted from an apply operand.
var OperandApplyRules = &OperandRules{OperandApplyRulesMap, OperandApplyRulesList}

// OperandApplyRulesMap OperandApplyRules is the rules rooted from an apply operand, organized as map, key is sub-set type.
var OperandApplyRulesMap = map[SetType][]rule.Rule{
	XFSetDeCorrelateApply: {
		decorrelateapply.NewXFDeCorrelateSimpleApply(),
	},
}

// OperandApplyRulesList OperandApplyRules is the rules rooted from an apply operand, organized as list.
var OperandApplyRulesList = []rule.Rule{
	decorrelateapply.NewXFDeCorrelateSimpleApply(),
}
