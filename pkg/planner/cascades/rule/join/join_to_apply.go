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

package join

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

var _ rule.Rule = &XFJoinToApply{}

// XFJoinToApply is a type of rule that aims to convert a join into apply mode,
// allowing runtime scalar attributes to be passed to the apply's probe side,
// thereby enhancing the likelihood of better index scans.
type XFJoinToApply struct {
	*rule.BaseRule
}

// NewJoinToApply creates a new JoinToApply rule.
func NewJoinToApply() *XFJoinToApply {
	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineAll), pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly))
	return &XFJoinToApply{
		BaseRule: rule.NewBaseRule(rule.XFJoinToApply, pa),
	}
}

// Match implements the Rule interface.
func (*XFJoinToApply) Match(_ base.LogicalPlan) bool {
	return true
}

// XForm implements thr Rule interface.
func (*XFJoinToApply) XForm(_ base.LogicalPlan) ([]base.LogicalPlan, error) {
	// todo: Check whether the join can be converted to apply.
	return nil, nil
}
