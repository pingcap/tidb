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
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type JoinToApply struct {
	*rule.BaseRule
}

// NewJoinToApply creates a new JoinToApply rule.
func NewJoinToApply() *JoinToApply {
	pa := pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineAll), pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly))
	return &JoinToApply{
		BaseRule: rule.NewBaseRule(rule.JOIN_TO_APPLY, pa),
	}
}

// Match implements the Rule interface.
func (r *JoinToApply) Match(holder *rule.GroupExprHolder, sctx sessionctx.Context) bool {
	return true
}

func (r *JoinToApply) XForm(holder *rule.GroupExprHolder, sctx sessionctx.Context) ([]*memo.GroupExpr, error) {
	// Check whether the join can be converted to apply.
	return nil, nil
}
