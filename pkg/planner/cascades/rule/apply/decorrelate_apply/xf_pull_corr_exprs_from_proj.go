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

package decorrelate_apply

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

var _ rule.Rule = &XFPullCorrelatedExprFromProj{}

// XFPullCorrelatedExprFromProj pull the correlated expression from projection as child of apply.
type XFPullCorrelatedExprFromProj struct {
	*rule.BaseRule
}

// NewXFPullCorrelatedExprFromProj creates a new JoinToApply rule.
func NewXFPullCorrelatedExprFromProj() *XFPullCorrelatedExprFromProj {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly))
	return &XFPullCorrelatedExprFromProj{
		BaseRule: rule.NewBaseRule(rule.XFPullCorrExprsFromProj, pa),
	}
}

// Match implements the Rule interface.
func (*XFPullCorrelatedExprFromProj) Match(applyGE base.LogicalPlan) bool {
	return true
}

// XForm implements thr Rule interface.
func (*XFPullCorrelatedExprFromProj) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, error) {
	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
	projGE := applyGE.Children()[1]
	proj := projGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection)

	return nil, nil
}
