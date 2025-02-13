// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

var _ rule.Rule = &XFDeCorrelateApplyBase{}

// XFDeCorrelateApplyBase pull the correlated expression from projection as child of apply.
type XFDeCorrelateApplyBase struct {
	*rule.BaseRule
}

// PreCheck implements the Rule interface.
func (*XFDeCorrelateApplyBase) PreCheck(applyGE base.LogicalPlan) bool {
	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
	if apply.NoDecorrelate {
		return false
	}
	return true
}
