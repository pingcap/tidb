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

package base

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// LogicalOptRule means a logical optimizing rule, which contains de-correlate, ppd, column pruning, etc.
type LogicalOptRule interface {
	// Optimize return parameters:
	// 1. base.LogicalPlan: The optimized base.LogicalPlan after rule is applied
	// 2. bool: Used to judge whether the plan is changed or not by logical rule.
	//	 If the plan is changed, it will return true.
	//	 The default value is false. It means that no interaction rule will be triggered.
	// 3. error: If there is error during the rule optimizer, it will be thrown
	Optimize(context.Context, LogicalPlan, *optimizetrace.LogicalOptimizeOp) (LogicalPlan, bool, error)
	Name() string
}
