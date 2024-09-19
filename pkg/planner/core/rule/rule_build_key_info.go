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

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// BuildKeySolver is used to build key info for logical plan.
type BuildKeySolver struct{}

// *************************** start implementation of LogicalOptRule interface ***************************

// Name implements base.LogicalOptRule.<0th> interface.
func (*BuildKeySolver) Name() string {
	return "build_keys"
}

// Optimize implements base.LogicalOptRule.<1st> interface.
func (*BuildKeySolver) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	buildKeyInfo(p)
	return p, planChanged, nil
}

// **************************** end implementation of LogicalOptRule interface ****************************

// buildKeyInfo recursively calls base.LogicalPlan's BuildKeyInfo method.
func buildKeyInfo(lp base.LogicalPlan) {
	for _, child := range lp.Children() {
		buildKeyInfo(child)
	}
	childSchema := make([]*expression.Schema, len(lp.Children()))
	for i, child := range lp.Children() {
		childSchema[i] = child.Schema()
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}
