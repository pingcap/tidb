// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// PushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type PushDownTopNOptimizer struct {
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (*PushDownTopNOptimizer) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PushDownTopN(nil, opt), planChanged, nil
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*PushDownTopNOptimizer) Name() string {
	return "topn_push_down"
}
