// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// SemiJoinRewriter rewrites semi join to inner join with aggregation.
// Note: This rewriter is only used for exists subquery.
// And it also requires the hint `SEMI_JOIN_REWRITE` or variable tidb_opt_enable_sem_join_rewrite
// to be set.
// For example:
//
//	select * from t where exists (select /*+ SEMI_JOIN_REWRITE() */ * from s where s.a = t.a);
//
// will be rewriten to:
//
//	select * from t join (select a from s group by a) s on t.a = s.a;
type SemiJoinRewriter struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (smj *SemiJoinRewriter) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	newLogicalPlan, err := smj.recursivePlan(p)
	return newLogicalPlan, planChanged, err
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*SemiJoinRewriter) Name() string {
	return "semi_join_rewrite"
}

func (smj *SemiJoinRewriter) recursivePlan(p base.LogicalPlan) (base.LogicalPlan, error) {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := smj.recursivePlan(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	join, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return p, nil
	}
	return join.SemiJoinRewrite()
}
