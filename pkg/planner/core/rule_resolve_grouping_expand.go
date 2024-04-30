// Copyright 2023 PingCAP, Inc.
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

// For normal rollup Expand construction, its logical Expand should be bound
// before aggregation for the following reason. (MDA only has the physical one)
//
// 1: aggregate group items should take care of the gid and gpos as additional
// group keys. (so did shuffle keys)
//
// 2: Grouping function / or raw group item can exist in select-list/order-by/
// having clause, we should rewrite grouping-function to catch those specified
// row with specified grouping_id. (which row your grouping function is cared)
//
//    eg: grouping(a);  -- receive normal column 'a' as args.
//         |   ^
//         |   +--------- fetching grouping sets meta specified by 'a' from
//         |         Expand OP and fill it back into grouping function meta.
//         v
//        grouping(gid)[meta] -- receive gid column as args after rewrite.
//                   meta describes which gid values you should care of.
//
// From the tree structure construction from bottom up, we maintained the current
// select block's expand OP when we step into a new subq. for each grouping function
// rewriting, we pull grouping meta directly out of current Expand OP from builder.
// Notice that, grouping function can exist in select-list/order-by/having clause.
//
// 3: Expand can be seen as a kind of leveled projection, this projection will project
// what thea child output and append grouping set columns & gid behind. Providing that
// child output columns are not pruned, after expand projection is built, this may block
// the column prune logic downward. Spark can do the pruning before the Expand built phase,
// to achieve this similar effect, put it in the last logical optimizing phase is much
// more reasonable.

// resolveExpand generating Expand projection list when all the logical optimization is done.
type resolveExpand struct {
}

// By now, rollup syntax will build a LogicalExpand from bottom up. In LogicalExpand itself, its schema out should be 3 parts:
//
// +---------------------------------------------------------------------+
//
//	child.output() + grouping sets column + genCol(gid, gpos)
//
// +---------------------------------------------------------------------+
//
//	select count(a) from t group by a, b+1, c with rollup
//
//	Aggregation: (group by a, col#1, c, gid); aggFunc: count(a)
//	    |
//	    +------> Expand: schema[a, a', col#1, c, gid]; L1-projection[a, a', col#1, c, 0],   L2-projection[a, a', col#1, null, 1]
//	               |                                  L3-projection[a, a', null, null, 2], L3-projection[a, null, null, null, 3]
//	               |
//	               +-------> Projection:   a,        a',      b+1 as column#1,      c
//	                                       |         +------------------------------+
//	                              (upper required)   (grouping sets columns appended)
//
// Expand operator itself is kind like a projection, while difference is that it has a multi projection list, named as leveled projection.
func (*resolveExpand) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	// As you see, Expand's leveled projection should be built after all column-prune is done. So we just make generating-leveled-projection
	// as the last rule of logical optimization, which is more clear. (spark has column prune action before building expand)
	newLogicalPlan, err := genExpand(p, opt)
	return newLogicalPlan, planChanged, err
}

func (*resolveExpand) name() string {
	return "resolve_expand"
}

func genExpand(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	for i, child := range p.Children() {
		np, err := genExpand(child, opt)
		if err != nil {
			return p, err
		}
		p.Children()[i] = np
	}
	if expand, ok := p.(*LogicalExpand); ok {
		expand.GenLevelProjections()
	}
	return p, nil
}
