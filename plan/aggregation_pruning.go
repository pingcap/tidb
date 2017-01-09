// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

type aggPrune struct {
	allocator *idAllocator
	ctx       context.Context
}

func (ap *aggPrune) pruneAggregation(p LogicalPlan) {
	for _, child := range p.GetChildren() {
		if agg, ok := child.(*Aggregation); ok {
			schemaByGroupby := expression.NewSchema(agg.groupByCols)
			for _, key := range agg.schema.Keys {
				if schemaByGroupby.GetColumnsIndices(key) == nil {
					continue
				}
				// GroupByCols has unique key. So this aggregation can be removed.
				proj := &Projection{
					Exprs:           make([]expression.Expression, 0, len(agg.AggFuncs)),
					baseLogicalPlan: newBaseLogicalPlan(Proj, ap.allocator),
				}
				proj.self = proj
				proj.initIDAndContext(ap.ctx)
				for _, fun := range agg.AggFuncs {
					proj.Exprs = append(proj.Exprs, fun.GetArgs()[0].Clone())
				}
				proj.SetSchema(agg.schema.Clone())
				InsertPlan(p, agg, proj)
				RemovePlan(agg)
				break
			}
		}
	}
	for _, child := range p.GetChildren() {
		ap.pruneAggregation(child)
	}
}
