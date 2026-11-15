// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/config"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
)

// AdviseTxnWithPlan preserves transaction plan advice while avoiding a timestamp
// prefetch that a known standalone SPFresh read never consumes. The executor
// consumes its own fresh TSO, including in explicit transactions and with
// tidb_snapshot. Other plans retain normal transaction warmup, including queries
// for which the planner deferred prefetch but selected a non-SPFresh plan.
func AdviseTxnWithPlan(ctx context.Context, sctx sessionctx.Context, plan base.Plan) error {
	if config.SPFreshEnabled() && standaloneSPFreshRead(plan) {
		sessiontxn.PrefetchSPFreshReadVersion(ctx, sctx)
		return sessiontxn.GetTxnManager(sctx).AdviseOptimizeWithPlan(plan)
	}
	return sessiontxn.AdviseOptimizeWithPlanAndThenWarmUp(sctx, plan)
}

func standaloneSPFreshRead(plan base.Plan) bool {
	switch p := plan.(type) {
	case *plannercore.Execute:
		return standaloneSPFreshRead(p.Plan)
	case *plannercore.PhysicalSPFreshVectorScan:
		return true
	case *plannercore.PhysicalProjection, *plannercore.PhysicalSelection,
		*plannercore.PhysicalLimit, *plannercore.PhysicalTopN, *plannercore.PhysicalSort:
		// These operators only consume their single child's rows. In particular,
		// joins, subqueries, locks and writes keep the ordinary warmup path.
		children := plan.(base.PhysicalPlan).Children()
		return len(children) == 1 && standaloneSPFreshRead(children[0])
	default:
		return false
	}
}
