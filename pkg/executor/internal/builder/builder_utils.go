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

package builder

import (
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

// ConstructTreeBasedDistExec constructs tree based DAGRequest
func ConstructTreeBasedDistExec(sctx sessionctx.Context, p plannercore.PhysicalPlan) ([]*tipb.Executor, error) {
	execPB, err := p.ToPB(sctx, kv.TiFlash)
	return []*tipb.Executor{execPB}, err
}

// ConstructListBasedDistExec constructs list based DAGRequest
func ConstructListBasedDistExec(sctx sessionctx.Context, plans []plannercore.PhysicalPlan) ([]*tipb.Executor, error) {
	executors := make([]*tipb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx, kv.TiKV)
		if err != nil {
			return nil, err
		}
		executors = append(executors, execPB)
	}
	return executors, nil
}

// ConstructDAGReq constructs DAGRequest for physical plans
func ConstructDAGReq(ctx sessionctx.Context, plans []plannercore.PhysicalPlan, storeType kv.StoreType) (dagReq *tipb.DAGRequest, err error) {
	dagReq = &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(ctx.GetSessionVars().Location())
	sc := ctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	if storeType == kv.TiFlash {
		var executors []*tipb.Executor
		executors, err = ConstructTreeBasedDistExec(ctx, plans[0])
		dagReq.RootExecutor = executors[0]
	} else {
		dagReq.Executors, err = ConstructListBasedDistExec(ctx, plans)
	}

	distsql.SetEncodeType(ctx, dagReq)
	return dagReq, err
}
