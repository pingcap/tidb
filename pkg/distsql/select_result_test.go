// Copyright 2019 PingCAP, Inc.
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

package distsql

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestUpdateCopRuntimeStats(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	sr := selectResult{ctx: ctx.GetDistSQLCtx(), storeType: kv.TiKV, stats: &selectResultRuntimeStats{}}
	require.Nil(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl)

	sr.rootPlanID = 1234
	backOffSleep := make(map[string]time.Duration, 1)
	backOffSleep["RegionMiss"] = time.Duration(100)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "a", BackoffSleep: backOffSleep}}, 0)
	// RuntimeStatsColl is nil, so the update doesn't take efffect
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(0))

	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	// refresh the ctx after assigning `RuntimeStatsColl`.
	sr.ctx = ctx.GetDistSQLCtx()
	i := uint64(1)
	sr.selectResp = &tipb.SelectResponse{
		ExecutionSummaries: []*tipb.ExecutorExecutionSummary{
			{TimeProcessedNs: &i, NumProducedRows: &i, NumIterations: &i},
		},
	}

	require.NotEqual(t, len(sr.copPlanIDs), len(sr.selectResp.GetExecutionSummaries()))

	backOffSleep["RegionMiss"] = time.Duration(200)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "callee", BackoffSleep: backOffSleep}}, 0)
	require.False(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.ExistsCopStats(1234))
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(200))

	sr.copPlanIDs = []int{sr.rootPlanID}
	require.NotNil(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl)
	require.Equal(t, len(sr.copPlanIDs), len(sr.selectResp.GetExecutionSummaries()))

	backOffSleep["RegionMiss"] = time.Duration(300)
	sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{CopExecDetails: execdetails.CopExecDetails{CalleeAddress: "callee", BackoffSleep: backOffSleep}}, 0)
	require.Equal(t, "tikv_task:{time:1ns, loops:1}", ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetCopStats(1234).String())
	require.Equal(t, sr.stats.backoffSleep["RegionMiss"], time.Duration(500))
}
