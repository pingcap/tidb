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
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"

	"github.com/stretchr/testify/require"
)

func TestUpdateCopRuntimeStats(t *testing.T) {
	t.Parallel()
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = new(stmtctx.StatementContext)
	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)

	sr := selectResult{ctx: ctx, storeType: kv.TiKV, rootPlanID: 1234}

	t.Run("TestNoSelectResponse", func(t *testing.T) {
		t.Parallel()
		sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{ExecDetails: execdetails.ExecDetails{CalleeAddress: "a"}}, 0)
		require.Nil(t, sr.copPlanIDs)
	})

	t.Run("TestSelectResponse", func(t *testing.T) {
		clean := sr.mockTestSelectResp()
		defer clean()
		sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{ExecDetails: execdetails.ExecDetails{CalleeAddress: "callee"}}, 0)
		require.False(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.ExistsCopStats(1234))

		require.NotNil(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl)
		require.Len(t, sr.selectResp.GetExecutionSummaries(), 1)
	})

	t.Run("TestSelectResponseAndCopPlanIDs", func(t *testing.T) {
		cleanSelectResp := sr.mockTestSelectResp()
		defer cleanSelectResp()

		cleanCopPlanIDs := sr.mockTestCopPlanIDs()
		defer cleanCopPlanIDs()

		sr.updateCopRuntimeStats(context.Background(), &copr.CopRuntimeStats{ExecDetails: execdetails.ExecDetails{CalleeAddress: "callee"}}, 0)
		require.Equal(t, ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetOrCreateCopStats(1234, "tikv").String(), "tikv_task:{time:1ns, loops:1}")
	})
}

func (r *selectResult) mockTestSelectResp() (f func()) {
	i := uint64(1)
	r.selectResp = &tipb.SelectResponse{
		ExecutionSummaries: []*tipb.ExecutorExecutionSummary{
			{TimeProcessedNs: &i, NumProducedRows: &i, NumIterations: &i},
		},
	}
	return func() {
		r.selectResp = nil
	}
}

func (r *selectResult) mockTestCopPlanIDs() func() {
	r.copPlanIDs = []int{r.rootPlanID}
	return func() {
		r.copPlanIDs = nil
	}
}
