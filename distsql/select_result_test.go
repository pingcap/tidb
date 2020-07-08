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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testSuite) TestUpdateCopRuntimeStats(c *C) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = new(stmtctx.StatementContext)
	sr := selectResult{ctx: ctx}
	c.Assert(ctx.GetSessionVars().StmtCtx.RuntimeStatsColl, IsNil)
	sr.rootPlanID = copPlan{}
	sr.updateCopRuntimeStats(&execdetails.ExecDetails{CalleeAddress: "a"}, 0)

	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl()
	t := uint64(1)
	sr.selectResp = &tipb.SelectResponse{
		ExecutionSummaries: []*tipb.ExecutorExecutionSummary{
			{TimeProcessedNs: &t, NumProducedRows: &t, NumIterations: &t},
		},
	}
	c.Assert(len(sr.selectResp.GetExecutionSummaries()) != len(sr.copPlanIDs), IsTrue)
	sr.updateCopRuntimeStats(&execdetails.ExecDetails{CalleeAddress: "callee"}, 0)
	c.Assert(ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.ExistsCopStats("callee"), IsFalse)

	sr.copPlanIDs = []fmt.Stringer{copPlan{}}
	c.Assert(ctx.GetSessionVars().StmtCtx.RuntimeStatsColl, NotNil)
	c.Assert(len(sr.selectResp.GetExecutionSummaries()), Equals, len(sr.copPlanIDs))
	sr.updateCopRuntimeStats(&execdetails.ExecDetails{CalleeAddress: "callee"}, 0)
	c.Assert(ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetCopStats("callee").String(), Equals, "time:1ns, loops:1")
}

type copPlan struct{}

func (p copPlan) String() string {
	return "callee"
}
