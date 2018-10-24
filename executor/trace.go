// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"github.com/pingcap/tidb/ast"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"strings"
)

// TraceExec represents a root executor of trace query.
type TraceExec struct {
	baseExecutor
	// stmtNode is the real query ast tree and it is used for building real query's plan.
	stmtNode ast.StmtNode

	builder *executorBuilder

	st sessionctx.SessionTracing

	exhausted bool

	originalSql string
}

const (
	tracePrefix = "trace"
)

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.exhausted {
		return nil
	}

	actualSql := strings.TrimPrefix(e.originalSql, tracePrefix)
	e.st.StartTracing(tracing.TiDBRecording, false, false)
	ctx = e.st.Ctx()
	ctx, _ = tracing.ChildSpan(ctx, "plan recording")
	e.st.TracePlanStart(ctx, actualSql)
	stmtPlan, err := plannercore.Optimize(e.builder.ctx, e.stmtNode, e.builder.is)
	e.st.TracePlanEnd(ctx, err)
	//pp, ok := stmtPlan.(plannercore.PhysicalPlan)
	//fmt.Printf("plan is %#v", stmtPlan)
	//if !ok {
	//	return errors.New("cannot cast logical plan to physical plan")
	//}
	//append select executor to trace executor
	ctx, _ = tracing.ChildSpan(ctx, "execution recording")
	e.st.TraceExecStart(ctx, "tidb")
	stmtExec := e.builder.build(stmtPlan)
	if err := stmtExec.Open(ctx); err != nil {
		errors.Trace(err)
	}

	stmtExecChk := stmtExec.newFirstChunk()
	for {
		if err := stmtExec.Next(ctx, stmtExecChk); err != nil {
			return errors.Trace(err)
		}
		if stmtExecChk.NumRows() == 0 {
			break
		}
	}

	e.st.TraceExecEnd(ctx, err, int(e.ctx.GetSessionVars().StmtCtx.AffectedRows()))

	chkFromTracing, err := e.st.GetSessionTrace()
	if err != nil {
		return nil
	}

	processTracingResults(chk, chkFromTracing)

	e.st.StopTracing()

	e.exhausted = true
	return nil
}

// TODO(zhexuany) need add message from kv layer.
func processTracingResults(dst *chunk.Chunk, src *chunk.Chunk) {
	for i := 0; i < src.NumRows(); i++ {
		dst.AppendTime(0, src.GetRow(i).GetTime(sessionctx.TraceTimestampCol))
		dst.AppendString(1, src.GetRow(i).GetString(sessionctx.TraceAgeCol))
		dst.AppendString(2, src.GetRow(i).GetString(sessionctx.TraceMsgCol))
		dst.AppendString(3, src.GetRow(i).GetString(sessionctx.TraceTagCol))
		dst.AppendString(4, src.GetRow(i).GetString(sessionctx.TraceLocCol))
		dst.AppendString(5, src.GetRow(i).GetString(sessionctx.TraceOpCol))
		dst.AppendInt64(6, src.GetRow(i).GetInt64(sessionctx.TraceSpanIdxCol))
	}
}
