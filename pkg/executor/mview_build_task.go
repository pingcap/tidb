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
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	importcore "github.com/pingcap/tidb/pkg/importinto"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// prepareMViewBuildTask compiles and checks an import under the internal
// statement's transaction; the SQL execution layer owns transaction cleanup.
func prepareMViewBuildTask(ctx context.Context, sctx sessionctx.Context, sql string, jobID int64) (*proto.Task, error) {
	node, _, err := importer.ParseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}
	if err := ResetContextOfStmt(sctx, node); err != nil {
		return nil, err
	}
	stmt, err := (&Compiler{Ctx: sctx}).Compile(ctx, node)
	if err != nil {
		return nil, err
	}
	plan, ok := stmt.Plan.(*plannercore.ImportInto)
	if !ok || !runsQueryOnWorker(plan) {
		return nil, errors.New("MV build requires a DXF query plan")
	}
	builder := newExecutorBuilder(ctx, sctx, sctx.GetLatestInfoSchema().(infoschema.InfoSchema), nil)
	built := builder.buildImportInto(plan)
	if builder.err != nil {
		return nil, builder.err
	}
	e := built.(*ImportIntoExec)
	if err := e.initController(ctx); err != nil {
		return nil, err
	}
	if err := e.prepareQuery(ctx); err != nil {
		return nil, err
	}
	checkCtx, err := CreateSession(sctx)
	if err != nil {
		return nil, err
	}
	defer CloseSession(checkCtx)
	inheritMViewMaintenanceFlag(sctx, checkCtx)
	if err := e.controller.CheckRequirements(ctx, checkCtx); err != nil {
		return nil, err
	}
	if err := e.controller.InitTiKVConfigs(ctx, checkCtx); err != nil {
		return nil, err
	}
	importPlan := e.controller.Plan
	meta, err := json.Marshal(&importcore.TaskMeta{DDLJobID: jobID, Plan: *importPlan, Stmt: sql})
	if err != nil {
		return nil, err
	}
	return &proto.Task{
		TaskBase: proto.TaskBase{
			Type:          proto.MViewBuild,
			RequiredSlots: importPlan.ThreadCnt,
			MaxNodeCount:  importPlan.MaxNodeCnt,
		},
		Meta: meta,
	}, nil
}

// prepareMViewBuildExec exposes the preparation result through an ordinary
// record set, so DDL needs no compiler-specific dependency or callback.
type prepareMViewBuildExec struct {
	exec.BaseExecutor
	stmt    *ast.PrepareMViewBuildStmt
	payload []byte
	done    bool
}

func (e *prepareMViewBuildExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	task, err := prepareMViewBuildTask(ctx, e.Ctx(), e.stmt.ImportSQL, e.stmt.DDLJobID)
	if err != nil {
		return err
	}
	e.payload, err = json.Marshal(task)
	return err
}

func (e *prepareMViewBuildExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.done {
		req.AppendBytes(0, e.payload)
		e.done = true
	}
	return nil
}
