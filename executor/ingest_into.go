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

package executor

import (
	"context"

	"github.com/pingcap/tidb/disttask/loaddata"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"golang.org/x/sync/errgroup"
)

// IngestIntoExec represents a ingest into executor.
type IngestIntoExec struct {
	baseExecutor
	importPlan *importer.Plan
	controller *importer.LoadDataController
	stmt       string

	detachHandled bool
}

func newIngestIntoExec(b baseExecutor, userSctx sessionctx.Context, plan *plannercore.IngestInto, tbl table.Table) (
	*IngestIntoExec, error) {
	importPlan, err := importer.NewIngestPlan(userSctx, plan, tbl)
	if err != nil {
		return nil, err
	}
	astArgs := importer.ASTArgsFromIngestPlan(plan)
	controller, err := importer.NewLoadDataController(importPlan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	return &IngestIntoExec{
		baseExecutor: b,
		importPlan:   importPlan,
		controller:   controller,
		stmt:         plan.Stmt,
	}, nil
}

// Next implements the Executor Next interface.
func (e *IngestIntoExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.GrowAndReset(e.maxChunkSize)
	if e.detachHandled {
		// need to return an empty req to indicate all results have been written
		return nil
	}

	if err2 := e.controller.InitDataFiles(ctx); err2 != nil {
		return err2
	}

	sqlExec := ctx.(sqlexec.SQLExecutor)
	if err2 := e.controller.CheckRequirements(ctx, sqlExec); err2 != nil {
		return err2
	}

	group, groupCtx := errgroup.WithContext(ctx)
	param := &importer.JobImportParam{
		Job: &asyncloaddata.Job{
			ID: 1,
		},
		Group:    group,
		GroupCtx: groupCtx,
		Done:     make(chan struct{}),
		Progress: asyncloaddata.NewProgress(false),
	}
	distImporter, err := loaddata.NewDistImporter(param, e.importPlan, e.stmt)
	if err != nil {
		return err
	}
	defer func() {
		_ = distImporter.Close()
	}()
	param.Progress.SourceFileSize = e.controller.TotalFileSize

	if e.controller.Detached {
		go func() {
			// error is stored in system table, so we can ignore it here
			//nolint: errcheck
			_ = e.doIngest(distImporter)
		}()
		req.AppendInt64(0, 1)
		e.detachHandled = true
	}
	return e.doIngest(distImporter)
}

func (*IngestIntoExec) doIngest(distImporter *loaddata.DistImporter) error {
	distImporter.Import()
	group := distImporter.Param().Group
	return group.Wait()
}
