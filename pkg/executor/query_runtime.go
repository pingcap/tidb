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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func init() { importer.RunImportQuery = runImportQuery }

func runImportQuery(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QueryPlan, sql string, memoryLimit int64,
	output chan<- importer.QueryChunk,
) (err error) {
	querySession, node, err := newImportQuerySession(ctx, sctx, q, sql)
	if err != nil {
		return err
	}

	e, err := buildImportQueryExecutor(ctx, querySession, node, memoryLimit)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := exec.Close(e); err == nil {
			err = closeErr
		}
	}()
	if err := exec.Open(ctx, e); err != nil {
		return err
	}

	fields := e.RetFieldTypes()
	maxChunkSize := querySession.GetSessionVars().MaxChunkSize
	var rowID int64
	for {
		chk := chunk.New(fields, 32, maxChunkSize)
		if err := exec.Next(ctx, e, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
		select {
		case output <- importer.QueryChunk{Fields: fields, Chk: chk, RowIDOffset: rowID}:
			rowID += int64(chk.NumRows())
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func buildImportQueryExecutor(
	ctx context.Context, querySession *importQuerySession, node ast.StmtNode, memoryLimit int64,
) (exec.Executor, error) {
	stmt, err := (&Compiler{Ctx: querySession}).Compile(ctx, node)
	if err != nil {
		return nil, err
	}
	// Apply the worker budget after compilation, which may apply a
	// SET_VAR(tidb_mem_quota_query) hint.
	vars := querySession.GetSessionVars()
	vars.MemQuotaQuery = memoryLimit
	vars.MemTracker.SetBytesLimit(memoryLimit)
	p, ok := stmt.Plan.(base.PhysicalPlan)
	if !ok {
		return nil, errors.New("import query did not produce a physical SELECT plan")
	}
	failpoint.InjectCall("afterImportQueryOptimize", p)
	b := newExecutorBuilder(ctx, querySession, querySession.schema, nil)
	b.forDataReaderBuilder = true
	b.dataReaderTS = querySession.GetSessionVars().SnapshotTS
	e := b.build(p)
	if b.err != nil {
		return nil, b.err
	}
	return e, nil
}
