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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

func init() { importer.RunImportQuery = runImportQuery }

func runImportQuery(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QuerySource, importSQL string, memQuota int64,
	output chan<- importer.QueryChunk,
) (err error) {
	e, err := buildImportQueryExecutor(ctx, sctx, q, importSQL, memQuota)
	if err != nil {
		return err
	}
	// Some executors only stop on the kill signal of the session.
	stop := context.AfterFunc(ctx, func() {
		sctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	})
	defer stop()
	defer func() {
		if closeErr := exec.Close(e); err == nil {
			err = closeErr
		}
	}()
	if err := exec.Open(ctx, e); err != nil {
		return err
	}

	return sendQueryChunks(ctx, e, output)
}

// sendQueryChunks sends the rows of e to output in chunks, which the encoders
// own after receiving them.
func sendQueryChunks(ctx context.Context, e exec.Executor, output chan<- importer.QueryChunk) error {
	fields := e.RetFieldTypes()
	var rowID int64
	chk := exec.NewFirstChunk(e)
	for {
		if err := exec.Next(ctx, e, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
		select {
		case output <- importer.QueryChunk{Fields: fields, Chk: chk, RowIDOffset: rowID}:
			rowID += int64(chk.NumRows())
			chk = chunk.Renew(chk, e.MaxChunkSize())
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// prepareImportQuery prepares sctx to run the SELECT of an import query at
// q.ReadTS, and returns the info schema that holds only the source tables at
// q.ReadTS and the SELECT to run.
func prepareImportQuery(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QuerySource, importSQL string,
) (infoschema.InfoSchema, ast.StmtNode, error) {
	vars := sctx.GetSessionVars()
	if err := q.RestoreStates(ctx, sctx); err != nil {
		return nil, nil, err
	}
	// Without a statistics provider, this worker uses static partition pruning.
	vars.PartitionPruneMode.Store("static")
	vars.InMViewMaintenance = q.InMViewMaintenance
	vars.InRestrictedSQL = true
	vars.InternalSQLScanUserTable = false
	vars.RequestSourceType = kv.InternalDistTask
	is, err := q.BuildMinimalInfoSchema(sctx.GetStore(), sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return nil, nil, err
	}
	importStmt, node, err := importer.ParseImportQuery(sctx, importSQL)
	if err != nil {
		return nil, nil, err
	}
	// Reset with the IMPORT INTO statement, so the SELECT is evaluated under the
	// same statement flags as classic IMPORT INTO.
	if err := ResetContextOfStmt(sctx, importStmt); err != nil {
		return nil, nil, err
	}
	// The statement reads the minimal info schema through the provider. The
	// InfoCache of the session is kept, so the few debug builtin functions that
	// read the latest schema, such as TIDB_ENCODE_RECORD_KEY, see the latest
	// schema of the runtime instead of the source tables at q.ReadTS.
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(
		ctx, &sessiontxn.EnterNewTxnRequest{
			Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
			Provider: staleread.NewStalenessTxnContextProvider(sctx, q.ReadTS, is),
		},
	); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS = q.ReadTS
	vars.SnapshotInfoschema = is
	return is, node, nil
}

func buildImportQueryExecutor(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QuerySource, importSQL string, memQuota int64,
) (exec.Executor, error) {
	is, node, err := prepareImportQuery(ctx, sctx, q, importSQL)
	if err != nil {
		return nil, err
	}
	stmt, err := (&Compiler{Ctx: sctx}).Compile(ctx, node)
	if err != nil {
		return nil, err
	}
	// Apply the quota after compilation, which may apply SET_VAR(tidb_mem_quota_query) hint.
	vars := sctx.GetSessionVars()
	vars.MemQuotaQuery = memQuota
	vars.MemTracker.SetBytesLimit(memQuota)
	p, ok := stmt.Plan.(base.PhysicalPlan)
	if !ok {
		return nil, errors.New("import query did not produce a physical SELECT plan")
	}
	failpoint.InjectCall("afterImportQueryOptimize", p)
	b := newExecutorBuilder(ctx, sctx, is, nil)
	e := b.build(p)
	if b.err != nil {
		return nil, b.err
	}
	return e, nil
}
