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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

func init() { importer.RunImportQuery = runImportQuery }

func parseImportQuery(sctx sessionctx.Context, sql string) (ast.StmtNode, error) {
	pa := parser.New()
	pa.SetSQLMode(sctx.GetSessionVars().SQLMode)
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	node, err := pa.ParseOneStmt(sql, charset, collation)
	if err != nil {
		return nil, err
	}
	if stmt, ok := node.(*ast.ImportIntoStmt); ok {
		var valid bool
		node, valid = stmt.Select.(ast.StmtNode)
		if !valid {
			return nil, errors.New("import query requires a SELECT")
		}
	}
	if _, ok := node.(ast.ResultSetNode); !ok {
		return nil, errors.New("import query requires a SELECT")
	}
	return node, nil
}

// CaptureImportQuery records the SELECT and source metadata after privilege
// checks. Optimization runs on the worker using its own statistics snapshot.
func CaptureImportQuery(sctx sessionctx.Context, sql string) (*importer.QueryPlan, error) {
	node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}
	q := &importer.QueryPlan{
		MemoryQuota: sctx.GetSessionVars().MemQuotaQuery,
		CurrentDB:   sctx.GetSessionVars().CurrentDB,
		Timestamp:   time.Now().Unix(),
		Keyspace:    sctx.GetStore().GetKeyspace(),
		// Preserve the original literals, charset introducers and escaping.
		// If this is an IMPORT statement, the worker extracts only its SELECT.
		SQL:         sql,
		SessionVars: make(map[string]string),
		Tables:      make(map[int64][]*model.TableInfo),
	}
	vars := sctx.GetSessionVars()
	for _, name := range []string{
		vardef.SQLModeVar, vardef.TimeZone, vardef.TiDBDistSQLScanConcurrency,
		vardef.CharacterSetClient, vardef.CharacterSetConnection, vardef.CharacterSetResults, vardef.CollationConnection,
		vardef.TiDBHashAggPartialConcurrency, vardef.TiDBHashAggFinalConcurrency,
		vardef.TiDBEnableVectorizedExpression, vardef.TiDBMaxTiFlashThreads,
		vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy,
		vardef.TiDBMaxBytesBeforeTiFlashExternalSort, vardef.TiFlashMemQuotaQueryPerNode,
		vardef.TiFlashQuerySpillRatio, vardef.TiFlashHashJoinVersion,
		vardef.TiFlashFineGrainedShuffleStreamCount, vardef.TiFlashFineGrainedShuffleBatchSize,
		vardef.TiDBIsolationReadEngines, vardef.TiDBAllowMPPExecution, vardef.TiDBEnforceMPPExecution,
	} {
		value, err := vars.GetSessionOrGlobalSystemVar(context.Background(), name)
		if err != nil {
			return nil, err
		}
		q.SessionVars[name] = value
	}
	q.SessionVars[vardef.TimeZone] = timeutil.ZoneName(vars.Location())
	q.PushDownFlags = vars.StmtCtx.PushDownFlags()
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	validator := &importQueryValidator{}
	ast.Walk(node, validator)
	if validator.err != nil {
		return nil, validator.err
	}
	dbs := make(map[int64]*model.DBInfo)
	seen := make(map[int64]bool)
	for _, n := range bindinfo.CollectTableNames(node) {
		name := n.Schema
		if name.L == "" {
			name = ast.NewCIStr(q.CurrentDB)
		}
		tbl, err := is.TableByName(context.Background(), name, n.Name)
		if err != nil {
			return nil, err
		}
		meta := tbl.Meta()
		if meta.IsView() || meta.TempTableType != model.TempTableNone || meta.TableCacheStatusType != model.TableCacheStatusDisable {
			return nil, errors.New("import query requires persistent, uncached source tables")
		}
		if seen[meta.ID] {
			continue
		}
		seen[meta.ID] = true
		db, ok := is.SchemaByName(name)
		if !ok {
			return nil, errors.New("import query source database disappeared")
		}
		captured := dbs[db.ID]
		if captured == nil {
			captured = db.Clone()
			captured.Deprecated.Tables = nil
			dbs[db.ID] = captured
			q.Databases = append(q.Databases, captured)
		}
		copy := meta.Clone()
		copy.DBID = db.ID
		q.Tables[db.ID] = append(q.Tables[db.ID], copy)
	}
	return q, nil
}

type importQueryValidator struct{ err error }

func (v *importQueryValidator) Enter(node ast.Node) bool {
	switch node.(type) {
	case *ast.WithClause, *ast.VariableExpr, ast.ParamMarkerExpr:
		v.err = errors.New("import query does not support CTEs, variables or parameters")
	}
	return v.err != nil
}

func (v *importQueryValidator) Leave(ast.Node) bool { return v.err == nil }

func runImportQuery(
	ctx context.Context, q *importer.QueryPlan, runtime importer.QueryRuntime, output chan<- importer.QueryChunk,
) (err error) {
	var opened exec.Executor
	defer func() {
		if r := recover(); r != nil {
			err = tidbutil.GetRecoverError(r)
		}
		if opened != nil {
			if closeErr := exec.Close(opened); err == nil {
				err = closeErr
			}
		}
	}()
	workerSession, node, err := newImportQuerySession(ctx, q, &runtime)
	if err != nil {
		return err
	}
	vars := workerSession.GetSessionVars()
	totalLimit := runtime.TotalMemoryLimit
	stmt, err := (&Compiler{Ctx: workerSession}).Compile(ctx, node)
	if err != nil {
		return err
	}
	// SET_VAR hints must not enlarge the budget allocated by DXF.
	if vars.MemQuotaQuery <= 0 || vars.MemQuotaQuery > totalLimit {
		vars.MemQuotaQuery = totalLimit
	}
	vars.MemTracker.SetBytesLimit(vars.MemQuotaQuery)
	p, ok := stmt.Plan.(base.PhysicalPlan)
	if !ok {
		return errors.New("import query did not produce a physical SELECT plan")
	}
	failpoint.InjectCall("afterImportQueryOptimize", p)
	failpoint.Inject("failAfterImportQueryOptimize", func() {
		failpoint.Return(errors.New("injected failure after import query optimization"))
	})
	b := newExecutorBuilder(ctx, workerSession, workerSession.schema, nil)
	b.forDataReaderBuilder, b.dataReaderTS = true, vars.SnapshotTS
	e := b.build(p)
	if b.err != nil {
		return b.err
	}
	if e == nil {
		return errors.New("import query built no executor")
	}
	opened = e
	if err := exec.Open(ctx, e); err != nil {
		return err
	}
	fields := e.RetFieldTypes()
	var rowID int64
	for {
		// Encoding consumes each chunk asynchronously, so no session chunk pool.
		chk := chunk.New(fields, 32, vars.MaxChunkSize)
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
