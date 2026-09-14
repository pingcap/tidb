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
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

func init() { importer.RunImportQuery = runImportQuery }

func parseImportQuery(sctx sessionctx.Context, sql string) (ast.StmtNode, error) {
	p := parser.New()
	p.SetSQLMode(sctx.GetSessionVars().SQLMode)
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	node, err := p.ParseOneStmt(sql, charset, collation)
	if err != nil {
		return nil, err
	}

	stmt, ok := node.(*ast.ImportIntoStmt)
	if !ok {
		return nil, errors.New("import query requires a SELECT")
	}

	node, ok = stmt.Select.(ast.StmtNode)
	if !ok {
		return nil, errors.New("import query requires a SELECT")
	}

	return node, nil
}

// CaptureImportQuery records the original SQL and source metadata after privilege checks.
func CaptureImportQuery(sctx sessionctx.Context, sql string) (*importer.QueryPlan, error) {
	node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}
	checker := &importQueryChecker{}
	if !ast.Walk(node, checker) {
		return nil, checker.err
	}

	q := &importer.QueryPlan{
		CurrentDB:   sctx.GetSessionVars().CurrentDB,
		Timestamp:   time.Now().Unix(),
		Keyspace:    sctx.GetStore().GetKeyspace(),
		SQL:         sql,
		SessionVars: make(map[string]string),
		Databases:   make(map[int64]*model.DBInfo),
		Tables:      make(map[int64][]*model.TableInfo),
	}
	vars := sctx.GetSessionVars()
	for _, name := range []string{
		vardef.SQLModeVar, vardef.TimeZone, vardef.TiDBDistSQLScanConcurrency,
		vardef.CharacterSetClient, vardef.CharacterSetConnection,
		vardef.CharacterSetResults, vardef.CollationConnection,
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
	nodeW := resolve.NewNodeW(node)
	ret := &plannercore.PreprocessorReturn{
		InfoSchema: sctx.GetLatestInfoSchema().(infoschema.InfoSchema),
	}
	if err := plannercore.Preprocess(
		context.Background(), sctx, nodeW,
		plannercore.WithPreprocessorReturn(ret),
	); err != nil {
		return nil, err
	}
	if ret.IsStaleness {
		return nil, errors.New("import query does not support stale reads")
	}

	seen := make(map[int64]bool)
	for _, tableName := range nodeW.GetResolveContext().GetTableNames() {
		tblInfo, dbInfo := tableName.TableInfo, tableName.DBInfo
		if tblInfo.IsView() ||
			tblInfo.TempTableType != model.TempTableNone ||
			tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
			return nil, errors.New("import query requires persistent, uncached source tables")
		}
		if !seen[tblInfo.ID] {
			seen[tblInfo.ID] = true
			q.Databases[dbInfo.ID] = dbInfo
			q.Tables[dbInfo.ID] = append(q.Tables[dbInfo.ID], tblInfo)
		}
	}
	return q, nil
}

type importQueryChecker struct {
	err error
}

func (*importQueryChecker) Enter(ast.Node) bool { return false }

func (c *importQueryChecker) Leave(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.VariableExpr:
		c.err = errors.New("import query does not support variables")
	case *ast.Join:
		// A single-table FROM also has a Join node, with no right side.
		if n.Right != nil {
			c.err = errors.New("import query does not support JOIN")
		}
	case *ast.WithClause:
		c.err = errors.New("import query does not support CTE")
	case *ast.OrderByClause:
		c.err = errors.New("import query does not support ORDER BY")
	}
	return c.err == nil
}

func runImportQuery(
	ctx context.Context, q *importer.QueryPlan, runtime importer.QueryRuntime,
	output chan<- importer.QueryChunk,
) (err error) {
	workerSession, node, err := newImportQuerySession(ctx, runtime.Session, q, runtime.TotalMemoryLimit, runtime.SessionPool)
	if err != nil {
		return err
	}
	defer workerSession.domain.Close()

	vars := workerSession.GetSessionVars()
	stmt, err := (&Compiler{Ctx: workerSession}).Compile(ctx, node)
	if err != nil {
		return err
	}
	p, ok := stmt.Plan.(base.PhysicalPlan)
	if !ok {
		return errors.New("import query did not produce a physical SELECT plan")
	}
	failpoint.InjectCall("afterImportQueryOptimize", p)
	failpoint.Inject("failAfterImportQueryOptimize", func() {
		failpoint.Return(errors.New("injected failure after import query optimization"))
	})
	spillOption := &builder.HashAggSpill{
		Storage: runtime.Storage, Prefix: runtime.Prefix, MemoryLimit: runtime.MemoryLimit,
	}
	b := newExecutorBuilder(ctx, workerSession, workerSession.schema, nil, spillOption)
	b.forDataReaderBuilder = true
	b.dataReaderTS = vars.SnapshotTS
	e := b.build(p)
	if b.err != nil {
		return b.err
	}
	if e == nil {
		return errors.New("import query built no executor")
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
	var rowID int64
	for {
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
