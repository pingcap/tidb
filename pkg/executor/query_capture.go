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
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

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

// CaptureImportQuery records source metadata and read TS after privilege checks.
func CaptureImportQuery(sctx sessionctx.Context, sql string) (*importer.QueryPlan, error) {
	node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}
	checker := &importQueryVariableChecker{}
	if !ast.Walk(node, checker) {
		return nil, checker.err
	}

	q := &importer.QueryPlan{
		CurrentDB:   sctx.GetSessionVars().CurrentDB,
		Timestamp:   time.Now().Unix(),
		Keyspace:    sctx.GetStore().GetKeyspace(),
		SessionVars: make(map[string]string),
		Databases:   make(map[int64]*model.DBInfo),
		Tables:      make(map[int64][]*model.TableInfo),
	}
	vars := sctx.GetSessionVars()
	// Keep inherited planning/execution settings here; remote scan and TiFlash
	// concurrency are independent of the TiDB worker CPU allocation.
	for _, name := range []string{
		vardef.SQLModeVar, vardef.TiDBDistSQLScanConcurrency,
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
	txnManager := sessiontxn.GetTxnManager(sctx)
	nodeW := resolve.NewNodeW(node)
	ret := &plannercore.PreprocessorReturn{
		InfoSchema: txnManager.GetTxnInfoSchema(),
	}
	if err := plannercore.Preprocess(
		context.Background(), sctx, nodeW,
		plannercore.WithPreprocessorReturn(ret),
		plannercore.InImportInto,
	); err != nil {
		return nil, err
	}
	if ret.IsStaleness {
		return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("stale reads in IMPORT INTO FROM SELECT")
	}

	seen := make(map[int64]bool)
	for _, tableName := range nodeW.GetResolveContext().GetTableNames() {
		tblInfo, dbInfo := tableName.TableInfo, tableName.DBInfo
		if tblInfo.IsView() ||
			tblInfo.TempTableType != model.TempTableNone ||
			tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
			return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("views, temporary or cached source tables in IMPORT INTO FROM SELECT")
		}
		if !seen[tblInfo.ID] {
			seen[tblInfo.ID] = true
			q.Databases[dbInfo.ID] = dbInfo
			q.Tables[dbInfo.ID] = append(q.Tables[dbInfo.ID], tblInfo)
		}
	}
	q.ReadTS, err = txnManager.GetStmtReadTS()
	if err != nil {
		return nil, err
	}
	return q, nil
}

type importQueryVariableChecker struct {
	err error
}

func (*importQueryVariableChecker) Enter(ast.Node) bool { return false }

func (c *importQueryVariableChecker) Leave(node ast.Node) bool {
	if _, ok := node.(*ast.VariableExpr); ok {
		c.err = plannererrors.ErrNotSupportedYet.GenWithStackByArgs("variables in IMPORT INTO FROM SELECT")
	}
	return c.err == nil
}
