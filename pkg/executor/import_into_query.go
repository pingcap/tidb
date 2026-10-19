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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

func parseStmt(sctx sessionctx.Context, sql string) (ast.StmtNode, error) {
	p := parser.New()
	p.SetSQLMode(sctx.GetSessionVars().SQLMode)
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	return p.ParseOneStmt(sql, charset, collation)
}

// parseImportQuery parses an IMPORT INTO ... FROM SELECT statement and returns
// it together with its SELECT.
func parseImportQuery(sctx sessionctx.Context, sql string) (*ast.ImportIntoStmt, ast.StmtNode, error) {
	node, err := parseStmt(sctx, sql)
	if err != nil {
		return nil, nil, err
	}

	stmt, ok := node.(*ast.ImportIntoStmt)
	if !ok {
		return nil, nil, errors.New("import query requires a SELECT")
	}

	selectNode, ok := stmt.Select.(ast.StmtNode)
	if !ok {
		return nil, nil, errors.New("import query requires a SELECT")
	}

	return stmt, selectNode, nil
}

// GenerateImportQueryPlan generates the query plan with the given query.
// It will collect session vars, table metas and ts.
func GenerateImportQueryPlan(sctx sessionctx.Context, sql string) (*importer.QueryPlan, error) {
	_, node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}

	q := &importer.QueryPlan{
		Timestamp: time.Now().Unix(),
		Keyspace:  sctx.GetStore().GetKeyspace(),
	}
	if q.SessionStates, err = encodeImportQueryStates(sctx); err != nil {
		return nil, err
	}
	txnManager := sessiontxn.GetTxnManager(sctx)
	if err := collectImportQueryTables(
		sctx, txnManager.GetTxnInfoSchema(),
		node, q, make(map[int64]bool)); err != nil {
		return nil, err
	}
	q.SchemaVersion = txnManager.GetTxnInfoSchema().SchemaMetaVersion()
	q.ReadTS, err = txnManager.GetStmtReadTS()
	return q, err
}

// encodeImportQueryStates encodes the states of the submitting session for the
// worker to run the SELECT with. Variables it has not set are global ones,
// which the worker reads from the same keyspace.
func encodeImportQueryStates(sctx sessionctx.Context) (*sessionstates.SessionStates, error) {
	states := &sessionstates.SessionStates{}
	if err := sctx.EncodeStates(context.Background(), states); err != nil {
		return nil, err
	}
	vars := sctx.GetSessionVars()
	// Pin the statement time, which session states keep only when it is set.
	timestamp, err := vars.GetSessionOrGlobalSystemVar(context.Background(), vardef.Timestamp)
	if err != nil {
		return nil, err
	}
	states.SystemVars[vardef.Timestamp] = timestamp
	// The statement may use another resource group than the session by a hint.
	states.ResourceGroupName = vars.StmtCtx.ResourceGroupName
	return states, nil
}

// collectImportQueryTables records the source tables of a resolved query,
// including the tables behind its views, which the worker expands again.
func collectImportQueryTables(
	sctx sessionctx.Context, is infoschema.InfoSchema, node ast.StmtNode,
	q *importer.QueryPlan, seen map[int64]bool,
) error {
	nodeW := resolve.NewNodeW(node)
	ret := &plannercore.PreprocessorReturn{InfoSchema: is}
	if err := plannercore.Preprocess(
		context.Background(), sctx, nodeW,
		plannercore.WithPreprocessorReturn(ret),
		plannercore.InImportInto,
	); err != nil {
		return err
	}
	if ret.IsStaleness {
		return plannererrors.ErrNotSupportedYet.GenWithStackByArgs("stale reads in IMPORT INTO FROM SELECT")
	}

	for _, tableName := range nodeW.GetResolveContext().GetTableNames() {
		tblInfo, dbInfo := tableName.TableInfo, tableName.DBInfo
		if tblInfo.TempTableType != model.TempTableNone {
			return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(
				"temporary source tables in IMPORT INTO FROM SELECT")
		}
		if seen[tblInfo.ID] {
			continue
		}
		seen[tblInfo.ID] = true
		q.Tables = append(q.Tables, importer.QueryTable{DBID: dbInfo.ID, ID: tblInfo.ID, Revision: tblInfo.Revision})

		// Parse view recursively
		if tblInfo.IsView() {
			viewNode, err := parseStmt(sctx, tblInfo.View.SelectStmt)
			if err != nil {
				return err
			}
			if err := collectImportQueryTables(sctx, is, viewNode, q, seen); err != nil {
				return err
			}
		}
	}
	return nil
}
