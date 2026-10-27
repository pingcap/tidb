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
	"github.com/pingcap/tidb/pkg/meta/metadef"
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

// newQueryParser returns a parser configured like the one that the planner
// parses a view definition with.
func newQueryParser(sctx sessionctx.Context) *parser.Parser {
	p := parser.New()
	p.SetParserConfig(sctx.GetSessionVars().BuildParserConfig())
	return p
}

// parseViewDefinition parses a view definition like the planner does when it
// expands the view: a stored definition does not depend on the SQL mode of the
// session that reads it.
func parseViewDefinition(sctx sessionctx.Context, sql string) (ast.StmtNode, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	return newQueryParser(sctx).ParseOneStmt(sql, charset, collation)
}

func parseImportQuery(sctx sessionctx.Context, sql string) (*ast.ImportIntoStmt, ast.StmtNode, error) {
	p := newQueryParser(sctx)
	p.SetSQLMode(sctx.GetSessionVars().SQLMode)
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	node, err := p.ParseOneStmt(sql, charset, collation)
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
func GenerateImportQueryPlan(sctx sessionctx.Context, sql string) (*importer.QueryPlan, error) {
	_, node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}

	sessionStates, err := encodeImportQueryStates(sctx)
	if err != nil {
		return nil, err
	}

	txnManager := sessiontxn.GetTxnManager(sctx)
	q := &importer.QueryPlan{
		Timestamp:     time.Now().Unix(),
		Keyspace:      sctx.GetStore().GetKeyspace(),
		SessionStates: sessionStates,
		SchemaVersion: txnManager.GetTxnInfoSchema().SchemaMetaVersion(),
	}
	if err := collectQueryTables(
		sctx, txnManager.GetTxnInfoSchema(),
		node, q, make(map[int64]bool)); err != nil {
		return nil, err
	}

	q.ReadTS, err = txnManager.GetStmtReadTS()
	return q, err
}

// encodeImportQueryStates encodes the states of the submitting session for the worker.
func encodeImportQueryStates(sctx sessionctx.Context) (*sessionstates.SessionStates, error) {
	states := &sessionstates.SessionStates{}
	if err := sctx.EncodeStates(context.Background(), states); err != nil {
		return nil, err
	}
	vars := sctx.GetSessionVars()
	timestamp, err := vars.GetSessionOrGlobalSystemVar(context.Background(), vardef.Timestamp)
	if err != nil {
		return nil, err
	}
	states.SystemVars[vardef.Timestamp] = timestamp
	states.ResourceGroupName = vars.StmtCtx.ResourceGroupName

	return states, nil
}

// checkQuerySourceTable rejects a source table that a worker cannot read the
// same way as the submitter, including temporary tables, sequences, and memtable.
func checkQuerySourceTable(dbInfo *model.DBInfo, tblInfo *model.TableInfo) error {
	var kind string
	switch {
	case tblInfo.TempTableType != model.TempTableNone:
		kind = "temporary source tables"
	case metadef.IsMemDB(dbInfo.Name.L):
		kind = "memory source tables"
	case tblInfo.IsSequence():
		kind = "sequences"
	default:
		return nil
	}
	return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(kind + " in IMPORT INTO FROM SELECT")
}

// collectQueryTables collects tables from a resolved query.
func collectQueryTables(
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
		if err := checkQuerySourceTable(dbInfo, tblInfo); err != nil {
			return err
		}
		if seen[tblInfo.ID] {
			continue
		}
		seen[tblInfo.ID] = true
		q.Tables = append(q.Tables, importer.SourceTable{DBID: dbInfo.ID, ID: tblInfo.ID, Revision: tblInfo.Revision})

		if tblInfo.IsView() {
			viewNode, err := parseViewDefinition(sctx, tblInfo.View.SelectStmt)
			if err != nil {
				return err
			}
			if err := collectQueryTables(sctx, is, viewNode, q, seen); err != nil {
				return err
			}
		}
	}
	return nil
}
