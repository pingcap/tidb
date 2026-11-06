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

package importer

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/importinto"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// QuerySource is the shared captured query description.
type QuerySource = importinto.QuerySource

// SourceTable identifies a source-table revision.
type SourceTable = importinto.SourceTable

// newQueryParser returns a parser with the parser config of the session, like
// the one the planner parses statements and view definitions with.
func newQueryParser(sctx sessionctx.Context) *parser.Parser {
	p := parser.New()
	p.SetParserConfig(sctx.GetSessionVars().BuildParserConfig())
	return p
}

// ParseImportQuery parses an IMPORT INTO ... FROM SELECT statement, and returns
// the statement and its SELECT.
func ParseImportQuery(sctx sessionctx.Context, sql string) (*ast.ImportIntoStmt, ast.StmtNode, error) {
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

// CaptureQuerySource captures the SELECT of the IMPORT INTO statement sql that
// sctx is running. It must run after the statement is planned and before sctx
// runs another statement, since it takes the source tables that the planner
// records in the statement context.
func CaptureQuerySource(sctx sessionctx.Context, sql string) (*QuerySource, error) {
	_, node, err := ParseImportQuery(sctx, sql)
	if err != nil {
		return nil, err
	}
	vars := sctx.GetSessionVars()
	if vars.StmtCtx.IsStaleness {
		return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("stale reads in IMPORT INTO FROM SELECT")
	}
	// The planner does not resolve the sequences of sequence functions.
	if hasSequenceFunc(node) {
		return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("sequences in IMPORT INTO FROM SELECT")
	}

	sessionStates, err := storeQueryStates(sctx)
	if err != nil {
		return nil, err
	}
	txnManager := sessiontxn.GetTxnManager(sctx)
	is := txnManager.GetTxnInfoSchema()
	q := &QuerySource{
		EncodeTimestamp:    time.Now().Unix(),
		SessionStates:      sessionStates,
		SchemaVersion:      is.SchemaMetaVersion(),
		InMViewMaintenance: sctx.GetSessionVars().InMViewMaintenance,
	}
	// Like the plan cache, take the tables that the planner resolved for the
	// statement, including those in the views it expands.
	ids := slices.Sorted(maps.Keys(vars.StmtCtx.RelatedTableIDs))
	for _, id := range ids {
		tbl, ok := is.TableByID(context.Background(), id)
		if !ok {
			return nil, errors.Errorf("source table %d not found in the info schema", id)
		}
		if err := checkQuerySourceTable(tbl); err != nil {
			return nil, err
		}
		tblInfo := tbl.Meta()
		q.Tables = append(q.Tables, SourceTable{DBID: tblInfo.DBID, ID: tblInfo.ID, Revision: tblInfo.Revision})
	}
	q.ReadTS, err = txnManager.GetStmtReadTS()
	return q, err
}

// PlanImportQueryForTest plans the SELECT of the IMPORT INTO statement sql in
// sctx like the statement does, so that CaptureQuerySource can capture it.
func PlanImportQueryForTest(sctx sessionctx.Context, sql string) error {
	stmt, node, err := ParseImportQuery(sctx, sql)
	if err != nil {
		return err
	}
	// Each statement starts with no related tables.
	clear(sctx.GetSessionVars().StmtCtx.RelatedTableIDs)
	// Preprocess the IMPORT INTO statement as it is, then plan its SELECT.
	stmtW := resolve.NewNodeW(stmt)
	ret := &plannercore.PreprocessorReturn{}
	if err := plannercore.Preprocess(context.Background(), sctx, stmtW,
		plannercore.WithPreprocessorReturn(ret), plannercore.InitTxnContextProvider); err != nil {
		return err
	}
	_, err = plannercore.BuildLogicalPlanForTest(context.Background(), sctx,
		resolve.NewNodeWWithCtx(node, stmtW.GetResolveContext()), ret.InfoSchema)
	return err
}

// hasSequenceFunc returns whether node calls a sequence function.
func hasSequenceFunc(node ast.Node) bool {
	v := &sequenceFuncFinder{}
	node.Accept(v)
	return v.found
}

type sequenceFuncFinder struct {
	found bool
}

func (v *sequenceFuncFinder) Enter(n ast.Node) (ast.Node, bool) {
	if f, ok := n.(*ast.FuncCallExpr); ok {
		switch f.FnName.L {
		case ast.NextVal, ast.LastVal, ast.SetVal:
			v.found = true
		}
	}
	return n, v.found
}

func (*sequenceFuncFinder) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// storeQueryStates encodes the states of the submitting session for the worker.
func storeQueryStates(sctx sessionctx.Context) (*sessionstates.SessionStates, error) {
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
// same way as the submitter, including temporary tables, sequences, and virtual tables.
func checkQuerySourceTable(tbl table.Table) error {
	tblInfo := tbl.Meta()
	var kind string
	switch {
	case tblInfo.TempTableType != model.TempTableNone:
		kind = "temporary source tables"
	case tbl.Type().IsVirtualTable():
		kind = "virtual source tables"
	case tblInfo.IsSequence():
		kind = "sequences"
	default:
		return nil
	}
	return plannererrors.ErrNotSupportedYet.GenWithStackByArgs(kind + " in IMPORT INTO FROM SELECT")
}
