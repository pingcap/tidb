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
	"github.com/pingcap/tidb/pkg/infoschema"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// QuerySource is the SELECT of IMPORT INTO ... FROM SELECT as the source of an
// import: what a worker needs to run the same query as the submitter, captured
// after tenant privilege checks.
type QuerySource struct {
	ReadTS        uint64
	SchemaVersion int64
	// Timestamp is the Unix time in seconds when the query is captured, which
	// the encoder takes as the timestamp of the query chunks.
	Timestamp int64
	// Tables lists the included source tables.
	Tables []SourceTable
	// SessionStates are the states of the submitting session.
	SessionStates *sessionstates.SessionStates
}

// SourceTable identifies a source table definition of an import query.
type SourceTable struct {
	DBID     int64
	ID       int64
	Revision uint64
}

// importQueryWorkerVars are decided by the worker instead of the submitter:
// the snapshot and transaction it reads in, and its memory budget.
var importQueryWorkerVars = map[string]struct{}{
	vardef.TiDBSnapshot:             {},
	vardef.TiDBReadStaleness:        {},
	vardef.TiDBTxnReadTS:            {},
	vardef.TiDBEnableExternalTSRead: {},
	vardef.AutoCommit:               {},
	vardef.TiDBTxnMode:              {},
	vardef.TxnIsolation:             {},
	vardef.TransactionIsolation:     {},
	vardef.TxReadOnly:               {},
	vardef.TransactionReadOnly:      {},
	vardef.TiDBMemQuotaQuery:        {},
}

// RestoreStates restores the captured session states into the worker session
// sctx, except the variables in importQueryWorkerVars.
func (q *QuerySource) RestoreStates(ctx context.Context, sctx sessionctx.Context) error {
	vars := sctx.GetSessionVars()
	states := *q.SessionStates
	states.SystemVars = make(map[string]string, len(q.SessionStates.SystemVars))
	for name, value := range q.SessionStates.SystemVars {
		if _, ok := importQueryWorkerVars[name]; ok {
			continue
		}
		// Internal session variables cannot be set in a session.
		if sv := variable.GetSysVar(name); sv != nil && sv.InternalSessionVariable {
			continue
		}
		states.SystemVars[name] = value
	}
	// The status tells whether the submitter is in a transaction, which the
	// worker's session is not, and the warnings belong to the statements of the
	// submitter before.
	states.Status = uint32(vars.Status())
	states.Warnings = nil
	return sctx.DecodeStates(ctx, &states)
}

// BuildMinimalInfoSchema builds an info schema that holds only the source
// tables at q.ReadTS, and fails if a table changed after the submitter resolved
// the query.
func (q *QuerySource) BuildMinimalInfoSchema(
	store tidbkv.Storage, parent infoschema.InfoSchema,
) (infoschema.InfoSchema, error) {
	m := meta.NewReader(store.GetSnapshot(tidbkv.NewVersion(q.ReadTS)))
	dbByID := make(map[int64]*model.DBInfo)
	dbs := make([]*model.DBInfo, 0, 1)
	for _, src := range q.Tables {
		db := dbByID[src.DBID]
		if db == nil {
			var err error
			if db, err = m.GetDatabase(src.DBID); err != nil {
				return nil, err
			}
			if db == nil {
				return nil, errors.Errorf("source database %d not found at the import query snapshot", src.DBID)
			}
			dbByID[src.DBID] = db
			dbs = append(dbs, db)
		}
		tbl, err := m.GetTable(src.DBID, src.ID)
		if err != nil {
			return nil, err
		}
		// The submitter resolved the query with this revision.
		if tbl == nil || tbl.Revision != src.Revision {
			return nil, errors.Errorf("source table %d changed before the import query snapshot", src.ID)
		}
		infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbl)
		infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
		tbl.DBID = db.ID
		tbl.TableCacheStatusType = model.TableCacheStatusDisable
		db.Deprecated.Tables = append(db.Deprecated.Tables, tbl)
	}
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), nil, nil, q.SchemaVersion); err != nil {
		return nil, err
	}
	return b.Build(q.ReadTS), nil
}

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
		Timestamp:     time.Now().Unix(),
		SessionStates: sessionStates,
		SchemaVersion: is.SchemaMetaVersion(),
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
