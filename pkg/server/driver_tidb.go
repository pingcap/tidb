// Copyright 2015 PingCAP, Inc.
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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
)

// TiDBDriver implements IDriver.
type TiDBDriver struct {
	store kv.Storage
}

// NewTiDBDriver creates a new TiDBDriver.
func NewTiDBDriver(store kv.Storage) *TiDBDriver {
	driver := &TiDBDriver{
		store: store,
	}
	return driver
}

// TiDBContext implements QueryCtx.
type TiDBContext struct {
	sessiontypes.Session
	stmts map[int]*TiDBStatement
}

// TiDBStatement implements PreparedStatement.
type TiDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *TiDBContext
	// this result set should have been closed before stored here. Only the `rowIterator` are used here. This field is
	// not moved out to reuse the logic inside functions `writeResultSet...`
	// TODO: move the `fetchedRows` into the statement, and remove the `ResultSet` from statement.
	rs resultset.CursorResultSet
	// the `rowContainer` should contain all pre-fetched results of the statement in `EXECUTE` command.
	// it's stored here to be closed in RESET and CLOSE command
	rowContainer *chunk.RowContainer
	sql          string

	hasActiveCursor bool
}

// ID implements PreparedStatement ID method.
func (ts *TiDBStatement) ID() int {
	return int(ts.id)
}

// Execute implements PreparedStatement Execute method.
func (ts *TiDBStatement) Execute(ctx context.Context, args []expression.Expression) (rs resultset.ResultSet, err error) {
	tidbRecordset, err := ts.ctx.ExecutePreparedStmt(ctx, ts.id, args)
	if err != nil {
		return nil, err
	}
	if tidbRecordset == nil {
		return
	}
	rs = resultset.New(tidbRecordset, ts.ctx.GetSessionVars().PreparedStmts[ts.id].(*core.PlanCacheStmt))
	return
}

// AppendParam implements PreparedStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return mysql.NewErr(mysql.ErrWrongArguments, "stmt_send_longdata")
	}
	// If len(data) is 0, append an empty byte slice to the end to distinguish no data and no parameter.
	if len(data) == 0 {
		ts.boundParams[paramID] = []byte{}
	} else {
		ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	}
	return nil
}

// NumParams implements PreparedStatement NumParams method.
func (ts *TiDBStatement) NumParams() int {
	return ts.numParams
}

// BoundParams implements PreparedStatement BoundParams method.
func (ts *TiDBStatement) BoundParams() [][]byte {
	return ts.boundParams
}

// SetParamsType implements PreparedStatement SetParamsType method.
func (ts *TiDBStatement) SetParamsType(paramsType []byte) {
	ts.paramsType = paramsType
}

// GetParamsType implements PreparedStatement GetParamsType method.
func (ts *TiDBStatement) GetParamsType() []byte {
	return ts.paramsType
}

// StoreResultSet stores ResultSet for stmt fetching
func (ts *TiDBStatement) StoreResultSet(rs resultset.CursorResultSet) {
	// the original reset set should have been closed, and it's only used to store the iterator through the rowContainer
	// so it's fine to just overwrite it.
	ts.rs = rs
}

// GetResultSet gets ResultSet associated this statement
func (ts *TiDBStatement) GetResultSet() resultset.CursorResultSet {
	return ts.rs
}

// Reset implements PreparedStatement Reset method.
func (ts *TiDBStatement) Reset() error {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}
	ts.hasActiveCursor = false

	if ts.rs != nil && ts.rs.GetRowContainerReader() != nil {
		ts.rs.GetRowContainerReader().Close()
	}
	ts.rs = nil

	if ts.rowContainer != nil {
		ts.rowContainer.GetMemTracker().Detach()
		ts.rowContainer.GetDiskTracker().Detach()

		rc := ts.rowContainer
		ts.rowContainer = nil

		err := rc.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Close implements PreparedStatement Close method.
func (ts *TiDBStatement) Close() error {
	if ts.rs != nil && ts.rs.GetRowContainerReader() != nil {
		ts.rs.GetRowContainerReader().Close()
	}

	if ts.rowContainer != nil {
		ts.rowContainer.GetMemTracker().Detach()
		ts.rowContainer.GetDiskTracker().Detach()

		err := ts.rowContainer.Close()
		if err != nil {
			return err
		}
	}

	// TODO close at tidb level
	if ts.ctx.GetSessionVars().TxnCtx != nil && ts.ctx.GetSessionVars().TxnCtx.CouldRetry {
		err := ts.ctx.DropPreparedStmt(ts.id)
		if err != nil {
			return err
		}
	} else {
		if ts.ctx.GetSessionVars().EnablePreparedPlanCache {
			preparedPointer := ts.ctx.GetSessionVars().PreparedStmts[ts.id]
			preparedObj, ok := preparedPointer.(*core.PlanCacheStmt)
			if !ok {
				return errors.Errorf("invalid PlanCacheStmt type")
			}
			bindSQL, _ := bindinfo.MatchSQLBindingForPlanCache(ts.ctx, preparedObj.PreparedAst.Stmt, &preparedObj.BindingInfo)
			cacheKey, err := core.NewPlanCacheKey(ts.ctx.GetSessionVars(), preparedObj.StmtText, preparedObj.StmtDB,
				preparedObj.SchemaVersion, 0, bindSQL, expression.ExprPushDownBlackListReloadTimeStamp.Load(), preparedObj.RelateVersion)
			if err != nil {
				return err
			}
			if !ts.ctx.GetSessionVars().IgnorePreparedCacheCloseStmt { // keep the plan in cache
				ts.ctx.GetSessionPlanCache().Delete(cacheKey)
			}
		}
		ts.ctx.GetSessionVars().RemovePreparedStmt(ts.id)
	}
	delete(ts.ctx.stmts, int(ts.id))
	return nil
}

// GetCursorActive implements PreparedStatement GetCursorActive method.
func (ts *TiDBStatement) GetCursorActive() bool {
	return ts.hasActiveCursor
}

// SetCursorActive implements PreparedStatement SetCursorActive method.
func (ts *TiDBStatement) SetCursorActive(fetchEnd bool) {
	ts.hasActiveCursor = fetchEnd
}

// StoreRowContainer stores a row container into the prepared statement
func (ts *TiDBStatement) StoreRowContainer(c *chunk.RowContainer) {
	ts.rowContainer = c
}

// GetRowContainer returns the row container of the statement
func (ts *TiDBStatement) GetRowContainer() *chunk.RowContainer {
	return ts.rowContainer
}

// OpenCtx implements IDriver.
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, _ string,
	tlsState *tls.ConnectionState, extensions *extension.SessionExtensions) (*TiDBContext, error) {
	se, err := session.CreateSession(qd.store)
	if err != nil {
		return nil, err
	}
	se.SetTLSState(tlsState)
	err = se.SetCollation(int(collation))
	if err != nil {
		return nil, err
	}
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	se.SetSessionStatesHandler(sessionstates.StatePrepareStmt, tc)
	se.SetExtensions(extensions)
	return tc, nil
}

// GetWarnings implements QueryCtx GetWarnings method.
func (tc *TiDBContext) GetWarnings() []contextutil.SQLWarn {
	return tc.GetSessionVars().StmtCtx.GetWarnings()
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	return tc.GetSessionVars().StmtCtx.WarningCount()
}

func (tc *TiDBContext) checkSandBoxMode(stmt ast.StmtNode) error {
	if !tc.Session.GetSessionVars().InRestrictedSQL && tc.InSandBoxMode() {
		switch stmt.(type) {
		case *ast.SetPwdStmt, *ast.AlterUserStmt:
		default:
			return servererr.ErrMustChangePassword.GenWithStackByArgs()
		}
	}
	return nil
}

// ExecuteStmt implements QueryCtx interface.
func (tc *TiDBContext) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (resultset.ResultSet, error) {
	var rs sqlexec.RecordSet
	var err error
	if err = tc.checkSandBoxMode(stmt); err != nil {
		return nil, err
	}
	if s, ok := stmt.(*ast.NonTransactionalDMLStmt); ok {
		rs, err = session.HandleNonTransactionalDML(ctx, s, tc.Session)
	} else {
		rs, err = tc.Session.ExecuteStmt(ctx, stmt)
	}
	if err != nil {
		tc.Session.GetSessionVars().StmtCtx.AppendError(err)
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return resultset.New(rs, nil), nil
}

// Close implements QueryCtx Close method.
func (tc *TiDBContext) Close() error {
	// close PreparedStatement associated with this connection
	for _, v := range tc.stmts {
		terror.Call(v.Close)
	}

	tc.Session.Close()
	return nil
}

// FieldList implements QueryCtx FieldList method.
func (tc *TiDBContext) FieldList(table string) (columns []*column.Info, err error) {
	fields, err := tc.Session.FieldList(table)
	if err != nil {
		return nil, err
	}
	columns = make([]*column.Info, 0, len(fields))
	for _, f := range fields {
		columns = append(columns, column.ConvertColumnInfo(f))
	}
	return columns, nil
}

// GetStatement implements QueryCtx GetStatement method.
func (tc *TiDBContext) GetStatement(stmtID int) PreparedStatement {
	tcStmt := tc.stmts[stmtID]
	if tcStmt != nil {
		return tcStmt
	}
	return nil
}

// Prepare implements QueryCtx Prepare method.
func (tc *TiDBContext) Prepare(sql string) (statement PreparedStatement, columns, params []*column.Info, err error) {
	stmtID, paramCount, fields, err := tc.Session.PrepareStmt(sql)
	if err != nil {
		return
	}
	stmt := &TiDBStatement{
		sql:         sql,
		id:          stmtID,
		numParams:   paramCount,
		boundParams: make([][]byte, paramCount),
		ctx:         tc,
	}
	statement = stmt
	columns = make([]*column.Info, len(fields))
	for i := range fields {
		columns[i] = column.ConvertColumnInfo(fields[i])
	}
	params = make([]*column.Info, paramCount)
	for i := range params {
		params[i] = &column.Info{
			Type: mysql.TypeBlob,
		}
	}
	tc.stmts[int(stmtID)] = stmt
	return
}

// GetStmtStats implements the sessionctx.Context interface.
func (tc *TiDBContext) GetStmtStats() *stmtstats.StatementStats {
	return tc.Session.GetStmtStats()
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (tc *TiDBContext) EncodeSessionStates(_ context.Context, _ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	sessionVars := tc.Session.GetSessionVars()
	sessionStates.PreparedStmts = make(map[uint32]*sessionstates.PreparedStmtInfo, len(sessionVars.PreparedStmts))
	for preparedID, preparedObj := range sessionVars.PreparedStmts {
		preparedStmt, ok := preparedObj.(*core.PlanCacheStmt)
		if !ok {
			return errors.Errorf("invalid PlanCacheStmt type")
		}
		sessionStates.PreparedStmts[preparedID] = &sessionstates.PreparedStmtInfo{
			StmtText: preparedStmt.StmtText,
			StmtDB:   preparedStmt.StmtDB,
		}
	}
	for name, id := range sessionVars.PreparedStmtNameToID {
		// Only text protocol statements have names.
		if preparedStmtInfo, ok := sessionStates.PreparedStmts[id]; ok {
			preparedStmtInfo.Name = name
		}
	}
	for id, stmt := range tc.stmts {
		// Only binary protocol statements have paramTypes.
		preparedStmtInfo, ok := sessionStates.PreparedStmts[uint32(id)]
		if !ok {
			return errors.Errorf("prepared statement %d not found", id)
		}
		// Bound params are sent by CMD_STMT_SEND_LONG_DATA, the proxy can wait for COM_STMT_EXECUTE.
		for _, boundParam := range stmt.BoundParams() {
			if boundParam != nil {
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("prepared statements have bound params")
			}
		}
		if stmt.GetCursorActive() {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("prepared statements have unfetched rows")
		}
		preparedStmtInfo.ParamTypes = stmt.GetParamsType()
	}
	return nil
}

// DecodeSessionStates implements SessionStatesHandler.DecodeSessionStates interface.
func (tc *TiDBContext) DecodeSessionStates(ctx context.Context, _ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	if len(sessionStates.PreparedStmts) == 0 {
		return nil
	}
	sessionVars := tc.Session.GetSessionVars()
	savedPreparedStmtID := sessionVars.GetNextPreparedStmtID()
	savedCurrentDB := sessionVars.CurrentDB
	defer func() {
		sessionVars.SetNextPreparedStmtID(savedPreparedStmtID - 1)
		sessionVars.CurrentDB = savedCurrentDB
	}()

	for id, preparedStmtInfo := range sessionStates.PreparedStmts {
		// Set the next id and currentDB manually.
		sessionVars.SetNextPreparedStmtID(id - 1)
		sessionVars.CurrentDB = preparedStmtInfo.StmtDB
		if preparedStmtInfo.Name == "" {
			// Binary protocol: add to sessionVars.PreparedStmts and TiDBContext.stmts.
			stmt, _, _, err := tc.Prepare(preparedStmtInfo.StmtText)
			if err != nil {
				return err
			}
			// Only binary protocol uses paramsType, which is passed from the first COM_STMT_EXECUTE.
			stmt.SetParamsType(preparedStmtInfo.ParamTypes)
		} else {
			// Text protocol: add to sessionVars.PreparedStmts and sessionVars.PreparedStmtNameToID.
			stmtText := strings.ReplaceAll(preparedStmtInfo.StmtText, "\\", "\\\\")
			stmtText = strings.ReplaceAll(stmtText, "'", "\\'")
			// Add single quotes because the sql_mode might contain ANSI_QUOTES.
			sql := fmt.Sprintf("PREPARE `%s` FROM '%s'", preparedStmtInfo.Name, stmtText)
			stmts, err := tc.Parse(ctx, sql)
			if err != nil {
				return err
			}
			if _, err = tc.ExecuteStmt(ctx, stmts[0]); err != nil {
				return err
			}
		}
	}
	return nil
}
